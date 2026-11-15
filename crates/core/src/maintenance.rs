//! Built-in lake maintenance, run by the worker between epochs so users don't schedule it.
//!
//! Maintenance commits are ordinary DuckLake transactions and can conflict with epoch commits of other
//! workers; they retry with backoff and give up for this round rather than delay the next epoch.
use crate::lake::{lit, CTL, LAKE};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    pub enabled: bool,
    pub flush_every: Duration,
    pub merge_every: Duration,
    pub expire_every: Duration,
    pub expire_older_than: Duration,
    pub cleanup_every: Duration,
    pub prune_every: Duration,
    pub keep_checkpoints: i64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            flush_every: Duration::from_secs(600),
            merge_every: Duration::from_secs(3600),
            expire_every: Duration::from_secs(3600),
            expire_older_than: Duration::from_secs(24 * 3600),
            cleanup_every: Duration::from_secs(24 * 3600),
            prune_every: Duration::from_secs(3600),
            keep_checkpoints: 1000,
        }
    }
}

pub struct Maintenance {
    cfg: MaintenanceConfig,
    last: HashMap<&'static str, Instant>,
}

impl Maintenance {
    pub fn new(cfg: MaintenanceConfig) -> Self {
        // Start the clocks now: the first round happens one period after startup, not on the first epoch.
        let now = Instant::now();
        let last = ["flush", "merge", "expire", "cleanup", "prune"].into_iter().map(|k| (k, now)).collect();
        Self { cfg, last }
    }

    fn due(&mut self, task: &'static str, every: Duration) -> bool {
        let last = self.last.entry(task).or_insert_with(Instant::now);
        if last.elapsed() >= every {
            *last = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn run_due(&mut self, conn: &duckdb::Connection, jobs: &[String]) {
        if !self.cfg.enabled {
            return;
        }
        let c = self.cfg.clone();
        let lake = lit(LAKE);
        if self.due("flush", c.flush_every) {
            run(conn, "flush inlined data", &format!("CALL ducklake_flush_inlined_data({lake})"));
        }
        if self.due("merge", c.merge_every) {
            run(conn, "merge adjacent files", &format!("CALL ducklake_merge_adjacent_files({lake})"));
        }
        if self.due("expire", c.expire_every) {
            match expire_sql(conn, c.expire_older_than) {
                Ok(Some(sql)) => run(conn, "expire snapshots", &sql),
                Ok(None) => {}
                Err(e) => warn!(target: "maintenance", task = "expire snapshots", error = %format!("{e:#}"), "failed"),
            }
        }
        if self.due("cleanup", c.cleanup_every) {
            run(
                conn,
                "delete unreferenced files",
                &format!("CALL ducklake_cleanup_old_files({lake}, older_than => now() - INTERVAL '{} seconds')", c.expire_older_than.as_secs()),
            );
        }
        if self.due("prune", c.prune_every) {
            for job in jobs {
                run(
                    conn,
                    "prune checkpoints",
                    &format!(
                        "DELETE FROM {CTL}.checkpoints WHERE job_id = {j} AND epoch <=
                           (SELECT max(epoch) FROM {CTL}.checkpoints WHERE job_id = {j}) - {}",
                        c.keep_checkpoints,
                        j = lit(job)
                    ),
                );
            }
        }
    }
}

/// The statements that expire snapshots older than `older_than`, or None when there is none to expire.
///
/// Never past a view: every snapshot from the oldest position of a view with lake inputs on is kept (its
/// next epoch reads the changes after its position, and `verify` and retractions read the table at it), so
/// a paused or slow view holds snapshots the way a replication slot holds WAL. And never without a record:
/// the commit time of each snapshot about to go is written to `snapshot_times` first, since rows written by
/// it keep that time as their `_commit_ts`.
pub fn expire_sql(conn: &duckdb::Connection, older_than: Duration) -> anyhow::Result<Option<String>> {
    let lake = lit(LAKE);
    let held = crate::catalog::lake_positions(conn)?.iter().filter_map(|p| p.snapshot).min();
    // Snapshot times are not strictly ordered by id, so the bound is the oldest time among the snapshots kept.
    let bound = match held {
        Some(p) => format!(
            "least(now() - INTERVAL '{} seconds', (SELECT min(snapshot_time) FROM ducklake_snapshots({lake}) WHERE snapshot_id >= {p}))",
            older_than.as_secs()
        ),
        None => format!("now() - INTERVAL '{} seconds'", older_than.as_secs()),
    };
    let (cutoff, n): (Option<String>, i64) = conn.query_row(
        &format!(
            "SELECT strftime(b, '%Y-%m-%d %H:%M:%S.%f%z'), (SELECT count(*) FROM ducklake_snapshots({lake}) WHERE snapshot_time < b)
             FROM (SELECT {bound} AS b)"
        ),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let Some(cutoff) = cutoff.filter(|_| n > 0) else { return Ok(None) };
    let cutoff = format!("TIMESTAMPTZ {}", lit(&cutoff));
    Ok(Some(format!(
        "INSERT INTO {CTL}.snapshot_times
           SELECT snapshot_id, ts FROM (
             SELECT snapshot_id, snapshot_time, greatest(max(timezone('UTC', snapshot_time)) OVER (ORDER BY snapshot_id),
                    (SELECT max(commit_ts) FROM {CTL}.snapshot_times)) AS ts
             FROM ducklake_snapshots({lake}))
           WHERE snapshot_time < {cutoff} AND snapshot_id NOT IN (SELECT snapshot_id FROM {CTL}.snapshot_times);
         CALL ducklake_expire_snapshots({lake}, older_than => {cutoff})"
    )))
}

fn run(conn: &duckdb::Connection, task: &str, sql: &str) {
    let t = Instant::now();
    for attempt in 1..=3u64 {
        match conn.execute_batch(sql) {
            Ok(()) => {
                info!(target: "maintenance", task, ms = t.elapsed().as_millis() as u64, "done");
                return;
            }
            Err(e) => {
                warn!(target: "maintenance", task, attempt, error = %e, "failed");
                std::thread::sleep(Duration::from_millis(200 * attempt));
            }
        }
    }
}
