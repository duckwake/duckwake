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
            run(
                conn,
                "expire snapshots",
                &format!(
                    "CALL ducklake_expire_snapshots({lake}, older_than => now() - INTERVAL '{} seconds')",
                    c.expire_older_than.as_secs()
                ),
            );
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
