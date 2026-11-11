//! Automatic failover: worker heartbeats, dead-worker detection and reassignment of orphaned views.
//!
//! Each worker owns one table, `__duckwake.worker_<id>`, with one row it updates every heartbeat: only its
//! owner writes it, so heartbeats never conflict with other workers (an UPDATE in DuckLake conflicts with any
//! concurrent UPDATE of the same table), and the one-row write stays inlined in the catalog.
//!
//! A worker is dead when its heartbeat counter has not moved for `lease_timeout`, timed on the observer's own
//! clock (as Kafka's group coordinator does), so clocks need not agree across machines. A worker that stops
//! cleanly marks itself `stopped` and is dead at once. A view is orphaned when it is running and its owner is
//! unassigned (`start` without a worker) or dead. Every live worker computes the same balanced plan for the
//! orphans and claims its share: a claim moves the lease only if the owner and lease epoch are still the
//! ones it read, and DuckLake's first-committer-wins settles two workers racing for one view. The new owner
//! starts the view from its last checkpoint; a dead owner that comes back is fenced by the bumped lease.
use crate::catalog::{lease_table, retry_conflicts};
use crate::lake::{ident, lit, CTL, LAKE};
use anyhow::{bail, Result};
use duckdb::Connection;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// The owner `start` gives a view no worker is named for: any live worker may claim it.
pub const ANY_WORKER: &str = "any";

const PREFIX: &str = "worker_";

pub fn worker_table(worker: &str) -> String {
    format!("{CTL}.{}", ident(&format!("{PREFIX}{worker}")))
}

pub fn check_worker_id(worker: &str) -> Result<()> {
    if worker.is_empty() || worker == ANY_WORKER {
        bail!("worker id {worker:?} is reserved; pick another");
    }
    Ok(())
}

/// Creates the worker's heartbeat table if needed and marks it running with a fresh beat.
pub fn register(conn: &Connection, worker: &str) -> Result<()> {
    check_worker_id(worker)?;
    let wt = worker_table(worker);
    retry_conflicts(|| {
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {wt} (worker_id VARCHAR, state VARCHAR, beat BIGINT, jobs INTEGER,
               started_at TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ)"
        ))?;
        Ok(())
    })?;
    retry_conflicts(|| {
        let n: i64 = conn.query_row(&format!("SELECT count(*) FROM {wt}"), [], |r| r.get(0))?;
        if n == 0 {
            conn.execute(&format!("INSERT INTO {wt} VALUES (?, 'running', 1, 0, now(), now())"), [worker])?;
        } else {
            conn.execute_batch(&format!(
                "UPDATE {wt} SET state = 'running', beat = beat + 1, jobs = 0, started_at = now(), heartbeat_at = now()"
            ))?;
        }
        Ok(())
    })
}

pub fn heartbeat(conn: &Connection, worker: &str, jobs: usize) -> Result<()> {
    let wt = worker_table(worker);
    retry_conflicts(|| {
        conn.execute(&format!("UPDATE {wt} SET beat = beat + 1, jobs = ?, heartbeat_at = now()"), [jobs as i64])?;
        Ok(())
    })
}

/// Marks the worker stopped, so the others take its views over at once instead of after `lease_timeout`.
pub fn deregister(conn: &Connection, worker: &str) -> Result<()> {
    let wt = worker_table(worker);
    retry_conflicts(|| {
        conn.execute_batch(&format!("UPDATE {wt} SET state = 'stopped', jobs = 0, heartbeat_at = now()"))?;
        Ok(())
    })
}

/// The heartbeat tables, as `(worker table name)`.
fn worker_tables(conn: &Connection) -> Result<Vec<String>> {
    let schema = CTL.rsplit('.').next().unwrap_or(CTL);
    let mut stmt = conn.prepare(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? AND starts_with(table_name, ?)
         ORDER BY table_name",
    )?;
    let names = stmt.query_map([LAKE, schema, PREFIX], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
    Ok(names)
}

/// Every worker's heartbeat: `(worker, beat, stopped)`.
pub fn beats(conn: &Connection) -> Result<Vec<(String, i64, bool)>> {
    let tables = worker_tables(conn)?;
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let sql = tables
        .iter()
        .map(|t| format!("SELECT worker_id, beat, state = 'stopped' FROM {CTL}.{}", ident(t)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<duckdb::Result<Vec<_>>>()?;
    Ok(rows)
}

/// The query behind `duckwake_workers()`: one row per worker that ever ran on this lake.
pub fn workers_query(conn: &Connection) -> Result<String> {
    let tables = worker_tables(conn)?;
    if tables.is_empty() {
        return Ok("SELECT NULL::VARCHAR AS worker, NULL::VARCHAR AS state, NULL::INTEGER AS views, NULL::BIGINT AS beat,
                          NULL::TIMESTAMP AS started_at, NULL::TIMESTAMP AS heartbeat_at, NULL::DOUBLE AS heartbeat_age_s WHERE false"
            .into());
    }
    let rows = tables
        .iter()
        .map(|t| format!("SELECT * FROM {CTL}.{}", ident(t)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    Ok(format!(
        "SELECT worker_id AS worker, state, jobs AS views, beat, started_at::TIMESTAMP AS started_at,
                heartbeat_at::TIMESTAMP AS heartbeat_at, round(epoch(now() - heartbeat_at), 1) AS heartbeat_age_s
         FROM ({rows}) ORDER BY worker"
    ))
}

/// Moves a view from `from` (None: unassigned) at lease epoch `lease` to `to`, bumping the lease. False when
/// the lease has moved on since it was read.
pub fn claim(conn: &Connection, job: &str, from: Option<&str>, lease: i64, to: &str) -> Result<bool> {
    conn.execute_batch("BEGIN")?;
    let res = conn.execute(
        &format!(
            "UPDATE {} SET owner_worker = ?, lease_epoch = lease_epoch + 1
             WHERE owner_worker IS NOT DISTINCT FROM {} AND lease_epoch = ?",
            lease_table(job),
            from.map_or("NULL".into(), lit)
        ),
        duckdb::params![to, lease],
    );
    match res {
        Ok(n) => {
            conn.execute_batch("COMMIT")?;
            Ok(n == 1)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e.into())
        }
    }
}

/// What one worker has seen of the others' heartbeats.
pub struct Liveness {
    timeout: Duration,
    /// Per worker: the last beat seen (None: no heartbeat table) and when it last changed, on this clock.
    seen: HashMap<String, (Option<i64>, bool, Instant)>,
}

impl Liveness {
    pub fn new(timeout: Duration) -> Self {
        Liveness { timeout, seen: HashMap::new() }
    }

    /// Records the heartbeats read now, and the owners of running views (an owner with no heartbeat table, such
    /// as a worker named in `start` that never ran, is watched too).
    pub fn observe(&mut self, beats: &[(String, i64, bool)], owners: impl Iterator<Item = String>, now: Instant) {
        let mut current: HashMap<String, (Option<i64>, bool)> =
            beats.iter().map(|(w, b, stopped)| (w.clone(), (Some(*b), *stopped))).collect();
        for o in owners {
            current.entry(o).or_insert((None, false));
        }
        self.seen.retain(|w, _| current.contains_key(w));
        for (w, (beat, stopped)) in current {
            match self.seen.get_mut(&w) {
                Some(s) if s.0 == beat && s.1 == stopped => {}
                Some(s) => *s = (beat, stopped, now),
                None => {
                    self.seen.insert(w, (beat, stopped, now));
                }
            }
        }
    }

    /// Dead: stopped, or no change for `timeout` since this worker started watching it. A worker first seen
    /// less than `timeout` ago gets the benefit of the doubt.
    pub fn dead(&self, worker: &str, now: Instant) -> bool {
        match self.seen.get(worker) {
            Some((_, true, _)) => true,
            Some((_, false, since)) => now.duration_since(*since) >= self.timeout,
            None => false,
        }
    }

    /// Workers that may take views: running and not dead.
    pub fn live(&self, now: Instant) -> Vec<String> {
        let mut v: Vec<String> =
            self.seen.iter().filter(|(w, s)| s.0.is_some() && !self.dead(w, now)).map(|(w, _)| w.clone()).collect();
        v.sort();
        v
    }
}

/// The orphaned views `me` should claim, as `(job, owner, lease)`, from the running views `(job, owner, lease)`
/// and the live workers (which include `me`). Each orphan, in job order, goes to the live worker with the
/// fewest views (ties by name), so every worker computing this from the same catalog gets the same plan.
pub fn plan(
    me: &str,
    running: &[(String, Option<String>, i64)],
    live: &[String],
    dead: impl Fn(&str) -> bool,
) -> Vec<(String, Option<String>, i64)> {
    let mut load: BTreeMap<&str, usize> = live.iter().map(|w| (w.as_str(), 0)).collect();
    for (_, owner, _) in running {
        if let Some(n) = owner.as_deref().and_then(|o| load.get_mut(o)) {
            *n += 1;
        }
    }
    let mut mine = Vec::new();
    for (job, owner, lease) in running {
        let orphan = match owner.as_deref() {
            None | Some(ANY_WORKER) => true,
            Some(o) => !load.contains_key(o) && dead(o),
        };
        if !orphan {
            continue;
        }
        let Some((&to, _)) = load.iter().min_by_key(|(w, n)| (**n, **w)) else { break };
        *load.get_mut(to).unwrap() += 1;
        if to == me {
            mine.push((job.clone(), owner.clone(), *lease));
        }
    }
    mine
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(jobs: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>, i64)> {
        jobs.iter().map(|(j, o)| (j.to_string(), o.map(String::from), 1)).collect()
    }

    #[test]
    fn orphans_spread_over_live_workers_by_load() {
        let running = run(&[("a", Some("w1")), ("b", Some("w1")), ("c", Some("w3")), ("d", None), ("e", Some("w3"))]);
        let live = vec!["w1".to_string(), "w2".to_string()];
        let dead = |w: &str| w == "w3";
        let ids = |me: &str| plan(me, &running, &live, dead).into_iter().map(|(j, _, _)| j).collect::<Vec<_>>();
        // w1 has 2 and w2 none: c, d and e go to w2, w2, then w1 or w2 by load (2 vs 2: w1 by name).
        assert_eq!(ids("w2"), vec!["c", "d"]);
        assert_eq!(ids("w1"), vec!["e"]);
    }

    #[test]
    fn owners_not_yet_known_dead_keep_their_views() {
        let running = run(&[("a", Some("w9"))]);
        assert!(plan("w1", &running, &["w1".into()], |_| false).is_empty());
        assert_eq!(plan("w1", &running, &["w1".into()], |_| true).len(), 1);
    }

    #[test]
    fn liveness_times_out_on_the_observer_clock() {
        let mut l = Liveness::new(Duration::from_secs(10));
        let t0 = Instant::now();
        l.observe(&[("w1".into(), 5, false), ("w2".into(), 7, false)], std::iter::empty(), t0);
        let t1 = t0 + Duration::from_secs(8);
        l.observe(&[("w1".into(), 6, false), ("w2".into(), 7, false)], std::iter::empty(), t1);
        let t2 = t0 + Duration::from_secs(11);
        assert!(!l.dead("w1", t2));
        assert!(l.dead("w2", t2));
        assert_eq!(l.live(t2), vec!["w1".to_string()]);
        l.observe(&[("w1".into(), 6, true)], std::iter::empty(), t2);
        assert!(l.dead("w1", t2));
    }
}
