//! Where views run: worker heartbeats, failover of a dead worker's views, and rebalancing between live
//! workers.
//!
//! Each worker owns one table, `__duckwake.worker_<id>`, with one row it updates every heartbeat: its state,
//! a beat counter, and the cost of each view it runs. Only its owner writes it (`drain` aside), so heartbeats
//! never conflict with other workers (an UPDATE in DuckLake conflicts with any concurrent UPDATE of the same
//! table), and the one-row write stays inlined in the catalog.
//!
//! **Liveness.** A running worker is dead when its beat has not moved for `lease_timeout`, timed on the
//! observer's own clock (as Kafka's group coordinator does), so clocks need not agree across machines. A worker
//! that stops cleanly marks itself `stopped`: it is restarting, so its views wait `stop_grace` for it before
//! they move (as Kafka Connect's `scheduled.rebalance.max.delay.ms`); `drain` is the way to take a worker
//! out for good. A `draining` worker is alive but takes no views and hands its own away.
//!
//! **Failover.** A view is orphaned when it is running and its owner is unassigned (`start` without a worker)
//! or dead. Every live worker computes the same plan for the orphans (largest first, each to the least loaded
//! live worker) and claims its share: a claim moves the lease only if the owner and lease epoch are still the
//! ones it read, and DuckLake's first-committer-wins settles two workers racing for one view. The new owner
//! starts the view from its last checkpoint; a dead owner that comes back is fenced by the bumped lease.
//!
//! **Rebalancing.** Load is the sum of the views' costs, the share of a worker each uses. Only the busiest
//! live worker moves views, and only views it has run longer than the cooldown: when it is above `(1+ε)` times
//! the average (or holds 2 more views than the least loaded worker), it hands at most `max_moves` views per
//! round to the least loaded, preferring a worker that already reads the view's source. A handoff waits for
//! the view's epoch in flight to commit, then moves the lease to the target, so nothing is fenced and nothing
//! staged is lost. Pinned views (`start --worker W`) never move this way, and go back to W after a failover.
use crate::catalog::{lease_table, retry_conflicts, RunningJob};
use crate::lake::{add_column, ident, lit, CTL, LAKE};
use anyhow::{bail, Result};
use duckdb::Connection;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// The owner `start` gives a view no worker is named for: any live worker may claim it.
pub const ANY_WORKER: &str = "any";

const PREFIX: &str = "worker_";

/// The least a view counts for: views that are idle still take a little of a worker, and a view nobody has
/// measured yet must not count as free.
pub const MIN_COST: f64 = 0.005;

pub fn worker_table(worker: &str) -> String {
    format!("{CTL}.{}", ident(&format!("{PREFIX}{worker}")))
}

pub fn check_worker_id(worker: &str) -> Result<()> {
    if worker.is_empty() || worker == ANY_WORKER {
        bail!("worker id {worker:?} is reserved; pick another");
    }
    Ok(())
}

/// A worker as its heartbeat row says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Running,
    /// Handing its views away; takes none.
    Draining,
    /// Stopped cleanly, probably restarting.
    Stopped,
}

impl WorkerState {
    fn parse(s: &str) -> Self {
        match s {
            "draining" => Self::Draining,
            "stopped" => Self::Stopped,
            _ => Self::Running,
        }
    }
}

/// Creates the worker's heartbeat table if needed and marks it running with a fresh beat (a restart also
/// ends a drain).
pub fn register(conn: &Connection, worker: &str) -> Result<()> {
    check_worker_id(worker)?;
    let wt = worker_table(worker);
    retry_conflicts(|| {
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {wt} (worker_id VARCHAR, state VARCHAR, beat BIGINT, jobs INTEGER,
               started_at TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ, load DOUBLE, costs VARCHAR, cooling VARCHAR)"
        ))?;
        Ok(())
    })?;
    // Tables of workers from before rebalancing.
    let table = format!("{PREFIX}{worker}");
    add_column(conn, &table, "load", "DOUBLE")?;
    add_column(conn, &table, "costs", "VARCHAR")?;
    add_column(conn, &table, "cooling", "VARCHAR")?;
    retry_conflicts(|| {
        let n: i64 = conn.query_row(&format!("SELECT count(*) FROM {wt}"), [], |r| r.get(0))?;
        if n == 0 {
            conn.execute(
                &format!(
                    "INSERT INTO {wt} (worker_id, state, beat, jobs, started_at, heartbeat_at, load, costs, cooling)
                     VALUES (?, 'running', 1, 0, now(), now(), 0, '{{}}', '[]')"
                ),
                [worker],
            )?;
        } else {
            conn.execute_batch(&format!(
                "UPDATE {wt} SET state = 'running', beat = beat + 1, jobs = 0, load = 0, costs = '{{}}', cooling = '[]',
                   started_at = now(), heartbeat_at = now()"
            ))?;
        }
        Ok(())
    })
}

/// One heartbeat: the beat moves, with the worker's views, what each costs, and which are within their
/// rebalance cooldown. The state is left alone, so a `drain` written by someone else is kept.
pub fn heartbeat(conn: &Connection, worker: &str, views: usize, costs: &BTreeMap<String, f64>, cooling: &[String]) -> Result<()> {
    let wt = worker_table(worker);
    // fold, not sum: an empty f64 sum is -0.0, which prints as such.
    let load: f64 = costs.values().fold(0.0, |a, c| a + c);
    let (costs, cooling) = (serde_json::to_string(costs)?, serde_json::to_string(cooling)?);
    retry_conflicts(|| {
        conn.execute(
            &format!("UPDATE {wt} SET beat = beat + 1, jobs = ?, load = ?, costs = ?, cooling = ?, heartbeat_at = now()"),
            duckdb::params![views as i64, load, costs, cooling],
        )?;
        Ok(())
    })
}

/// Marks the worker stopped: it is restarting, and its views wait `stop_grace` for it.
pub fn deregister(conn: &Connection, worker: &str) -> Result<()> {
    let wt = worker_table(worker);
    retry_conflicts(|| {
        conn.execute_batch(&format!("UPDATE {wt} SET state = 'stopped', jobs = 0, load = 0, costs = '{{}}', heartbeat_at = now()"))?;
        Ok(())
    })
}

/// Asks a worker to hand all its views to the others and take none (before taking it out for good). It
/// stays draining until it restarts.
pub fn drain(conn: &Connection, worker: &str) -> Result<()> {
    check_worker_id(worker)?;
    if !worker_tables(conn)?.iter().any(|t| t.strip_prefix(PREFIX) == Some(worker)) {
        bail!("no worker {worker:?} has run on this lake");
    }
    let wt = worker_table(worker);
    retry_conflicts(|| {
        conn.execute_batch(&format!("UPDATE {wt} SET state = 'draining' WHERE state <> 'stopped'"))?;
        Ok(())
    })
}

/// The heartbeat tables' names.
fn worker_tables(conn: &Connection) -> Result<Vec<String>> {
    let schema = CTL.rsplit('.').next().unwrap_or(CTL);
    let mut stmt = conn.prepare(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? AND starts_with(table_name, ?)
         ORDER BY table_name",
    )?;
    let names = stmt.query_map([LAKE, schema, PREFIX], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
    Ok(names)
}

/// A worker's heartbeat row.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerRow {
    pub id: String,
    pub beat: i64,
    pub state: WorkerState,
    /// What each of its views cost at its last heartbeat (share of a worker).
    pub costs: HashMap<String, f64>,
    /// Its views within their rebalance cooldown.
    pub cooling: HashSet<String>,
}

/// Every worker's heartbeat row.
pub fn workers(conn: &Connection) -> Result<Vec<WorkerRow>> {
    let tables = worker_tables(conn)?;
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let sql = tables
        .iter()
        .map(|t| format!("SELECT worker_id, beat, state, coalesce(costs, '{{}}'), coalesce(cooling, '[]') FROM {CTL}.{}", ident(t)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .map(|(id, beat, state, costs, cooling)| WorkerRow {
            id,
            beat,
            state: WorkerState::parse(&state),
            costs: serde_json::from_str(&costs).unwrap_or_default(),
            cooling: serde_json::from_str(&cooling).unwrap_or_default(),
        })
        .collect())
}

/// The query behind `duckwake_workers()`: one row per worker that ever ran on this lake.
pub fn workers_query(conn: &Connection) -> Result<String> {
    let tables = worker_tables(conn)?;
    if tables.is_empty() {
        return Ok("SELECT NULL::VARCHAR AS worker, NULL::VARCHAR AS state, NULL::INTEGER AS views, NULL::DOUBLE AS load,
                          NULL::BIGINT AS beat, NULL::TIMESTAMP AS started_at, NULL::TIMESTAMP AS heartbeat_at,
                          NULL::DOUBLE AS heartbeat_age_s WHERE false"
            .into());
    }
    let rows = tables
        .iter()
        .map(|t| {
            format!("SELECT worker_id, state, jobs, load, beat, started_at, heartbeat_at FROM {CTL}.{}", ident(t))
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    Ok(format!(
        "SELECT worker_id AS worker, state, jobs AS views, round(coalesce(load, 0), 3) AS load, beat,
                started_at::TIMESTAMP AS started_at, heartbeat_at::TIMESTAMP AS heartbeat_at,
                round(epoch(now() - heartbeat_at), 1) AS heartbeat_age_s
         FROM ({rows}) ORDER BY worker"
    ))
}

/// Moves a view from `from` (None: unassigned) at lease epoch `lease` to `to`, bumping the lease. False when
/// the lease has moved on since it was read. Used by failover claims and by rebalancing handoffs alike.
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
    stop_grace: Duration,
    /// Per worker: the last beat and state seen (beat None: no heartbeat table) and when either last changed,
    /// on this clock.
    seen: HashMap<String, (Option<i64>, WorkerState, Instant)>,
}

impl Liveness {
    pub fn new(timeout: Duration, stop_grace: Duration) -> Self {
        Liveness { timeout, stop_grace, seen: HashMap::new() }
    }

    /// Records the heartbeats read now, and the owners of running views (an owner with no heartbeat table,
    /// such as a worker named in `start` that never ran, is watched too).
    pub fn observe(&mut self, rows: &[WorkerRow], owners: impl Iterator<Item = String>, now: Instant) {
        let mut current: HashMap<String, (Option<i64>, WorkerState)> =
            rows.iter().map(|w| (w.id.clone(), (Some(w.beat), w.state))).collect();
        for o in owners {
            current.entry(o).or_insert((None, WorkerState::Running));
        }
        self.seen.retain(|w, _| current.contains_key(w));
        for (w, (beat, state)) in current {
            match self.seen.get_mut(&w) {
                Some(s) if s.0 == beat && s.1 == state => {}
                Some(s) => *s = (beat, state, now),
                None => {
                    self.seen.insert(w, (beat, state, now));
                }
            }
        }
    }

    /// Dead: stopped for `stop_grace`, or no change for `timeout` since this worker started watching it. A
    /// worker first seen less than that ago gets the benefit of the doubt.
    pub fn dead(&self, worker: &str, now: Instant) -> bool {
        match self.seen.get(worker) {
            Some((_, WorkerState::Stopped, since)) => now.duration_since(*since) >= self.stop_grace,
            Some((_, _, since)) => now.duration_since(*since) >= self.timeout,
            None => false,
        }
    }

    /// Workers that may take views: running (not draining or stopped) and not dead.
    pub fn targets(&self, now: Instant) -> Vec<String> {
        let mut v: Vec<String> = self
            .seen
            .iter()
            .filter(|(w, s)| s.0.is_some() && s.1 == WorkerState::Running && !self.dead(w, now))
            .map(|(w, _)| w.clone())
            .collect();
        v.sort();
        v
    }
}

/// A running view as the planners see it.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewInfo {
    pub id: String,
    pub owner: Option<String>,
    pub lease: i64,
    pub pinned_to: Option<String>,
    /// Share of a worker it uses (at least `MIN_COST`).
    pub cost: f64,
    /// The sources it reads.
    pub sources: Vec<String>,
}

/// The planners' view of the running views: each with its cost as its owner last reported it (a view not
/// measured yet costs the mean of the measured ones) and the sources it reads.
pub fn view_infos(running: &[RunningJob], rows: &[WorkerRow], sources: &HashMap<String, Vec<String>>) -> Vec<ViewInfo> {
    let owner_cost = |j: &RunningJob| -> Option<f64> {
        let owner = j.owner.as_deref()?;
        rows.iter().find(|w| w.id == owner)?.costs.get(&j.id).copied()
    };
    let known: Vec<f64> = running.iter().filter_map(owner_cost).collect();
    let fallback = if known.is_empty() { MIN_COST } else { (known.iter().sum::<f64>() / known.len() as f64).max(MIN_COST) };
    running
        .iter()
        .map(|j| ViewInfo {
            id: j.id.clone(),
            owner: j.owner.clone().filter(|o| o != ANY_WORKER),
            lease: j.lease,
            pinned_to: j.pinned_to.clone(),
            cost: owner_cost(j).map_or(fallback, |c| c.max(MIN_COST)),
            sources: sources.get(&j.id).cloned().unwrap_or_default(),
        })
        .collect()
}

/// Load and view count per target worker, from the views they own.
fn loads(views: &[ViewInfo], workers: &[String]) -> BTreeMap<String, (f64, usize)> {
    let mut out: BTreeMap<String, (f64, usize)> = workers.iter().map(|w| (w.clone(), (0.0, 0))).collect();
    for v in views {
        if let Some(l) = v.owner.as_deref().and_then(|o| out.get_mut(o)) {
            l.0 += v.cost;
            l.1 += 1;
        }
    }
    out
}

/// The least loaded worker of `among` (by load, then views, then name), skipping `except`.
fn least_loaded<'a>(loads: &'a BTreeMap<String, (f64, usize)>, except: Option<&str>) -> Option<&'a String> {
    loads
        .iter()
        .filter(|(w, _)| Some(w.as_str()) != except)
        .min_by(|a, b| a.1 .0.total_cmp(&b.1 .0).then(a.1 .1.cmp(&b.1 .1)).then(a.0.cmp(b.0)))
        .map(|(w, _)| w)
}

/// The orphaned views `me` should claim: running views unassigned or owned by a dead worker, largest first,
/// each to the least loaded live worker (ties by name). Every worker computing this from the same catalog
/// gets the same plan. `targets` are the live workers that take views, `me` among them when it does.
pub fn claims<'a>(me: &str, views: &'a [ViewInfo], targets: &[String], dead: impl Fn(&str) -> bool) -> Vec<&'a ViewInfo> {
    let mut load = loads(views, targets);
    let mut orphans: Vec<&ViewInfo> = views
        .iter()
        .filter(|v| match v.owner.as_deref() {
            None => true,
            Some(o) => !load.contains_key(o) && dead(o),
        })
        .collect();
    orphans.sort_by(|a, b| b.cost.total_cmp(&a.cost).then(a.id.cmp(&b.id)));
    let mut mine = Vec::new();
    for v in orphans {
        let Some(to) = least_loaded(&load, None).cloned() else { break };
        let l = load.get_mut(&to).unwrap();
        l.0 += v.cost;
        l.1 += 1;
        if to == me {
            mine.push(v);
        }
    }
    mine
}

/// How eagerly the busiest worker gives views away.
#[derive(Debug, Clone, Copy)]
pub struct RebalanceConfig {
    /// How often a worker checks the balance.
    pub every: Duration,
    /// A view runs at least this long on a worker before it may move again.
    pub cooldown: Duration,
    /// Most views one worker gives away per round.
    pub max_moves: usize,
    /// Imbalance tolerated: the busiest worker may carry up to (1 + epsilon) times the average.
    pub epsilon: f64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        RebalanceConfig { every: Duration::from_secs(30), cooldown: Duration::from_secs(300), max_moves: 2, epsilon: 0.2 }
    }
}

/// Why a view moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Drain,
    /// Back to the worker it is pinned to.
    Pin,
    /// The busiest worker carries too much load.
    Load,
    /// The busiest worker holds two views more than the least loaded one (a worker joined).
    Count,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Reason::Drain => "drain",
            Reason::Pin => "pinned",
            Reason::Load => "load",
            Reason::Count => "count",
        }
    }
}

/// Rounds in a row a load imbalance must last before views move for it: one catching up after a restart, or
/// a burst of events, is not worth a move.
pub const LOAD_ROUNDS: u32 = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct Move {
    pub job: String,
    pub lease: i64,
    pub to: String,
    pub reason: Reason,
}

/// A load gap below this (a twentieth of a worker) is not worth a move.
const MIN_GAP: f64 = 0.05;
/// The busiest worker must be at least this much (a tenth of a worker) above the average, besides the ratio:
/// lightly loaded workers are not worth balancing, however uneven.
const ABS_MARGIN: f64 = 0.1;
/// What a target that does not read the view's source costs extra, in load: a second reader of the source.
const AFFINITY: f64 = 0.02;

/// The views `me` should hand to other workers now. `targets` are the live running workers (`me` among them
/// unless it is draining); `cooling` are the views that moved or started within the cooldown, on any worker.
/// Only the busiest worker that has a view it may move gives, so two workers never pile onto one target.
/// `by_load` false plans only the moves that need no load imbalance (drain, pins, view counts): the caller
/// allows load moves once the imbalance has lasted `LOAD_ROUNDS` rounds.
pub fn moves(
    me: &str,
    my_state: WorkerState,
    views: &[ViewInfo],
    targets: &[String],
    cooling: &HashSet<String>,
    cfg: &RebalanceConfig,
    by_load: bool,
) -> Vec<Move> {
    let mut out = Vec::new();
    let mut load = loads(views, targets);
    let mine: Vec<&ViewInfo> = views.iter().filter(|v| v.owner.as_deref() == Some(me)).collect();
    let mv = |v: &ViewInfo, to: &str, reason| Move { job: v.id.clone(), lease: v.lease, to: to.to_string(), reason };

    // Draining: everything goes, largest first, each to the least loaded worker.
    if my_state == WorkerState::Draining {
        let mut all = mine.clone();
        all.sort_by(|a, b| b.cost.total_cmp(&a.cost).then(a.id.cmp(&b.id)));
        for v in all {
            let Some(to) = least_loaded(&load, Some(me)).cloned() else { break };
            let l = load.get_mut(&to).unwrap();
            l.0 += v.cost;
            l.1 += 1;
            out.push(mv(v, &to, Reason::Drain));
        }
        return out;
    }
    if my_state != WorkerState::Running || !load.contains_key(me) {
        return out;
    }
    // A pinned view goes back to its worker once that worker is live again.
    let mut moved: HashSet<&str> = HashSet::new();
    for v in &mine {
        if let Some(pin) = v.pinned_to.as_deref().filter(|p| *p != me && load.contains_key(*p)) {
            out.push(mv(v, pin, Reason::Pin));
            moved.insert(&v.id);
            let (from, to) = (load[me], load[pin]);
            load.insert(me.to_string(), (from.0 - v.cost, from.1 - 1));
            load.insert(pin.to_string(), (to.0 + v.cost, to.1 + 1));
        }
    }
    // Balance: only the busiest worker with a view it may move gives, at most max_moves views per round.
    let reads = |w: &str, source: &str| views.iter().any(|v| v.owner.as_deref() == Some(w) && v.sources.iter().any(|s| s == source));
    let mut balanced = 0;
    while balanced < cfg.max_moves && load.len() > 1 {
        let movable = |v: &ViewInfo| v.pinned_to.is_none() && !cooling.contains(&v.id) && !moved.contains(v.id.as_str());
        let busiest = load
            .iter()
            .filter(|(w, _)| views.iter().any(|v| v.owner.as_deref() == Some(w.as_str()) && movable(v)))
            .max_by(|a, b| a.1 .0.total_cmp(&b.1 .0).then(a.1 .1.cmp(&b.1 .1)).then(b.0.cmp(a.0)))
            .map(|(w, _)| w.clone());
        if busiest.as_deref() != Some(me) {
            break;
        }
        let Some(to) = least_loaded(&load, Some(me)).cloned() else { break };
        let ((my_load, my_count), (to_load, to_count)) = (load[me], load[&to]);
        let avg = load.values().map(|l| l.0).sum::<f64>() / load.len() as f64;
        let candidates: Vec<&&ViewInfo> = mine.iter().filter(|v| movable(v)).collect();
        let shares = |v: &ViewInfo| v.sources.iter().any(|s| reads(&to, s));
        let overloaded = my_load > avg + (cfg.epsilon * avg).max(ABS_MARGIN) && my_load - to_load > MIN_GAP;
        let (pick, reason) = if by_load && overloaded {
            // The view that best evens out the pair, a little against targets that do not read its source.
            let pick = candidates
                .iter()
                .filter(|v| to_load + v.cost < my_load)
                .min_by(|a, b| {
                    let score = |v: &ViewInfo| (my_load - v.cost).max(to_load + v.cost) + if shares(v) { 0.0 } else { AFFINITY };
                    score(a).total_cmp(&score(b)).then(a.id.cmp(&b.id))
                })
                .copied();
            (pick, Reason::Load)
        } else if my_count >= to_count + 2 {
            // Even on load, two views more than the least loaded worker: move the cheapest that does not
            // overload the target.
            let pick = candidates
                .iter()
                .filter(|v| to_load + v.cost <= (my_load - v.cost).max((1.0 + cfg.epsilon) * avg))
                .min_by(|a, b| a.cost.total_cmp(&b.cost).then(shares(b).cmp(&shares(a))).then(a.id.cmp(&b.id)))
                .copied();
            (pick, Reason::Count)
        } else {
            (None, Reason::Count)
        };
        let Some(v) = pick else { break };
        out.push(mv(v, &to, reason));
        moved.insert(&v.id);
        load.insert(me.to_string(), (my_load - v.cost, my_count - 1));
        load.insert(to.clone(), (to_load + v.cost, to_count + 1));
        balanced += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(id: &str, owner: Option<&str>, cost: f64, sources: &[&str]) -> ViewInfo {
        ViewInfo {
            id: id.into(),
            owner: owner.map(String::from),
            lease: 1,
            pinned_to: None,
            cost,
            sources: sources.iter().map(|s| s.to_string()).collect(),
        }
    }
    fn names(ws: &[&str]) -> Vec<String> {
        ws.iter().map(|w| w.to_string()).collect()
    }
    fn plan(me: &str, views: &[ViewInfo], targets: &[&str], cooling: &[&str]) -> Vec<(String, String)> {
        let cooling = cooling.iter().map(|s| s.to_string()).collect();
        moves(me, WorkerState::Running, views, &names(targets), &cooling, &RebalanceConfig::default(), true)
            .into_iter()
            .map(|m| (m.job, m.to))
            .collect()
    }

    #[test]
    fn orphans_go_largest_first_to_the_least_loaded() {
        let views = vec![
            view("a", Some("w1"), 0.3, &[]),
            view("b", Some("w3"), 0.1, &[]),
            view("c", Some("w3"), 0.4, &[]),
            view("d", None, 0.2, &[]),
        ];
        let targets = names(&["w1", "w2"]);
        let dead = |w: &str| w == "w3";
        let ids = |me: &str| claims(me, &views, &targets, dead).into_iter().map(|v| v.id.clone()).collect::<Vec<_>>();
        // w1 0.3, w2 0: c (0.4) -> w2, d (0.2) -> w1 (0.3 < 0.4), b (0.1) -> w2 (0.4 < 0.5).
        assert_eq!(ids("w2"), vec!["c", "b"]);
        assert_eq!(ids("w1"), vec!["d"]);
    }

    #[test]
    fn owners_not_yet_known_dead_keep_their_views() {
        let views = vec![view("a", Some("w9"), 0.1, &[])];
        assert!(claims("w1", &views, &names(&["w1"]), |_| false).is_empty());
        assert_eq!(claims("w1", &views, &names(&["w1"]), |_| true).len(), 1);
    }

    #[test]
    fn a_joining_worker_gets_views_from_the_busiest() {
        let views: Vec<ViewInfo> =
            (0..6).map(|i| view(&format!("v{i}"), Some(if i < 4 { "w1" } else { "w2" }), 0.1, &[])).collect();
        // w1 holds 4, w2 2, w3 none: w1 gives two (max_moves), w2 gives none.
        let from_w1 = plan("w1", &views, &["w1", "w2", "w3"], &[]);
        assert_eq!(from_w1.len(), 2);
        assert!(from_w1.iter().all(|(_, to)| to == "w3"));
        assert!(plan("w2", &views, &["w1", "w2", "w3"], &[]).is_empty());
    }

    #[test]
    fn lightly_loaded_workers_are_not_balanced_by_load() {
        // Uneven, but every worker far below a full one: no load move; counts are within one.
        let views = vec![
            view("a", Some("w1"), 0.1, &[]),
            view("b", Some("w1"), 0.08, &[]),
            view("c", Some("w2"), 0.05, &[]),
            view("d", Some("w2"), 0.05, &[]),
            view("e", Some("w3"), 0.12, &[]),
        ];
        for w in ["w1", "w2", "w3"] {
            assert!(plan(w, &views, &["w1", "w2", "w3"], &[]).is_empty(), "{w}");
        }
    }

    #[test]
    fn load_moves_wait_for_the_caller_but_count_moves_do_not() {
        let mut views = vec![view("heavy", Some("w1"), 0.6, &[]), view("z", Some("w2"), 0.1, &[])];
        views.push(view("l", Some("w1"), 0.05, &[]));
        let cfg = RebalanceConfig::default();
        let t = names(&["w1", "w2"]);
        assert_eq!(moves("w1", WorkerState::Running, &views, &t, &HashSet::new(), &cfg, true)[0].reason, Reason::Load);
        assert!(moves("w1", WorkerState::Running, &views, &t, &HashSet::new(), &cfg, false).is_empty());
        let spread: Vec<ViewInfo> = (0..4).map(|i| view(&format!("v{i}"), Some("w1"), 0.01, &[])).collect();
        let m = moves("w1", WorkerState::Running, &spread, &t, &HashSet::new(), &cfg, false);
        assert!(!m.is_empty() && m.iter().all(|m| m.reason == Reason::Count));
    }

    #[test]
    fn balanced_workers_move_nothing() {
        let views = vec![view("a", Some("w1"), 0.2, &[]), view("b", Some("w2"), 0.21, &[]), view("c", Some("w2"), 0.01, &[])];
        assert!(plan("w2", &views, &["w1", "w2"], &[]).is_empty());
        assert!(plan("w1", &views, &["w1", "w2"], &[]).is_empty());
    }

    #[test]
    fn the_heavy_view_stays_when_moving_it_only_shifts_the_imbalance() {
        let mut views = vec![view("heavy", Some("w1"), 0.6, &[]), view("z", Some("w2"), 0.5, &[])];
        views.extend((0..4).map(|i| view(&format!("l{i}"), Some("w1"), 0.05, &[])));
        // w1 0.8, w2 0.5: `heavy` on w2 would make it 1.1; light views even the pair out instead.
        let m = plan("w1", &views, &["w1", "w2"], &[]);
        assert!(!m.is_empty());
        assert!(m.iter().all(|(job, _)| job != "heavy"), "{m:?}");
    }

    #[test]
    fn a_lone_heavy_view_moves_in_one_step() {
        let mut views = vec![view("heavy", Some("w1"), 0.6, &[])];
        views.extend((0..4).map(|i| view(&format!("l{i}"), Some("w1"), 0.05, &[])));
        // w2 empty: moving `heavy` (one move) evens the pair better than moving all four light views.
        assert_eq!(plan("w1", &views, &["w1", "w2"], &[]), vec![("heavy".to_string(), "w2".to_string())]);
    }

    #[test]
    fn cooling_and_pinned_views_stay() {
        let mut views: Vec<ViewInfo> = (0..4).map(|i| view(&format!("v{i}"), Some("w1"), 0.1, &[])).collect();
        views[0].pinned_to = Some("w1".into());
        let m = plan("w1", &views, &["w1", "w2"], &["v1", "v2"]);
        assert_eq!(m, vec![("v3".to_string(), "w2".to_string())]);
    }

    #[test]
    fn when_the_busiest_can_move_nothing_the_next_one_gives() {
        // w1 is busiest but all its views are cooling; w2 has movable views and is above the average too.
        let mut views: Vec<ViewInfo> = (0..4).map(|i| view(&format!("a{i}"), Some("w1"), 0.1, &[])).collect();
        views.extend((0..3).map(|i| view(&format!("b{i}"), Some("w2"), 0.1, &[])));
        let m = plan("w2", &views, &["w1", "w2", "w3"], &["a0", "a1", "a2", "a3"]);
        assert!(!m.is_empty() && m.iter().all(|(_, to)| to == "w3"), "{m:?}");
        assert!(plan("w1", &views, &["w1", "w2", "w3"], &["a0", "a1", "a2", "a3"]).is_empty());
    }

    #[test]
    fn a_pinned_view_goes_home_when_its_worker_is_back() {
        let mut views = vec![view("p", Some("w2"), 0.1, &[]), view("q", Some("w2"), 0.1, &[])];
        views[0].pinned_to = Some("w1".into());
        assert_eq!(plan("w2", &views, &["w1", "w2"], &[]), vec![("p".to_string(), "w1".to_string())]);
        // Not while w1 is down.
        assert!(plan("w2", &views, &["w2"], &[]).is_empty());
    }

    #[test]
    fn affinity_prefers_a_target_that_reads_the_source() {
        let views = vec![
            view("a", Some("w1"), 0.2, &["clicks"]),
            view("b", Some("w1"), 0.2, &["orders"]),
            view("c", Some("w1"), 0.2, &["clicks"]),
            view("d", Some("w2"), 0.05, &["orders"]),
        ];
        assert_eq!(plan("w1", &views, &["w1", "w2"], &[]).first(), Some(&("b".to_string(), "w2".to_string())));
    }

    #[test]
    fn a_draining_worker_hands_everything_off() {
        let views = vec![view("a", Some("w1"), 0.3, &[]), view("b", Some("w1"), 0.1, &[]), view("c", Some("w2"), 0.2, &[])];
        let m = moves("w1", WorkerState::Draining, &views, &names(&["w2", "w3"]), &HashSet::new(), &RebalanceConfig::default(), true);
        let to: Vec<(&str, &str)> = m.iter().map(|m| (m.job.as_str(), m.to.as_str())).collect();
        assert_eq!(to, vec![("a", "w3"), ("b", "w2")]);
        assert!(m.iter().all(|m| m.reason == Reason::Drain));
    }

    #[test]
    fn liveness_times_out_on_the_observer_clock() {
        let row = |id: &str, beat, state| WorkerRow { id: id.into(), beat, state, costs: HashMap::new(), cooling: HashSet::new() };
        let mut l = Liveness::new(Duration::from_secs(10), Duration::from_secs(30));
        let t0 = Instant::now();
        l.observe(&[row("w1", 5, WorkerState::Running), row("w2", 7, WorkerState::Running)], std::iter::empty(), t0);
        let t1 = t0 + Duration::from_secs(8);
        l.observe(&[row("w1", 6, WorkerState::Running), row("w2", 7, WorkerState::Running)], std::iter::empty(), t1);
        let t2 = t0 + Duration::from_secs(11);
        assert!(!l.dead("w1", t2));
        assert!(l.dead("w2", t2));
        assert_eq!(l.targets(t2), vec!["w1".to_string()]);
        // A clean stop is a restart: dead only after the grace, and never a target meanwhile.
        l.observe(&[row("w1", 6, WorkerState::Stopped)], std::iter::empty(), t2);
        assert!(!l.dead("w1", t2 + Duration::from_secs(29)));
        assert!(l.dead("w1", t2 + Duration::from_secs(30)));
        assert!(l.targets(t2).is_empty());
        // Draining: alive, but takes no views.
        l.observe(&[row("w1", 7, WorkerState::Draining)], std::iter::empty(), t2);
        assert!(!l.dead("w1", t2));
        assert!(l.targets(t2).is_empty());
    }
}
