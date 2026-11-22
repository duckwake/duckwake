//! The worker: a scheduler thread drives a micro-batch loop over every job assigned to this worker.
//!
//! One epoch, for all of the worker's jobs at once:
//! 1. Poll: drain each reader's prefetched Kafka chunks (up to `max_rows` messages). A reader feeds one job,
//!    or with `shared_readers` every job of a source at the same position (a group). With Arrow decode the
//!    readers have already decoded the chunks.
//! 2. Stage (in the in-memory catalog), first once per group: append the decoded rows, decode in SQL what
//!    Arrow did not, split dead letters. Then per job: advance the watermark, assign windows, split late
//!    rows, and compute the epoch's sink rows (stateless) or partial aggregates. With `stage_threads > 1`
//!    the groups, then the jobs, are staged in parallel, each thread on its own connection.
//! 3. Commit: one DuckLake transaction for all jobs — lease fence, sink rows, state MERGE + emit of closed
//!    windows, late rows, dead letters and one checkpoint row per job. That is one snapshot per worker per
//!    epoch; it either lands whole or not at all, and recovery replays from the committed offsets.
//!
//! With `pipeline` the commit runs on a thread and connection of its own while the next epoch is staged.
//! Commits stay in order and at most one epoch is staged ahead. A failed commit fails the worker as before,
//! and the epoch staged ahead is thrown away with it. Staging tables the commit reads come in two sets, by
//! epoch parity, so staging the next epoch never overwrites them.
use crate::catalog::{self, lease_table, state_table, Checkpoint};
use crate::failover;
use crate::circuit::{Circuit, Cost, Counter, EpochCtx, SourceInput};
use crate::sources::kafka::{Chunk, Reader};
use crate::lake::{connect_again, ident, lit, quote_table, LakeConfig, CTL, LAKE};
use crate::maintenance::{Maintenance, MaintenanceConfig};
use crate::model::{parse_duration, JobDef, LakeInput, SourceDef, COMMIT_TS, LAKE_BACKFILL, LAKE_POSITION};
use crate::sources::kafka::decode::{decode_sql, invalid_json_where, tombstones_sql, IS_JSON_OBJECT};
use anyhow::{anyhow, Context, Result};
use duckdb::Connection;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tracing::{info, warn};

#[derive(Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub lake: LakeConfig,
    pub maintenance: MaintenanceConfig,
    /// Stop after this many committed epochs (tests).
    pub max_epochs: Option<u64>,
    /// How often to look for newly assigned / paused jobs.
    pub refresh_every: Duration,
    /// Failover: a worker whose heartbeat has not moved for this long is dead, and its views move to the live
    /// workers. None: this worker heartbeats but never claims another's views, and does not rebalance.
    pub lease_timeout: Option<Duration>,
    /// How long the views of a cleanly stopped (restarting) worker wait for it before they move.
    pub stop_grace: Duration,
    /// Moving views from the busiest worker to the least loaded one; None: views move only on failover,
    /// drain and pins.
    pub rebalance: Option<failover::RebalanceConfig>,
    /// Threads that stage in parallel, each with its own connection; 1 stages everything on the scheduler
    /// thread, 0 means half the cores.
    pub stage_threads: usize,
    /// Stage the next epoch while the previous one commits.
    pub pipeline: bool,
    /// Jobs of one source at the same position share one reader, one decode and one staged copy of the events.
    pub shared_readers: bool,
    /// Skip the epoch transaction's writes that have nothing to write (no window closing, no late rows, no
    /// dead letters, no changed groups), and write every job's checkpoint and dead letters in one statement each.
    pub lean_commit: bool,
    /// Live per-view numbers for a metrics endpoint.
    pub metrics: Option<Arc<crate::metrics::Metrics>>,
    /// How long a view with lake inputs may leave its position behind the lake's newest snapshot when it has
    /// nothing to write: after that it commits the position alone, so an idle view does not keep maintenance
    /// from expiring snapshots.
    pub position_refresh: Duration,
}

struct JobRuntime {
    def: Arc<JobDef>,
    /// The view as an incremental circuit; its steps are the SQL each epoch runs.
    circuit: Arc<Circuit>,
    lease: i64,
    /// The last epoch handed to the commit (with `pipeline`, possibly still committing). Each input holds the
    /// position after it, which the next epoch starts from.
    epoch: i64,
    /// The view's watermark: the oldest of its sources', once every source has one.
    watermark_us: Option<i64>,
    max_event_time_us: Option<i64>,
    interval: Duration,
    /// The sources the view reads, in `Plan::sources` order.
    inputs: Vec<Input>,
    /// The last round of epochs this job was part of. A global aggregate staged in the round right after it
    /// reads that round's new state, which may not have committed yet, through the overlay.
    last_round: Option<u64>,
    /// With `lean_commit`, a windowed job's distinct window ends in its state table after `epoch`: read from
    /// the table at start, then kept up to date from each epoch's partials and watermark. A window can close
    /// only when one of them is at or below the watermark.
    open_ends: Option<BTreeSet<i64>>,
    /// When this worker started the job: it may move again once the rebalance cooldown has passed.
    started: Instant,
    /// The share of a worker the job uses: a moving average of its staging, source and commit time per
    /// second of wall time. None until its first epoch commits.
    cost: Option<f64>,
    last_commit: Option<Instant>,
    /// When the size of the job's state was last read (`check_limits`).
    state_checked: Option<Instant>,
}

/// How often a worker reads the other workers' metrics addresses for service discovery.
const TARGETS_EVERY: Duration = Duration::from_secs(15);

/// How often a worker reads the size of its views' state, for `max_state_rows` and the metric.
const STATE_CHECK_EVERY: Duration = Duration::from_secs(10);

/// Weight of the newest epoch in a job's moving-average cost.
const COST_ALPHA: f64 = 0.2;

/// Where an input's rows come from.
enum Feed {
    /// A Kafka source, read by a reader group.
    Kafka { group: u64 },
    /// A lake table or view, read through its change feed by the job itself. Its position is the last
    /// snapshot read, kept as partition 0 of the input's offsets (`LAKE_POSITION`); `commit_us` is
    /// `_commit_ts` at that snapshot, carried so the running maximum continues across epochs. While the view
    /// backfills, the position stays at its start snapshot and partition 1 (`LAKE_BACKFILL`) holds the last
    /// row id read there.
    Lake { lake: Arc<LakeInput>, commit_us: Option<i64> },
}

/// A source as one job reads it: where it comes from, its position and its watermark.
struct Input {
    name: String,
    columns: Arc<Vec<(String, String)>>,
    feed: Feed,
    offsets: BTreeMap<i32, i64>,
    part_max_ts: HashMap<i32, i64>,
    part_last_data: HashMap<i32, Instant>,
    watermark_us: Option<i64>,
    delay_us: i64,
    idle_timeout: Duration,
}

impl Input {
    /// The reader group of a Kafka input.
    fn group(&self) -> Option<u64> {
        match self.feed {
            Feed::Kafka { group, .. } => Some(group),
            Feed::Lake { .. } => None,
        }
    }

    /// Takes in an epoch of this source and returns its watermark after it: the minimum over active
    /// partitions of their newest event time, minus the delay. Partitions idle for longer than `idle_timeout`
    /// are excluded; one that is active but has not reported yet holds it back.
    fn advance(&mut self, src: &SourceStaged) -> Option<i64> {
        let now = Instant::now();
        for p in &src.active {
            self.part_last_data.insert(*p, now);
        }
        for &(p, ts) in &src.part_max {
            let e = self.part_max_ts.entry(p).or_insert(ts);
            *e = (*e).max(ts);
        }
        let active: Vec<i32> =
            self.part_last_data.iter().filter(|(_, at)| at.elapsed() < self.idle_timeout).map(|(p, _)| *p).collect();
        let candidate = if active.is_empty() || active.iter().any(|p| !self.part_max_ts.contains_key(p)) {
            None
        } else {
            active.iter().map(|p| self.part_max_ts[p]).min().map(|m| m - self.delay_us)
        };
        match (self.watermark_us, candidate) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }
}

/// A reader and the jobs it feeds: one job, or with `shared_readers` every job of a source at the same
/// position. Each epoch the group's chunks are staged once (`typed_g<id>`, `dead_g<id>_<parity>`) and every
/// member stages its view from there, so every member's next offsets equal the group's.
struct Group {
    source: Arc<SourceDef>,
    reader: Reader,
    offsets: BTreeMap<i32, i64>,
    members: BTreeSet<String>,
    /// The smallest `max_rows` of the members: the most one epoch drains.
    max_rows: usize,
}

/// One group's epoch, staged once for all its members.
struct SourceStaged {
    typed: String,
    dead: String,
    /// Next offsets after this epoch's chunks, and the partitions that had any.
    offsets: BTreeMap<i32, i64>,
    active: Vec<i32>,
    /// Newest event time per partition among the rows that decoded.
    part_max: Vec<(i32, i64)>,
    rows_in: usize,
    /// Messages decoded in SQL: all of them in SQL mode, the fallback (and tombstones) with Arrow.
    rows_sql: usize,
    bisections: usize,
    rows_dead: i64,
    /// Of `rows_dead`, the tombstones: an upsert view reads them as deletes, not dead letters.
    rows_tombstones: i64,
    /// Debezium events carrying `decode::UNAVAILABLE`: how many, and the first one's partition and offset.
    unavailable: Option<(i64, i32, i64)>,
    stage_ms: u128,
}

#[derive(Default, Clone)]
struct Staged {
    /// Per input: the position after this epoch, the source's watermark, its dead letters.
    inputs: Vec<StagedInput>,
    watermark_us: Option<i64>,
    max_event_time_us: Option<i64>,
    rows_in: usize,
    rows_sql: usize,
    bisections: usize,
    rows_dead: i64,
    rows_late: i64,
    rows_out: i64,
    /// Rows stateful operators merge into their state: windowed partials, touched groups or distinct rows.
    rows_staged: i64,
    /// Rows the view's table changes by: rows appended, or groups of an upsert sink.
    rows_changed: i64,
    /// Windowed: whether a window closes this epoch, and the open window ends after it (with `lean_commit`).
    closes: bool,
    open_ends: Option<BTreeSet<i64>>,
    /// Staging this job's view, and its group's events (shared with the other members).
    stage_ms: u128,
    source_ms: u128,
}

#[derive(Default, Clone, Debug)]
struct StagedInput {
    offsets: BTreeMap<i32, i64>,
    watermark_us: Option<i64>,
    rows_dead: i64,
    /// Lake inputs: `_commit_ts` at the snapshot read up to, and how many snapshots the lake is ahead of it.
    commit_us: Option<i64>,
    snapshots_behind: i64,
    /// Lake inputs: this epoch read the last page of the input's backfill.
    backfill_done: bool,
}

/// A view's lease table is gone: the view was dropped while this worker still ran it. Its epoch is fenced,
/// as if the lease had moved, and the others in the transaction commit without it.
#[derive(Debug)]
struct LeaseGone(String);

impl std::fmt::Display for LeaseGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "view {} was dropped", self.0)
    }
}

impl std::error::Error for LeaseGone {}

fn is_missing_table(e: &duckdb::Error) -> bool {
    let m = e.to_string();
    m.contains("Catalog Error") && m.contains("does not exist")
}

/// A view input broke what the view relies on, such as an insert-only table that had rows updated or
/// deleted: the view stops (status `failed`), the worker and its other views go on.
#[derive(Debug)]
struct StopJob(String);

impl std::fmt::Display for StopJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StopJob {}

/// Snapshots one epoch of a lake input reads at most, so a view far behind catches up in bounded steps.
const MAX_SNAPSHOTS: i64 = 1000;

/// A job's staging table for the changes of its `k`-th input, a lake input.
/// The staged rows of a job's `k`-th input, a lake input, for the run of the job that holds `lease`. A run's
/// tables are dropped after it leaves the worker, on the commit thread when epochs are pipelined, so a view
/// started again soon after must not stage into a table of the same name.
fn lake_typed(job: &str, lease: i64, k: usize) -> String {
    mem("typed", &format!("l{job}_{lease}_{k}"))
}

/// One job's epoch as the commit needs it. It owns everything, so it can go to the commit thread.
struct CommitJob {
    id: String,
    lease: i64,
    epoch: i64,
    def: Arc<JobDef>,
    staged: Staged,
    circuit: Arc<Circuit>,
    /// The table set this epoch staged into.
    parity: u64,
    /// Per input: its name, columns and, for a Kafka source, its group's dead letters of this epoch.
    inputs: Vec<CommitInput>,
    lag: i64,
}

struct CommitInput {
    name: String,
    columns: Arc<Vec<(String, String)>>,
    dead: Option<String>,
}

/// One epoch of every job of the worker: one DuckLake transaction.
struct CommitBatch {
    round: u64,
    lean: bool,
    jobs: Vec<CommitJob>,
    poll_stage_ms: u128,
    /// Staging tables nothing reads once this epoch has committed (groups merged or gone).
    drop_after: Vec<String>,
}

/// An epoch that committed: the batch with `rows_out` filled in, and the jobs fenced out of it.
struct Committed {
    batch: CommitBatch,
    fenced: Vec<String>,
    timing: CommitTiming,
    commit_ms: u128,
    attempts: u64,
    /// When COMMIT returned: the epoch's output became visible.
    visible_at: chrono::DateTime<chrono::Utc>,
}

/// Where an epoch transaction's time goes, in ms summed over views: `writes` is every statement before
/// `COMMIT` (the kinds below plus the commit message), `commit` the `COMMIT` itself.
#[derive(Default, Debug)]
struct CommitTiming {
    lease: u128,
    state: u128,
    sink: u128,
    late: u128,
    dead: u128,
    checkpoint: u128,
    writes: u128,
    commit: u128,
}

/// Fault injection for the crash and fencing tests.
/// - `DUCKWAKE_CRASH_AT=<point>[,<point>]`: abort the process there when a random draw falls under
///   `DUCKWAKE_CRASH_PROB` (default 1.0), to hit every step of the epoch.
/// - `DUCKWAKE_PAUSE_AT=<point>`: sleep `DUCKWAKE_PAUSE_SECS` there once, after skipping the first `DUCKWAKE_PAUSE_SKIP`
///   hits, like a long GC pause; with the commit transaction open this turns the worker into a zombie.
pub(crate) fn crash_point(point: &str) {
    static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let skip: u64 = std::env::var("DUCKWAKE_PAUSE_SKIP").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    if std::env::var("DUCKWAKE_PAUSE_AT").is_ok_and(|p| p == point) && HITS.fetch_add(1, Ordering::Relaxed) == skip {
        let secs: u64 = std::env::var("DUCKWAKE_PAUSE_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
        warn!(point, secs, "pause injected");
        std::thread::sleep(Duration::from_secs(secs));
    }
    if let Ok(at) = std::env::var("DUCKWAKE_CRASH_AT") {
        if at.split(',').any(|p| p == point) {
            let prob: f64 = std::env::var("DUCKWAKE_CRASH_PROB").ok().and_then(|p| p.parse().ok()).unwrap_or(1.0);
            if rand::random::<f64>() < prob {
                eprintln!("crash injected at {point}");
                std::process::abort();
            }
        }
    }
}

/// Creates a worker's spill directory, or removes what DuckDB left in it when the worker last died.
fn clear_spill_dir(dir: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating the spill directory {}", dir.display()))?;
    for e in std::fs::read_dir(dir)?.flatten() {
        if e.file_name().to_string_lossy().starts_with("duckdb_temp") {
            let _ = std::fs::remove_file(e.path());
        }
    }
    Ok(())
}

pub fn run_worker(cfg: WorkerConfig, rt: Handle, shutdown: Arc<AtomicBool>) -> Result<()> {
    if cfg.lake.catalog.split_whitespace().any(|kv| kv.starts_with("password=")) {
        warn!(
            "the catalog password is in the connection string, where process listings and shell history show it; \
             set PGPASSWORD or use a .pgpass file instead"
        );
    }
    let mut cfg = cfg;
    if let Some(t) = cfg.lease_timeout {
        // A worker frozen inside its catalog commit would block every writer, failover's claims included; let
        // PostgreSQL end its session once it has been stalled as long as the others wait to call it dead.
        if cfg.lake.catalog.contains("options=") && !cfg.lake.catalog.contains("idle_in_transaction_session_timeout") {
            warn!("the catalog string sets options; add -c idle_in_transaction_session_timeout so a frozen worker cannot block the lake");
        }
        cfg.lake.catalog = crate::lake::bound_idle_transactions(&cfg.lake.catalog, t);
    }
    // Workers on one machine must not spill into each other's files, and a crashed one leaves its own behind.
    let temp = std::path::Path::new(cfg.lake.temp_dir.as_deref().unwrap_or(".tmp")).join(&cfg.worker_id);
    clear_spill_dir(&temp)?;
    cfg.lake.temp_dir = Some(temp.to_string_lossy().into_owned());
    // An epoch that does not fit in the memory limit fails; the worker then starts again from its checkpoints
    // with half as many rows per epoch, in a fresh instance, instead of failing on the same epoch for ever.
    let mut rows_cap: Option<usize> = None;
    // A catalog that cannot be reached is waited out, then the worker starts again from its checkpoints in a
    // fresh instance (#27): what it staged or was committing when the catalog went is redone or found
    // committed, as after a crash.
    let mut outage = Outage::default();
    loop {
        let mut w = match start_worker(cfg.clone(), rt.clone(), rows_cap) {
            Ok(w) => w,
            Err(e) if crate::lake::is_catalog_unavailable(&e) => {
                if shutdown.load(Ordering::Relaxed) {
                    warn!(error = %format!("{e:#}"), "stopping while the catalog cannot be reached");
                    return Ok(());
                }
                outage.wait(&e, &cfg, &shutdown);
                continue;
            }
            Err(e) => return Err(e),
        };
        outage.end(&cfg);
        match w.run(&shutdown) {
            Err(e) if is_out_of_memory(&e) && !shutdown.load(Ordering::Relaxed) => {
                let largest = w.jobs.values().map(|j| j.def.max_rows).max().unwrap_or(100_000);
                let cap = rows_cap.unwrap_or(largest).min(largest) / 2;
                drop(w);
                if cap < MIN_EPOCH_ROWS {
                    return Err(e.context(format!(
                        "out of memory even with {MIN_EPOCH_ROWS} rows per epoch: raise --memory-limit or run fewer views on this worker"
                    )));
                }
                warn!(rows_per_epoch = cap, error = %format!("{e:#}"), "an epoch ran out of memory; starting again from the last checkpoints with smaller epochs");
                if let Some(m) = &cfg.metrics {
                    m.record_out_of_memory(cap);
                }
                clear_spill_dir(&temp)?;
                rows_cap = Some(cap);
            }
            Err(e) if crate::lake::is_catalog_unavailable(&e) => {
                // The instance goes with its connections; its ingest threads stop and its commit thread
                // finishes or fails first (their Drop).
                drop(w);
                if shutdown.load(Ordering::Relaxed) {
                    warn!(error = %format!("{e:#}"), "stopping while the catalog cannot be reached; the views move after the lease timeout");
                    return Ok(());
                }
                clear_spill_dir(&temp)?;
                outage.wait(&e, &cfg, &shutdown);
            }
            res => {
                res?;
                // The others take this worker's views over at once instead of after the lease timeout.
                match failover::deregister(&w.conns[0], &w.cfg.worker_id) {
                    Err(e) if crate::lake::is_catalog_unavailable(&e) => {
                        warn!(error = %format!("{e:#}"), "could not mark the worker stopped: the catalog cannot be reached; its views move after the lease timeout")
                    }
                    r => r?,
                }
                info!(worker = %w.cfg.worker_id, "worker stopped");
                return Ok(());
            }
        }
    }
}

/// A view this worker should run but cannot start yet, and when it tries again.
struct Waiting {
    /// What it waits for: `the topic` or `the Kafka brokers`.
    reason: &'static str,
    since: Instant,
    backoff: Duration,
    next_try: Instant,
}

impl Waiting {
    const FIRST_BACKOFF: Duration = Duration::from_secs(5);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
}

/// A catalog outage the worker is waiting out (#27): when it began, and how long the next wait is.
#[derive(Default)]
struct Outage {
    since: Option<Instant>,
    backoff: Duration,
}

impl Outage {
    /// The longest wait between two tries.
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    /// Waits before the next try: 1 s, doubling up to 30 s, or until `shutdown`. The outage's start is logged
    /// as a warning and recorded in the metrics; each later try is logged.
    fn wait(&mut self, e: &anyhow::Error, cfg: &WorkerConfig, shutdown: &AtomicBool) {
        let error = format!("{e:#}");
        match self.since {
            None => {
                warn!(worker = %cfg.worker_id, error = %error, "the catalog cannot be reached; waiting for it, then resuming from the last checkpoints");
                if let Some(m) = &cfg.metrics {
                    m.set_catalog_reachable(false);
                }
                self.since = Some(Instant::now());
            }
            Some(since) => info!(worker = %cfg.worker_id, down_s = since.elapsed().as_secs(), error = %error, "the catalog is still unreachable"),
        }
        self.backoff = if self.backoff.is_zero() { Duration::from_secs(1) } else { (self.backoff * 2).min(Self::MAX_BACKOFF) };
        let until = Instant::now() + self.backoff;
        while Instant::now() < until && !shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The worker started again: the outage, if there was one, is over.
    fn end(&mut self, cfg: &WorkerConfig) {
        if let Some(since) = self.since.take() {
            info!(worker = %cfg.worker_id, down_s = since.elapsed().as_secs(), "the catalog answers again; resumed from the last checkpoints");
            if let Some(m) = &cfg.metrics {
                m.set_catalog_reachable(true);
            }
        }
        self.backoff = Duration::ZERO;
    }
}

/// Views that must stop, with the reason.
type Stopped = Vec<(String, String)>;

/// Rows per epoch below which a worker out of memory gives up instead of halving its epochs again.
const MIN_EPOCH_ROWS: usize = 1_000;

fn is_out_of_memory(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("Out of Memory Error")
}

/// Opens the worker's instance, registers it and builds its scheduler; `rows_cap` caps every view's rows per
/// epoch (after running out of memory).
fn start_worker(cfg: WorkerConfig, rt: Handle, rows_cap: Option<usize>) -> Result<Worker> {
    let conn = cfg.lake.open()?;
    // Nothing a worker runs depends on the order rows were inserted in, and keeping it makes inserts buffer
    // whole results: without it they stream, and stay inside the memory limit.
    conn.execute_batch("SET preserve_insertion_order = false")?;
    crate::lake::bootstrap(&conn)?;
    crate::lake::check_view_ids(&conn)?;
    catalog::set_control_inlining(&conn)?;
    failover::register(&conn, &cfg.worker_id, cfg.metrics.as_ref().and_then(|m| m.advertised()).as_deref())?;
    if let Some(m) = &cfg.metrics {
        m.set_worker(&cfg.worker_id);
        m.set_build(&conn.query_row("SELECT library_version FROM pragma_version()", [], |r| r.get::<_, String>(0))?);
        m.set_up(true);
    }
    let threads = match cfg.stage_threads {
        0 => std::thread::available_parallelism().map_or(1, |n| (n.get() / 2).max(1)),
        n => n,
    };
    let mut conns = vec![conn];
    for _ in 1..threads {
        conns.push(connect_again(&conns[0])?);
    }
    let committer = if cfg.pipeline {
        Committer::spawn(connect_again(&conns[0])?, cfg.worker_id.clone(), cfg.maintenance.clone())?
    } else {
        Committer::Inline { maint: Maintenance::new(cfg.maintenance.clone()), done: None }
    };
    info!(
        worker = %cfg.worker_id,
        stage_threads = threads,
        pipeline = cfg.pipeline,
        shared_readers = cfg.shared_readers,
        lean_commit = cfg.lean_commit,
        lease_timeout_s = cfg.lease_timeout.map(|t| t.as_secs_f64()),
        memory_limit = %conns[0].query_row("SELECT current_setting('memory_limit')", [], |r| r.get::<_, String>(0))?,
        duckdb_threads = conns[0].query_row("SELECT current_setting('threads')", [], |r| r.get::<_, i64>(0))?,
        temp_dir = %cfg.lake.temp_dir.as_deref().unwrap_or_default(),
        rows_per_epoch_cap = rows_cap,
        "worker started"
    );
    let liveness = failover::Liveness::new(
        cfg.lease_timeout.unwrap_or(Duration::MAX),
        cfg.stop_grace,
        heartbeat_every(cfg.lease_timeout),
    );
    Ok(Worker {
        rows_cap,
        last_targets: None,
        held_checked: None,
        liveness,
        last_heartbeat: Instant::now(),
        last_rebalance: Instant::now(),
        cfg,
        rt,
        conns,
        committer,
        jobs: BTreeMap::new(),
        ingests: BTreeMap::new(),
        groups: BTreeMap::new(),
        next_group: 0,
        rounds: 0,
        committed: 0,
        drops: Vec::new(),
        waiting: BTreeMap::new(),
        load_rounds: 0,
    })
}

/// Runs a worker until `shutdown`, restarting it from its last checkpoint 5 s after it fails (a view that
/// cannot be staged, Kafka or the catalog unreachable). Failures and restarts are recorded in `metrics`.
pub fn supervise(
    make_cfg: impl Fn() -> Result<WorkerConfig>,
    rt: Handle,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    while !shutdown.load(Ordering::Relaxed) {
        let mut cfg = make_cfg()?;
        cfg.metrics = Some(metrics.clone());
        match run_worker(cfg, rt.clone(), shutdown.clone()) {
            Ok(()) => break,
            Err(e) => {
                let msg = format!("{e:#}");
                tracing::error!(error = %msg, "worker failed; restarting from the last checkpoint in 5 s");
                metrics.record_failure(&msg);
                let t = Instant::now();
                while t.elapsed() < Duration::from_secs(5) && !shutdown.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
    metrics.set_up(false);
    Ok(())
}

struct Worker {
    cfg: WorkerConfig,
    rt: Handle,
    /// Staging connections, one per staging thread. The first also does the control work: refresh, job
    /// start, maintenance and (without `pipeline`) the commit.
    conns: Vec<Connection>,
    committer: Committer,
    jobs: BTreeMap<String, JobRuntime>,
    /// Database sources' ingest jobs (PostgreSQL, MySQL), each on its own thread (`sources::database`).
    ingests: BTreeMap<String, crate::sources::database::Handle>,
    groups: BTreeMap<u64, Group>,
    next_group: u64,
    /// Epochs handed to the commit so far; the one staged next stages into table set `(rounds + 1) % 2`.
    rounds: u64,
    committed: u64,
    /// Staging tables to drop once the next epoch has committed.
    drops: Vec<String>,
    /// The other workers' heartbeats as this one has seen them.
    liveness: failover::Liveness,
    last_heartbeat: Instant,
    last_rebalance: Instant,
    /// Views this worker should run but cannot start yet: their topic does not exist, or no broker answers.
    waiting: BTreeMap<String, Waiting>,
    /// Rebalance rounds in a row that found this worker carrying too much load.
    load_rounds: u32,
    /// Most rows per epoch for any view, after an epoch ran out of memory; None: each view's `max_rows`.
    rows_cap: Option<usize>,
    /// When the scrape targets were last read from the lake.
    last_targets: Option<Instant>,
    /// When the snapshots each view holds were last read (`report_held_snapshots`).
    held_checked: Option<Instant>,
}

/// How often a worker heartbeats: well inside the lease timeout, and at least every 5 s so workers with a
/// shorter timeout than this one's still see it alive.
fn heartbeat_every(lease_timeout: Option<Duration>) -> Duration {
    lease_timeout.map_or(Duration::from_secs(5), |t| (t / 5).clamp(Duration::from_millis(500), Duration::from_secs(5)))
}

impl Drop for Worker {
    /// A worker that fails is built again; its ingest threads end with it (their leases are fenced anyway).
    fn drop(&mut self) {
        for h in self.ingests.values() {
            h.stop();
        }
    }
}

impl Worker {
    fn run(&mut self, shutdown: &AtomicBool) -> Result<()> {
        let mut last_refresh = Instant::now() - self.cfg.refresh_every;
        let mut last_epoch = Instant::now();
        while !shutdown.load(Ordering::Relaxed) && !self.reached_max_epochs() {
            if let Some(c) = self.committer.try_finish()? {
                self.apply(c);
                continue;
            }
            if self.last_heartbeat.elapsed() >= heartbeat_every(self.cfg.lease_timeout) {
                let costs: BTreeMap<String, f64> =
                    self.jobs.iter().filter_map(|(id, j)| j.cost.map(|c| (id.clone(), c))).collect();
                let cooldown = self.cfg.rebalance.map_or(Duration::ZERO, |r| r.cooldown);
                let cooling: Vec<String> =
                    self.jobs.iter().filter(|(_, j)| j.started.elapsed() < cooldown).map(|(id, _)| id.clone()).collect();
                let waiting: BTreeMap<&str, &str> = self.waiting.iter().map(|(id, w)| (id.as_str(), w.reason)).collect();
                failover::heartbeat(&self.conns[0], &self.cfg.worker_id, self.jobs.len() + self.ingests.len(), &costs, &cooling, &waiting)?;
                if let Some(m) = &self.cfg.metrics {
                    m.set_views_waiting(self.waiting.len());
                }
                self.liveness.beat();
                self.last_heartbeat = Instant::now();
            }
            if last_refresh.elapsed() >= self.cfg.refresh_every {
                self.refresh()?;
                self.check_limits()?;
                last_refresh = Instant::now();
            }
            self.check_ingests()?;
            if self.jobs.is_empty() {
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            // Next epoch at the shortest job interval, or sooner when a reader's buffer reaches its row cap.
            let interval = self.jobs.values().map(|j| j.interval).min().unwrap();
            let full = self.groups.values().any(|g| g.reader.buffered() >= g.max_rows);
            if last_epoch.elapsed() < interval && !full {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            last_epoch = Instant::now();
            let (batch, stopped) = self.stage_epoch()?;
            // Views that stopped are out of the epoch; the others commit it.
            for (id, why) in stopped {
                self.fail_job(&id, &why)?;
            }
            if let Some(mut batch) = batch {
                if self.committer.in_flight() {
                    crash_point("staged_ahead");
                }
                // Commits go in order: the previous epoch lands (or fails the worker) before this one is handed over.
                if let Some(c) = self.committer.finish()? {
                    self.apply(c);
                }
                // Jobs fenced by that commit are gone, and so is what they staged.
                batch.jobs.retain(|c| self.jobs.contains_key(&c.id));
                if batch.jobs.is_empty() {
                    self.drops.append(&mut batch.drop_after);
                } else {
                    self.committer.submit(&self.conns[0], &self.cfg.worker_id, batch)?;
                    if !self.cfg.pipeline {
                        if let Some(c) = self.committer.finish()? {
                            self.apply(c);
                        }
                    }
                }
            }
            if let Committer::Inline { maint, .. } = &mut self.committer {
                // Maintenance runs between epochs, never inside one.
                crash_point("maintenance");
                maint.run_due(&self.conns[0], &self.jobs.keys().cloned().collect::<Vec<_>>());
            }
        }
        // Let the epoch in flight land, and the ingest jobs finish theirs.
        if let Some(c) = self.committer.finish()? {
            self.apply(c);
        }
        for (_, h) in std::mem::take(&mut self.ingests) {
            h.join();
        }
        Ok(())
    }

    /// Marks failed the ingest jobs whose source broke a rule the mirror relies on, and lets go of those whose
    /// thread ended (its lease moved: the next refresh sees it).
    fn check_ingests(&mut self) -> Result<()> {
        let failed: Vec<(String, String)> = self.ingests.iter().filter_map(|(id, h)| h.failure().map(|why| (id.clone(), why))).collect();
        for (id, why) in failed {
            self.fail_job(&id, &why)?;
        }
        self.ingests.retain(|_, h| !h.finished());
        Ok(())
    }

    /// Starts a database source's ingest job on its own thread and connection. False when it is not ours.
    fn start_ingest(&mut self, id: &str) -> Result<bool> {
        let conn = &self.conns[0];
        let spec = crate::sources::database::Spec::load(conn, id)?;
        let Some(lease) = catalog::take_lease(conn, id, &self.cfg.worker_id)? else { return Ok(false) };
        let settings = crate::sources::database::Settings {
            worker: self.cfg.worker_id.clone(),
            position_refresh: self.cfg.position_refresh,
            metrics: self.cfg.metrics.clone(),
        };
        let h = crate::sources::database::Handle::spawn(crate::lake::connect_again(conn)?, settings, id.to_string(), lease, spec);
        self.ingests.insert(id.to_string(), h);
        Ok(true)
    }

    /// The snapshots each view with lake inputs holds, from its position to the lake's newest: for the views
    /// this worker runs, and for those no worker runs (paused, stopped or failed), which hold snapshots too.
    /// Read every `STATE_CHECK_EVERY`. A failed reading is logged and skipped: it is only a gauge.
    fn report_held_snapshots(&mut self, m: &Arc<crate::metrics::Metrics>) {
        if self.held_checked.is_some_and(|t| t.elapsed() < STATE_CHECK_EVERY) {
            return;
        }
        self.held_checked = Some(Instant::now());
        match self.held_snapshots() {
            Ok(held) => m.set_snapshots_held(held),
            Err(e) => warn!(error = %format!("{e:#}"), "reading the snapshots views hold failed"),
        }
    }

    fn held_snapshots(&self) -> Result<BTreeMap<String, (i64, Option<i64>)>> {
        let conn = &self.conns[0];
        let running: HashSet<String> = catalog::running_jobs(conn)?.into_iter().filter(|j| j.owner.is_some()).map(|j| j.id).collect();
        let held: Vec<(String, i64)> = catalog::lake_positions(conn)?
            .into_iter()
            .filter(|p| self.jobs.contains_key(&p.job) || !running.contains(&p.job))
            .filter_map(|p| Some((p.job, p.snapshot?)))
            .collect();
        if held.is_empty() {
            return Ok(BTreeMap::new());
        }
        // One read of the snapshot list for every view: the newest snapshot, and the time of each position.
        let ids = held.iter().map(|(_, s)| s.to_string()).collect::<Vec<_>>().join(", ");
        let mut stmt = conn.prepare(&format!(
            "SELECT snapshot_id, epoch_us(snapshot_time), max(snapshot_id) OVER () FROM ducklake_snapshots({})
             WHERE snapshot_id IN ({ids}) OR snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshots({}))",
            lit(LAKE),
            lit(LAKE)
        ))?;
        let mut times = HashMap::new();
        let mut current = 0;
        for r in stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?)))? {
            let (id, us, newest) = r?;
            times.insert(id, us);
            current = newest;
        }
        Ok(held.into_iter().map(|(job, at)| (job, (current - at, times.get(&at).copied()))).collect())
    }

    /// Reads the size of each stateful view's state every `STATE_CHECK_EVERY` (the state as last committed),
    /// and fails a view whose state has outgrown its `max_state_rows`: the worker and its other views go on.
    fn check_limits(&mut self) -> Result<()> {
        // Every worker with an endpoint serves the list of all of them (`GET /sd`): Prometheus needs one address.
        if let Some(m) = self.cfg.metrics.as_ref().filter(|_| self.last_targets.is_none_or(|t| t.elapsed() >= TARGETS_EVERY)) {
            m.set_targets(failover::scrape_targets(&self.conns[0])?);
            self.last_targets = Some(Instant::now());
        }
        if let Some(m) = self.cfg.metrics.clone() {
            self.report_held_snapshots(&m);
            let (used, spilled, limit): (i64, i64, String) = self.conns[0].query_row(
                "SELECT coalesce(sum(memory_usage_bytes), 0)::BIGINT, coalesce(sum(temporary_storage_bytes), 0)::BIGINT,
                        current_setting('memory_limit') FROM duckdb_memory()",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            m.set_duckdb_memory(used.max(0) as u64, spilled.max(0) as u64, crate::process::parse_size(&limit).unwrap_or(0));
        }
        let due: Vec<String> = self
            .jobs
            .iter()
            .filter(|(_, j)| j.def.plan.has_state() && j.state_checked.is_none_or(|t| t.elapsed() >= STATE_CHECK_EVERY))
            .map(|(id, _)| id.clone())
            .collect();
        let mut over = Vec::new();
        for id in due {
            let rows: i64 = self.conns[0].query_row(&format!("SELECT count(*) FROM {}", state_table(&id)), [], |r| r.get(0))?;
            let j = self.jobs.get_mut(&id).unwrap();
            j.state_checked = Some(Instant::now());
            if let Some(m) = &self.cfg.metrics {
                m.set_state_rows(&id, rows);
            }
            if let Some(max) = j.def.limits.max_state_rows.filter(|m| rows as u64 > *m) {
                over.push((
                    id,
                    format!(
                        "its state holds {rows} rows, over its max_state_rows of {max}: bound the view (a window, fewer \
                         groups) or raise the limit (alter-view --max-state-rows), then start it again"
                    ),
                ));
            }
        }
        for (id, why) in over {
            self.fail_job(&id, &why)?;
        }
        Ok(())
    }

    /// Marks a view failed and drops it from this worker; the others go on. A view dropped meanwhile, or whose
    /// lease has moved on (reset, replaced, stopped or taken over while this epoch staged), is only let go: the
    /// error belongs to the run that was fenced, and a dropped view has nothing to mark.
    fn fail_job(&mut self, id: &str, why: &str) -> Result<()> {
        let held = self.jobs.get(id).map(|j| j.lease).or(self.ingests.get(id).map(|h| h.lease));
        let failed = catalog::fail_if_held(&self.conns[0], id, &self.cfg.worker_id, held)?;
        if failed {
            warn!(job = %id, error = %why, "view stopped: marking it failed");
            if let Some(m) = &self.cfg.metrics {
                m.record_view_failed();
            }
        } else {
            info!(job = %id, error = %why, "view dropped or fenced while it failed: letting it go without marking it failed");
        }
        self.remove_job(id);
        Ok(())
    }

    fn reached_max_epochs(&self) -> bool {
        self.cfg.max_epochs.is_some_and(|m| self.committed >= m)
    }

    /// Picks up jobs newly assigned to this worker and drops ones paused, stopped or taken over. With
    /// failover, first claims this worker's share of the views whose owner is dead or unassigned, or else
    /// hands views away: all of them while draining, pinned ones back home, and a few when this is the
    /// busiest worker (`failover`).
    fn refresh(&mut self) -> Result<()> {
        let mut running = catalog::running_jobs(&self.conns[0])?;
        if self.cfg.lease_timeout.is_some() && self.place(&running)? {
            running = catalog::running_jobs(&self.conns[0])?;
        }
        let mine: BTreeMap<String, i64> = running
            .into_iter()
            .filter(|j| j.owner.as_deref() == Some(self.cfg.worker_id.as_str()))
            .map(|j| (j.id, j.lease))
            .collect();
        let released: Vec<String> = self
            .jobs
            .iter()
            .map(|(id, j)| (id, j.lease))
            .chain(self.ingests.iter().map(|(id, h)| (id, h.lease)))
            .filter(|(id, lease)| mine.get(*id) != Some(lease))
            .map(|(id, _)| id.clone())
            .collect();
        for id in released {
            info!(job = %id, "job released (paused, stopped or reassigned)");
            self.remove_job(&id);
        }
        let new: Vec<String> = mine.keys().filter(|id| !self.jobs.contains_key(*id) && !self.ingests.contains_key(*id)).cloned().collect();
        // A job coming back reads its checkpoint, so its previous run's last commit must have landed first.
        if new.iter().any(|id| self.committer.includes(id)) {
            if let Some(c) = self.committer.finish()? {
                self.apply(c);
            }
        }
        self.waiting.retain(|id, _| mine.contains_key(id));
        for id in new {
            if self.waiting.get(&id).is_some_and(|w| w.next_try > Instant::now()) {
                continue;
            }
            let started = match catalog::ingest_source(&self.conns[0], &id)? {
                Some(_) => self.start_ingest(&id),
                None => self.start_job(&id),
            };
            match started {
                Ok(_) => {
                    if let Some(w) = self.waiting.remove(&id) {
                        info!(job = %id, waited_s = w.since.elapsed().as_secs(), "done waiting for {}; job started", w.reason);
                    }
                }
                // Not the view's fault: the worker waits the outage out and starts it again (#27).
                Err(e) if crate::lake::is_catalog_unavailable(&e) => return Err(e),
                Err(e) if crate::lake::is_conflict(&e) => {
                    warn!(job = %id, error = %format!("{e:#}"), "lease race; retrying on next refresh");
                }
                // A topic created after the view started: wait for it, as Kafka consumers do. Checked every
                // refresh: the check is one metadata request.
                Err(e) if crate::sources::kafka::is_topic_missing(&e) => self.wait_for(&id, "the topic", Duration::ZERO, &e),
                // Brokers that cannot be reached at all: wait for them too (#26), with backoff, since each try
                // waits for a connection.
                Err(e) if crate::sources::kafka::is_brokers_unreachable(&e) => {
                    let next = self.waiting.get(&id).filter(|w| w.reason == "the Kafka brokers").map_or(Waiting::FIRST_BACKOFF, |w| (w.backoff * 2).min(Waiting::MAX_BACKOFF));
                    self.wait_for(&id, "the Kafka brokers", next, &e);
                }
                // Also when the view was stopped or dropped while it started (a dbt --full-refresh drops and
                // creates views while workers run them): then there is nothing to mark, and this worker carries
                // on with its other views.
                Err(e) => self.fail_job(&id, &format!("cannot start job: {e:#}"))?,
            }
        }
        // The gauge follows at once; `duckwake_jobs` (from the heartbeat row) within a heartbeat.
        if let Some(m) = &self.cfg.metrics {
            m.set_views_waiting(self.waiting.len());
        }
        Ok(())
    }

    /// Records that view `id` waits for `reason`, to try again after `backoff`; logged when the wait begins
    /// or its reason changes.
    fn wait_for(&mut self, id: &str, reason: &'static str, backoff: Duration, e: &anyhow::Error) {
        let now = Instant::now();
        match self.waiting.get_mut(id) {
            Some(w) if w.reason == reason => {
                w.backoff = backoff;
                w.next_try = now + backoff;
            }
            _ => {
                warn!(job = %id, error = %format!("{e:#}"), retry_in_s = backoff.as_secs(), "waiting for {reason}");
                self.waiting.insert(id.to_string(), Waiting { reason, since: now, backoff, next_try: now + backoff });
            }
        }
    }

    /// One placement round: claims, or else handoffs (never both, so a handoff never works from ownership a
    /// claim just changed). True when a view changed hands.
    fn place(&mut self, running: &[catalog::RunningJob]) -> Result<bool> {
        let me = self.cfg.worker_id.clone();
        let rows = failover::workers(&self.conns[0])?;
        let now = Instant::now();
        let owners = running.iter().filter_map(|j| j.owner.clone()).filter(|o| o != failover::ANY_WORKER);
        self.liveness.observe(&rows, owners, now);
        let my_state = rows.iter().find(|w| w.id == me).map_or(failover::WorkerState::Running, |w| w.state);
        let mut targets = self.liveness.targets(now);
        if my_state == failover::WorkerState::Running && !targets.contains(&me) {
            targets.push(me.clone());
            targets.sort();
        }
        let views = failover::view_infos(running, &rows, &HashMap::new());
        if my_state == failover::WorkerState::Running && self.claim_orphans(&views, &targets, now)? {
            return Ok(true);
        }
        let draining = my_state == failover::WorkerState::Draining;
        let Some(cfg) = self.cfg.rebalance.or(draining.then(failover::RebalanceConfig::default)) else { return Ok(false) };
        if !draining && self.last_rebalance.elapsed() < cfg.every {
            return Ok(false);
        }
        self.last_rebalance = Instant::now();
        // Rebalancing prefers targets that already read a view's source.
        let sources = catalog::running_sources(&self.conns[0])?;
        let views = failover::view_infos(running, &rows, &sources);
        // Views within their cooldown, here (as of now) and on the other workers (as of their last heartbeat).
        let mut cooling: HashSet<String> =
            self.jobs.iter().filter(|(_, j)| j.started.elapsed() < cfg.cooldown).map(|(id, _)| id.clone()).collect();
        cooling.extend(rows.iter().filter(|w| w.id != me).flat_map(|w| w.cooling.iter().cloned()));
        // A load imbalance must last LOAD_ROUNDS rounds before views move for it; drains, pins and view
        // counts do not wait.
        let planned = failover::moves(&me, my_state, &views, &targets, &cooling, &cfg, true);
        let by_load = planned.iter().any(|m| m.reason == failover::Reason::Load);
        self.load_rounds = if by_load { self.load_rounds + 1 } else { 0 };
        let plan = if by_load && self.load_rounds < failover::LOAD_ROUNDS {
            failover::moves(&me, my_state, &views, &targets, &cooling, &cfg, false)
        } else {
            planned
        };
        let mut moved = false;
        for m in plan {
            if m.reason == failover::Reason::Load {
                self.load_rounds = 0;
            }
            moved |= self.hand_off(&m)?;
        }
        Ok(moved)
    }

    /// Claims this worker's share of the orphaned views (see `failover`); true when it claimed any.
    fn claim_orphans(&mut self, views: &[failover::ViewInfo], targets: &[String], now: Instant) -> Result<bool> {
        let conn = &self.conns[0];
        let me = self.cfg.worker_id.as_str();
        let mut claimed = false;
        for v in failover::claims(me, views, targets, |w| self.liveness.dead(w, now)) {
            match failover::claim(conn, &v.id, v.owner.as_deref(), v.lease, me) {
                Ok(true) => {
                    info!(job = %v.id, from = v.owner.as_deref().unwrap_or("unassigned"), "claimed orphaned view");
                    if let Some(m) = &self.cfg.metrics {
                        m.record_claim();
                    }
                    claimed = true;
                }
                Ok(false) => {}
                // Another worker claimed it first.
                Err(e) if crate::lake::is_conflict(&e) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(claimed)
    }

    /// Moves one of this worker's views to another worker: its epoch in flight commits first, then the lease
    /// moves, so nothing is fenced and nothing staged is lost; the target starts from that checkpoint.
    fn hand_off(&mut self, m: &failover::Move) -> Result<bool> {
        if self.committer.includes(&m.job) {
            if let Some(c) = self.committer.finish()? {
                self.apply(c);
            }
        }
        // A view that committed its last epoch elsewhere in the meantime is not ours to give.
        let lease = match self.jobs.get(&m.job) {
            Some(j) => j.lease,
            None => m.lease,
        };
        match failover::claim(&self.conns[0], &m.job, Some(&self.cfg.worker_id), lease, &m.to) {
            Ok(true) => {
                info!(job = %m.job, to = %m.to, reason = m.reason.as_str(), "handed off view");
                self.remove_job(&m.job);
                if let Some(metrics) = &self.cfg.metrics {
                    metrics.record_move();
                }
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(e) if crate::lake::is_conflict(&e) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Recovery: take the lease, read the latest checkpoint, seek each partition to its offset. State needs no
    /// restore with the DuckLake backend: the state table is as of the last commit. With shared readers a job
    /// at the position of a group of its source joins that group. Returns false when the job is not ours.
    fn start_job(&mut self, id: &str) -> Result<bool> {
        let conn = &self.conns[0];
        let (mut def, sources) = catalog::load_compiled(conn, id)?;
        // After an epoch ran out of memory the worker runs every view with fewer rows per epoch: Kafka reads
        // and backfill pages alike take them from here.
        def.max_rows = self.rows_cap.map_or(def.max_rows, |c| c.min(def.max_rows));
        // Recomputed by a cascaded reset: it starts from the snapshot current now.
        if !def.lake_inputs.is_empty() && def.start_snapshot.is_none() {
            let d = catalog::take_start_snapshot(conn, id)?;
            (def.start_snapshot, def.start_commit_us) = (d.start_snapshot, d.start_commit_us);
        }
        let names = def.plan.sources();
        // Every Kafka topic must exist before the lease is taken: a job waiting for its topic retries each
        // refresh, and taking the lease each time would commit a snapshot for nothing.
        for s in names.iter().filter_map(|n| sources.get(n)) {
            let opts = s.options.clone();
            self.rt.block_on(async move {
                let c = crate::sources::kafka::client(&opts).await?;
                crate::sources::kafka::partitions(&c, &opts.topic).await
            })?;
        }
        let Some(lease) = catalog::take_lease(conn, id, &self.cfg.worker_id)? else { return Ok(false) };
        catalog::set_sink_inlining(conn, &def)?;
        // One checkpoint row holds every source's offsets.
        let ckpt = catalog::latest_checkpoint(conn, id, &names[0])?.unwrap_or_default();
        let now = Instant::now();
        let mut inputs = Vec::new();
        for name in &names {
            let start = catalog::latest_checkpoint(conn, id, name)?.map(|c| c.offsets).unwrap_or_default();
            if let Some(lake) = def.lake_inputs.iter().find(|l| &l.name == name) {
                // The last snapshot read, or the one current when the view was created.
                let s0 = def.start_snapshot.context("lake input without a start snapshot")?;
                let position = start.get(&LAKE_POSITION).copied().unwrap_or(s0);
                // Where a backfill stands: the cursor in the checkpoint, or before the first row when the
                // view has not committed yet. No cursor in a checkpoint means the backfill is done.
                let cursor = start.get(&LAKE_BACKFILL).copied().or((def.backfill && ckpt.epoch == 0).then_some(-1));
                if !crate::lake::snapshot_exists(conn, position)? {
                    anyhow::bail!(
                        "view {id}: its position in {}, snapshot {position}, has expired (the oldest snapshot the lake has \
                         is {}), so the changes after it are gone. Recompute the view from its inputs' current rows: \
                         create-view --replace (replace := true)",
                        lake.table,
                        crate::lake::oldest_snapshot(conn)?
                    );
                }
                let commit_us = catalog::commit_us_at(conn, position)?;
                match cursor {
                    Some(c) => info!(job = %id, input = %name, table = %lake.table, snapshot = position, after_row = c, "backfilling"),
                    None => info!(job = %id, input = %name, table = %lake.table, snapshot = position, "reading change feed"),
                }
                let mut offsets = BTreeMap::from([(LAKE_POSITION, position)]);
                offsets.extend(cursor.map(|c| (LAKE_BACKFILL, c)));
                inputs.push(Input {
                    name: name.clone(),
                    columns: Arc::new(lake.columns.clone()),
                    feed: Feed::Lake { lake: Arc::new(lake.clone()), commit_us },
                    offsets,
                    part_max_ts: HashMap::new(),
                    part_last_data: HashMap::new(),
                    watermark_us: ckpt.watermark_us,
                    delay_us: 0,
                    idle_timeout: Duration::MAX,
                });
                continue;
            }
            let source = Arc::new(sources.get(name).with_context(|| format!("source {name} of the view is gone"))?.clone());
            let decoder = source.options.arrow_decoder()?.map(Arc::new);
            let max_rows = def.max_rows;
            let (reader, offsets) = Reader::start(&self.rt, &source.options, &start, max_rows, decoder)?;
            let joined = self
                .groups
                .iter()
                .find(|(_, g)| self.cfg.shared_readers && g.source == source && g.offsets == offsets)
                .map(|(gid, _)| *gid);
            let group = match joined {
                Some(gid) => {
                    // The group's reader is at the same position; this job's own is dropped unused.
                    let g = self.groups.get_mut(&gid).unwrap();
                    g.members.insert(id.to_string());
                    g.max_rows = g.max_rows.min(max_rows);
                    gid
                }
                None => {
                    let gid = self.next_group;
                    self.next_group += 1;
                    let members = BTreeSet::from([id.to_string()]);
                    let g = Group { source: source.clone(), reader, offsets: offsets.clone(), members, max_rows };
                    self.groups.insert(gid, g);
                    gid
                }
            };
            info!(job = %id, source = %name, offsets = ?offsets, group, "reading source");
            inputs.push(Input {
                name: name.clone(),
                columns: Arc::new(source.options.columns.clone()),
                delay_us: parse_duration(&source.options.watermark_delay)?.as_micros() as i64,
                idle_timeout: parse_duration(&source.options.idle_timeout)?,
                part_last_data: offsets.keys().map(|p| (*p, now)).collect(),
                part_max_ts: HashMap::new(),
                // The checkpoint keeps the view's watermark, at or below every source's: a safe start.
                watermark_us: ckpt.watermark_us,
                offsets,
                feed: Feed::Kafka { group },
            });
        }
        let circuit = Arc::new(Circuit::compile(&def.view())?);
        // Kept with `lean_commit`, and always for views with lake inputs: their watermark moves with every
        // snapshot, so only a window that closes makes an epoch without rows worth committing.
        let open_ends = if (self.cfg.lean_commit || !def.lake_inputs.is_empty()) && circuit.emits_final() {
            let mut stmt = self.conns[0].prepare(&format!("SELECT DISTINCT epoch_us(__we) FROM {}", state_table(id)))?;
            let ends = stmt.query_map([], |r| r.get::<_, i64>(0))?.collect::<duckdb::Result<BTreeSet<_>>>()?;
            Some(ends)
        } else {
            None
        };
        let Checkpoint { epoch, watermark_us, max_event_time_us, .. } = ckpt;
        info!(job = %id, lease, epoch, "job started");
        if let Some(m) = &self.cfg.metrics {
            let delay_us = inputs.iter().map(|i| i.delay_us).max().unwrap_or(0);
            m.set_view_config(id, parse_duration(&def.epoch_interval)?.as_secs_f64(), delay_us as f64 / 1e6, def.limits.max_state_rows);
        }
        let j = JobRuntime {
            interval: parse_duration(&def.epoch_interval)?,
            def: Arc::new(def),
            circuit,
            lease,
            epoch,
            watermark_us,
            max_event_time_us,
            inputs,
            last_round: None,
            open_ends,
            started: Instant::now(),
            cost: None,
            last_commit: None,
            state_checked: None,
        };
        self.jobs.insert(id.to_string(), j);
        Ok(true)
    }

    fn remove_job(&mut self, id: &str) {
        if let Some(h) = self.ingests.remove(id) {
            // Its thread finishes the epoch in hand, which the moved lease fences, and ends.
            h.stop();
            if let Some(m) = &self.cfg.metrics {
                m.forget(id);
            }
            return;
        }
        let Some(j) = self.jobs.remove(id) else { return };
        if let Some(m) = &self.cfg.metrics {
            m.forget(id);
        }
        for (k, input) in j.inputs.iter().enumerate() {
            let Some(gid) = input.group() else {
                self.drops.push(lake_typed(id, j.lease, k));
                continue;
            };
            let Some(g) = self.groups.get_mut(&gid) else { continue };
            g.members.remove(id);
            if g.members.is_empty() {
                self.groups.remove(&gid);
                self.drops.extend(group_tables(gid));
            } else {
                // Each job's max_rows is already capped (`start_job`).
                g.max_rows = g.members.iter().map(|m| self.jobs[m].def.max_rows).min().unwrap();
            }
        }
    }

    /// Takes in a committed epoch: drops the jobs it fenced and logs the others.
    fn apply(&mut self, c: Committed) {
        for id in &c.fenced {
            warn!(job = %id, "lease lost (the view was stopped, paused, replaced or taken over); dropping it");
            self.remove_job(id);
        }
        self.committed += 1;
        if let Some(m) = &self.cfg.metrics {
            m.record_commit(c.batch.poll_stage_ms, c.commit_ms, c.attempts);
        }
        let now = Instant::now();
        let n_jobs = c.batch.jobs.len().max(1) as f64;
        for j in c.batch.jobs.iter().filter(|j| !c.fenced.contains(&j.id)) {
            let s = &j.staged;
            // The share of the worker this epoch took: the job's staging, its part of the source read (shared
            // by the group's members) and of the commit, over the wall time since its previous epoch.
            let readers = self.jobs.get(&j.id).and_then(|rt| {
                rt.inputs.iter().find_map(|i| i.group()).and_then(|g| self.groups.get(&g)).map(|g| g.members.len())
            });
            let busy_ms = s.stage_ms as f64 + s.source_ms as f64 / readers.unwrap_or(1).max(1) as f64 + c.commit_ms as f64 / n_jobs;
            if let Some(rt) = self.jobs.get_mut(&j.id) {
                let wall_ms = rt.last_commit.map_or(rt.interval.as_millis() as f64, |t| (now - t).as_millis() as f64).max(1.0);
                let share = (busy_ms / wall_ms).min(4.0);
                rt.cost = Some(rt.cost.map_or(share, |c| (1.0 - COST_ALPHA) * c + COST_ALPHA * share));
                rt.last_commit = Some(now);
                if let (Some(m), Some(cost)) = (&self.cfg.metrics, rt.cost) {
                    m.set_cost(&j.id, cost);
                }
            }
            // Newest event committed to the moment it became visible (negative for event times in the future).
            let freshness_ms = s.max_event_time_us.map(|us| (c.visible_at.timestamp_micros() - us) / 1000);
            if let Some(m) = &self.cfg.metrics {
                // A job released while this epoch was in flight is not this worker's any more.
                if self.jobs.contains_key(&j.id) {
                    m.record_job(
                        &j.id,
                        crate::metrics::EpochStats {
                            epoch: j.epoch,
                            lag: j.lag,
                            watermark_us: s.watermark_us,
                            max_event_time_us: s.max_event_time_us,
                            rows_in: s.rows_in as u64,
                            rows_out: s.rows_out.max(0) as u64,
                            rows_late: s.rows_late.max(0) as u64,
                            rows_dead: s.rows_dead.max(0) as u64,
                            stage_ms: s.stage_ms + s.source_ms,
                            commit_freshness_s: freshness_ms.map(|ms| ms as f64 / 1000.0),
                        },
                    );
                }
            }
            info!(
                target: "epoch",
                job = %j.id,
                epoch = j.epoch,
                rows_in = s.rows_in,
                rows_sql = s.rows_sql,
                bisections = s.bisections,
                rows_out = s.rows_out,
                rows_late = s.rows_late,
                rows_dead = s.rows_dead,
                watermark = %fmt_us(s.watermark_us),
                lag = j.lag,
                freshness_ms = freshness_ms.unwrap_or(-1),
                stage_ms = s.stage_ms,
                source_ms = s.source_ms,
                poll_stage_ms = c.batch.poll_stage_ms,
                commit_ms = c.commit_ms,
                writes_ms = c.timing.writes,
                commit_call_ms = c.timing.commit,
                attempts = c.attempts,
                offsets = ?s.inputs.iter().map(|i| &i.offsets).collect::<Vec<_>>(),
                "epoch committed"
            );
        }
    }

    /// Polls and stages the next epoch of every job. Returns it for the commit, or None when there is nothing
    /// to commit: no new rows and no watermark moved.
    /// Stages every job's next epoch. A job that must stop (a broken input promise, a limit) is left out and
    /// returned with the reason, for the caller to fail once the batch is built: the events its group read
    /// this epoch are the other members' too, so the epoch goes on without it rather than being dropped.
    fn stage_epoch(&mut self) -> Result<(Option<CommitBatch>, Stopped)> {
        let t0 = Instant::now();
        let Worker { cfg, conns, jobs, groups, rounds, drops, .. } = self;
        let round = *rounds + 1;
        let parity = round % 2;

        // 1. Poll, the most advanced group of each source first. With shared readers a group behind another
        //    group of its source reads only up to that group's new position, so it can join it there.
        let mut order: Vec<u64> = groups.keys().copied().collect();
        order.sort_by_key(|gid| std::cmp::Reverse(groups[gid].offsets.values().sum::<i64>()));
        let mut drained: Vec<(u64, BTreeMap<i32, i64>, Vec<Chunk>)> = Vec::new();
        for gid in order {
            let upto = cfg
                .shared_readers
                .then(|| {
                    let g = &groups[&gid];
                    drained.iter().find(|(lead, head, _)| groups[lead].source == g.source && behind(&g.offsets, head))
                })
                .flatten()
                .map(|(_, head, _)| head.clone());
            let g = groups.get_mut(&gid).unwrap();
            let mut chunks = Vec::new();
            g.reader
                .drain(g.max_rows, upto.as_ref(), &mut chunks)
                .with_context(|| format!("source {}: read failed", g.source.name))?;
            let mut head = g.offsets.clone();
            for c in &chunks {
                head.insert(c.partition, c.last_offset + 1);
            }
            drained.push((gid, head, chunks));
        }
        crash_point("poll");

        // 2. Stage each group's events once.
        let items: Vec<_> =
            drained.into_iter().map(|(gid, head, chunks)| (gid, groups[&gid].source.clone(), head, chunks)).collect();
        let sources: BTreeMap<u64, SourceStaged> = parallel(conns, items, |conn, (gid, source, head, chunks)| {
            let s = stage_source(conn, gid, &source, head, &chunks, parity)
                .with_context(|| format!("source {}: staging failed", source.name))?;
            Ok((gid, s))
        })
        .into_iter()
        .collect::<Result<_>>()?;

        // 3. Stage each job's view from its group's tables.
        let prev = *rounds;
        let items: Vec<_> = jobs
            .iter_mut()
            .map(|(id, j)| {
                let overlay = (cfg.pipeline && prev > 0 && j.last_round == Some(prev)).then_some(prev % 2);
                let srcs: Vec<Option<&SourceStaged>> = j.inputs.iter().map(|i| i.group().map(|g| &sources[&g])).collect();
                (id, j, srcs, overlay)
            })
            .collect();
        let results = parallel(conns, items, |conn, (id, j, srcs, overlay)| {
            stage_view(conn, id, j, &srcs, parity, overlay).with_context(|| format!("job {id}: staging failed"))
        });
        // A view whose input broke a promise it relies on stops alone, and so does one this worker no longer holds:
        // dropped, stopped or replaced while it staged (a dbt --full-refresh does that to running views), it fails
        // on its own missing tables, and the caller lets it go without marking it failed. Any other failure fails
        // the worker.
        let mut staged: Vec<(String, Staged)> = Vec::new();
        let mut stopped = Vec::new();
        for (id, r) in jobs.keys().zip(results) {
            match r {
                Ok(s) if jobs[id].def.limits.max_late_rows.is_some_and(|m| s.rows_late as u64 > m) => stopped.push((
                    id.clone(),
                    format!(
                        "an epoch has {} late rows, over its max_late_rows of {}: check the source's event times and \
                         watermark_delay, or raise the limit (alter-view --max-late-rows), then start it again",
                        s.rows_late,
                        jobs[id].def.limits.max_late_rows.unwrap_or_default()
                    ),
                )),
                Ok(s) => staged.push((id.clone(), s)),
                Err(e) if e.downcast_ref::<StopJob>().is_some() => stopped.push((id.clone(), format!("{e:#}"))),
                Err(e) if crate::lake::is_catalog_unavailable(&e) => return Err(e),
                Err(e) if !catalog::holds_lease(&conns[0], id, &cfg.worker_id, jobs[id].lease)? => {
                    stopped.push((id.clone(), format!("{e:#}")))
                }
                Err(e) => return Err(e),
            }
        }
        // Nothing new and no window can close: skip the commit instead of writing an empty snapshot. A view
        // with lake inputs sees its watermark move with every snapshot, its own commits' included, so for it
        // only rows or a closing window count, and, for a windowed one, its watermark moving a whole slide past
        // the one it last recorded: views windowing on its window_start or window_end read that recorded
        // watermark, and would wait for the next `position_refresh` once its inputs go idle (it then has no window
        // to close).
        // The end of a backfill is committed even without rows, and so is a lake position left behind for
        // `position_refresh`: maintenance keeps every snapshot from the oldest position on.
        let slide_passed = |j: &JobRuntime, s: &Staged| match (&j.def.plan.window, j.watermark_us, s.watermark_us) {
            (Some(w), Some(old), Some(new)) => new - old >= w.slide_us,
            (Some(_), None, Some(_)) => true,
            _ => false,
        };
        let worth = |j: &JobRuntime, s: &Staged| {
            s.rows_in > 0
                || (s.watermark_us != j.watermark_us
                    && (j.def.lake_inputs.is_empty() || (j.def.plan.is_windowed() && (s.closes || slide_passed(j, s)))))
                || s.inputs.iter().any(|i| i.backfill_done)
                || (j.last_commit.unwrap_or(j.started).elapsed() >= cfg.position_refresh
                    && j.inputs.iter().zip(&s.inputs).any(|(i, si)| i.group().is_none() && si.offsets != i.offsets))
        };
        // (Nothing worth it means no remaining job's group read events, so none are lost by skipping.)
        if !staged.iter().any(|(id, s)| worth(&jobs[id], s)) {
            return Ok((None, stopped));
        }

        // 4. This is every job's next epoch; the one after is staged from here.
        *rounds = round;
        for (gid, s) in &sources {
            if let Some(g) = groups.get_mut(gid) {
                g.offsets = s.offsets.clone();
            }
        }
        let mut batch = Vec::new();
        for (id, s) in staged {
            let j = jobs.get_mut(&id).unwrap();
            j.epoch += 1;
            for (input, si) in j.inputs.iter_mut().zip(&s.inputs) {
                input.offsets = si.offsets.clone();
                input.watermark_us = si.watermark_us;
                if let Feed::Lake { commit_us, .. } = &mut input.feed {
                    *commit_us = si.commit_us.or(*commit_us);
                }
            }
            j.watermark_us = s.watermark_us;
            j.max_event_time_us = s.max_event_time_us;
            j.last_round = Some(round);
            j.open_ends = s.open_ends.clone();
            batch.push(CommitJob {
                id: id.clone(),
                lease: j.lease,
                epoch: j.epoch,
                def: j.def.clone(),
                circuit: j.circuit.clone(),
                parity,
                inputs: j
                    .inputs
                    .iter()
                    .map(|i| CommitInput { name: i.name.clone(), columns: i.columns.clone(), dead: i.group().map(|g| sources[&g].dead.clone()) })
                    .collect(),
                // Kafka messages behind, plus snapshots behind for lake inputs.
                lag: j
                    .inputs
                    .iter()
                    .zip(&s.inputs)
                    .map(|(i, si)| match i.group() {
                        Some(g) => groups[&g].reader.lag(&si.offsets),
                        None => si.snapshots_behind,
                    })
                    .sum(),
                staged: s,
            });
        }
        let mut drop_after = std::mem::take(drops);
        if cfg.shared_readers {
            merge_groups(groups, jobs, &mut drop_after);
        }
        Ok((Some(CommitBatch { round, lean: cfg.lean_commit, jobs: batch, poll_stage_ms: t0.elapsed().as_millis(), drop_after }), stopped))
    }
}

/// True when position `a` is at or before position `b` on every partition (and both cover the same ones).
fn behind(a: &BTreeMap<i32, i64>, b: &BTreeMap<i32, i64>) -> bool {
    a.len() == b.len() && a.iter().all(|(p, o)| b.get(p).is_some_and(|x| o <= x))
}

/// Merges groups of one source that have reached the same position: their jobs share one reader from now on.
/// The merged-away group's tables are dropped once this epoch, which still reads them, has committed.
fn merge_groups(groups: &mut BTreeMap<u64, Group>, jobs: &mut BTreeMap<String, JobRuntime>, drops: &mut Vec<String>) {
    let ids: Vec<u64> = groups.keys().copied().collect();
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            let same = match (groups.get(a), groups.get(b)) {
                (Some(x), Some(y)) => x.source == y.source && x.offsets == y.offsets,
                _ => false,
            };
            if !same {
                continue;
            }
            let gone = groups.remove(b).unwrap();
            for m in &gone.members {
                for input in jobs.get_mut(m).into_iter().flat_map(|j| j.inputs.iter_mut()) {
                    if let Feed::Kafka { group, .. } = &mut input.feed {
                        if *group == *b {
                            *group = *a;
                        }
                    }
                }
            }
            let g = groups.get_mut(a).unwrap();
            g.members.extend(gone.members);
            g.max_rows = g.max_rows.min(gone.max_rows);
            info!(source = %g.source.name, group = a, merged = b, jobs = g.members.len(), "views caught up and now share one reader");
            drops.extend(group_tables(*b));
        }
    }
}

/// Runs `f` over `items` on up to one thread per connection, each thread with its own connection; with one
/// connection or one item, inline on the caller's thread. Results come back in item order.
fn parallel<T: Send, R: Send>(conns: &mut [Connection], items: Vec<T>, f: impl Fn(&Connection, T) -> R + Sync) -> Vec<R> {
    let n = items.len();
    if conns.len() < 2 || n < 2 {
        return items.into_iter().map(|t| f(&conns[0], t)).collect();
    }
    let queue = Mutex::new(items.into_iter().enumerate());
    let results: Mutex<Vec<Option<R>>> = Mutex::new((0..n).map(|_| None).collect());
    std::thread::scope(|s| {
        for conn in conns.iter_mut().take(n) {
            let (queue, results, f) = (&queue, &results, &f);
            s.spawn(move || loop {
                let next = queue.lock().unwrap().next();
                let Some((i, t)) = next else { break };
                let r = f(&*conn, t);
                results.lock().unwrap()[i] = Some(r);
            });
        }
    });
    results.into_inner().unwrap().into_iter().map(|r| r.expect("every item ran")).collect()
}

fn mem(name: &str, key: &str) -> String {
    format!("memory.main.{}", ident(&format!("{name}_{key}")))
}

/// A group's staging tables.
fn group_tables(gid: u64) -> Vec<String> {
    let g = format!("g{gid}");
    vec![mem("raw", &g), mem("ok", &g), mem("typed", &g), mem("dead", &format!("{g}_0")), mem("dead", &format!("{g}_1"))]
}

/// Stages a group's chunks once for all its members: typed rows (appended as Arrow decoded them, the rest
/// decoded in SQL) and dead letters. `offsets` is the group's position after these chunks. Nothing here
/// touches the lake.
fn stage_source(
    conn: &Connection,
    gid: u64,
    source: &SourceDef,
    offsets: BTreeMap<i32, i64>,
    chunks: &[Chunk],
    parity: u64,
) -> Result<SourceStaged> {
    let t = Instant::now();
    let g = format!("g{gid}");
    let (raw, ok, typed, dead) = (mem("raw", &g), mem("ok", &g), mem("typed", &g), mem("dead", &format!("{g}_{parity}")));

    // Rows decoded in Arrow are appended typed; the rest are staged raw for the SQL decode below.
    let typed_cols: Vec<String> = source.options.columns.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {typed} (_partition INTEGER, _offset BIGINT, payload VARCHAR, _key VARCHAR, {}, _err VARCHAR);
         CREATE OR REPLACE TABLE {raw} (_partition INTEGER, _offset BIGINT, payload VARCHAR, _key VARCHAR);",
        typed_cols.join(", ")
    ))?;
    if chunks.iter().any(|c| c.fast.as_ref().is_some_and(|b| b.num_rows() > 0)) {
        let mut app = conn.appender_to_catalog_and_db(&format!("typed_{g}"), "memory", "main")?;
        for b in chunks.iter().filter_map(|c| c.fast.as_ref()).filter(|b| b.num_rows() > 0) {
            app.append_record_batch(crate::sources::kafka::decode::staging_batch(b)?)?;
        }
        app.flush()?;
    }
    {
        let mut app = conn.appender_to_catalog_and_db(&format!("raw_{g}"), "memory", "main")?;
        for m in chunks.iter().flat_map(|c| &c.slow) {
            app.append_row(duckdb::params![m.partition, m.offset, m.payload_text().as_deref(), m.key_text()])?;
        }
        app.flush()?;
    }

    // JSON parse errors must be split off before extracting fields, which would raise on malformed input.
    // Tombstones are typed rows with `_err` = 'tombstone': upsert views take them as deletes, the others as
    // dead letters (`dead` holds them all; a job's commit leaves out the ones it reads).
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {ok} AS SELECT * FROM {raw}
           WHERE {IS_JSON_OBJECT};
         INSERT INTO {typed} BY NAME {decode};
         INSERT INTO {typed} BY NAME {tombstones};
         CREATE OR REPLACE TABLE {dead} AS
           SELECT _partition, _offset, payload, 'invalid json' AS _err FROM {raw}
            WHERE {invalid}
           UNION ALL SELECT _partition, _offset, payload, _err FROM {typed} WHERE _err IS NOT NULL;",
        decode = decode_sql(source, &ok),
        tombstones = tombstones_sql(source, &raw),
        invalid = invalid_json_where(),
    ))?;
    let (rows_dead, rows_tombstones): (i64, i64) = conn.query_row(
        &format!("SELECT count(*), count(*) FILTER (WHERE _err = {}) FROM {dead}", lit(crate::sources::kafka::decode::TOMBSTONE)),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;

    let unavailable = if source.options.is_debezium() {
        let (n, p, o): (i64, Option<i32>, Option<i64>) = conn.query_row(
            &format!(
                "SELECT count(*), arg_min(_partition, (_partition, _offset)), arg_min(_offset, (_partition, _offset))
                   FROM {dead} WHERE _err = {}",
                lit(crate::sources::kafka::decode::UNAVAILABLE_ERR)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        (n > 0).then(|| (n, p.unwrap_or(0), o.unwrap_or(0)))
    } else {
        None
    };
    // Without an event time (a Debezium source may have none) no partition has a newest event time, and the
    // source has no watermark: its views have no windows to close.
    let part_max = match &source.options.event_time {
        Some(et) => conn
            .prepare(&format!(
                "SELECT _partition, epoch_us(max({})) FROM {typed} WHERE _err IS NULL GROUP BY ALL",
                ident(et)
            ))?
            .query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i64>(1)?)))?
            .collect::<duckdb::Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    let active: BTreeSet<i32> = chunks.iter().map(|c| c.partition).collect();
    Ok(SourceStaged {
        typed,
        dead,
        offsets,
        active: active.into_iter().collect(),
        part_max,
        rows_in: chunks.iter().map(|c| c.len).sum(),
        rows_sql: chunks.iter().map(|c| c.slow.len()).sum(),
        bisections: chunks.iter().map(|c| c.bisections).sum(),
        rows_dead,
        rows_tombstones,
        unavailable,
        stage_ms: t.elapsed().as_millis(),
    })
}

/// Stages the changes of a job's `k`-th input, a lake input, in snapshots `(from, to]`: rows in the layout of a
/// Kafka group's typed table (`_partition` the snapshot, `_offset` the rowid), with `_commit_ts` the running
/// maximum of `snapshot_time` from `carried` on, and the weight `__w` of each change. Reads the lake and writes
/// only the memory catalog. Returns the staged input and `_commit_ts` at `to`.
///
/// The view holds every row of the input (a backfilled view), or the rows written after its floor, the
/// snapshot a `start := 'latest'` view was created at. An input that retracts stages a delete or an
/// update's old version with the `_commit_ts` of the version it removes, so it cancels the row the view took
/// in, and drops it when that version is from the floor or before: the view never had it. An insert-only
/// input must stay so: an update or delete stops the view.
#[allow(clippy::too_many_arguments)]
fn stage_lake(
    conn: &Connection,
    job: &str,
    lease: i64,
    k: usize,
    lake: &LakeInput,
    def: &JobDef,
    from: i64,
    to: i64,
    carried: Option<i64>,
) -> Result<(SourceStaged, Option<i64>)> {
    let t = Instant::now();
    let typed = lake_typed(job, lease, k);
    let cols: Vec<&(String, String)> = lake.columns.iter().filter(|(c, _)| c != COMMIT_TS).collect();
    let defs: Vec<String> = lake.columns.iter().map(|(c, ty)| format!("{} {ty}", ident(c))).collect();
    let staged = |rows_in: usize| SourceStaged {
        typed: typed.clone(),
        dead: String::new(),
        offsets: BTreeMap::from([(LAKE_POSITION, to.max(from))]),
        active: vec![],
        part_max: vec![],
        rows_in,
        rows_sql: 0,
        bisections: 0,
        rows_dead: 0,
        rows_tombstones: 0,
        unavailable: None,
        stage_ms: t.elapsed().as_millis(),
    };
    if to <= from {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {typed} (_partition INTEGER, _offset BIGINT, payload VARCHAR, _change VARCHAR, {}, _err VARCHAR, __w INTEGER)",
            defs.join(", ")
        ))?;
        return Ok((staged(0), carried));
    }
    // By id, so a renamed table keeps working; a dropped one (even if recreated under its name) stops the view.
    let (schema, table) = crate::lake::table_by_id(conn, lake.table_id)?
        .ok_or_else(|| StopJob(format!("{} (DuckLake table id {}) was dropped", lake.table, lake.table_id)))?;
    let carried_sql = carried.map(|us| format!("make_timestamp({us})")).unwrap_or("NULL".into());
    let select: Vec<String> = cols.iter().map(|(c, ty)| format!("CAST(c.{} AS {ty}) AS {}", ident(c), ident(c))).collect();
    // A counted view's table holds each row once with its multiplicity.
    let rows = if lake.counted { format!("c.{}", ident(crate::circuit::ops::COUNT_COLUMN)) } else { "1".into() };
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {typed} AS
           WITH snaps AS (
             SELECT snapshot_id, max(timezone('UTC', snapshot_time)) OVER (ORDER BY snapshot_id) AS ts
             FROM ducklake_snapshots({lake_lit}) WHERE snapshot_id > {from} AND snapshot_id <= {to})
           SELECT c.snapshot_id::INTEGER AS _partition, c.rowid::BIGINT AS _offset, NULL::VARCHAR AS payload,
                  c.change_type AS _change, {select}, greatest(s.ts, coalesce({carried_sql}, s.ts)) AS {commit_ts},
                  NULL::VARCHAR AS _err,
                  (CASE WHEN c.change_type IN ('insert', 'update_postimage') THEN 1 ELSE -1 END * {rows})::INTEGER AS __w
           FROM ducklake_table_changes({lake_lit}, {schema_lit}, {table_lit}, {next}, {to}) c JOIN snaps s USING (snapshot_id)",
        lake_lit = lit(LAKE),
        schema_lit = lit(&schema),
        table_lit = lit(&table),
        next = from + 1,
        select = select.join(", "),
        commit_ts = ident(COMMIT_TS),
    ))
    .with_context(|| format!("reading the changes of {} in snapshots ({from}, {to}]", lake.table))?;
    let (retracted, at): (i64, Option<i64>) = conn.query_row(
        &format!("SELECT count(*), min(_partition) FROM {typed} WHERE _change NOT IN ('insert', 'update_postimage')"),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if retracted > 0 && !lake.retracts {
        return Err(StopJob(format!(
            "view {job} needs {} to be insert-only, but snapshot {} updated or deleted rows of it ({retracted} {}): \
             a windowed view reads lake tables insert-only, since a window is final once the watermark passes it",
            lake.name,
            at.unwrap_or(0),
            if retracted == 1 { "change" } else { "changes" }
        ))
        .into());
    }
    if retracted > 0 {
        retraction_versions(conn, job, &typed, lake, &schema, &table, def, from, to)?;
    }
    let rows_in: i64 = conn.query_row(&format!("SELECT count(*) FROM {typed}"), [], |r| r.get(0))?;
    let newest: Option<i64> = conn.query_row(
        &format!(
            "SELECT epoch_us(max(timezone('UTC', snapshot_time))) FROM ducklake_snapshots({}) WHERE snapshot_id > {from} AND snapshot_id <= {to}",
            lit(LAKE)
        ),
        [],
        |r| r.get(0),
    )?;
    let commit_us = match (carried, newest) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    Ok((staged(rows_in as usize), commit_us))
}

/// Gives each retraction staged in `typed` (a delete, or an update's old version) the version it removes:
/// the newest earlier write of its row in the range, or else its version at `from`, the snapshot read up to
/// (row ids stay with a row through updates and are never reused). A retraction of a version written at or
/// before the view's floor is dropped; any other gets that version's `_commit_ts` (`row_commit_ts`).
#[allow(clippy::too_many_arguments)]
fn retraction_versions(
    conn: &Connection,
    job: &str,
    typed: &str,
    lake: &LakeInput,
    schema: &str,
    table: &str,
    def: &JobDef,
    from: i64,
    to: i64,
) -> Result<()> {
    let versions = mem("typed", &format!("v{job}"));
    let tbl = quote_table(&format!("{LAKE}.{schema}.{table}"))?;
    // Within a snapshot an update's old version comes before its new one.
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {versions} AS
           SELECT _partition, _offset, _change, written FROM (
             SELECT _partition, _offset, _change,
                    max(CASE WHEN _change IN ('insert', 'update_postimage') THEN _partition END) OVER (
                      PARTITION BY _offset ORDER BY _partition, _change IN ('insert', 'update_postimage')
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS written
             FROM {typed})
           WHERE _change NOT IN ('insert', 'update_postimage')"
    ))?;
    let (lo, hi): (Option<i64>, Option<i64>) =
        conn.query_row(&format!("SELECT min(_offset), max(_offset) FROM {versions} WHERE written IS NULL"), [], |r| Ok((r.get(0)?, r.get(1)?)))?;
    if let (Some(lo), Some(hi)) = (lo, hi) {
        conn.execute_batch(&format!(
            "UPDATE {versions} v SET written = p.snapshot_id
             FROM (SELECT rowid, snapshot_id FROM {tbl} AT (VERSION => {from})
                   WHERE rowid BETWEEN {lo} AND {hi} AND rowid IN (SELECT _offset FROM {versions} WHERE written IS NULL)) p
             WHERE v._offset = p.rowid AND v.written IS NULL"
        ))
        .with_context(|| format!("reading the versions {} had at snapshot {from}", lake.table))?;
    }
    let lost: i64 = conn.query_row(&format!("SELECT count(*) FROM {versions} WHERE written IS NULL"), [], |r| r.get(0))?;
    if lost > 0 {
        return Err(StopJob(format!(
            "view {job}: {lost} rows {} deleted or updated in snapshots ({from}, {to}] are neither in that range nor at snapshot {from}",
            lake.table
        ))
        .into());
    }
    let key = "t._partition = v._partition AND t._offset = v._offset AND t._change = v._change";
    if let Some(floor) = def.floor() {
        conn.execute_batch(&format!("DELETE FROM {typed} t USING {versions} v WHERE {key} AND v.written <= {floor}"))?;
        conn.execute_batch(&format!("DELETE FROM {versions} WHERE written <= {floor}"))?;
    }
    conn.execute_batch(&format!(
        "UPDATE {typed} t SET {ct} = v.ts
         FROM (SELECT v.*, {ts} AS ts FROM {versions} v LEFT JOIN ({times}) s ON s.snapshot_id = v.written) v
         WHERE {key};
         DROP TABLE {versions};",
        ct = ident(COMMIT_TS),
        ts = row_commit_ts(def),
        times = crate::lake::commit_times_sql(to, &format!("SELECT written FROM {versions}")),
    ))?;
    Ok(())
}

/// The `_commit_ts` of a row version, given its writing snapshot's time as `s.ts` (NULL when unknown): that
/// time, or `_commit_ts` at the view's start snapshot for a version whose snapshot expired before DuckWake's
/// maintenance recorded its time.
pub(crate) fn row_commit_ts(def: &JobDef) -> String {
    match def.start_commit_us {
        Some(us) => format!("coalesce(s.ts, make_timestamp({us}))"),
        None => "s.ts".into(),
    }
}

/// Stages one page of the backfill of a job's `k`-th input, a lake input: up to `max_rows` of the rows the
/// table held at the start snapshot `s0`, in row-id order after `cursor`, as inserts in the layout
/// `stage_lake` gives changes. The staged offsets keep the cursor at the last row read, or drop it when this
/// was the last page: the input then follows the changes after `s0`. A crash replays the page from the
/// committed cursor, so no row is read twice or skipped.
#[allow(clippy::too_many_arguments)]
fn stage_backfill(
    conn: &Connection,
    job: &str,
    lease: i64,
    k: usize,
    lake: &LakeInput,
    def: &JobDef,
    s0: i64,
    cursor: i64,
) -> Result<SourceStaged> {
    let t = Instant::now();
    let typed = lake_typed(job, lease, k);
    let (schema, table) = crate::lake::table_by_id(conn, lake.table_id)?
        .ok_or_else(|| StopJob(format!("{} (DuckLake table id {}) was dropped", lake.table, lake.table_id)))?;
    let tbl = quote_table(&format!("{LAKE}.{schema}.{table}"))?;
    let select: Vec<String> =
        lake.columns.iter().filter(|(c, _)| c != COMMIT_TS).map(|(c, ty)| format!("CAST(p.{} AS {ty}) AS {}", ident(c), ident(c))).collect();
    let rows = if lake.counted { format!("p.{}", ident(crate::circuit::ops::COUNT_COLUMN)) } else { "1".into() };
    let floor = def.floor().map(|f| format!(" AND t.snapshot_id > {f}")).unwrap_or_default();
    let page = mem("typed", &format!("p{job}_{k}"));
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {page} AS
           SELECT t.rowid AS __rid, t.snapshot_id AS __sid, t.* FROM {tbl} AS t AT (VERSION => {s0})
           WHERE t.rowid > {cursor}{floor} ORDER BY t.rowid LIMIT {limit};
         CREATE OR REPLACE TABLE {typed} AS
           SELECT p.__sid::INTEGER AS _partition, p.__rid::BIGINT AS _offset, NULL::VARCHAR AS payload,
                  'insert' AS _change, {select}, {ts} AS {commit_ts}, NULL::VARCHAR AS _err, ({rows})::INTEGER AS __w
           FROM {page} p LEFT JOIN ({times}) s ON s.snapshot_id = p.__sid;
         DROP TABLE {page};",
        limit = def.max_rows,
        select = select.join(", "),
        ts = row_commit_ts(def),
        commit_ts = ident(COMMIT_TS),
        times = crate::lake::commit_times_sql(s0, &format!("SELECT __sid FROM {page}")),
    ))
    .with_context(|| format!("backfilling {} at snapshot {s0}", lake.table))?;
    let (rows_in, last): (i64, Option<i64>) =
        conn.query_row(&format!("SELECT count(*), max(_offset) FROM {typed}"), [], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let mut offsets = BTreeMap::from([(LAKE_POSITION, s0)]);
    // A full page: there may be more. A short one is the last.
    if rows_in as usize >= def.max_rows {
        offsets.insert(LAKE_BACKFILL, last.unwrap_or(cursor));
    }
    Ok(SourceStaged {
        typed,
        dead: String::new(),
        offsets,
        active: vec![],
        part_max: vec![],
        rows_in: rows_in as usize,
        rows_sql: 0,
        bisections: 0,
        rows_dead: 0,
        rows_tombstones: 0,
        unavailable: None,
        stage_ms: t.elapsed().as_millis(),
    })
}

/// The watermark of view `job` as of snapshot `at`: the one its newest checkpoint up to then recorded.
fn watermark_at(conn: &Connection, job: &str, at: i64) -> Result<Option<i64>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT epoch_us(watermark) FROM {CTL}.checkpoints AT (VERSION => {at}) WHERE job_id = ? ORDER BY epoch DESC LIMIT 1"
    ))?;
    let mut rows = stmt.query([job])?;
    Ok(rows.next()?.map(|r| r.get::<_, Option<i64>>(0)).transpose()?.flatten())
}

/// Stages one job's view from its group's typed rows: the watermark, windows and late rows, then the epoch's
/// sink rows (stateless) or partial aggregates. `overlay` is the table set of the previous epoch when that
/// epoch may still be committing: a global aggregate reads the state of the groups it touched from there.
fn stage_view(conn: &Connection, id: &str, j: &mut JobRuntime, kafka: &[Option<&SourceStaged>], parity: u64, overlay: Option<u64>) -> Result<Staged> {
    let t = Instant::now();
    let def = j.def.clone();
    let plan = &def.plan;

    // A Debezium event with a value Debezium left out cannot be applied: the view stops rather than keep a row
    // that is not the table's.
    for (input, src) in j.inputs.iter().zip(kafka) {
        if let Some((n, p, o)) = src.and_then(|s| s.unavailable) {
            return Err(StopJob(format!(
                "source {}: the Debezium event at partition {p}, offset {o} ({n} in this epoch) has a column Debezium \
                 left out ({}: an unchanged TOASTed value of a PostgreSQL table without REPLICA IDENTITY FULL), so \
                 its row cannot be rebuilt. Set REPLICA IDENTITY FULL on the table, take a new Debezium snapshot of \
                 it, and replace the view",
                input.name,
                crate::sources::kafka::decode::UNAVAILABLE
            ))
            .into());
        }
    }

    // Lake inputs, read to one snapshot S for all of them: a transaction that changed two of their tables
    // upstream is seen whole or not at all.
    // While any of them backfills, each reads a page of its rows at the start snapshot (or nothing, once its
    // own backfill is done) and no changes: they all stay at the start snapshot until every backfill is done.
    let mut lakes: Vec<Option<(SourceStaged, Option<i64>, i64)>> = (0..j.inputs.len()).map(|_| None).collect();
    let mut backfilling = vec![false; j.inputs.len()];
    if j.inputs.iter().any(|i| i.group().is_none()) {
        let current = crate::lake::current_snapshot(conn)?;
        let from = j.inputs.iter().filter(|i| i.group().is_none()).map(|i| i.offsets[&LAKE_POSITION]).min().unwrap();
        let any_backfill = j.inputs.iter().any(|i| i.offsets.contains_key(&LAKE_BACKFILL));
        let to = if any_backfill { from } else { current.min(from + MAX_SNAPSHOTS).max(from) };
        for (k, input) in j.inputs.iter().enumerate() {
            if let Feed::Lake { lake, commit_us } = &input.feed {
                let (s, commit) = match input.offsets.get(&LAKE_BACKFILL) {
                    Some(&cursor) => {
                        let s = stage_backfill(conn, id, j.lease, k, lake, &def, input.offsets[&LAKE_POSITION], cursor)?;
                        backfilling[k] = s.offsets.contains_key(&LAKE_BACKFILL);
                        (s, *commit_us)
                    }
                    None => stage_lake(conn, id, j.lease, k, lake, &def, input.offsets[&LAKE_POSITION], to, *commit_us)?,
                };
                lakes[k] = Some((s, commit, current - to));
            }
        }
    }
    let srcs: Vec<&SourceStaged> = kafka.iter().zip(&lakes).map(|(k, l)| k.or(l.as_ref().map(|(s, _, _)| s)).unwrap()).collect();

    // Each source's watermark, and the view's: the oldest of them once every source has one, never moving back.
    // A lake input's is exact: `_commit_ts` at S for windows on it (and for views without windows), or for
    // windows on a windowed view's window columns that view's watermark at S, which every row it writes later
    // has its `window_end` above.
    let window_col = plan.window.as_ref().map(|w| (plan.source.clone(), w.time_col.clone()));
    let mut watermarks: Vec<Option<i64>> = Vec::new();
    for (k, (input, (src, lake))) in j.inputs.iter_mut().zip(srcs.iter().zip(&lakes)).enumerate() {
        let Some((_, commit_us, _)) = lake else {
            watermarks.push(input.advance(src));
            continue;
        };
        // A backfill reads rows in row-id order, not commit order: the watermark waits for its last page,
        // then takes the start snapshot's.
        if backfilling[k] {
            watermarks.push(input.watermark_us);
            continue;
        }
        let Feed::Lake { lake: l, .. } = &input.feed else { unreachable!() };
        let candidate = match window_col.as_ref().filter(|(s, _)| *s == input.name).map(|(_, c)| c.as_str()) {
            Some(c @ ("window_start" | "window_end")) => {
                let up = l.view.as_deref().context("window columns of a lake input without a view")?;
                let w = watermark_at(conn, up, src.offsets[&LAKE_POSITION])?;
                if c == "window_end" { w } else { w.map(|w| w - l.window_size_us.unwrap_or(0)) }
            }
            _ => *commit_us,
        };
        if let Some(c) = commit_us {
            input.part_max_ts.insert(0, *c);
        }
        let w = match (input.watermark_us, candidate) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        watermarks.push(w);
    }
    let candidate = watermarks.iter().copied().collect::<Option<Vec<i64>>>().and_then(|w| w.into_iter().min());
    let watermark_us = match (j.watermark_us, candidate) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    // Monotone across restarts (part_max_ts starts empty), so it bounds every event time read so far.
    let max_event_time_us = j.inputs.iter().flat_map(|i| i.part_max_ts.values().copied()).chain(j.max_event_time_us).max();

    let x = EpochCtx {
        job: id,
        plan,
        sink: quote_table(&def.sink)?,
        late_table: def.late_data.as_deref().map(quote_table).transpose()?,
        sources: j
            .inputs
            .iter()
            .zip(&srcs)
            .map(|(i, src)| SourceInput { name: &i.name, columns: &i.columns, typed: src.typed.clone() })
            .collect(),
        parity,
        overlay,
        // Late = assigned to a window that an earlier epoch already closed (window_end <= previous watermark).
        prev_watermark: j.watermark_us.map(|w| format!("make_timestamp({w})")).unwrap_or("NULL".into()),
        watermark: watermark_us.map(|w| format!("make_timestamp({w})")),
        epoch: (j.epoch + 1).to_string(),
    };
    let (mut rows_late, mut rows_staged, mut rows_changed, mut closes, mut open_ends) = (0, 0, 0, true, None);
    for step in j.circuit.stage_steps(&x)? {
        conn.execute_batch(&step.sql)?;
        if let Some((counter, table)) = &step.count {
            let n: i64 = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
            *match counter {
                Counter::Late => &mut rows_late,
                Counter::Staged => &mut rows_staged,
                Counter::Changed => &mut rows_changed,
            } = n;
        }
    }
    if let (Some(prev_ends), true) = (&j.open_ends, j.circuit.emits_final()) {
        // The state's window ends after this epoch's MERGE; the emit closes those at or below the watermark.
        let mut ends = prev_ends.clone();
        let mut stmt = conn.prepare(&format!("SELECT DISTINCT epoch_us(__we) FROM {}", mem("out", &format!("{id}_{parity}"))))?;
        for e in stmt.query_map([], |r| r.get::<_, i64>(0))? {
            ends.insert(e?);
        }
        closes = watermark_us.is_some_and(|wm| ends.first().is_some_and(|e| *e <= wm));
        if let Some(wm) = watermark_us {
            ends.retain(|e| *e > wm);
        }
        open_ends = Some(ends);
    }
    let inputs: Vec<StagedInput> = srcs
        .iter()
        .zip(watermarks)
        .zip(&lakes)
        .zip(&j.inputs)
        .map(|(((src, w), l), input)| StagedInput {
            offsets: src.offsets.clone(),
            watermark_us: w,
            // An upsert view reads the source's tombstones as deletes: they are not its dead letters.
            rows_dead: src.rows_dead - if j.def.upsert.is_some() { src.rows_tombstones } else { 0 },
            commit_us: l.as_ref().and_then(|(_, c, _)| *c),
            snapshots_behind: l.as_ref().map_or(0, |(_, _, b)| *b),
            backfill_done: input.offsets.contains_key(&LAKE_BACKFILL) && !src.offsets.contains_key(&LAKE_BACKFILL),
        })
        .collect();
    let rows_dead = inputs.iter().map(|i| i.rows_dead).sum();
    Ok(Staged {
        inputs,
        watermark_us,
        max_event_time_us,
        rows_in: srcs.iter().map(|s| s.rows_in).sum(),
        rows_sql: srcs.iter().map(|s| s.rows_sql).sum(),
        bisections: srcs.iter().map(|s| s.bisections).sum(),
        rows_dead,
        rows_late,
        rows_out: 0,
        rows_staged,
        rows_changed,
        closes,
        open_ends,
        stage_ms: t.elapsed().as_millis(),
        source_ms: srcs.iter().map(|s| s.stage_ms).max().unwrap_or(0),
    })
}

/// Runs the epoch commits: inline on the scheduler's connection, or with `pipeline` on a thread and
/// connection of its own, which also runs maintenance between commits. A worker has one, so the size of the
/// inline variant does not matter.
#[allow(clippy::large_enum_variant)]
enum Committer {
    Inline {
        maint: Maintenance,
        done: Option<Committed>,
    },
    Thread {
        tx: Option<mpsc::Sender<CommitBatch>>,
        rx: mpsc::Receiver<Result<Committed>>,
        /// Jobs of the epoch handed over and not yet taken back with `finish`.
        inflight: Option<BTreeSet<String>>,
        handle: Option<std::thread::JoinHandle<()>>,
    },
}

impl Committer {
    fn spawn(conn: Connection, worker: String, maintenance: MaintenanceConfig) -> Result<Self> {
        let (tx, batches) = mpsc::channel();
        let (results, rx) = mpsc::channel();
        let handle = std::thread::Builder::new()
            .name("commit".into())
            .spawn(move || commit_loop(conn, worker, maintenance, batches, results))?;
        Ok(Committer::Thread { tx: Some(tx), rx, inflight: None, handle: Some(handle) })
    }

    fn submit(&mut self, conn: &Connection, worker: &str, batch: CommitBatch) -> Result<()> {
        match self {
            Committer::Inline { done, .. } => {
                *done = Some(commit_epoch(conn, worker, batch)?);
                Ok(())
            }
            Committer::Thread { tx, inflight, .. } => {
                *inflight = Some(batch.jobs.iter().map(|c| c.id.clone()).collect());
                tx.as_ref().unwrap().send(batch).map_err(|_| anyhow!("the commit thread has stopped"))
            }
        }
    }

    /// The epoch handed over last, once it has committed; waits for it while it is in flight.
    fn finish(&mut self) -> Result<Option<Committed>> {
        match self {
            Committer::Inline { done, .. } => Ok(done.take()),
            Committer::Thread { rx, inflight, .. } => {
                if inflight.take().is_none() {
                    return Ok(None);
                }
                rx.recv().map_err(|_| anyhow!("the commit thread has stopped"))?.map(Some)
            }
        }
    }

    /// `finish` without waiting.
    fn try_finish(&mut self) -> Result<Option<Committed>> {
        match self {
            Committer::Inline { done, .. } => Ok(done.take()),
            Committer::Thread { rx, inflight, .. } => {
                if inflight.is_none() {
                    return Ok(None);
                }
                match rx.try_recv() {
                    Ok(r) => {
                        *inflight = None;
                        r.map(Some)
                    }
                    Err(mpsc::TryRecvError::Empty) => Ok(None),
                    Err(mpsc::TryRecvError::Disconnected) => Err(anyhow!("the commit thread has stopped")),
                }
            }
        }
    }

    fn in_flight(&self) -> bool {
        matches!(self, Committer::Thread { inflight: Some(_), .. })
    }

    fn includes(&self, job: &str) -> bool {
        matches!(self, Committer::Thread { inflight: Some(ids), .. } if ids.contains(job))
    }
}

impl Drop for Committer {
    /// Lets a commit in flight finish before the worker goes, so a restarted worker reads its checkpoint.
    fn drop(&mut self) {
        if let Committer::Thread { tx, handle, .. } = self {
            tx.take();
            if let Some(h) = handle.take() {
                let _ = h.join();
            }
        }
    }
}

/// The commit thread: commits epochs in the order they arrive and runs maintenance between them.
fn commit_loop(
    conn: Connection,
    worker: String,
    maintenance: MaintenanceConfig,
    batches: mpsc::Receiver<CommitBatch>,
    results: mpsc::Sender<Result<Committed>>,
) {
    let mut maint = Maintenance::new(maintenance);
    let mut jobs: Vec<String> = Vec::new();
    loop {
        match batches.recv_timeout(Duration::from_millis(500)) {
            Ok(batch) => {
                jobs = batch.jobs.iter().map(|c| c.id.clone()).collect();
                let res = commit_epoch(&conn, &worker, batch);
                let failed = res.is_err();
                if results.send(res).is_err() || failed {
                    return;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
        // Maintenance runs between epochs, never inside one.
        crash_point("maintenance");
        maint.run_due(&conn, &jobs);
    }
}

/// Commits one epoch, retrying DuckLake write conflicts, then drops staging tables nothing reads any more.
fn commit_epoch(conn: &Connection, worker: &str, mut batch: CommitBatch) -> Result<Committed> {
    let t = Instant::now();
    let mut attempt = 0;
    let mut dropped: Vec<String> = Vec::new();
    loop {
        attempt += 1;
        let (mut fenced, mut timing) = (Vec::new(), CommitTiming::default());
        let res = if batch.jobs.is_empty() { Ok(()) } else { commit(conn, worker, &mut batch, &mut fenced, &mut timing) };
        fenced.extend(dropped.iter().cloned());
        match res {
            Err(e) if e.downcast_ref::<LeaseGone>().is_some() => {
                let id = e.downcast_ref::<LeaseGone>().unwrap().0.clone();
                warn!(job = %id, "view dropped during its epoch; committing the others without it");
                batch.jobs.retain(|c| c.id != id);
                dropped.push(id);
                attempt -= 1;
            }
            Ok(()) => {
                let visible_at = chrono::Utc::now();
                crash_point("after_commit");
                let commit_ms = t.elapsed().as_millis();
                info!(
                    target: "commit",
                    round = batch.round,
                    jobs = batch.jobs.len(),
                    commit_ms,
                    writes_ms = timing.writes,
                    commit_call_ms = timing.commit,
                    lease_ms = timing.lease,
                    state_ms = timing.state,
                    sink_ms = timing.sink,
                    late_ms = timing.late,
                    dead_ms = timing.dead,
                    checkpoint_ms = timing.checkpoint,
                    attempts = attempt,
                    "epoch commit timing"
                );
                for table in &batch.drop_after {
                    if let Err(e) = conn.execute_batch(&format!("DROP TABLE IF EXISTS {table}")) {
                        warn!(table, error = %e, "cannot drop staging table");
                    }
                }
                return Ok(Committed { batch, fenced, timing, commit_ms, attempts: attempt, visible_at });
            }
            Err(e) if crate::lake::is_conflict(&e) && attempt < 4 => {
                warn!(attempt, error = %format!("{e:#}"), "epoch commit conflict, retrying");
                std::thread::sleep(Duration::from_millis(100 * attempt));
            }
            Err(e) => return Err(e.context("epoch commit failed")),
        }
    }
}

/// The wall clock as a SQL TIMESTAMPTZ. A checkpoint's `committed_at` takes it when the view's writes are done:
/// `now()` would give the transaction's start, and DuckLake's snapshot time is taken before the commit lands too.
/// What remains after it is the COMMIT call itself; `Committed::visible_at` marks the end of that.
fn wall_clock() -> String {
    format!("to_timestamp({:.6})", chrono::Utc::now().timestamp_micros() as f64 / 1e6)
}

/// Runs `f` and adds its duration to `acc`.
fn timed<T>(acc: &mut u128, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let r = f();
    *acc += t.elapsed().as_millis();
    r
}

/// The epoch transaction. Writes only the lake; reads staging tables from the in-memory catalog.
fn commit(conn: &Connection, worker: &str, batch: &mut CommitBatch, fenced: &mut Vec<String>, tm: &mut CommitTiming) -> Result<()> {
    let t_writes = Instant::now();
    conn.execute_batch("BEGIN")?;
    let res = (|| -> Result<()> {
        let epochs: serde_json::Map<String, serde_json::Value> = batch.jobs.iter().map(|c| (c.id.clone(), json!(c.epoch))).collect();
        let info = json!({ "worker": worker, "jobs": epochs });
        conn.execute_batch(&format!(
            "CALL {LAKE}.set_commit_message('duckwake', {}, extra_info => {})",
            lit(&format!("epoch commit by {worker}")),
            lit(&info.to_string())
        ))?;
        crash_point("commit_begin");
        // With `lean`, writes with nothing to write are skipped, and the checkpoints and dead letters of all
        // jobs go into one INSERT each after the loop.
        let lean = batch.lean;
        let (mut dead_rows, mut checkpoint_rows) = (Vec::new(), Vec::new());
        for c in batch.jobs.iter_mut() {
            let (id, epoch) = (&c.id, c.epoch);
            // Fencing: a worker that lost the lease updates 0 rows; one that loses it while this transaction
            // is open gets a DuckLake write conflict at COMMIT.
            let n = timed(&mut tm.lease, || {
                conn.execute(
                    &format!("UPDATE {} SET last_epoch = ? WHERE owner_worker = ? AND lease_epoch = ?", lease_table(id)),
                    duckdb::params![epoch, worker, c.lease],
                )
            })
            .map_err(|e| if is_missing_table(&e) { anyhow::Error::new(LeaseGone(id.clone())) } else { e.into() })?;
            if n == 0 {
                fenced.push(id.clone());
                continue;
            }
            let s = &mut c.staged;
            let x = EpochCtx {
                job: id,
                plan: &c.def.plan,
                sink: quote_table(&c.def.sink)?,
                late_table: c.def.late_data.as_deref().map(quote_table).transpose()?,
                sources: c
                    .inputs
                    .iter()
                    .map(|i| SourceInput { name: &i.name, columns: &i.columns, typed: String::new() })
                    .collect(),
                parity: c.parity,
                overlay: None,
                prev_watermark: String::new(),
                watermark: s.watermark_us.map(|w| format!("make_timestamp({w})")),
                epoch: epoch.to_string(),
            };
            let upserts = c.circuit.upserts();
            for step in c.circuit.commit_steps(&x)? {
                let none = step.skip_if_none.is_some_and(|k| match k {
                    Counter::Late => s.rows_late == 0,
                    Counter::Staged => s.rows_staged == 0,
                    Counter::Changed => s.rows_changed == 0,
                });
                if !lean || (!none && (!step.needs_close || s.closes)) {
                    let acc = match step.cost {
                        Cost::Stage | Cost::State => &mut tm.state,
                        Cost::Sink => &mut tm.sink,
                        Cost::Late => &mut tm.late,
                    };
                    let n = timed(acc, || conn.execute(&step.sql, []))?;
                    if step.output_rows {
                        s.rows_out = n as i64;
                    }
                }
                if step.crash_after {
                    crash_point("commit_mid");
                }
            }
            if upserts {
                s.rows_out = s.rows_changed;
            }
            // One checkpoint row with every source's offsets.
            let offsets: serde_json::Map<String, serde_json::Value> = c
                .inputs
                .iter()
                .zip(&s.inputs)
                .map(|(i, si)| (i.name.clone(), json!(si.offsets.iter().map(|(p, o)| (p.to_string(), json!(o))).collect::<serde_json::Map<_, _>>())))
                .collect();
            let offsets = serde_json::Value::Object(offsets);
            let ts = |us: Option<i64>| us.map(|w| format!("make_timestamp({w})")).unwrap_or("NULL".into());
            // An upsert view reads tombstones as deletes; every other view keeps them as dead letters.
            let not_tombstones = if c.def.upsert.is_some() { format!(" WHERE _err <> {}", lit(crate::sources::kafka::decode::TOMBSTONE)) } else { String::new() };
            if lean {
                for (i, _) in c.inputs.iter().zip(&s.inputs).filter(|(_, si)| si.rows_dead > 0) {
                    let Some(dead) = &i.dead else { continue };
                    dead_rows.push(format!(
                        "SELECT {}, {epoch}, {}, _partition, _offset, payload, _err, now() FROM {dead}{not_tombstones}",
                        lit(id),
                        lit(&i.name)
                    ));
                }
                checkpoint_rows.push(format!(
                    "({}, {epoch}, {}, {}, 'ducklake', NULL, {}, {}, {}, {}, {}, {}, {}, {{committed_at}}, {})",
                    lit(id),
                    lit(&offsets.to_string()),
                    ts(s.watermark_us),
                    s.rows_in,
                    s.rows_out,
                    s.rows_late,
                    s.rows_dead,
                    ts(s.max_event_time_us),
                    c.lag,
                    lit(worker),
                    s.rows_sql
                ));
                continue;
            }
            for (src, dead) in c.inputs.iter().filter_map(|i| i.dead.as_ref().map(|d| (i, d))) {
                timed(&mut tm.dead, || {
                    conn.execute(
                        &format!("INSERT INTO {CTL}.dead_letter SELECT ?, ?, ?, _partition, _offset, payload, _err, now() FROM {dead}{not_tombstones}"),
                        duckdb::params![id, epoch, src.name],
                    )
                })?;
            }
            timed(&mut tm.checkpoint, || {
                conn.execute(
                    &format!(
                        "INSERT INTO {CTL}.checkpoints (job_id, epoch, source_offsets, watermark, state_backend, state_ref,
                           rows_in, rows_out, rows_late, rows_dead, max_event_time, lag, worker, committed_at, rows_sql)
                         VALUES (?, ?, ?, {}, 'ducklake', NULL, ?, ?, ?, ?, {}, ?, ?, {}, ?)",
                        ts(s.watermark_us),
                        ts(s.max_event_time_us),
                        wall_clock()
                    ),
                    duckdb::params![
                        id,
                        epoch,
                        offsets.to_string(),
                        s.rows_in as i64,
                        s.rows_out,
                        s.rows_late,
                        s.rows_dead,
                        c.lag,
                        worker,
                        s.rows_sql as i64
                    ],
                )
            })?;
        }
        if !dead_rows.is_empty() {
            timed(&mut tm.dead, || conn.execute_batch(&format!("INSERT INTO {CTL}.dead_letter {}", dead_rows.join(" UNION ALL "))))?;
        }
        if !checkpoint_rows.is_empty() {
            timed(&mut tm.checkpoint, || {
                conn.execute_batch(&format!(
                    "INSERT INTO {CTL}.checkpoints (job_id, epoch, source_offsets, watermark, state_backend, state_ref,
                       rows_in, rows_out, rows_late, rows_dead, max_event_time, lag, worker, committed_at, rows_sql)
                     VALUES {}",
                    checkpoint_rows.join(", ").replace("{committed_at}", &wall_clock())
                ))
            })?;
        }
        crash_point("before_commit");
        Ok(())
    })();
    tm.writes = t_writes.elapsed().as_millis();
    match res {
        Ok(()) => timed(&mut tm.commit, || conn.execute_batch("COMMIT")).map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            e.into()
        }),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

pub fn fmt_us(us: Option<i64>) -> String {
    us.and_then(chrono::DateTime::from_timestamp_micros)
        .map(|d| d.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "-".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A run's staging tables are dropped after it leaves the worker, on the commit thread when epochs are
    /// pipelined, so possibly after the next run of the same view has started: two runs never share a table.
    #[test]
    fn each_run_of_a_view_stages_into_its_own_table() {
        assert_ne!(lake_typed("v", 3, 0), lake_typed("v", 4, 0));
        assert_ne!(lake_typed("v", 3, 0), lake_typed("v", 3, 1));
        // Lease and input are the last two parts of the name, so views whose names contain them stay apart.
        assert_ne!(lake_typed("v_1", 2, 0), lake_typed("v", 12, 0));
        assert_ne!(lake_typed("v_1", 2, 0), lake_typed("v", 1, 20));
    }
}
