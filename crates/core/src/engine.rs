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
use crate::kafka::{Chunk, Reader};
use crate::lake::{connect_again, ident, lit, quote_table, LakeConfig, CTL, LAKE};
use crate::maintenance::{Maintenance, MaintenanceConfig};
use crate::model::{parse_duration, JobDef, SourceDef};
use crate::plan::{decode_sql, GlobalTables, IS_JSON_OBJECT};
use anyhow::{anyhow, Context, Result};
use duckdb::Connection;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tracing::{info, warn};

pub struct WorkerConfig {
    pub worker_id: String,
    pub lake: LakeConfig,
    pub maintenance: MaintenanceConfig,
    /// Stop after this many committed epochs (tests).
    pub max_epochs: Option<u64>,
    /// How often to look for newly assigned / paused jobs.
    pub refresh_every: Duration,
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
}

struct JobRuntime {
    def: Arc<JobDef>,
    source: Arc<SourceDef>,
    lease: i64,
    /// The last epoch handed to the commit (with `pipeline`, possibly still committing) and the position
    /// after it, which the next epoch starts from.
    epoch: i64,
    watermark_us: Option<i64>,
    max_event_time_us: Option<i64>,
    offsets: BTreeMap<i32, i64>,
    part_max_ts: HashMap<i32, i64>,
    part_last_data: HashMap<i32, Instant>,
    delay_us: i64,
    idle_timeout: Duration,
    interval: Duration,
    /// The group whose reader feeds this job.
    group: u64,
    /// The last round of epochs this job was part of. A global aggregate staged in the round right after it
    /// reads that round's new state, which may not have committed yet, through the overlay.
    last_round: Option<u64>,
    /// With `lean_commit`, a windowed job's distinct window ends in its state table after `epoch`: read from
    /// the table at start, then kept up to date from each epoch's partials and watermark. A window can close
    /// only when one of them is at or below the watermark.
    open_ends: Option<BTreeSet<i64>>,
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
    stage_ms: u128,
}

#[derive(Default, Clone)]
struct Staged {
    offsets: BTreeMap<i32, i64>,
    watermark_us: Option<i64>,
    max_event_time_us: Option<i64>,
    rows_in: usize,
    rows_sql: usize,
    bisections: usize,
    rows_dead: i64,
    rows_late: i64,
    rows_out: i64,
    /// Rows the commit would write from staging: windowed partials, global-aggregate touched groups, or
    /// stateless sink rows.
    rows_staged: i64,
    /// Global aggregates: groups whose sink row changes.
    rows_changed: i64,
    /// Windowed: whether a window closes this epoch, and the open window ends after it (with `lean_commit`).
    closes: bool,
    open_ends: Option<BTreeSet<i64>>,
    /// Staging this job's view, and its group's events (shared with the other members).
    stage_ms: u128,
    source_ms: u128,
}

/// One job's epoch as the commit needs it. It owns everything, so it can go to the commit thread.
struct CommitJob {
    id: String,
    lease: i64,
    epoch: i64,
    def: Arc<JobDef>,
    source: Arc<SourceDef>,
    staged: Staged,
    /// This epoch's staging tables: the view's sink rows or partials, late rows, global-aggregate tables,
    /// and its group's dead letters.
    out: String,
    late: String,
    global: Option<GlobalTables>,
    dead: String,
    lag: i64,
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
fn crash_point(point: &str) {
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

pub fn run_worker(cfg: WorkerConfig, rt: Handle, shutdown: Arc<AtomicBool>) -> Result<()> {
    let conn = cfg.lake.open()?;
    crate::lake::bootstrap(&conn)?;
    if let Some(m) = &cfg.metrics {
        m.set_worker(&cfg.worker_id);
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
        "worker started"
    );
    let mut w = Worker {
        cfg,
        rt,
        conns,
        committer,
        jobs: BTreeMap::new(),
        groups: BTreeMap::new(),
        next_group: 0,
        rounds: 0,
        committed: 0,
        drops: Vec::new(),
    };
    w.run(&shutdown)?;
    info!(worker = %w.cfg.worker_id, "worker stopped");
    Ok(())
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
    groups: BTreeMap<u64, Group>,
    next_group: u64,
    /// Epochs handed to the commit so far; the one staged next stages into table set `(rounds + 1) % 2`.
    rounds: u64,
    committed: u64,
    /// Staging tables to drop once the next epoch has committed.
    drops: Vec<String>,
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
            if last_refresh.elapsed() >= self.cfg.refresh_every {
                self.refresh()?;
                last_refresh = Instant::now();
            }
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
            if let Some(mut batch) = self.stage_epoch()? {
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
        // Let the epoch in flight land.
        if let Some(c) = self.committer.finish()? {
            self.apply(c);
        }
        Ok(())
    }

    fn reached_max_epochs(&self) -> bool {
        self.cfg.max_epochs.is_some_and(|m| self.committed >= m)
    }

    /// Picks up jobs newly assigned to this worker and drops ones paused, stopped or taken over.
    fn refresh(&mut self) -> Result<()> {
        let running = catalog::running_jobs(&self.conns[0])?;
        let mine: BTreeMap<String, i64> = running
            .into_iter()
            .filter(|(_, owner, _)| owner.as_deref() == Some(self.cfg.worker_id.as_str()))
            .map(|(id, _, lease)| (id, lease))
            .collect();
        let released: Vec<String> =
            self.jobs.iter().filter(|(id, j)| mine.get(*id) != Some(&j.lease)).map(|(id, _)| id.clone()).collect();
        for id in released {
            info!(job = %id, "job released (paused, stopped or reassigned)");
            self.remove_job(&id);
        }
        let new: Vec<String> = mine.keys().filter(|id| !self.jobs.contains_key(*id)).cloned().collect();
        // A job coming back reads its checkpoint, so its previous run's last commit must have landed first.
        if new.iter().any(|id| self.committer.includes(id)) {
            if let Some(c) = self.committer.finish()? {
                self.apply(c);
            }
        }
        for id in new {
            match self.start_job(&id) {
                Ok(_) => {}
                Err(e) if crate::lake::is_conflict(&e) => {
                    warn!(job = %id, error = %format!("{e:#}"), "lease race; retrying on next refresh");
                }
                Err(e) => {
                    warn!(job = %id, error = %format!("{e:#}"), "cannot start job; marking failed");
                    catalog::set_status(&self.conns[0], &id, crate::model::JobStatus::Failed, None)?;
                }
            }
        }
        Ok(())
    }

    /// Recovery: take the lease, read the latest checkpoint, seek each partition to its offset. State needs no
    /// restore with the DuckLake backend: the state table is as of the last commit. With shared readers a job
    /// at the position of a group of its source joins that group. Returns false when the job is not ours.
    fn start_job(&mut self, id: &str) -> Result<bool> {
        let conn = &self.conns[0];
        let Some(lease) = catalog::take_lease(conn, id, &self.cfg.worker_id)? else { return Ok(false) };
        let (def, _) = catalog::load_job(conn, id)?;
        catalog::set_sink_inlining(conn, &def)?;
        let sources = catalog::load_sources(conn)?;
        let source = Arc::new(sources.get(&def.plan.source).context("source of job is gone")?.clone());
        let ckpt = catalog::latest_checkpoint(conn, id, &source.name)?.unwrap_or_default();
        let decoder = source.options.arrow_decoder()?.map(Arc::new);
        let (reader, offsets) = Reader::start(&self.rt, &source.options, &ckpt.offsets, def.max_rows, decoder)?;
        // Recreate the staging table the compiled SQL reads.
        crate::plan::compile(conn, id, &def.sql, &sources)?;
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
                g.max_rows = g.max_rows.min(def.max_rows);
                gid
            }
            None => {
                let gid = self.next_group;
                self.next_group += 1;
                let g = Group {
                    source: source.clone(),
                    reader,
                    offsets: offsets.clone(),
                    members: BTreeSet::from([id.to_string()]),
                    max_rows: def.max_rows,
                };
                self.groups.insert(gid, g);
                gid
            }
        };
        let open_ends = if self.cfg.lean_commit && def.plan.is_windowed() {
            let mut stmt = self.conns[0].prepare(&format!("SELECT DISTINCT epoch_us(__we) FROM {}", state_table(id)))?;
            let ends = stmt.query_map([], |r| r.get::<_, i64>(0))?.collect::<duckdb::Result<BTreeSet<_>>>()?;
            Some(ends)
        } else {
            None
        };
        let now = Instant::now();
        let Checkpoint { epoch, watermark_us, max_event_time_us, .. } = ckpt;
        info!(job = %id, lease, epoch, offsets = ?offsets, group, "job started");
        let j = JobRuntime {
            delay_us: parse_duration(&source.options.watermark_delay)?.as_micros() as i64,
            idle_timeout: parse_duration(&source.options.idle_timeout)?,
            interval: parse_duration(&def.epoch_interval)?,
            part_last_data: offsets.keys().map(|p| (*p, now)).collect(),
            part_max_ts: HashMap::new(),
            offsets,
            def: Arc::new(def),
            source,
            lease,
            epoch,
            watermark_us,
            max_event_time_us,
            group,
            last_round: None,
            open_ends,
        };
        self.jobs.insert(id.to_string(), j);
        Ok(true)
    }

    fn remove_job(&mut self, id: &str) {
        let Some(j) = self.jobs.remove(id) else { return };
        if let Some(m) = &self.cfg.metrics {
            m.forget(id);
        }
        let Some(g) = self.groups.get_mut(&j.group) else { return };
        g.members.remove(id);
        if g.members.is_empty() {
            self.groups.remove(&j.group);
            self.drops.extend(group_tables(j.group));
        } else {
            g.max_rows = g.members.iter().map(|m| self.jobs[m].def.max_rows).min().unwrap();
        }
    }

    /// Takes in a committed epoch: drops the jobs it fenced and logs the others.
    fn apply(&mut self, c: Committed) {
        for id in &c.fenced {
            warn!(job = %id, "lease lost: another worker owns this job now; dropping it");
            self.remove_job(id);
        }
        self.committed += 1;
        if let Some(m) = &self.cfg.metrics {
            m.record_commit(c.batch.poll_stage_ms, c.commit_ms, c.attempts);
        }
        for j in c.batch.jobs.iter().filter(|j| !c.fenced.contains(&j.id)) {
            let s = &j.staged;
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
                offsets = ?s.offsets,
                "epoch committed"
            );
        }
    }

    /// Polls and stages the next epoch of every job. Returns it for the commit, or None when there is nothing
    /// to commit: no new rows and no watermark moved.
    fn stage_epoch(&mut self) -> Result<Option<CommitBatch>> {
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
                let src = &sources[&j.group];
                (id, j, src, overlay)
            })
            .collect();
        let staged: Vec<Staged> = parallel(conns, items, |conn, (id, j, src, overlay)| {
            stage_view(conn, id, j, src, parity, overlay).with_context(|| format!("job {id}: staging failed"))
        })
        .into_iter()
        .collect::<Result<_>>()?;
        if !jobs.values().zip(&staged).any(|(j, s)| s.rows_in > 0 || s.watermark_us != j.watermark_us) {
            // Nothing new and no window can close: skip the commit instead of writing an empty snapshot.
            return Ok(None);
        }

        // 4. This is every job's next epoch; the one after is staged from here.
        *rounds = round;
        for (gid, s) in &sources {
            if let Some(g) = groups.get_mut(gid) {
                g.offsets = s.offsets.clone();
            }
        }
        let mut batch = Vec::new();
        for ((id, j), s) in jobs.iter_mut().zip(staged) {
            j.epoch += 1;
            j.offsets = s.offsets.clone();
            j.watermark_us = s.watermark_us;
            j.max_event_time_us = s.max_event_time_us;
            j.last_round = Some(round);
            j.open_ends = s.open_ends.clone();
            let set = format!("{id}_{parity}");
            let global = match &j.def.plan.global {
                Some(_) => Some(GlobalTables::new(id, state_table(id), quote_table(&j.def.sink)?, &j.def.plan, parity)),
                None => None,
            };
            batch.push(CommitJob {
                id: id.clone(),
                lease: j.lease,
                epoch: j.epoch,
                def: j.def.clone(),
                source: j.source.clone(),
                out: mem("out", &set),
                late: mem("late", &set),
                global,
                dead: sources[&j.group].dead.clone(),
                lag: groups[&j.group].reader.lag(&s.offsets),
                staged: s,
            });
        }
        let mut drop_after = std::mem::take(drops);
        if cfg.shared_readers {
            merge_groups(groups, jobs, &mut drop_after);
        }
        Ok(Some(CommitBatch { round, lean: cfg.lean_commit, jobs: batch, poll_stage_ms: t0.elapsed().as_millis(), drop_after }))
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
                if let Some(j) = jobs.get_mut(m) {
                    j.group = *a;
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
        "CREATE OR REPLACE TABLE {typed} (_partition INTEGER, _offset BIGINT, payload VARCHAR, {}, _err VARCHAR);
         CREATE OR REPLACE TABLE {raw} (_partition INTEGER, _offset BIGINT, payload VARCHAR);",
        typed_cols.join(", ")
    ))?;
    if chunks.iter().any(|c| c.fast.as_ref().is_some_and(|b| b.num_rows() > 0)) {
        let mut app = conn.appender_to_catalog_and_db(&format!("typed_{g}"), "memory", "main")?;
        for b in chunks.iter().filter_map(|c| c.fast.as_ref()).filter(|b| b.num_rows() > 0) {
            app.append_record_batch(crate::decode::staging_batch(b)?)?;
        }
        app.flush()?;
    }
    {
        let mut app = conn.appender_to_catalog_and_db(&format!("raw_{g}"), "memory", "main")?;
        for m in chunks.iter().flat_map(|c| &c.slow) {
            let payload = m.payload.as_deref().map(String::from_utf8_lossy);
            app.append_row(duckdb::params![m.partition, m.offset, payload.as_deref()])?;
        }
        app.flush()?;
    }

    // JSON parse errors must be split off before extracting fields, which would raise on malformed input.
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {ok} AS SELECT * FROM {raw}
           WHERE {IS_JSON_OBJECT};
         INSERT INTO {typed} BY NAME {decode};
         CREATE OR REPLACE TABLE {dead} AS
           SELECT _partition, _offset, payload, 'invalid json' AS _err FROM {raw}
            WHERE NOT ({IS_JSON_OBJECT})
           UNION ALL SELECT _partition, _offset, payload, _err FROM {typed} WHERE _err IS NOT NULL;",
        decode = decode_sql(source, &ok)
    ))?;
    let rows_dead: i64 = conn.query_row(&format!("SELECT count(*) FROM {dead}"), [], |r| r.get(0))?;

    let et = ident(&source.options.event_time);
    let mut stmt = conn.prepare(&format!(
        "SELECT _partition, epoch_us(max({et})) FROM {typed} WHERE _err IS NULL GROUP BY ALL"
    ))?;
    let part_max = stmt
        .query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<duckdb::Result<Vec<_>>>()?;
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
        stage_ms: t.elapsed().as_millis(),
    })
}

/// Stages one job's view from its group's typed rows: the watermark, windows and late rows, then the epoch's
/// sink rows (stateless) or partial aggregates. `overlay` is the table set of the previous epoch when that
/// epoch may still be committing: a global aggregate reads the state of the groups it touched from there.
fn stage_view(conn: &Connection, id: &str, j: &mut JobRuntime, src: &SourceStaged, parity: u64, overlay: Option<u64>) -> Result<Staged> {
    let t = Instant::now();
    let def = j.def.clone();
    let plan = &def.plan;
    let set = format!("{id}_{parity}");
    let (typed, win, late, input, out) = (&src.typed, mem("win", id), mem("late", &set), &plan.input_table, mem("out", &set));

    // Watermark: min over active partitions of their max event time, minus the delay. Partitions idle for
    // longer than idle_timeout are excluded; one that is active but has not reported yet holds it back.
    let now = Instant::now();
    for p in &src.active {
        j.part_last_data.insert(*p, now);
    }
    for &(p, ts) in &src.part_max {
        let e = j.part_max_ts.entry(p).or_insert(ts);
        *e = (*e).max(ts);
    }
    let active: Vec<i32> = j
        .part_last_data
        .iter()
        .filter(|(_, at)| at.elapsed() < j.idle_timeout)
        .map(|(p, _)| *p)
        .collect();
    let candidate = if active.is_empty() || active.iter().any(|p| !j.part_max_ts.contains_key(p)) {
        None
    } else {
        active.iter().map(|p| j.part_max_ts[p]).min().map(|m| m - j.delay_us)
    };
    let watermark_us = match (j.watermark_us, candidate) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    // Monotone across restarts (part_max_ts starts empty), so it bounds every event time read so far.
    let max_event_time_us = j.part_max_ts.values().copied().chain(j.max_event_time_us).max();

    let cols: Vec<String> = j.source.options.columns.iter().map(|(c, _)| ident(c)).collect();
    let cols = cols.join(", ");
    let count = |table: &str| -> Result<i64> { Ok(conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?) };
    let (mut rows_late, mut rows_changed, mut closes, mut open_ends) = (0, 0, true, None);
    let rows_staged;
    if plan.is_windowed() {
        // Late = assigned to a window that an earlier epoch already closed (window_end <= previous watermark).
        let prev = j.watermark_us.map(|w| format!("make_timestamp({w})")).unwrap_or("NULL".into());
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {win} AS {w};
             CREATE OR REPLACE TABLE {late} AS SELECT * FROM {win} WHERE window_end <= {prev};
             CREATE OR REPLACE TABLE {input} AS
               SELECT {cols}, window_start, window_end FROM {win} WHERE {prev} IS NULL OR window_end > {prev};
             CREATE OR REPLACE TABLE {out} AS {delta};",
            w = plan.window_sql(&format!("(SELECT _partition, _offset, {cols} FROM {typed} WHERE _err IS NULL)")),
            delta = plan.delta_sql
        ))?;
        rows_late = count(&late)?;
        rows_staged = count(&out)?;
        if let Some(prev_ends) = &j.open_ends {
            // The state's window ends after this epoch's MERGE; the emit closes those at or below the watermark.
            let mut ends = prev_ends.clone();
            let mut stmt = conn.prepare(&format!("SELECT DISTINCT epoch_us(__we) FROM {out}"))?;
            for e in stmt.query_map([], |r| r.get::<_, i64>(0))? {
                ends.insert(e?);
            }
            closes = watermark_us.is_some_and(|wm| ends.first().is_some_and(|e| *e <= wm));
            if let Some(wm) = watermark_us {
                ends.retain(|e| *e > wm);
            }
            open_ends = Some(ends);
        }
    } else if let Some(g) = &plan.global {
        // Source rows enter the operator chain with weight +1 (sources are append-only).
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {input} AS SELECT {cols}, 1::INTEGER AS __w FROM {typed} WHERE _err IS NULL"
        ))?;
        let mut t = GlobalTables::new(id, state_table(id), quote_table(&def.sink)?, plan, parity);
        if let Some(prev) = overlay {
            let prev_new = GlobalTables::new(id, state_table(id), String::new(), plan, prev).new_state;
            t.state = g.overlay_sql(&t.state, &prev_new, &plan.state_columns);
        }
        for stmt in g.stage_sql(&t, &plan.state_columns) {
            conn.execute_batch(&stmt)?;
        }
        rows_staged = count(&t.delta)?;
        rows_changed = count(&t.changed)?;
    } else {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {input} AS SELECT {cols} FROM {typed} WHERE _err IS NULL;
             CREATE OR REPLACE TABLE {out} AS {delta};",
            delta = plan.delta_sql
        ))?;
        rows_staged = count(&out)?;
    }
    Ok(Staged {
        offsets: src.offsets.clone(),
        watermark_us,
        max_event_time_us,
        rows_in: src.rows_in,
        rows_sql: src.rows_sql,
        bisections: src.bisections,
        rows_dead: src.rows_dead,
        rows_late,
        rows_out: 0,
        rows_staged,
        rows_changed,
        closes,
        open_ends,
        stage_ms: t.elapsed().as_millis(),
        source_ms: src.stage_ms,
    })
}

/// Runs the epoch commits: inline on the scheduler's connection, or with `pipeline` on a thread and
/// connection of its own, which also runs maintenance between commits.
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
    loop {
        attempt += 1;
        let (mut fenced, mut timing) = (Vec::new(), CommitTiming::default());
        match commit(conn, worker, &mut batch, &mut fenced, &mut timing) {
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
            })?;
            if n == 0 {
                fenced.push(id.clone());
                continue;
            }
            let s = &mut c.staged;
            let plan = &c.def.plan;
            let sink = quote_table(&c.def.sink)?;
            let sink_cols: Vec<String> = plan.sink_columns.iter().map(|(c, _)| ident(c)).chain(["_duckwake_epoch".into()]).collect();
            let sink_cols = sink_cols.join(", ");
            if plan.is_windowed() {
                let st = state_table(id);
                if !lean || s.rows_staged > 0 {
                    timed(&mut tm.state, || conn.execute_batch(&plan.merge_sql(&st, &c.out)))?;
                }
                crash_point("commit_mid");
                if let Some(wm) = s.watermark_us.filter(|_| !lean || s.closes) {
                    let wm = format!("make_timestamp({wm})");
                    s.rows_out = timed(&mut tm.sink, || {
                        conn.execute(
                            &format!("INSERT INTO {sink} ({sink_cols}) SELECT {}, {epoch} FROM {st} WHERE __we <= {wm}", plan.final_select),
                            [],
                        )
                    })? as i64;
                    timed(&mut tm.state, || conn.execute_batch(&format!("DELETE FROM {st} WHERE __we <= {wm}")))?;
                }
                if let Some(late) = c.def.late_data.as_ref().filter(|_| !lean || s.rows_late > 0) {
                    let cols: Vec<String> = c.source.options.columns.iter().map(|(c, _)| ident(c)).collect();
                    let cols = cols.join(", ");
                    let sql = format!(
                        "INSERT INTO {} ({cols}, window_start, window_end, _source_partition, _source_offset, _duckwake_epoch)
                         SELECT {cols}, window_start, window_end, _partition, _offset, {epoch} FROM {}",
                        quote_table(late)?,
                        c.late
                    );
                    timed(&mut tm.late, || conn.execute_batch(&sql))?;
                }
            } else if let Some(g) = &plan.global {
                // Upsert sink: replace the touched groups' state and the changed groups' rows.
                let t = c.global.as_ref().context("global aggregate without its tables")?;
                // State replace (statements 0, 1), then sink upsert (2, 3).
                for (i, stmt) in g.commit_sql(t, &plan.state_columns, epoch).iter().enumerate() {
                    let nothing = if i < 2 { s.rows_staged == 0 } else { s.rows_changed == 0 };
                    if !lean || !nothing {
                        timed(if i < 2 { &mut tm.state } else { &mut tm.sink }, || conn.execute_batch(stmt))?;
                    }
                    if i == 1 {
                        crash_point("commit_mid");
                    }
                }
                s.rows_out = s.rows_changed;
            } else {
                if !lean || s.rows_staged > 0 {
                    s.rows_out = timed(&mut tm.sink, || {
                        conn.execute(&format!("INSERT INTO {sink} ({sink_cols}) SELECT *, {epoch} FROM {}", c.out), [])
                    })? as i64;
                }
                crash_point("commit_mid");
            }
            let offsets = json!({ &c.source.name: s.offsets.iter().map(|(p, o)| (p.to_string(), json!(o))).collect::<serde_json::Map<_, _>>() });
            let ts = |us: Option<i64>| us.map(|w| format!("make_timestamp({w})")).unwrap_or("NULL".into());
            if lean {
                if s.rows_dead > 0 {
                    dead_rows.push(format!(
                        "SELECT {}, {epoch}, {}, _partition, _offset, payload, _err, now() FROM {}",
                        lit(id),
                        lit(&c.source.name),
                        c.dead
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
            timed(&mut tm.dead, || {
                conn.execute(
                    &format!(
                        "INSERT INTO {CTL}.dead_letter SELECT ?, ?, ?, _partition, _offset, payload, _err, now() FROM {}",
                        c.dead
                    ),
                    duckdb::params![id, epoch, c.source.name],
                )
            })?;
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
