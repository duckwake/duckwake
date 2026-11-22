//! The ingest job of a database source (PostgreSQL, `postgres`; MySQL, `mysql`): it keeps one mirror table
//! per source table (`mirror`), reading the source's changes through a `Reader`. The rules, from
//! `docs/design/database-cdc-sources.md`:
//!
//! 1. Whole transactions only: a reader hands over whole transactions, and an epoch applies each one or none.
//! 2. The position (a PostgreSQL LSN, a MySQL GTID set) is written to `__duckwake.checkpoints` in the epoch's
//!    DuckLake transaction with the mirror rows.
//! 3. Nothing is acknowledged to the source before that commit (PostgreSQL's slot is advanced after it; MySQL
//!    keeps no state for a reader), so a crash in between costs a reread, never a loss.
//! 4. A restart reads the position from the lake; a source that can no longer give the changes after it (a slot
//!    gone, binlogs purged) stops the job, which a reset starts over from a new snapshot.
//! 5. Changes of one key apply in commit order; an epoch writes each touched key's last state.
//! 6. A changed primary key deletes the old key. TRUNCATE empties the mirror. PostgreSQL leaves unchanged large
//!    (TOAST) values out of an update: they come from the key's previous state, in the epoch or the mirror.
//!
//! The first epoch copies every table under one consistent snapshot of the source; how the reader joins that
//! snapshot to the change stream is its own (`Reader::snapshot`).
use crate::lake::{ident, lit, quote_table, CTL, LAKE};
pub mod catalog;
pub mod mirror;
pub mod mysql;
pub mod postgres;

use mirror::{cast_sql, MirrorTable};
use anyhow::Result;
use duckdb::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// One column value of a change, as a reader hands it over.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    /// A large (TOASTed) value the change did not touch: PostgreSQL leaves it out, and its value is the one
    /// the row had before.
    Unchanged,
    /// The value's text form.
    Text(String),
}

/// The source broke a rule the mirror relies on: the job stops (status `failed`) with this message.
#[derive(Debug)]
pub struct Stop(pub String);

impl std::fmt::Display for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Stop {}

/// The job's lease moved (stopped, paused, reset, dropped or taken over): this run ends without committing.
#[derive(Debug)]
struct Fenced;

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lease lost")
    }
}

impl std::error::Error for Fenced {}

pub(crate) fn stop(msg: String) -> anyhow::Error {
    anyhow::Error::new(Stop(msg))
}

/// What the worker gives an ingest job's thread.
#[derive(Clone)]
pub struct Settings {
    pub worker: String,
    /// How long an idle source may leave its position behind the source's log before committing it alone.
    pub position_refresh: Duration,
    pub metrics: Option<Arc<crate::metrics::Metrics>>,
}

/// A database source as the worker starts its ingest job.
#[derive(Clone)]
pub enum Spec {
    Postgres(crate::sources::database::postgres::PgOptions),
    Mysql(crate::sources::database::mysql::MyOptions),
}

impl Spec {
    /// The source named `name`, of either kind.
    pub fn load(conn: &Connection, name: &str) -> Result<Spec> {
        match crate::catalog::source_kind(conn, name)?.as_deref() {
            Some(crate::sources::database::mysql::KIND) => Ok(Spec::Mysql(crate::sources::database::mysql::load(conn, name)?)),
            _ => Ok(Spec::Postgres(crate::sources::database::catalog::load_postgres(conn, name)?)),
        }
    }

    fn interval(&self) -> Duration {
        let s = match self {
            Spec::Postgres(o) => &o.epoch_interval,
            Spec::Mysql(o) => &o.epoch_interval,
        };
        crate::model::parse_duration(s).unwrap_or(Duration::from_secs(2))
    }

    pub fn tables(&self) -> &[MirrorTable] {
        match self {
            Spec::Postgres(o) => &o.tables,
            Spec::Mysql(o) => &o.tables,
        }
    }

    fn reader(&self, conn: &Connection, job: &str) -> Result<Box<dyn Reader>> {
        Ok(match self {
            Spec::Postgres(o) => Box::new(crate::sources::database::postgres::PgReader::open(conn, job, o)?),
            Spec::Mysql(o) => Box::new(crate::sources::database::mysql::MyReader::open(job, o)?),
        })
    }
}

/// Where a source's changes stand: `offset` goes into the checkpoint's offsets (it only grows: an LSN, a count
/// of transactions), `state` into its `state_ref` (what else the reader needs to go on).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Position {
    pub offset: u64,
    pub state: serde_json::Value,
}

/// What one read of a source's changes found.
pub(crate) struct Read {
    /// The position after the transactions read (or further, where the source says nothing before it changed).
    pub pos: Position,
    /// The read stopped at `max_changes`: more is waiting.
    pub full: bool,
    pub txns: u64,
    /// Of `txns`, those already in the lake or the snapshot.
    pub skipped: u64,
    pub changes: i64,
    pub newest_us: Option<i64>,
    /// How far the source is ahead of `pos` (bytes of WAL, transactions of binlog).
    pub lag: i64,
    /// The reader wants `pos` committed even with nothing to write (the source keeps its log until then).
    pub due: bool,
}

/// A database source's changes, as the ingest job reads them.
pub(crate) trait Reader {
    /// `checkpoints.state_backend` of this source's checkpoints.
    fn backend(&self) -> &'static str;
    fn describe(&self, pos: &Position) -> String;
    /// The first epoch: copies every table, under one consistent snapshot, into `staging(i)` (memory tables
    /// cast like the mirrors), and puts into `batch` what must be applied over the copy in the same commit.
    /// Returns the position the stream goes on from, and how many changes `batch` took.
    fn snapshot(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String, batch: &mut Batch) -> Result<(Position, i64)>;
    /// The tables copied as they are now, under one snapshot, into `staging(i)`: for `verify`.
    fn copy(&mut self, conn: &Connection, staging: &dyn Fn(usize) -> String) -> Result<()>;
    /// The whole transactions after `pos`, into `batch`.
    fn read(&mut self, conn: &Connection, pos: &Position, batch: &mut Batch) -> Result<Read>;
    /// After the commit of `pos`: what the source may now let go of.
    fn committed(&mut self, conn: &Connection, pos: &Position);
}

/// Compares the mirrors of source `name` with its tables now (`duckwake verify`).
pub fn verify(conn: &Connection, name: &str) -> Result<(bool, String)> {
    let spec = Spec::load(conn, name)?;
    let mut reader = spec.reader(conn, name)?;
    let staging = |i: usize| format!("memory.main.{}", ident(&format!("verify_{name}_{i}")));
    reader.copy(conn, &staging)?;
    crate::sources::database::mirror::compare(conn, name, spec.tables(), &staging)
}

/// An ingest job running on its own thread and connection, so a long snapshot never holds up the views.
pub struct Handle {
    pub lease: i64,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    failed: mpsc::Receiver<String>,
}

impl Handle {
    pub fn spawn(conn: Connection, settings: Settings, job: String, lease: i64, spec: Spec) -> Handle {
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, failed) = mpsc::channel();
        let flag = stop.clone();
        let thread = std::thread::Builder::new()
            .name(format!("ingest-{job}"))
            .spawn(move || run(conn, settings, job, lease, spec, flag, tx))
            .expect("spawning an ingest thread");
        Handle { lease, stop, thread: Some(thread), failed }
    }

    /// Why the job stopped, once: the worker marks it failed.
    pub fn failure(&self) -> Option<String> {
        self.failed.try_recv().ok()
    }

    /// Asks the thread to finish after its current epoch. It commits nothing once its lease moved.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn finished(&self) -> bool {
        self.thread.as_ref().is_none_or(|t| t.is_finished())
    }

    pub fn join(mut self) {
        self.stop();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn run(conn: Connection, s: Settings, job: String, lease: i64, spec: Spec, stop_flag: Arc<AtomicBool>, failed: mpsc::Sender<String>) {
    let interval = spec.interval();
    let mut ingest: Option<Ingest> = None;
    let mut backoff = Duration::from_secs(1);
    let wait = |d: Duration| {
        let until = Instant::now() + d;
        while Instant::now() < until && !stop_flag.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(50).min(until - Instant::now()));
        }
    };
    while !stop_flag.load(Ordering::Relaxed) {
        let started = Instant::now();
        let res = (|| -> Result<Outcome> {
            if ingest.is_none() {
                ingest = Some(Ingest::open(&conn, &s, &job, lease, &spec)?);
            }
            ingest.as_mut().unwrap().epoch()
        })();
        match res {
            Ok(outcome) => {
                backoff = Duration::from_secs(1);
                // A full read leaves more waiting in the source: go on at once.
                if outcome != Outcome::Full {
                    wait(interval.saturating_sub(started.elapsed()));
                }
            }
            Err(e) if e.is::<Fenced>() => {
                info!(job = %job, "ingest job's lease moved; stopping");
                return;
            }
            Err(e) if e.is::<Stop>() => {
                let _ = failed.send(format!("{e:#}"));
                return;
            }
            Err(e) => {
                // A database or catalog that cannot be reached, a slot another process holds: start again
                // from the lake, with a new connection.
                warn!(job = %job, error = %format!("{e:#}"), retry_in_s = backoff.as_secs(), "ingest epoch failed; retrying");
                ingest = None;
                wait(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Idle,
    Committed,
    /// Committed, and the read stopped at `max_changes`.
    Full,
}

/// A row's state at the end of the epoch so far.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Final {
    Deleted,
    /// `fill`: the key whose stored row holds the values still `Unchanged`.
    Row { values: Vec<Value>, fill: Option<Vec<String>> },
}

/// One table's changes in an epoch: each touched key's last state.
#[derive(Debug, Default)]
pub(crate) struct TableChanges {
    truncated: bool,
    keys: HashMap<Vec<String>, Final>,
}

/// A change of one transaction, kept by a reader until the transaction ends.
pub(crate) enum Change {
    Upsert { table: usize, old_key: Option<Vec<String>>, values: Vec<Value> },
    Delete { table: usize, key: Vec<String> },
    Truncate { tables: Vec<usize> },
}

/// Applies changes in commit order to the epoch's per-table states.
pub(crate) struct Batch<'a> {
    tables: &'a [MirrorTable],
    keys: Vec<Vec<usize>>,
    changes: Vec<TableChanges>,
}

impl<'a> Batch<'a> {
    pub(crate) fn new(tables: &'a [MirrorTable]) -> Self {
        Batch { tables, keys: tables.iter().map(|t| t.key_positions()).collect(), changes: tables.iter().map(|_| TableChanges::default()).collect() }
    }

    pub(crate) fn key(&self, t: usize, values: &[Value]) -> Result<Vec<String>> {
        self.keys[t]
            .iter()
            .map(|&i| match values.get(i) {
                Some(Value::Text(s)) => Ok(s.clone()),
                v => Err(stop(format!("a change of {} has no value for key column {}: {v:?}", self.tables[t].source_name(), self.tables[t].columns[i].name))),
            })
            .collect()
    }

    pub(crate) fn apply(&mut self, c: Change) -> Result<()> {
        match c {
            Change::Upsert { table, old_key, values } => {
                let key = self.key(table, &values)?;
                let old = old_key.unwrap_or_else(|| key.clone());
                let tc = &mut self.changes[table];
                // The previous state of the row: under its old key, which a key change deletes.
                let prev = if old != key { tc.keys.insert(old.clone(), Final::Deleted) } else { tc.keys.get(&key).cloned() };
                let unchanged = values.contains(&Value::Unchanged);
                let row = match (prev, unchanged) {
                    (_, false) => Final::Row { values, fill: None },
                    (Some(Final::Row { values: pv, fill }), true) => {
                        let values: Vec<Value> = values.into_iter().zip(pv).map(|(v, p)| if v == Value::Unchanged { p } else { v }).collect();
                        let fill = fill.filter(|_| values.contains(&Value::Unchanged));
                        Final::Row { values, fill }
                    }
                    (Some(Final::Deleted), true) => {
                        return Err(stop(format!(
                            "an update of {} key {old:?} left a large value unchanged, but the row was deleted before it",
                            self.tables[table].source_name()
                        )))
                    }
                    (None, true) if tc.truncated => {
                        return Err(stop(format!(
                            "an update of {} key {old:?} left a large value unchanged, but the table was truncated before it",
                            self.tables[table].source_name()
                        )))
                    }
                    (None, true) => Final::Row { values, fill: Some(old) },
                };
                tc.keys.insert(key, row);
            }
            Change::Delete { table, key } => {
                self.changes[table].keys.insert(key, Final::Deleted);
            }
            Change::Truncate { tables } => {
                for t in tables {
                    self.changes[t].truncated = true;
                    self.changes[t].keys.clear();
                }
            }
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.changes.iter().all(|c| !c.truncated && c.keys.is_empty())
    }
}

struct Ingest<'c> {
    conn: &'c Connection,
    s: Settings,
    job: String,
    lease: i64,
    tables: Vec<MirrorTable>,
    reader: Box<dyn Reader>,
    epoch: i64,
    /// None until the snapshot is committed.
    pos: Option<Position>,
    last_commit: Instant,
}

impl<'c> Ingest<'c> {
    fn open(conn: &'c Connection, s: &Settings, job: &str, lease: i64, spec: &Spec) -> Result<Self> {
        let reader = spec.reader(conn, job)?;
        for t in spec.tables() {
            let (_, schema, table) = crate::lake::qualified_table(&t.mirror)?;
            crate::catalog::set_table_inlining(conn, &schema, &table, 10_000)?;
        }
        let last = conn
            .query_row(
                &format!(
                    "SELECT epoch, source_offsets->{}->>'0', state_ref FROM {CTL}.checkpoints WHERE job_id = ? ORDER BY epoch DESC LIMIT 1",
                    lit(job)
                ),
                [job],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Option<String>>(2)?)),
            )
            .ok();
        let (epoch, pos) = match last {
            Some((e, Some(offset), state)) => {
                let state = state.map(|s| serde_json::from_str(&s)).transpose()?.unwrap_or_else(|| serde_json::json!({}));
                (e, Some(Position { offset: offset.parse()?, state }))
            }
            Some((e, None, _)) => (e, None),
            None => (0, None),
        };
        match &pos {
            Some(p) => info!(job, at = %reader.describe(p), "ingesting changes"),
            None => info!(job, tables = spec.tables().len(), "no position yet: taking a snapshot"),
        }
        Ok(Ingest { conn, s: s.clone(), job: job.into(), lease, tables: spec.tables().to_vec(), reader, epoch, pos, last_commit: Instant::now() })
    }

    fn staging(&self, what: &str, i: usize) -> String {
        staging_table(&self.job, what, i)
    }

    /// The first epoch: every table copied under one snapshot, and what the reader puts over it.
    fn snapshot_epoch(&mut self) -> Result<Outcome> {
        let started = Instant::now();
        let tables = self.tables.clone();
        let mut batch = Batch::new(&tables);
        let job = self.job.clone();
        let snap = move |i: usize| staging_table(&job, "snap", i);
        let (pos, changes) = self.reader.snapshot(self.conn, &snap, &mut batch)?;
        let (staged, _) = self.stage(&batch)?;
        let rows = self.commit(&pos, changes, 0, None, |me| {
            let mut n = 0;
            for (i, t) in me.tables.iter().enumerate() {
                let m = quote_table(&t.mirror)?;
                me.conn.execute(&format!("DELETE FROM {m}"), [])?;
                n += me.conn.execute(&format!("INSERT INTO {m} SELECT * FROM {}", me.staging("snap", i)), [])? as i64;
            }
            // Changes the reader read past the snapshot, over it.
            Ok(n + me.write(&batch, &staged)?)
        })?;
        self.drop_staging(&staged, true);
        self.reader.committed(self.conn, &pos);
        info!(job = %self.job, rows, changes, at = %self.reader.describe(&pos), secs = started.elapsed().as_secs_f64(), "snapshot committed");
        self.pos = Some(pos);
        Ok(Outcome::Committed)
    }

    fn epoch(&mut self) -> Result<Outcome> {
        let Some(pos) = self.pos.clone() else { return self.snapshot_epoch() };
        let started = Instant::now();
        let tables = self.tables.clone();
        let mut batch = Batch::new(&tables);
        let read = self.reader.read(self.conn, &pos, &mut batch)?;
        if batch.is_empty() {
            let refresh = read.txns > 0 || read.due || self.last_commit.elapsed() >= self.s.position_refresh;
            if read.pos == pos || !refresh {
                return Ok(Outcome::Idle);
            }
        }
        let (staged, filled) = self.stage(&batch)?;
        let rows_out = self.commit(&read.pos, read.changes, read.lag, read.newest_us, |me| me.write(&batch, &staged))?;
        crate::engine::crash_point("ingest_after_commit");
        self.reader.committed(self.conn, &read.pos);
        self.drop_staging(&staged, false);
        if let Some(m) = &self.s.metrics {
            m.record_job(
                &self.job,
                crate::metrics::EpochStats {
                    epoch: self.epoch,
                    lag: read.lag,
                    watermark_us: None,
                    max_event_time_us: read.newest_us,
                    rows_in: read.changes as u64,
                    rows_out: rows_out as u64,
                    rows_late: 0,
                    rows_dead: 0,
                    stage_ms: started.elapsed().as_millis(),
                    commit_freshness_s: read.newest_us.map(|u| (chrono::Utc::now().timestamp_micros() - u) as f64 / 1e6),
                },
            );
        }
        info!(
            job = %self.job,
            epoch = self.epoch,
            transactions = read.txns,
            skipped = read.skipped,
            changes = read.changes,
            rows_out,
            filled_from_mirror = filled,
            at = %self.reader.describe(&read.pos),
            "ingest epoch committed"
        );
        let full = read.full;
        self.pos = Some(read.pos);
        Ok(if full { Outcome::Full } else { Outcome::Committed })
    }

    fn drop_staging(&self, staged: &[bool], snapshot: bool) {
        for (i, s) in staged.iter().enumerate() {
            let mut names = Vec::new();
            if *s {
                names.extend([self.staging("chg", i), self.staging("row", i)]);
            }
            if snapshot {
                names.push(self.staging("snap", i));
            }
            for n in names {
                let _ = self.conn.execute_batch(&format!("DROP TABLE IF EXISTS {n}"));
            }
        }
    }

    /// Stages each changed table's rows in memory, its new rows already cast and their unchanged values filled
    /// from the mirror, before the lake transaction opens. Returns which tables have a staging table.
    fn stage(&self, batch: &Batch) -> Result<(Vec<bool>, usize)> {
        let mut out = Vec::new();
        let filled = batch.changes.iter().flat_map(|c| c.keys.values()).filter(|f| matches!(f, Final::Row { fill: Some(_), .. })).count();
        for (i, (t, tc)) in self.tables.iter().zip(&batch.changes).enumerate() {
            if tc.keys.is_empty() {
                out.push(false);
                continue;
            }
            let nk = t.key.len();
            let chg = self.staging("chg", i);
            let mut defs: Vec<String> = vec!["_row VARCHAR".into(), "_unch VARCHAR".into()];
            defs.extend((0..nk).map(|k| format!("k{k} VARCHAR")));
            defs.extend((0..nk).map(|k| format!("f{k} VARCHAR")));
            defs.extend((0..t.columns.len()).map(|c| format!("c{c} VARCHAR")));
            self.conn.execute_batch(&format!("CREATE OR REPLACE TABLE {chg} ({})", defs.join(", ")))?;
            {
                let mut app = self.conn.appender_to_catalog_and_db(&format!("ingest_{}_chg{i}", self.job), "memory", "main")?;
                for (key, f) in &tc.keys {
                    let mut row: Vec<Option<String>> = Vec::with_capacity(2 + 2 * nk + t.columns.len());
                    match f {
                        Final::Deleted => {
                            row.push(Some("f".into()));
                            row.push(None);
                            row.extend(key.iter().map(|k| Some(k.clone())));
                            row.extend((0..nk).map(|_| None));
                            row.extend((0..t.columns.len()).map(|_| None));
                        }
                        Final::Row { values, fill } => {
                            let unch: Vec<String> = values.iter().enumerate().filter(|(_, v)| **v == Value::Unchanged).map(|(c, _)| c.to_string()).collect();
                            row.push(Some("t".into()));
                            row.push((!unch.is_empty()).then(|| format!(",{},", unch.join(","))));
                            row.extend(key.iter().map(|k| Some(k.clone())));
                            match fill {
                                Some(fk) => row.extend(fk.iter().map(|k| Some(k.clone()))),
                                None => row.extend((0..nk).map(|_| None)),
                            }
                            row.extend(values.iter().map(|v| match v {
                                Value::Text(s) => Some(s.clone()),
                                Value::Null | Value::Unchanged => None,
                            }));
                        }
                    }
                    app.append_row(duckdb::appender_params_from_iter(row))?;
                }
            }
            // The new rows, cast; a value still unchanged comes from the stored row of its fill key.
            let kp = t.key_positions();
            let key_match = |a: &str, col: &str| -> String {
                kp.iter()
                    .enumerate()
                    .map(|(k, &p)| format!("m.{} = {}", ident(&t.columns[p].name), cast_sql(&t.columns[p], &format!("{a}.{col}{k}"))))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            let cols: Vec<String> = t
                .columns
                .iter()
                .enumerate()
                .map(|(c, col)| {
                    format!(
                        "CASE WHEN contains(s._unch, ',{c},') THEN m.{q} ELSE {} END AS {q}",
                        cast_sql(col, &format!("s.c{c}")),
                        q = ident(&col.name)
                    )
                })
                .collect();
            let fills = format!(
                "SELECT * FROM {} m WHERE EXISTS (SELECT 1 FROM {chg} s WHERE s._unch IS NOT NULL AND {})",
                quote_table(&t.mirror)?,
                key_match("s", "f")
            );
            self.conn
                .execute_batch(&format!(
                    "CREATE OR REPLACE TABLE {} AS SELECT {}, s._unch IS NOT NULL AND m.__found IS NULL AS __missing
                     FROM {chg} s LEFT JOIN (SELECT *, true AS __found FROM ({fills})) m ON s._unch IS NOT NULL AND {}
                     WHERE s._row = 't'",
                    self.staging("row", i),
                    cols.join(", "),
                    key_match("s", "f")
                ))
                .map_err(|e| cast_error(e, t))?;
            let missing: i64 = self.conn.query_row(&format!("SELECT count(*) FROM {} WHERE __missing", self.staging("row", i)), [], |r| r.get(0))?;
            if missing > 0 {
                return Err(stop(format!(
                    "{missing} updates of {} left a large (TOAST) value unchanged for rows the mirror does not have",
                    t.source_name()
                )));
            }
            out.push(true);
        }
        Ok((out, filled))
    }

    /// The mirror writes of an epoch, inside its lake transaction. Returns the rows written.
    fn write(&self, batch: &Batch, staged: &[bool]) -> Result<i64> {
        let mut n = 0;
        for (i, (t, tc)) in self.tables.iter().zip(&batch.changes).enumerate() {
            let m = quote_table(&t.mirror)?;
            if tc.truncated {
                n += self.conn.execute(&format!("DELETE FROM {m}"), [])? as i64;
            }
            if !staged[i] {
                continue;
            }
            let kp = t.key_positions();
            let on: Vec<String> = kp
                .iter()
                .enumerate()
                .map(|(k, &p)| format!("m.{} = {}", ident(&t.columns[p].name), cast_sql(&t.columns[p], &format!("c.k{k}"))))
                .collect();
            if !tc.truncated {
                n += self.conn.execute(&format!("DELETE FROM {m} m WHERE EXISTS (SELECT 1 FROM {} c WHERE {})", self.staging("chg", i), on.join(" AND ")), [])? as i64;
            }
            let cols: Vec<String> = t.columns.iter().map(|c| ident(&c.name)).collect();
            n += self.conn.execute(&format!("INSERT INTO {m} SELECT {} FROM {}", cols.join(", "), self.staging("row", i)), [])? as i64;
        }
        Ok(n)
    }

    /// One lake transaction: fencing, the writes, the checkpoint. Returns what `writes` returned.
    fn commit(&mut self, pos: &Position, rows_in: i64, lag: i64, newest_us: Option<i64>, writes: impl FnOnce(&Self) -> Result<i64>) -> Result<i64> {
        let epoch = self.epoch + 1;
        let c = self.conn;
        c.execute_batch("BEGIN")?;
        let res = (|| -> Result<i64> {
            let info = serde_json::json!({ "worker": self.s.worker, "jobs": { &self.job: epoch } });
            c.execute_batch(&format!(
                "CALL {LAKE}.set_commit_message('duckwake', {}, extra_info => {})",
                lit(&format!("ingest epoch by {}", self.s.worker)),
                lit(&info.to_string())
            ))?;
            let n = c
                .execute(
                    &format!("UPDATE {} SET last_epoch = ? WHERE owner_worker = ? AND lease_epoch = ?", crate::catalog::lease_table(&self.job)),
                    duckdb::params![epoch, self.s.worker, self.lease],
                )
                .map_err(|e| if e.to_string().contains("does not exist") { anyhow::Error::new(Fenced) } else { e.into() })?;
            if n == 0 {
                return Err(anyhow::Error::new(Fenced));
            }
            let written = writes(self)?;
            let offsets = serde_json::json!({ &self.job: { "0": pos.offset } });
            let ts = newest_us.map(|u| format!("make_timestamp({u})")).unwrap_or("NULL".into());
            c.execute(
                &format!(
                    "INSERT INTO {CTL}.checkpoints (job_id, epoch, source_offsets, watermark, state_backend, state_ref,
                       rows_in, rows_out, rows_late, rows_dead, max_event_time, lag, worker, committed_at, rows_sql)
                     VALUES (?, ?, ?, NULL, ?, ?, ?, ?, 0, 0, {ts}, ?, ?, now(), 0)"
                ),
                duckdb::params![
                    self.job,
                    epoch,
                    offsets.to_string(),
                    self.reader.backend(),
                    pos.state.to_string(),
                    if rows_in > 0 { rows_in } else { written },
                    written,
                    lag,
                    self.s.worker
                ],
            )?;
            crate::engine::crash_point("ingest_before_commit");
            Ok(written)
        })();
        match res {
            Ok(n) => match c.execute_batch("COMMIT") {
                Ok(()) => {
                    self.epoch = epoch;
                    self.last_commit = Instant::now();
                    Ok(n)
                }
                Err(e) => {
                    let _ = c.execute_batch("ROLLBACK");
                    let e = anyhow::Error::new(e);
                    // A conflict on the lease table: the lease moved while the transaction was open.
                    if crate::lake::is_conflict(&e) && crate::catalog::lease(c, &self.job).map(|(_, l)| l != self.lease).unwrap_or(true) {
                        return Err(anyhow::Error::new(Fenced));
                    }
                    Err(e)
                }
            },
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }
}

/// A memory table an ingest job of `job` stages in.
pub(crate) fn staging_table(job: &str, what: &str, i: usize) -> String {
    format!("memory.main.{}", ident(&format!("ingest_{job}_{what}{i}")))
}

/// A value that does not fit its mirror column stops the job, naming the table: the type mapping needs a look.
pub(crate) fn cast_error(e: duckdb::Error, t: &MirrorTable) -> anyhow::Error {
    let m = e.to_string();
    if m.contains("Conversion Error") || m.contains("Could not convert") {
        return stop(format!("a value of {} does not fit its mirror column: {m}", t.source_name()));
    }
    e.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::database::mirror::MirrorColumn;

    fn table() -> Vec<MirrorTable> {
        let col = |n: &str| MirrorColumn { name: n.into(), source_type: "text".into(), oid: 25, typmod: -1, duck_type: "VARCHAR".into() };
        vec![MirrorTable { schema: "public".into(), name: "t".into(), mirror: "lake.s.t".into(), columns: vec![col("id"), col("a"), col("big")], key: vec!["id".into()] }]
    }

    fn t(s: &str) -> Value {
        Value::Text(s.into())
    }

    fn row(b: &Batch, k: &str) -> Option<Final> {
        b.changes[0].keys.get(&vec![k.to_string()]).cloned()
    }

    #[test]
    fn changes_of_a_key_apply_in_order() {
        let tables = table();
        let mut b = Batch::new(&tables);
        b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("1"), t("x"), t("BIG")] }).unwrap();
        // An unchanged large value comes from the epoch's own earlier version.
        b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("1"), t("y"), Value::Unchanged] }).unwrap();
        assert_eq!(row(&b, "1"), Some(Final::Row { values: vec![t("1"), t("y"), t("BIG")], fill: None }));
        // A key change deletes the old key and carries the row over.
        b.apply(Change::Upsert { table: 0, old_key: Some(vec!["1".into()]), values: vec![t("2"), t("z"), Value::Unchanged] }).unwrap();
        assert_eq!(row(&b, "1"), Some(Final::Deleted));
        assert_eq!(row(&b, "2"), Some(Final::Row { values: vec![t("2"), t("z"), t("BIG")], fill: None }));
        b.apply(Change::Delete { table: 0, key: vec!["2".into()] }).unwrap();
        assert_eq!(row(&b, "2"), Some(Final::Deleted));
        // Re-inserted after its delete: a new row.
        b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("2"), t("w"), Value::Null] }).unwrap();
        assert_eq!(row(&b, "2"), Some(Final::Row { values: vec![t("2"), t("w"), Value::Null], fill: None }));
    }

    #[test]
    fn unchanged_values_not_in_the_epoch_come_from_the_mirror() {
        let tables = table();
        let mut b = Batch::new(&tables);
        b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("5"), t("a"), Value::Unchanged] }).unwrap();
        assert_eq!(row(&b, "5"), Some(Final::Row { values: vec![t("5"), t("a"), Value::Unchanged], fill: Some(vec!["5".into()]) }));
        // Moved to a new key: still filled from the old key's stored row.
        b.apply(Change::Upsert { table: 0, old_key: Some(vec!["5".into()]), values: vec![t("6"), t("b"), Value::Unchanged] }).unwrap();
        assert_eq!(row(&b, "6"), Some(Final::Row { values: vec![t("6"), t("b"), Value::Unchanged], fill: Some(vec!["5".into()]) }));
        assert_eq!(row(&b, "5"), Some(Final::Deleted));
        // Then set: no fill needed any more.
        b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("6"), t("c"), t("NEW")] }).unwrap();
        assert_eq!(row(&b, "6"), Some(Final::Row { values: vec![t("6"), t("c"), t("NEW")], fill: None }));
    }

    #[test]
    fn truncate_and_impossible_fills() {
        let tables = table();
        let mut b = Batch::new(&tables);
        b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("1"), t("x"), t("BIG")] }).unwrap();
        b.apply(Change::Truncate { tables: vec![0] }).unwrap();
        assert!(b.changes[0].truncated && b.changes[0].keys.is_empty());
        assert!(!b.is_empty(), "a truncate alone is a change");
        let e = b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("1"), t("y"), Value::Unchanged] }).unwrap_err();
        assert!(e.is::<Stop>() && e.to_string().contains("truncated"));
        let mut b = Batch::new(&tables);
        b.apply(Change::Delete { table: 0, key: vec!["3".into()] }).unwrap();
        let e = b.apply(Change::Upsert { table: 0, old_key: None, values: vec![t("3"), t("y"), Value::Unchanged] }).unwrap_err();
        assert!(e.is::<Stop>() && e.to_string().contains("deleted"));
        let e = b.key(0, &[Value::Null, t("a"), t("b")]).unwrap_err();
        assert!(e.is::<Stop>());
    }
}
