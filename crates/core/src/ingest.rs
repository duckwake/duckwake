//! The ingest job of a PostgreSQL source: it keeps one mirror table per source table, reading the source's
//! logical replication slot through DuckDB's postgres extension (`pg_logical_slot_peek_binary_changes`, so the
//! database keeps every change until DuckWake has committed it). The rules, from
//! `docs/design/database-cdc-sources.md`:
//!
//! 1. Whole transactions only: the slot returns whole transactions, and an epoch applies each one or none.
//! 2. The position is the end LSN of the last transaction applied (or a flush LSN the slot had nothing
//!    before), written to `__duckwake.checkpoints` in the epoch's DuckLake transaction with the mirror rows.
//! 3. The slot is advanced to that position only after the commit, so it never runs ahead of the lake; after a
//!    crash in between, the transactions the slot sends again end at or before the position and are skipped.
//! 4. A restart reads the position from the lake. A slot whose confirmed position is ahead of it, gone or
//!    invalidated has lost changes: the job stops and asks for a reset, which takes a new snapshot.
//! 5. Changes of one key apply in commit order; an epoch writes each touched key's last state.
//! 6. An update leaves unchanged large (TOAST) values out: they come from the key's previous state, in the epoch
//!    or the mirror. A changed primary key deletes the old key. TRUNCATE empties the mirror.
//!
//! The first epoch copies every table under one REPEATABLE READ snapshot taken after the slot was created, and
//! records the snapshot: transactions the copy already holds are skipped when the slot sends them.
use crate::lake::{ident, lit, quote_table, CTL, LAKE};
use crate::pgoutput::{self, format_lsn, parse_lsn, Message, Snapshot, Value};
use crate::pgsource::{PgOptions, PgTable};
use anyhow::{bail, Context, Result};
use duckdb::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use tracing::{info, warn};

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

fn stop(msg: String) -> anyhow::Error {
    anyhow::Error::new(Stop(msg))
}

/// What the worker gives an ingest job's thread.
#[derive(Clone)]
pub struct Settings {
    pub worker: String,
    /// How long an idle source may leave its position, and the slot, behind the database's WAL.
    pub position_refresh: Duration,
    pub metrics: Option<Arc<crate::metrics::Metrics>>,
}

/// An ingest job running on its own thread and connection, so a long snapshot never holds up the views.
pub struct Handle {
    pub lease: i64,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    failed: mpsc::Receiver<String>,
}

impl Handle {
    pub fn spawn(conn: Connection, settings: Settings, job: String, lease: i64, opts: PgOptions) -> Handle {
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, failed) = mpsc::channel();
        let flag = stop.clone();
        let thread = std::thread::Builder::new()
            .name(format!("ingest-{job}"))
            .spawn(move || run(conn, settings, job, lease, opts, flag, tx))
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

fn run(conn: Connection, s: Settings, job: String, lease: i64, opts: PgOptions, stop_flag: Arc<AtomicBool>, failed: mpsc::Sender<String>) {
    let interval = opts.interval().unwrap_or(Duration::from_secs(2));
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
                ingest = Some(Ingest::open(&conn, &s, &job, lease, &opts)?);
            }
            ingest.as_mut().unwrap().epoch()
        })();
        match res {
            Ok(outcome) => {
                backoff = Duration::from_secs(1);
                // A full read leaves more waiting in the slot: go on at once.
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
enum Final {
    Deleted,
    /// `fill`: the key whose stored row holds the values still `Unchanged`.
    Row { values: Vec<Value>, fill: Option<Vec<String>> },
}

/// One table's changes in an epoch: each touched key's last state.
#[derive(Debug, Default)]
struct TableChanges {
    truncated: bool,
    keys: HashMap<Vec<String>, Final>,
}

/// A change of one transaction, kept until its Commit arrives.
enum Change {
    Upsert { table: usize, old_key: Option<Vec<String>>, values: Vec<Value> },
    Delete { table: usize, key: Vec<String> },
    Truncate { tables: Vec<usize> },
}

/// Applies changes in commit order to the epoch's per-table states.
struct Batch<'a> {
    tables: &'a [PgTable],
    keys: Vec<Vec<usize>>,
    changes: Vec<TableChanges>,
}

impl<'a> Batch<'a> {
    fn new(tables: &'a [PgTable]) -> Self {
        Batch { tables, keys: tables.iter().map(|t| t.key_positions()).collect(), changes: tables.iter().map(|_| TableChanges::default()).collect() }
    }

    fn key(&self, t: usize, values: &[Value]) -> Result<Vec<String>> {
        self.keys[t]
            .iter()
            .map(|&i| match values.get(i) {
                Some(Value::Text(s)) => Ok(s.clone()),
                v => Err(stop(format!("a change of {} has no value for key column {}: {v:?}", self.tables[t].source_name(), self.tables[t].columns[i].name))),
            })
            .collect()
    }

    fn apply(&mut self, c: Change) -> Result<()> {
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

    fn is_empty(&self) -> bool {
        self.changes.iter().all(|c| !c.truncated && c.keys.is_empty())
    }
}

struct Ingest<'c> {
    conn: &'c Connection,
    s: Settings,
    job: String,
    lease: i64,
    opts: PgOptions,
    alias: String,
    epoch: i64,
    /// None until the snapshot is committed.
    lsn: Option<u64>,
    /// The copy's snapshot, until every transaction it may have seen has been read (`clear_after`).
    snapshot: Option<String>,
    clear_after: Option<u64>,
    last_commit: Instant,
    /// Relations the slot described, as the source's tables (None: one the source does not mirror).
    relations: HashMap<u32, Option<usize>>,
}

/// An idle source commits its position once it is this far behind, so the slot lets go of the WAL.
const POSITION_BYTES: u64 = 64 << 20;

impl<'c> Ingest<'c> {
    fn open(conn: &'c Connection, s: &Settings, job: &str, lease: i64, opts: &PgOptions) -> Result<Self> {
        let alias = crate::pgsource::attach(conn, job, opts, true)?;
        for t in &opts.tables {
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
        let (epoch, lsn, state) = match last {
            Some((e, l, st)) => (e, l.map(|l| l.parse::<u64>()).transpose()?, st),
            None => (0, None, None),
        };
        let state: serde_json::Value = state.map(|s| serde_json::from_str(&s)).transpose()?.unwrap_or_default();
        let snapshot = state.get("snapshot").and_then(|v| v.as_str()).map(String::from);
        let clear_after = state.get("clear_after").and_then(|v| v.as_str()).map(parse_lsn).transpose()?;
        match lsn {
            Some(l) => info!(job, slot = %opts.slot, lsn = %format_lsn(l), "ingesting from the slot"),
            None => info!(job, slot = %opts.slot, tables = opts.tables.len(), "no position yet: taking a snapshot"),
        }
        Ok(Ingest {
            conn,
            s: s.clone(),
            job: job.into(),
            lease,
            opts: opts.clone(),
            alias,
            epoch,
            lsn,
            snapshot,
            clear_after,
            last_commit: Instant::now(),
            relations: HashMap::new(),
        })
    }

    fn slot_lit(&self) -> String {
        lit(&self.opts.slot)
    }

    fn staging(&self, what: &str, i: usize) -> String {
        format!("memory.main.{}", ident(&format!("ingest_{}_{what}{i}", self.job)))
    }

    /// The first epoch: every table copied under one snapshot taken after the slot was created.
    fn snapshot_epoch(&mut self) -> Result<Outcome> {
        let started = Instant::now();
        let slot = crate::pgsource::rows(self.conn, &self.alias, &format!("SELECT wal_status FROM pg_replication_slots WHERE slot_name = {}", self.slot_lit()))?;
        match slot.first().map(|r| r[0].as_deref()) {
            Some(Some("lost")) => {
                warn!(job = %self.job, slot = %self.opts.slot, "slot was invalidated; creating it again for the snapshot");
                crate::pgsource::execute(self.conn, &self.alias, &format!("SELECT pg_drop_replication_slot({})", self.slot_lit()))?;
                crate::pgsource::create_slot(self.conn, &self.alias, &self.opts.slot)?;
            }
            Some(_) => {}
            None => crate::pgsource::create_slot(self.conn, &self.alias, &self.opts.slot)?,
        }
        // One DuckDB transaction is one PostgreSQL REPEATABLE READ transaction: every postgres_query in it reads
        // the same snapshot. The rows land in memory first, so the lake transaction stays short.
        self.conn.execute_batch("BEGIN")?;
        let copied = (|| -> Result<(String, u64)> {
            let r = crate::pgsource::row(
                self.conn,
                &self.alias,
                &format!("SELECT pg_current_snapshot()::text, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = {}", self.slot_lit()),
            )?;
            let snap = r[0].clone().context("snapshot")?;
            let lsn = parse_lsn(r[1].as_deref().context("the slot has no confirmed position")?)?;
            for (i, t) in self.opts.tables.iter().enumerate() {
                let cols: Vec<String> = t.columns.iter().map(|c| format!("{} AS {}", crate::pgsource::cast_sql(c, &ident(&c.name)), ident(&c.name))).collect();
                self.conn
                    .execute(
                        &format!(
                            "CREATE OR REPLACE TABLE {} AS SELECT {} FROM postgres_query({}, {})",
                            self.staging("snap", i),
                            cols.join(", "),
                            lit(self.alias.trim_matches('"')),
                            lit(&crate::pgsource::copy_sql(t))
                        ),
                        [],
                    )
                    .map_err(|e| cast_error(e, t))?;
            }
            Ok((snap, lsn))
        })();
        let (snap, lsn) = match copied {
            Ok(c) => {
                self.conn.execute_batch("COMMIT")?;
                c
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        };
        let state = serde_json::json!({ "snapshot": snap });
        let rows = self.commit(lsn, &state, 0, 0, None, |me| {
            let mut n = 0;
            for (i, t) in me.opts.tables.iter().enumerate() {
                let m = quote_table(&t.mirror)?;
                me.conn.execute(&format!("DELETE FROM {m}"), [])?;
                n += me.conn.execute(&format!("INSERT INTO {m} SELECT * FROM {}", me.staging("snap", i)), [])? as i64;
            }
            Ok(n)
        })?;
        for i in 0..self.opts.tables.len() {
            let _ = self.conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", self.staging("snap", i)));
        }
        self.lsn = Some(lsn);
        self.snapshot = Some(snap);
        info!(job = %self.job, rows, lsn = %format_lsn(lsn), secs = started.elapsed().as_secs_f64(), "snapshot committed");
        Ok(Outcome::Committed)
    }

    fn epoch(&mut self) -> Result<Outcome> {
        let Some(lsn) = self.lsn else { return self.snapshot_epoch() };
        let started = Instant::now();
        // Before the read: the flush position (everything before it the read below decodes), and with a
        // snapshot, the oldest transaction still running.
        let r = crate::pgsource::row(
            self.conn,
            &self.alias,
            &format!(
                "SELECT pg_current_wal_flush_lsn()::text, pg_current_wal_insert_lsn()::text,
                        pg_snapshot_xmin(pg_current_snapshot())::text, s.confirmed_flush_lsn::text, s.wal_status
                 FROM (SELECT 1) x LEFT JOIN pg_replication_slots s ON s.slot_name = {}",
                self.slot_lit()
            ),
        )?;
        let flush = parse_lsn(r[0].as_deref().context("flush lsn")?)?;
        let insert = parse_lsn(r[1].as_deref().context("insert lsn")?)?;
        let xmin_now: u64 = r[2].as_deref().context("xmin")?.parse()?;
        let reset = format!("reset it to take a new snapshot: duckwake reset {} --to-epoch 0", self.job);
        let Some(confirmed) = r[3].as_deref() else {
            return Err(stop(format!("replication slot {} is gone, so the changes since the last epoch are lost; {reset}", self.opts.slot)));
        };
        if r[4].as_deref() == Some("lost") {
            return Err(stop(format!(
                "replication slot {} was invalidated (max_slot_wal_keep_size), so the changes since the last epoch are lost; {reset}",
                self.opts.slot
            )));
        }
        let confirmed = parse_lsn(confirmed)?;
        if confirmed > lsn {
            return Err(stop(format!(
                "replication slot {} has confirmed {}, past the lake's position {}: something else advanced it, and those changes are lost; {reset}",
                self.opts.slot,
                format_lsn(confirmed),
                format_lsn(lsn)
            )));
        }

        let mut stmt = self.conn.prepare(&format!(
            "SELECT data FROM postgres_query({}, {})",
            lit(self.alias.trim_matches('"')),
            lit(&format!(
                "SELECT data FROM pg_logical_slot_peek_binary_changes({}, NULL, {}, 'proto_version', '1', 'publication_names', {})",
                self.slot_lit(),
                self.opts.max_changes,
                lit(&self.opts.publication)
            ))
        ))?;
        let messages = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
        let full = messages.len() >= self.opts.max_changes;
        let tables = self.opts.tables.clone();
        let mut batch = Batch::new(&tables);
        let snapshot = self.snapshot.as_deref().map(Snapshot::parse).transpose()?;
        let (mut pos, mut txns, mut skipped, mut rows_in, mut newest_us) = (lsn, 0u64, 0u64, 0i64, None);
        // The transaction being read: whether it applies, and its changes.
        let mut txn: Option<(bool, i64, Vec<Change>)> = None;
        for data in &messages {
            match pgoutput::decode(data)? {
                Message::Begin { final_lsn, commit_us, xid } => {
                    // Applied before a crash (its commit is before the position), or already in the copy.
                    let applies = final_lsn >= lsn && !snapshot.as_ref().is_some_and(|s| s.sees(xid));
                    txn = Some((applies, commit_us, Vec::new()));
                }
                Message::Commit { end_lsn, .. } => {
                    let (applies, commit_us, changes) = txn.take().context("a Commit without its Begin")?;
                    txns += 1;
                    if applies {
                        for c in changes {
                            batch.apply(c)?;
                        }
                        newest_us = Some(commit_us);
                    } else {
                        skipped += 1;
                    }
                    pos = pos.max(end_lsn);
                }
                Message::Relation { id, schema, name, columns, .. } => {
                    let table = tables.iter().position(|t| t.schema == schema && t.name == name);
                    if let Some(i) = table {
                        check_columns(&tables[i], &columns)?;
                    }
                    self.relations.insert(id, table);
                }
                Message::Insert { rel, new } => {
                    if let Some(t) = self.table(rel, &mut txn)? {
                        rows_in += 1;
                        txn.as_mut().unwrap().2.push(Change::Upsert { table: t, old_key: None, values: new });
                    }
                }
                Message::Update { rel, old, new } => {
                    if let Some(t) = self.table(rel, &mut txn)? {
                        rows_in += 1;
                        let old_key = old.map(|o| batch.key(t, &o)).transpose()?;
                        txn.as_mut().unwrap().2.push(Change::Upsert { table: t, old_key, values: new });
                    }
                }
                Message::Delete { rel, old } => {
                    if let Some(t) = self.table(rel, &mut txn)? {
                        rows_in += 1;
                        let key = batch.key(t, &old)?;
                        txn.as_mut().unwrap().2.push(Change::Delete { table: t, key });
                    }
                }
                Message::Truncate { rels } => {
                    let mut ts = Vec::new();
                    for rel in rels {
                        if let Some(t) = self.table(rel, &mut txn)? {
                            ts.push(t);
                        }
                    }
                    if let Some(x) = txn.as_mut() {
                        x.2.push(Change::Truncate { tables: ts });
                    }
                }
                Message::Other(_) => {}
            }
        }
        // Nothing before the flush position was left out: the slot can move there.
        if !full {
            pos = pos.max(flush);
        }
        // The snapshot is needed until every transaction that was running when it was taken has ended and been
        // read: once the oldest running one is newer than its xmax, every one ends before `insert`.
        let mut clear_after = self.clear_after;
        if let Some(s) = &snapshot {
            if clear_after.is_none() && xmin_now >= s.xmax {
                clear_after = Some(insert);
            }
        }
        let forget = clear_after.is_some_and(|c| pos >= c);
        let state = match (forget, &self.snapshot) {
            (false, Some(snap)) => serde_json::json!({ "snapshot": snap, "clear_after": clear_after.map(format_lsn) }),
            _ => serde_json::json!({}),
        };
        if batch.is_empty() {
            let refresh = txns > 0 || self.last_commit.elapsed() >= self.s.position_refresh || pos.saturating_sub(lsn) >= POSITION_BYTES;
            if pos <= lsn || !refresh {
                return Ok(Outcome::Idle);
            }
        }
        let (staged, filled) = self.stage(&batch)?;
        let lag = flush.saturating_sub(pos) as i64;
        let rows_out = self.commit(pos, &state, rows_in, lag, newest_us, |me| me.write(&batch, &staged))?;
        crate::engine::crash_point("ingest_after_commit");
        self.lsn = Some(pos);
        if forget {
            info!(job = %self.job, "every transaction the snapshot saw has been read; forgetting it");
            self.snapshot = None;
            clear_after = None;
        }
        self.clear_after = clear_after;
        self.advance(pos);
        for (i, _) in staged.iter().enumerate().filter(|(_, s)| **s) {
            let _ = self.conn.execute_batch(&format!("DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {}", self.staging("chg", i), self.staging("row", i)));
        }
        if let Some(m) = &self.s.metrics {
            m.record_job(
                &self.job,
                crate::metrics::EpochStats {
                    epoch: self.epoch,
                    lag,
                    watermark_us: None,
                    max_event_time_us: newest_us,
                    rows_in: rows_in as u64,
                    rows_out: rows_out as u64,
                    rows_late: 0,
                    rows_dead: 0,
                    stage_ms: started.elapsed().as_millis(),
                    commit_freshness_s: newest_us.map(|u| (chrono::Utc::now().timestamp_micros() - u) as f64 / 1e6),
                },
            );
        }
        info!(
            job = %self.job,
            epoch = self.epoch,
            transactions = txns,
            skipped,
            changes = rows_in,
            rows_out,
            filled_from_mirror = filled,
            lsn = %format_lsn(pos),
            "ingest epoch committed"
        );
        Ok(if full { Outcome::Full } else { Outcome::Committed })
    }

    /// The source table a change belongs to; None for a table the source does not mirror.
    fn table(&self, rel: u32, txn: &mut Option<(bool, i64, Vec<Change>)>) -> Result<Option<usize>> {
        let r = self.relations.get(&rel).context("a change for a relation the slot did not describe")?;
        let Some((applies, _, _)) = txn.as_ref() else { bail!("a change outside a transaction") };
        Ok(r.filter(|_| *applies))
    }

    /// Stages each changed table's rows in memory, its new rows already cast and their unchanged values filled
    /// from the mirror, before the lake transaction opens. Returns which tables have a staging table.
    fn stage(&self, batch: &Batch) -> Result<(Vec<bool>, usize)> {
        let mut out = Vec::new();
        let filled = batch.changes.iter().flat_map(|c| c.keys.values()).filter(|f| matches!(f, Final::Row { fill: Some(_), .. })).count();
        for (i, (t, tc)) in self.opts.tables.iter().zip(&batch.changes).enumerate() {
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
                    .map(|(k, &p)| format!("m.{} = {}", ident(&t.columns[p].name), crate::pgsource::cast_sql(&t.columns[p], &format!("{a}.{col}{k}"))))
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
                        crate::pgsource::cast_sql(col, &format!("s.c{c}")),
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
        for (i, (t, tc)) in self.opts.tables.iter().zip(&batch.changes).enumerate() {
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
                .map(|(k, &p)| format!("m.{} = {}", ident(&t.columns[p].name), crate::pgsource::cast_sql(&t.columns[p], &format!("c.k{k}"))))
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
    fn commit(
        &mut self,
        pos: u64,
        state: &serde_json::Value,
        rows_in: i64,
        lag: i64,
        newest_us: Option<i64>,
        writes: impl FnOnce(&Self) -> Result<i64>,
    ) -> Result<i64> {
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
            let offsets = serde_json::json!({ &self.job: { "0": pos } });
            let ts = newest_us.map(|u| format!("make_timestamp({u})")).unwrap_or("NULL".into());
            c.execute(
                &format!(
                    "INSERT INTO {CTL}.checkpoints (job_id, epoch, source_offsets, watermark, state_backend, state_ref,
                       rows_in, rows_out, rows_late, rows_dead, max_event_time, lag, worker, committed_at, rows_sql)
                     VALUES (?, ?, ?, NULL, 'pgoutput', ?, ?, ?, 0, 0, {ts}, ?, ?, now(), 0)"
                ),
                duckdb::params![self.job, epoch, offsets.to_string(), state.to_string(), if rows_in > 0 { rows_in } else { written }, written, lag, self.s.worker],
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

    /// Lets the slot release what the lake now holds. A failure only delays it: the next epoch skips what
    /// the slot sends again, and advances it then.
    fn advance(&self, pos: u64) {
        let sql = format!("SELECT end_lsn::text FROM pg_replication_slot_advance({}, {})", self.slot_lit(), lit(&format_lsn(pos)));
        if let Err(e) = crate::pgsource::row(self.conn, &self.alias, &sql) {
            warn!(job = %self.job, error = %format!("{e:#}"), "advancing the slot failed; the next epoch retries");
        }
    }
}

/// The columns a `Relation` message describes must be the ones the mirror was created with.
fn check_columns(t: &PgTable, cols: &[pgoutput::Column]) -> Result<()> {
    let same = cols.len() == t.columns.len() && cols.iter().zip(&t.columns).all(|(a, b)| a.name == b.name && a.type_oid == b.oid);
    if !same {
        let now: Vec<String> = cols.iter().map(|c| format!("{} (type {})", c.name, c.type_oid)).collect();
        let was: Vec<String> = t.columns.iter().map(|c| format!("{} {}", c.name, c.pg_type)).collect();
        return Err(stop(format!(
            "the columns of {} changed in PostgreSQL: now {}, the mirror has {}. Schema changes are not applied yet: \
             drop the source and create it again",
            t.source_name(),
            now.join(", "),
            was.join(", ")
        )));
    }
    Ok(())
}

/// A value that does not fit its mirror column stops the job, naming the table: the type mapping needs a look.
fn cast_error(e: duckdb::Error, t: &PgTable) -> anyhow::Error {
    let m = e.to_string();
    if m.contains("Conversion Error") || m.contains("Could not convert") {
        return stop(format!("a value of {} does not fit its mirror column: {m}", t.source_name()));
    }
    e.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgsource::PgColumn;

    fn table() -> Vec<PgTable> {
        let col = |n: &str| PgColumn { name: n.into(), pg_type: "text".into(), oid: 25, typmod: -1, duck_type: "VARCHAR".into() };
        vec![PgTable { schema: "public".into(), name: "t".into(), mirror: "lake.s.t".into(), columns: vec![col("id"), col("a"), col("big")], key: vec!["id".into()] }]
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
