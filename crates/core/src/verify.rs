//! Differential check for exactly-once: recompute the view as one batch query over everything the job has
//! committed (Kafka offsets in its latest checkpoint) and compare with the sink as multisets, so a
//! duplicate or a gap shows up as a difference.
//!
//! Lateness depends on how events fell into epochs, which differs between runs with crashes, so the batch
//! input excludes exactly the rows the job routed to its late-data table; those must in turn be unique.
//!
//! Once retention has deleted the topic's oldest events, the check covers what is still retained: it starts
//! at the oldest checkpoint whose offsets are all retained and compares only sink rows that depend on events
//! read after it (append sinks: later epochs; windowed sinks: windows starting after every event time the
//! job had seen by then). Global aggregates fold in all history, so they need the whole topic.
//!
//! A lake input (a lake table or another view's table, read through its change feed) needs no retention:
//! its batch input is the table as of the snapshot in the latest checkpoint (`AT (VERSION => P)`), the rows
//! written after the view was created, with `_commit_ts` computed as the job computes it. Rows updated or
//! deleted since are simply not there, which is what the view's retractions must have done to its table.
use crate::catalog::{self, state_table};
use crate::model::{LakeInput, SourceDef, COMMIT_TS, LAKE_BACKFILL, LAKE_POSITION};
use crate::kafka;
use crate::lake::{ident, lit, quote_table, CTL};
use crate::decode::{decode_sql, invalid_json_where, tombstones_sql, IS_JSON_OBJECT};
use anyhow::{bail, Context, Result};
use duckdb::Connection;
use tokio::runtime::Handle;

#[derive(Debug)]
pub struct Report {
    pub job: String,
    /// Set when retention forced a partial check: only output of epochs after this one was compared.
    pub from_epoch: Option<i64>,
    pub epoch: i64,
    pub watermark: String,
    pub kafka_rows: usize,
    /// Rows of the view's lake inputs (lake tables and views) the batch read.
    pub lake_rows: i64,
    pub expected_rows: i64,
    pub sink_rows: i64,
    pub missing_from_sink: i64,
    pub unexpected_in_sink: i64,
    pub dead_expected: i64,
    pub dead_mismatch: i64,
    pub late_rows: i64,
    pub late_duplicates: i64,
    pub closed_windows_left_in_state: i64,
    /// When the output differs: up to five rows on each side, so a failure shows the values, not just counts.
    pub differing_rows: Vec<String>,
}

impl Report {
    /// What the batch recomputation read: `12000 events`, `340 lake rows`, or both.
    pub fn inputs(&self) -> String {
        match (self.kafka_rows, self.lake_rows) {
            (k, 0) => format!("{k} events"),
            (0, l) => format!("{l} lake rows"),
            (k, l) => format!("{k} events and {l} lake rows"),
        }
    }

    pub fn ok(&self) -> bool {
        self.missing_from_sink == 0
            && self.unexpected_in_sink == 0
            && self.dead_mismatch == 0
            && self.late_duplicates == 0
            && self.closed_windows_left_in_state == 0
    }
}

fn count(conn: &Connection, sql: &str) -> Result<i64> {
    Ok(conn.query_row(&format!("SELECT count(*) FROM ({sql})"), [], |r| r.get(0))?)
}

/// How far apart, relative to the larger, two DOUBLE or FLOAT values may be and still match: 10 significant
/// digits. Floating-point sums merged across epochs add in another order than one batch query does, and the
/// statistics computed from running sums (`corr`, `regr_slope`, the variances) lose digits to cancellation
/// that DuckDB's online algorithms do not: over `scripts/backfill_test.sh`'s data they agree to 12-14 digits.
pub const FLOAT_TOLERANCE: f64 = 1e-9;

/// Compares the rows of `expected` and `sink` as multisets over `columns`: every column exactly, except DOUBLE
/// and FLOAT ones, which match within `FLOAT_TOLERANCE` of the larger value, or of 1 below it. The floor is for
/// results that are exactly 0: a view that retracts reaches them by adding and subtracting sums, which leaves a
/// residue (`covar_samp` of 5e-15 against 0 in `backfill_test.sh`'s `bf_quiet`, CI 2026-11-22). Returns how many expected rows the sink lacks, how
/// many sink rows were not expected, and up to five of each.
///
/// Rounding both sides to 10 digits instead, as this did before, fails whenever two values a hair apart fall on
/// either side of a rounding step, which `bf_spread` in `backfill_test.sh` hit about one run in six. Rows that
/// differ exactly are set aside first; of those, rows equal in their exact columns are paired in the order of
/// their floats, and a pair whose floats are all within the tolerance matches.
fn compare(conn: &Connection, expected: &str, sink: &str, columns: &[(String, String)]) -> Result<(i64, i64, Vec<String>)> {
    let is_float = |t: &str| t == "DOUBLE" || t == "FLOAT";
    let cols: Vec<String> = columns.iter().map(|(c, _)| ident(c)).collect();
    let exact: Vec<String> = columns.iter().filter(|(_, t)| !is_float(t)).map(|(c, _)| ident(c)).collect();
    let floats: Vec<String> = columns.iter().filter(|(_, t)| is_float(t)).map(|(c, _)| ident(c)).collect();
    let list = cols.join(", ");
    let (m, u) = ("memory.main.v_missing", "memory.main.v_unexpected");
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {m} AS SELECT {list} FROM {expected} EXCEPT ALL SELECT {list} FROM {sink};
         CREATE OR REPLACE TABLE {u} AS SELECT {list} FROM {sink} EXCEPT ALL SELECT {list} FROM {expected};"
    ))?;
    if !floats.is_empty() && count(conn, &format!("SELECT * FROM {m}"))? > 0 && count(conn, &format!("SELECT * FROM {u}"))? > 0 {
        let by = if exact.is_empty() { String::new() } else { format!("PARTITION BY {}", exact.join(", ")) };
        let order = floats.join(", ");
        let close = |c: &String| {
            format!("(a.{c} IS NOT DISTINCT FROM b.{c} OR abs(a.{c} - b.{c}) <= {FLOAT_TOLERANCE:e} * greatest(abs(a.{c}), abs(b.{c}), 1))")
        };
        let on: Vec<String> = exact
            .iter()
            .map(|c| format!("a.{c} IS NOT DISTINCT FROM b.{c}"))
            .chain(["a.__rn = b.__rn".to_string()])
            .chain(floats.iter().map(close))
            .collect();
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {m} AS SELECT *, row_number() OVER ({by} ORDER BY {order}) AS __rn FROM {m};
             CREATE OR REPLACE TABLE {u} AS SELECT *, row_number() OVER ({by} ORDER BY {order}) AS __rn FROM {u};
             CREATE OR REPLACE TABLE memory.main.v_close AS
               SELECT a.rowid AS a_id, b.rowid AS b_id FROM {m} a JOIN {u} b ON {};
             DELETE FROM {m} WHERE rowid IN (SELECT a_id FROM memory.main.v_close);
             DELETE FROM {u} WHERE rowid IN (SELECT b_id FROM memory.main.v_close);",
            on.join(" AND ")
        ))?;
    }
    let (missing, unexpected) = (format!("SELECT {list} FROM {m}"), format!("SELECT {list} FROM {u}"));
    let (n_missing, n_unexpected) = (count(conn, &missing)?, count(conn, &unexpected)?);
    let mut rows = Vec::new();
    if n_missing > 0 || n_unexpected > 0 {
        rows.extend(sample(conn, "missing", &missing)?);
        rows.extend(sample(conn, "unexpected", &unexpected)?);
    }
    Ok((n_missing, n_unexpected, rows))
}

/// Up to five rows of `sql`, each printed as a struct and prefixed with `label`.
fn sample(conn: &Connection, label: &str, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("SELECT t::VARCHAR FROM ({sql}) t LIMIT 5"))?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    rows.map(|r| Ok(format!("{label}: {}", r?))).collect()
}

/// A transaction that rolls back unless `commit` ends it: an error between BEGIN and COMMIT must not leave the
/// connection, which the SQL functions share, inside a failed transaction.
struct Txn<'a> {
    conn: &'a Connection,
    open: bool,
}

impl<'a> Txn<'a> {
    fn begin(conn: &'a Connection) -> Result<Self> {
        conn.execute_batch("BEGIN")?;
        Ok(Txn { conn, open: true })
    }

    fn commit(mut self) -> Result<()> {
        self.open = false;
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }
}

impl Drop for Txn<'_> {
    fn drop(&mut self) {
        if self.open {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

/// The columns of a view's first input as its staged rows carry them (a window's columns left out of a windowed
/// view's), quoted: the columns its late-data table holds.
fn plan_stream_columns(plan: &crate::plan::Plan, def: &crate::model::JobDef, sources: &std::collections::HashMap<String, SourceDef>) -> Result<Vec<String>> {
    let name = plan.sources().into_iter().next().context("a view without an input")?;
    let columns = match (sources.get(&name), def.lake_inputs.iter().find(|l| l.name == name)) {
        (Some(s), _) => s.options.columns.clone(),
        (None, Some(l)) => l.columns.clone(),
        (None, None) => bail!("input {name} is gone"),
    };
    Ok(columns
        .iter()
        .filter(|(c, _)| !(plan.is_windowed() && crate::circuit::codegen::is_window_column(c)))
        .map(|(c, _)| ident(c))
        .collect())
}

pub fn verify(conn: &Connection, rt: &Handle, job: &str) -> Result<Report> {
    let (def, _) = catalog::load_job(conn, job)?;
    let sources = catalog::view_sources(&def, catalog::load_sources(conn)?);
    let plan = catalog::compile(conn, job, &def.sql, &sources, &def.lake_inputs, def.upsert.as_ref())?;
    // The view's inputs, the first holding a windowed view's only stream: Kafka sources or lake inputs.
    let names = plan.sources();
    let lakes: Vec<Option<&LakeInput>> = names.iter().map(|n| def.lake_inputs.iter().find(|l| &l.name == n)).collect();
    let srcs: Vec<Option<&SourceDef>> = names
        .iter()
        .zip(&lakes)
        .map(|(n, l)| if l.is_some() { Ok(None) } else { sources.get(n).map(Some).with_context(|| format!("source {n} is gone")) })
        .collect::<Result<_>>()?;
    // A windowed view over a lake input has an exact watermark, so it has no late rows to reconstruct, unless
    // its input updates or deletes: a retraction for a window already closed is a late correction.
    if plan.is_windowed() && def.late_data.is_none() {
        if lakes[0].is_none() {
            bail!("verify needs a late-data table for windowed views (dropped late rows cannot be reconstructed)");
        }
        let dropped: i64 = conn.query_row(
            &format!("SELECT coalesce(sum(rows_late), 0) FROM {CTL}.checkpoints WHERE job_id = ?"),
            [job],
            |r| r.get(0),
        )?;
        if dropped > 0 {
            bail!(
                "{dropped} late corrections (changes to windows already closed) were counted and dropped, and the windows still \
                 hold what they changed; verify needs a late-data table to reconstruct them (replace the view with --late-data)"
            );
        }
    }

    let earliest = srcs
        .iter()
        .map(|s| s.map_or(Ok(Default::default()), |s| rt.block_on(kafka::earliest_offsets(&s.options))))
        .collect::<Result<Vec<_>>>()?;
    let truncated = earliest.iter().any(|e| e.values().any(|&o| o > 0));
    if truncated && plan.needs_whole_topic() {
        let topics: Vec<&str> = srcs.iter().flatten().map(|s| s.options.topic.as_str()).collect();
        bail!("retention has deleted the oldest events of {topics:?}, and a {} view depends on all of them", plan.kind_name());
    }

    // Snapshot the lake side in one transaction so checkpoint, sink, late rows and state agree.
    let txn = Txn::begin(conn)?;
    let ckpt = catalog::latest_checkpoint(conn, job, &names[0])?.context("job has no checkpoint yet")?;
    // Every source's offsets in that checkpoint row (and in each older one: they are the same rows).
    let ends: Vec<_> = names.iter().map(|n| Ok(catalog::latest_checkpoint(conn, job, n)?.unwrap_or_default().offsets)).collect::<Result<_>>()?;
    let (base, sink_filter) = if truncated {
        let all = names.iter().map(|n| catalog::all_checkpoints(conn, job, n)).collect::<Result<Vec<_>>>()?;
        let retained = |i: usize| {
            all.iter().zip(&earliest).all(|(cs, e)| e.iter().all(|(p, &o)| cs[i].offsets.get(p).copied().unwrap_or(0) >= o))
        };
        let i = (0..all[0].len()).find(|&i| retained(i)).with_context(|| {
            format!("no checkpoint left starts inside the retained range of the view's topics (earliest offsets {earliest:?})")
        })?;
        let filter = if plan.is_windowed() {
            // Every event read up to the base checkpoint has an event time at or before this bound.
            let bound = all[0].iter().filter(|c| c.epoch <= all[0][i].epoch).filter_map(|c| c.max_event_time_us).max();
            bound.map(|b| format!("window_start > make_timestamp({b})")).unwrap_or("true".into())
        } else {
            format!("_duckwake_epoch > {}", all[0][i].epoch)
        };
        (Some((all[0][i].epoch, all.iter().map(|cs| cs[i].offsets.clone()).collect::<Vec<_>>())), filter)
    } else {
        (None, "true".into())
    };
    let sink_cols: Vec<String> = plan.sink_columns.iter().map(|(c, _)| ident(c)).collect();
    let sink_cols = sink_cols.join(", ");
    // A counted table holds each row once with its multiplicity: as a multiset, that many rows.
    let mut sink = quote_table(&def.sink)?;
    if crate::circuit::Circuit::compile(&def.view())?.counts_rows() {
        sink = format!("{sink}, range({})", ident(crate::circuit::ops::COUNT_COLUMN));
    }
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE memory.main.v_sink AS SELECT {sink_cols} FROM {sink} WHERE {sink_filter};
         CREATE OR REPLACE TABLE memory.main.v_dead AS SELECT source, source_partition AS _partition, source_offset AS _offset
           FROM {CTL}.dead_letter WHERE job_id = '{job}';"
    ))?;
    // Late rows: those added to a closed window (+1) are left out of the batch input; over an input that
    // retracts, those taken out of one (-1, a late correction) are added back, as the closed window still counts
    // them (`v_late_back`).
    let late_stream_cols: Vec<String> = plan_stream_columns(&plan, &def, &sources)?;
    if let Some(late) = &def.late_data {
        let weight = if plan.retracts() { ident(crate::circuit::ops::LATE_WEIGHT) } else { "1".to_string() };
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE memory.main.v_late AS
               SELECT _source_partition AS _partition, _source_offset AS _offset, window_start FROM {t} WHERE {weight} > 0;
             CREATE OR REPLACE TABLE memory.main.v_late_back AS
               SELECT {cols}, window_start, window_end FROM {t} WHERE {weight} < 0;",
            t = quote_table(late)?,
            cols = if late_stream_cols.is_empty() { "NULL AS __none".to_string() } else { late_stream_cols.join(", ") },
        ))?;
    } else {
        conn.execute_batch("CREATE OR REPLACE TABLE memory.main.v_late (_partition INTEGER, _offset BIGINT, window_start TIMESTAMP)")?;
    }
    if plan.is_windowed() {
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE memory.main.v_state AS SELECT __ws, __we FROM {}", state_table(job)))?;
    }
    txn.commit()?;

    // Each source's events from the base checkpoint (or the start) to the latest one, decoded as the job did.
    let from: Vec<_> = (0..srcs.len()).map(|k| base.as_ref().map(|(_, b)| b[k].clone()).unwrap_or_default()).collect();
    let in_range = |k: usize| -> String {
        let range: Vec<String> = from[k].iter().map(|(p, o)| format!("(_partition = {p} AND _offset >= {o})")).collect();
        if range.is_empty() { "true".into() } else { range.join(" OR ") }
    };
    if base.is_some() {
        // Dead and late rows before the base checkpoint come from events no longer retained.
        let keep: Vec<String> = names.iter().enumerate().map(|(k, n)| format!("(source = {} AND ({}))", lit(n), in_range(k))).collect();
        conn.execute_batch(&format!("DELETE FROM memory.main.v_dead WHERE NOT ({})", keep.join(" OR ")))?;
        if def.late_data.is_some() {
            conn.execute_batch(&format!("DELETE FROM memory.main.v_late WHERE NOT ({})", in_range(0)))?;
        }
    }
    let (mut kafka_rows, mut lake_rows) = (0, 0);
    let mut dead_expected = vec!["SELECT NULL::VARCHAR AS source, NULL::INTEGER AS _partition, NULL::BIGINT AS _offset WHERE false".to_string()];
    for (k, src) in srcs.iter().enumerate() {
        let Some(src) = src else {
            // A lake input: the table as of the checkpoint's snapshot. Every row of it for a backfilled view,
            // up to the backfill's cursor while it runs; else the rows written after the view's floor.
            let l = lakes[k].unwrap();
            let p = ends[k].get(&LAKE_POSITION).copied().or(def.start_snapshot).context("lake input without a position")?;
            let mut keep: Vec<String> = def.floor().map(|f| format!("t.snapshot_id > {f}")).into_iter().collect();
            keep.extend(ends[k].get(&LAKE_BACKFILL).map(|c| format!("t.rowid <= {c}")));
            let keep = if keep.is_empty() { "true".to_string() } else { keep.join(" AND ") };
            let (schema, table) = crate::lake::table_by_id(conn, l.table_id)?.with_context(|| format!("{} was dropped", l.table))?;
            let tbl = quote_table(&format!("{}.{schema}.{table}", crate::lake::LAKE))?;
            let select: Vec<String> = l
                .columns
                .iter()
                .filter(|(c, _)| c != COMMIT_TS)
                .map(|(c, ty)| format!("CAST(t.{} AS {ty}) AS {}", ident(c), ident(c)))
                .collect();
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TABLE memory.main.v_typed_{k} AS
                   SELECT t.snapshot_id::INTEGER AS _partition, t.rowid::BIGINT AS _offset, {select}, {ts} AS {ct},
                          NULL::VARCHAR AS _err
                   FROM {tbl} AS t AT (VERSION => {p}) LEFT JOIN ({times}) s ON s.snapshot_id = t.snapshot_id{rows}
                   WHERE {keep}",
                select = select.join(", "),
                ts = crate::engine::row_commit_ts(&def),
                ct = ident(COMMIT_TS),
                times = crate::lake::commit_times_sql(p, &format!("SELECT snapshot_id FROM {tbl} AT (VERSION => {p})")),
                rows = if l.counted { format!(", range(t.{})", ident(crate::circuit::ops::COUNT_COLUMN)) } else { String::new() },
            ))?;
            lake_rows += count(conn, &format!("SELECT * FROM memory.main.v_typed_{k}"))?;
            continue;
        };
        let msgs = rt.block_on(kafka::read_between(&src.options, &from[k], &ends[k]))?;
        kafka_rows += msgs.len();
        let raw = format!("v_raw_{k}");
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE memory.main.{raw} (_partition INTEGER, _offset BIGINT, payload VARCHAR, _key VARCHAR)"
        ))?;
        {
            let mut app = conn.appender_to_catalog_and_db(&raw, "memory", "main")?;
            for m in &msgs {
                app.append_row(duckdb::params![m.partition, m.offset, m.payload_text().as_deref(), m.key_text()])?;
            }
            app.flush()?;
        }
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE memory.main.v_ok_{k} AS SELECT * FROM memory.main.{raw} WHERE {IS_JSON_OBJECT};
             CREATE OR REPLACE TABLE memory.main.v_typed_{k} AS {} UNION ALL {};",
            decode_sql(src, &format!("memory.main.v_ok_{k}")),
            tombstones_sql(src, &format!("memory.main.{raw}"))
        ))?;
        // An upsert view reads tombstones as deletes; for every other view they are dead letters.
        let tombstones_dead = if plan.upsert.is_some() { format!(" AND _err <> {}", lit(crate::decode::TOMBSTONE)) } else { String::new() };
        dead_expected.push(format!(
            "SELECT {n} AS source, _partition, _offset FROM memory.main.{raw} WHERE {invalid}
             UNION ALL SELECT {n}, _partition, _offset FROM memory.main.v_typed_{k} WHERE _err IS NOT NULL{tombstones_dead}",
            n = lit(&src.name),
            invalid = invalid_json_where(),
        ));
    }
    let dead_expected = &dead_expected.join(" UNION ALL ");
    let dead_mismatch = count(conn, &format!("({dead_expected}) EXCEPT ALL (SELECT * FROM memory.main.v_dead)"))?
        + count(conn, &format!("(SELECT * FROM memory.main.v_dead) EXCEPT ALL ({dead_expected})"))?;

    // The valid rows of source k, and its columns.
    let columns_of = |k: usize| -> &[(String, String)] {
        match (srcs[k], lakes[k]) {
            (Some(s), _) => &s.options.columns,
            (None, Some(l)) => &l.columns,
            (None, None) => &[],
        }
    };
    let cols_of = |k: usize| columns_of(k).iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", ");
    // A window's columns replace any the stream has (a windowed view's table, read by another).
    let stream_cols = columns_of(0)
        .iter()
        .filter(|(c, _)| !(plan.is_windowed() && crate::circuit::codegen::is_window_column(c)))
        .map(|(c, _)| ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let valid = |k: usize| format!("(SELECT _partition, _offset, {} FROM memory.main.v_typed_{k} WHERE _err IS NULL)", cols_of(k));
    let (cols, valid0) = (cols_of(0), valid(0));
    let valid = &valid0;
    let (mut late_rows, mut late_duplicates, mut left_in_state) = (0, 0, 0);
    if plan.is_windowed() {
        let wm = ckpt.watermark_us.map(|w| format!("make_timestamp({w})")).unwrap_or("NULL".into());
        let back = if def.late_data.is_some() && plan.retracts() {
            format!(" UNION ALL SELECT {stream_cols}, window_start, window_end FROM memory.main.v_late_back WHERE window_end <= {wm} AND {sink_filter}")
        } else {
            String::new()
        };
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {input} AS
               SELECT {stream_cols}, window_start, window_end FROM ({w}) x
               WHERE window_end <= {wm} AND {sink_filter}
                 AND NOT EXISTS (SELECT 1 FROM memory.main.v_late l
                                 WHERE l._partition = x._partition AND l._offset = x._offset AND l.window_start = x.window_start){back}",
            input = plan.input_table,
            w = plan.window_sql(valid)
        ))?;
        late_rows = count(conn, "SELECT * FROM memory.main.v_late")?;
        late_duplicates = late_rows - count(conn, "SELECT DISTINCT * FROM memory.main.v_late")?;
        left_in_state = count(conn, &format!("SELECT * FROM memory.main.v_state WHERE __we <= {wm}"))?;
    } else if plan.upsert.is_some() {
        // Every message, tombstones too: the batch query takes each key's current row (`UpsertSpec::batch_sql`).
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {} AS SELECT {cols}, _partition, _offset, _key, _err IS NOT NULL AS __del
               FROM memory.main.v_typed_0 WHERE _err IS NULL OR _err = {}",
            plan.input_table,
            lit(crate::decode::TOMBSTONE)
        ))?;
    } else if plan.is_global() {
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT {cols}, 1::INTEGER AS __w FROM {valid}", plan.input_table))?;
    } else if plan.union.is_empty() {
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT {cols} FROM {valid}", plan.input_table))?;
    } else {
        // Each branch's staging table, from its own source.
        for b in &plan.union {
            let k = names.iter().position(|n| *n == b.source).unwrap();
            let v = format!("(SELECT _partition, _offset, {} FROM memory.main.v_typed_{k} WHERE _err IS NULL)", cols_of(k));
            conn.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT {} FROM {v}", b.input_table, cols_of(k)))?;
        }
    }
    conn.execute_batch(&format!("CREATE OR REPLACE TABLE memory.main.v_expected AS {}", plan.batch_sql))?;
    let (missing_from_sink, unexpected_in_sink, differing_rows) =
        compare(conn, "memory.main.v_expected", "memory.main.v_sink", &plan.sink_columns)?;
    Ok(Report {
        job: job.into(),
        from_epoch: base.map(|(epoch, _)| epoch),
        epoch: ckpt.epoch,
        watermark: crate::engine::fmt_us(ckpt.watermark_us),
        kafka_rows,
        lake_rows,
        expected_rows: count(conn, "SELECT * FROM memory.main.v_expected")?,
        sink_rows: count(conn, "SELECT * FROM memory.main.v_sink")?,
        missing_from_sink,
        unexpected_in_sink,
        dead_expected: count(conn, dead_expected)?,
        dead_mismatch,
        late_rows,
        late_duplicates,
        closed_windows_left_in_state: left_in_state,
        differing_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(expected: &str, sink: &str) -> (i64, i64, Vec<String>) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE e (g VARCHAR, n BIGINT, r DOUBLE); INSERT INTO e VALUES {expected};
             CREATE TABLE s (g VARCHAR, n BIGINT, r DOUBLE); INSERT INTO s VALUES {sink};"
        ))
        .unwrap();
        let cols = [("g", "VARCHAR"), ("n", "BIGINT"), ("r", "DOUBLE")].map(|(c, t)| (c.to_string(), t.to_string()));
        compare(&conn, "e", "s", &cols).unwrap()
    }

    #[test]
    fn floats_a_hair_apart_match_even_across_a_rounding_step() {
        // 0.123456789500001 and 0.123456789499999: 2e-15 apart, but the old rounding gave 0.12345679 and 0.123456789.
        let (m, u, rows) = run("('a', 1, 0.123456789500001), ('b', 2, -0.0016807972449711778)",
                               "('a', 1, 0.123456789499999), ('b', 2, -0.0016807972449720)");
        assert_eq!((m, u), (0, 0), "{rows:?}");
    }

    #[test]
    fn a_residue_next_to_zero_matches() {
        let (m, u, rows) = run("('a', 1, 0.0)", "('a', 1, 5.329070518200751e-15)");
        assert_eq!((m, u), (0, 0), "{rows:?}");
        let (m, u, _) = run("('a', 1, 0.0)", "('a', 1, 0.000001)");
        assert_eq!((m, u), (1, 1), "a real difference near zero still shows");
    }

    #[test]
    fn floats_further_apart_than_the_tolerance_differ() {
        let (m, u, rows) = run("('a', 1, 0.5), ('b', 2, 3.0)", "('a', 1, 0.5000001), ('b', 2, 3.0)");
        assert_eq!((m, u), (1, 1));
        assert_eq!(rows, ["missing: {'g': a, 'n': 1, 'r': 0.5}", "unexpected: {'g': a, 'n': 1, 'r': 0.5000001}"]);
    }

    #[test]
    fn other_columns_compare_exactly_and_rows_as_multisets() {
        // n differs by one; a duplicated row in the sink is one row too many; NULL and NaN match themselves.
        let (m, u, _) = run("('a', 1, 0.5), ('b', 2, NULL), ('c', 3, 'NaN'), ('d', 4, 1.0)",
                            "('a', 2, 0.5), ('b', 2, NULL), ('c', 3, 'NaN'), ('d', 4, 1.0), ('d', 4, 1.0)");
        assert_eq!((m, u), (1, 2));
    }

    #[test]
    fn rows_with_the_same_exact_columns_pair_by_their_floats() {
        let (m, u, rows) = run("('a', 1, 1.0), ('a', 1, 2.0), ('a', 1, 3.0)",
                               "('a', 1, 3.0000000000001), ('a', 1, 1.0000000000001), ('a', 1, 2.0)");
        assert_eq!((m, u), (0, 0), "{rows:?}");
    }
}
