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
    // A windowed view over a lake input has an exact watermark, so it has no late rows to reconstruct.
    if plan.is_windowed() && def.late_data.is_none() && lakes[0].is_none() {
        bail!("verify needs a late-data table for windowed views (dropped late rows cannot be reconstructed)");
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
    if let Some(late) = &def.late_data {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE memory.main.v_late AS SELECT _source_partition AS _partition, _source_offset AS _offset, window_start FROM {}",
            quote_table(late)?
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
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {input} AS
               SELECT {stream_cols}, window_start, window_end FROM ({w}) x
               WHERE window_end <= {wm} AND {sink_filter}
                 AND NOT EXISTS (SELECT 1 FROM memory.main.v_late l
                                 WHERE l._partition = x._partition AND l._offset = x._offset AND l.window_start = x.window_start)",
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
    // Floating-point sums merged across epochs add in a different order than one batch query, so
    // DOUBLE and FLOAT columns are compared to 10 significant digits; everything else exactly.
    let approx = |table: &str| -> String {
        let cols: Vec<String> = plan
            .sink_columns
            .iter()
            .map(|(c, t)| {
                let c = ident(c);
                if t == "DOUBLE" || t == "FLOAT" {
                    format!("CASE WHEN {c} IS NULL OR {c} = 0 OR isinf({c}) OR isnan({c}) THEN {c} ELSE round({c}, 9 - ceil(log10(abs({c})))::INTEGER) END AS {c}")
                } else {
                    c
                }
            })
            .collect();
        format!("CREATE OR REPLACE TABLE {table} AS SELECT {} FROM {table}", cols.join(", "))
    };
    conn.execute_batch(&approx("memory.main.v_expected"))?;
    conn.execute_batch(&approx("memory.main.v_sink"))?;

    Ok(Report {
        job: job.into(),
        from_epoch: base.map(|(epoch, _)| epoch),
        epoch: ckpt.epoch,
        watermark: crate::engine::fmt_us(ckpt.watermark_us),
        kafka_rows,
        lake_rows,
        expected_rows: count(conn, "SELECT * FROM memory.main.v_expected")?,
        sink_rows: count(conn, "SELECT * FROM memory.main.v_sink")?,
        missing_from_sink: count(conn, "SELECT * FROM memory.main.v_expected EXCEPT ALL SELECT * FROM memory.main.v_sink")?,
        unexpected_in_sink: count(conn, "SELECT * FROM memory.main.v_sink EXCEPT ALL SELECT * FROM memory.main.v_expected")?,
        dead_expected: count(conn, dead_expected)?,
        dead_mismatch,
        late_rows,
        late_duplicates,
        closed_windows_left_in_state: left_in_state,
    })
}
