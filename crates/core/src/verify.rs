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
use crate::catalog::{self, state_table};
use crate::kafka;
use crate::lake::{ident, lit, quote_table, CTL};
use crate::decode::{decode_sql, IS_JSON_OBJECT};
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

pub fn verify(conn: &Connection, rt: &Handle, job: &str) -> Result<Report> {
    let (def, _) = catalog::load_job(conn, job)?;
    let sources = catalog::load_sources(conn)?;
    let plan = crate::plan::compile(conn, job, &def.sql, &sources)?;
    if plan.is_windowed() && def.late_data.is_none() {
        bail!("verify needs a late-data table for windowed views (dropped late rows cannot be reconstructed)");
    }
    // The view's sources, the first holding a windowed view's only stream.
    let names = plan.sources();
    let srcs = names.iter().map(|n| sources.get(n).with_context(|| format!("source {n} is gone"))).collect::<Result<Vec<_>>>()?;

    let earliest = srcs.iter().map(|s| rt.block_on(kafka::earliest_offsets(&s.options))).collect::<Result<Vec<_>>>()?;
    let truncated = earliest.iter().any(|e| e.values().any(|&o| o > 0));
    if truncated && plan.needs_whole_topic() {
        let topics: Vec<&str> = srcs.iter().map(|s| s.options.topic.as_str()).collect();
        bail!("retention has deleted the oldest events of {topics:?}, and a {} view depends on all of them", plan.kind_name());
    }

    // Snapshot the lake side in one transaction so checkpoint, sink, late rows and state agree.
    conn.execute_batch("BEGIN")?;
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
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE memory.main.v_sink AS SELECT {sink_cols} FROM {} WHERE {sink_filter};
         CREATE OR REPLACE TABLE memory.main.v_dead AS SELECT source, source_partition AS _partition, source_offset AS _offset
           FROM {CTL}.dead_letter WHERE job_id = '{job}';",
        quote_table(&def.sink)?
    ))?;
    if let Some(late) = &def.late_data {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE memory.main.v_late AS SELECT _source_partition AS _partition, _source_offset AS _offset, window_start FROM {}",
            quote_table(late)?
        ))?;
    }
    if plan.is_windowed() {
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE memory.main.v_state AS SELECT __ws, __we FROM {}", state_table(job)))?;
    }
    conn.execute_batch("COMMIT")?;

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
    let mut kafka_rows = 0;
    let mut dead_expected = Vec::new();
    for (k, src) in srcs.iter().enumerate() {
        let msgs = rt.block_on(kafka::read_between(&src.options, &from[k], &ends[k]))?;
        kafka_rows += msgs.len();
        let raw = format!("v_raw_{k}");
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE memory.main.{raw} (_partition INTEGER, _offset BIGINT, payload VARCHAR)"))?;
        {
            let mut app = conn.appender_to_catalog_and_db(&raw, "memory", "main")?;
            for m in &msgs {
                let payload = m.payload.as_deref().map(String::from_utf8_lossy);
                app.append_row(duckdb::params![m.partition, m.offset, payload.as_deref()])?;
            }
            app.flush()?;
        }
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE memory.main.v_ok_{k} AS SELECT * FROM memory.main.{raw} WHERE {IS_JSON_OBJECT};
             CREATE OR REPLACE TABLE memory.main.v_typed_{k} AS {};",
            decode_sql(src, &format!("memory.main.v_ok_{k}"))
        ))?;
        dead_expected.push(format!(
            "SELECT {n} AS source, _partition, _offset FROM memory.main.{raw} WHERE NOT ({IS_JSON_OBJECT})
             UNION ALL SELECT {n}, _partition, _offset FROM memory.main.v_typed_{k} WHERE _err IS NOT NULL",
            n = lit(&src.name)
        ));
    }
    let dead_expected = &dead_expected.join(" UNION ALL ");
    let dead_mismatch = count(conn, &format!("({dead_expected}) EXCEPT ALL (SELECT * FROM memory.main.v_dead)"))?
        + count(conn, &format!("(SELECT * FROM memory.main.v_dead) EXCEPT ALL ({dead_expected})"))?;

    // The valid rows of source k, and its columns.
    let cols_of = |k: usize| srcs[k].options.columns.iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", ");
    let valid = |k: usize| format!("(SELECT _partition, _offset, {} FROM memory.main.v_typed_{k} WHERE _err IS NULL)", cols_of(k));
    let (cols, valid0) = (cols_of(0), valid(0));
    let valid = &valid0;
    let (mut late_rows, mut late_duplicates, mut left_in_state) = (0, 0, 0);
    if plan.is_windowed() {
        let wm = ckpt.watermark_us.map(|w| format!("make_timestamp({w})")).unwrap_or("NULL".into());
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {input} AS
               SELECT {cols}, window_start, window_end FROM ({w}) x
               WHERE window_end <= {wm} AND {sink_filter}
                 AND NOT EXISTS (SELECT 1 FROM memory.main.v_late l
                                 WHERE l._partition = x._partition AND l._offset = x._offset AND l.window_start = x.window_start)",
            input = plan.input_table,
            w = plan.window_sql(&valid)
        ))?;
        late_rows = count(conn, "SELECT * FROM memory.main.v_late")?;
        late_duplicates = late_rows - count(conn, "SELECT DISTINCT * FROM memory.main.v_late")?;
        left_in_state = count(conn, &format!("SELECT * FROM memory.main.v_state WHERE __we <= {wm}"))?;
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
