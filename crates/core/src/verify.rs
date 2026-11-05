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
use crate::lake::{ident, quote_table, CTL};
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
    let src = &sources[&def.plan.source];
    let plan = crate::plan::compile(conn, job, &def.sql, &sources)?;
    if plan.is_windowed() && def.late_data.is_none() {
        bail!("verify needs a late-data table for windowed views (dropped late rows cannot be reconstructed)");
    }

    let earliest = rt.block_on(kafka::earliest_offsets(&src.options))?;
    let truncated = earliest.values().any(|&o| o > 0);
    if truncated && plan.needs_whole_topic() {
        bail!("retention has deleted the oldest events of {:?}, and a {} view depends on all of them", src.options.topic, plan.kind_name());
    }

    // Snapshot the lake side in one transaction so checkpoint, sink, late rows and state agree.
    conn.execute_batch("BEGIN")?;
    let ckpt = catalog::latest_checkpoint(conn, job, &src.name)?.context("job has no checkpoint yet")?;
    let (base, sink_filter) = if truncated {
        let all = catalog::all_checkpoints(conn, job, &src.name)?;
        let retained = |c: &catalog::Checkpoint| {
            earliest.iter().all(|(p, &e)| c.offsets.get(p).copied().unwrap_or(0) >= e)
        };
        let base = all.iter().find(|c| retained(c)).cloned().with_context(|| {
            format!("no checkpoint left starts inside the retained range of {:?} (earliest offsets {earliest:?})", src.options.topic)
        })?;
        let filter = if plan.is_windowed() {
            // Every event read up to `base` has an event time at or before this bound.
            let bound = all.iter().filter(|c| c.epoch <= base.epoch).filter_map(|c| c.max_event_time_us).max();
            bound.map(|b| format!("window_start > make_timestamp({b})")).unwrap_or("true".into())
        } else {
            format!("_duckwake_epoch > {}", base.epoch)
        };
        (Some(base), filter)
    } else {
        (None, "true".into())
    };
    let sink_cols: Vec<String> = plan.sink_columns.iter().map(|(c, _)| ident(c)).collect();
    let sink_cols = sink_cols.join(", ");
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE memory.main.v_sink AS SELECT {sink_cols} FROM {} WHERE {sink_filter};
         CREATE OR REPLACE TABLE memory.main.v_dead AS SELECT source_partition AS _partition, source_offset AS _offset
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

    let from = base.as_ref().map(|b| b.offsets.clone()).unwrap_or_default();
    if base.is_some() {
        // Dead and late rows before the base checkpoint come from events no longer retained.
        let range: Vec<String> = from.iter().map(|(p, o)| format!("(_partition = {p} AND _offset >= {o})")).collect();
        let range = range.join(" OR ");
        conn.execute_batch(&format!("DELETE FROM memory.main.v_dead WHERE NOT ({range})"))?;
        if def.late_data.is_some() {
            conn.execute_batch(&format!("DELETE FROM memory.main.v_late WHERE NOT ({range})"))?;
        }
    }
    let msgs = rt.block_on(kafka::read_between(&src.options, &from, &ckpt.offsets))?;
    conn.execute_batch("CREATE OR REPLACE TABLE memory.main.v_raw (_partition INTEGER, _offset BIGINT, payload VARCHAR)")?;
    {
        let mut app = conn.appender_to_catalog_and_db("v_raw", "memory", "main")?;
        for m in &msgs {
            let payload = m.payload.as_deref().map(String::from_utf8_lossy);
            app.append_row(duckdb::params![m.partition, m.offset, payload.as_deref()])?;
        }
        app.flush()?;
    }
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TABLE memory.main.v_ok AS SELECT * FROM memory.main.v_raw
           WHERE {IS_JSON_OBJECT};
         CREATE OR REPLACE TABLE memory.main.v_typed AS {};",
        decode_sql(src, "memory.main.v_ok")
    ))?;
    let dead_expected = &format!(
        "SELECT _partition, _offset FROM memory.main.v_raw WHERE NOT ({IS_JSON_OBJECT})
         UNION ALL SELECT _partition, _offset FROM memory.main.v_typed WHERE _err IS NOT NULL"
    );
    let dead_mismatch = count(conn, &format!("({dead_expected}) EXCEPT ALL (SELECT * FROM memory.main.v_dead)"))?
        + count(conn, &format!("(SELECT * FROM memory.main.v_dead) EXCEPT ALL ({dead_expected})"))?;

    let cols: Vec<String> = src.options.columns.iter().map(|(c, _)| ident(c)).collect();
    let cols = cols.join(", ");
    let valid = format!("(SELECT _partition, _offset, {cols} FROM memory.main.v_typed WHERE _err IS NULL)");
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
    } else {
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT {cols} FROM {valid}", plan.input_table))?;
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
        from_epoch: base.map(|b| b.epoch),
        epoch: ckpt.epoch,
        watermark: crate::engine::fmt_us(ckpt.watermark_us),
        kafka_rows: msgs.len(),
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
