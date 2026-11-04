//! Runs a view epoch by epoch through its circuit's own SQL against an in-memory lake, and checks the view's
//! table against the view run as one batch query over every row so far: QΔ = D ∘ Q ∘ I, end to end.
use super::{setup, SINK};
use crate::codegen::{staging, EpochCtx};
use crate::plan;
use crate::sql::{ident, quote_table, state_table};
use crate::{Circuit, View};

/// Rows `from..to` of the test stream: one per second of event time, with NULLs in some columns.
fn rows(from: i64, to: i64) -> String {
    format!(
        "SELECT i AS _offset, TIMESTAMP '2026-01-01' + to_seconds(i) AS ts,
                CASE WHEN i % 17 = 0 THEN NULL ELSE 'g' || (i * 7 % 5) END AS game_id, 'u' || (i % 9) AS user_id,
                CASE WHEN i % 11 = 0 THEN NULL ELSE ((i * 37) % 13)::DECIMAL(18,2) END AS amount
         FROM range({from}, {to}) t(i)"
    )
}

/// Runs `sql` over `epochs` (ranges of rows) and checks the view's table after every epoch; a windowed view's
/// once, after the last epoch, whose watermark closes every window. With `pipelined` each epoch is staged
/// before the previous one commits, as a pipelined worker does: the previous commit lands before the staging
/// on even epochs and after it on odd ones.
pub(crate) fn check_view(sql: &str, epochs: &[(i64, i64)], pipelined: bool) {
    let (c, sources) = setup();
    c.execute_batch("INSERT INTO games SELECT 'g' || i, ['Aurora', 'Basalt'][i % 2 + 1] FROM range(4) t(i)").unwrap();
    let p = plan::compile(&c, "v", sql, &sources).unwrap();
    let cols = sources["clicks"].columns.clone();
    let names = |cs: &[(String, String)]| cs.iter().map(|(n, _)| ident(n)).collect::<Vec<_>>().join(", ");
    let typed = |cs: &[(String, String)]| cs.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect::<Vec<_>>().join(", ");
    c.execute_batch(&format!(
        "ATTACH ':memory:' AS lake; CREATE SCHEMA lake.__duckwake; CREATE SCHEMA lake.analytics;
         CREATE TABLE {} ({}, _duckwake_epoch BIGINT);
         CREATE TABLE memory.main.seen ({});",
        quote_table(SINK).unwrap(),
        typed(&p.sink_columns),
        typed(&cols)
    ))
    .unwrap();
    if !p.state_columns.is_empty() {
        c.execute_batch(&format!("CREATE TABLE {} ({})", state_table("v"), typed(&p.state_columns))).unwrap();
    }
    let circuit = Circuit::compile(&View { id: "v", sink: SINK, late_data: None, plan: &p }).unwrap();
    let windowed = p.window.is_some();
    // Event time is the row number in seconds and the watermark the newest event time, so no row is late.
    let watermark = |e: usize| {
        if windowed && e + 1 == epochs.len() {
            "TIMESTAMP '9999-01-01'".to_string()
        } else {
            format!("TIMESTAMP '2026-01-01' + to_seconds({})", epochs[e].1 - 1)
        }
    };
    let ctx = |e: usize| EpochCtx {
        job: "v",
        plan: &p,
        sink: quote_table(SINK).unwrap(),
        late_table: None,
        source_columns: &cols,
        typed: staging("typed", "g0"),
        parity: e as u64 % 2,
        overlay: (pipelined && e > 0).then(|| (e as u64 - 1) % 2),
        prev_watermark: if e == 0 { "NULL".into() } else { watermark(e - 1) },
        watermark: Some(watermark(e)),
        epoch: (e + 1).to_string(),
    };
    let run = |stmts: Vec<String>| {
        for s in stmts {
            c.execute_batch(&s).unwrap_or_else(|err| panic!("{err}\n{s}"));
        }
    };
    let commit = |e: usize| {
        run(circuit.commit_steps(&ctx(e)).unwrap().into_iter().map(|s| s.sql).collect());
        let (from, to) = epochs[e];
        c.execute_batch(&format!("INSERT INTO memory.main.seen SELECT {} FROM ({})", names(&cols), rows(from, to))).unwrap();
    };
    let check = |committed: usize| {
        if windowed && committed < epochs.len() {
            return;
        }
        let input = if windowed {
            format!("SELECT {}, window_start, window_end, 1::INTEGER AS __w FROM ({})", names(&cols), p.window_sql("memory.main.seen"))
        } else {
            format!("SELECT *, 1::INTEGER AS __w FROM memory.main.seen")
        };
        c.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {} AS {input}; CREATE OR REPLACE TABLE memory.main.expected AS {};",
            p.input_table, p.batch_sql
        ))
        .unwrap();
        // DOUBLE results (avg) to 9 decimals: merged partials add in another order than one batch query.
        let cols: Vec<String> = p
            .sink_columns
            .iter()
            .map(|(n, t)| if t == "DOUBLE" || t == "FLOAT" { format!("round({0}, 9) AS {0}", ident(n)) } else { ident(n) })
            .collect();
        let cols = cols.join(", ");
        let diff: i64 = c
            .query_row(
                &format!(
                    "SELECT (SELECT count(*) FROM (SELECT {cols} FROM memory.main.expected EXCEPT ALL SELECT {cols} FROM {sink}))
                          + (SELECT count(*) FROM (SELECT {cols} FROM {sink} EXCEPT ALL SELECT {cols} FROM memory.main.expected))",
                    sink = quote_table(SINK).unwrap()
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(diff, 0, "after {committed} epochs of {sql}");
    };
    for e in 0..epochs.len() {
        if pipelined && e > 0 && e % 2 == 0 {
            commit(e - 1);
        }
        let (from, to) = epochs[e];
        c.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {} AS SELECT 0::INTEGER AS _partition, _offset, {}, NULL::VARCHAR AS _err FROM ({})",
            staging("typed", "g0"),
            names(&cols),
            rows(from, to)
        ))
        .unwrap();
        run(circuit.stage_steps(&ctx(e)).unwrap().into_iter().map(|s| s.sql).collect());
        if !pipelined {
            commit(e);
            check(e + 1);
            continue;
        }
        if e > 0 && e % 2 == 1 {
            commit(e - 1);
        }
        if e > 0 {
            check(e);
        }
    }
    if pipelined {
        commit(epochs.len() - 1);
        check(epochs.len());
    }
    let n: i64 = c.query_row(&format!("SELECT count(*) FROM {}", quote_table(SINK).unwrap()), [], |r| r.get(0)).unwrap();
    assert!(n > 0, "the view's table is empty for {sql}");
}
