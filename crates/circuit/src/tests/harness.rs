//! Runs a view epoch by epoch through its circuit's own SQL against an in-memory lake, and checks the view's
//! table against the view run as one batch query over every row so far: QΔ = D ∘ Q ∘ I, end to end.
use super::{setup, SINK};
use crate::codegen::{staging, EpochCtx, SourceInput};
use crate::plan;
use crate::sql::{ident, quote_table, state_table};
use crate::{Circuit, View};

/// Epoch `e`'s rows of a test source, each with its weight: one insert per second of event time in the epoch's
/// range, with NULLs in some columns. `purchases` has other values than `clicks`, under `price`. `changes` has
/// `clicks`' rows and also retracts: of the previous epoch's rows it deletes every third and updates the next
/// (−1 for the old version, +1 for one in another group), and some rows it inserts and deletes within the
/// same epoch. A row is changed at most once, so every retraction is of a row as it stands.
fn rows(source: &str, epochs: &[(i64, i64)], e: usize) -> String {
    let (from, to) = epochs[e];
    let click = |i: &str, version: &str| {
        format!(
            "{i} AS _offset, TIMESTAMP '2026-01-01' + to_seconds({i}) AS ts,
             CASE WHEN {i} % 17 = 0 THEN NULL ELSE 'g' || (({i} + {version}) * 7 % 5) END AS game_id, 'u' || ({i} % 9 + {version}) AS user_id,
             CASE WHEN {i} % 11 = 0 THEN NULL ELSE (({i} * 37) % 13 + {version})::DECIMAL(18,2) END AS amount"
        )
    };
    match source {
        "purchases" => format!(
            "SELECT i AS _offset, TIMESTAMP '2026-01-01' + to_seconds(i) AS ts,
                    'g' || (i * 3 % 4) AS game_id, 'u' || (i % 5) AS user_id,
                    CASE WHEN i % 7 = 0 THEN NULL ELSE ((i * 11) % 17)::DECIMAL(18,2) END AS price, 1 AS __w
             FROM range({from}, {to}) t(i)"
        ),
        "changes" => {
            let (pf, pt) = if e == 0 { (0, 0) } else { epochs[e - 1] };
            format!(
                "SELECT {ins}, 1 AS __w FROM range({from}, {to}) t(i)
                 UNION ALL SELECT {ins}, -1 FROM range({from}, {to}) t(i) WHERE i % 13 = 5
                 UNION ALL SELECT {ins}, -1 FROM range({pf}, {pt}) t(i) WHERE i % 13 <> 5 AND i % 3 IN (0, 1)
                 UNION ALL SELECT {upd}, 1 FROM range({pf}, {pt}) t(i) WHERE i % 13 <> 5 AND i % 3 = 1",
                ins = click("i", "0"),
                upd = click("i", "1"),
            )
        }
        _ => format!("SELECT {}, 1 AS __w FROM range({from}, {to}) t(i)", click("i", "0")),
    }
}

/// Runs `sql` over `epochs` (ranges of rows) and checks the view's table after every epoch; a windowed view's
/// once, after the last epoch, whose watermark closes every window. With `pipelined` each epoch is staged
/// before the previous one commits, as a pipelined worker does: the previous commit lands before the staging
/// on even epochs and after it on odd ones.
pub(crate) fn check_view(sql: &str, epochs: &[(i64, i64)], pipelined: bool) {
    let (c, sources) = setup();
    c.execute_batch("INSERT INTO games SELECT 'g' || i, ['Aurora', 'Basalt'][i % 2 + 1] FROM range(4) t(i)").unwrap();
    let p = plan::compile(&c, "v", sql, &sources).unwrap();
    let circuit = Circuit::compile(&View { id: "v", sink: SINK, late_data: None, plan: &p }).unwrap();
    let names_of = p.sources();
    let cols: Vec<Vec<(String, String)>> = names_of.iter().map(|n| sources[n].columns.clone()).collect();
    let names = |cs: &[(String, String)]| cs.iter().map(|(n, _)| ident(n)).collect::<Vec<_>>().join(", ");
    let typed = |cs: &[(String, String)]| cs.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect::<Vec<_>>().join(", ");
    c.execute_batch(&format!(
        "ATTACH ':memory:' AS lake; CREATE SCHEMA lake.__duckwake; CREATE SCHEMA lake.analytics;
         CREATE TABLE {} ({}, _duckwake_epoch BIGINT);",
        quote_table(SINK).unwrap(),
        typed(&[p.sink_columns.clone(), circuit.sink_extra_columns()].concat()),
    ))
    .unwrap();
    // The rows each source has committed so far, with their weights, and as the multiset they add up to.
    let seen = |k: usize| format!("memory.main.seen_{k}");
    for (k, cs) in cols.iter().enumerate() {
        c.execute_batch(&format!("CREATE TABLE {} ({}, __w INTEGER)", seen(k), typed(cs))).unwrap();
    }
    let net = |k: usize| {
        let n = names(&cols[k]);
        format!("(SELECT {n} FROM (SELECT {n}, sum(__w)::BIGINT AS __n FROM {} GROUP BY ALL HAVING sum(__w) > 0) t, range(t.__n))", seen(k))
    };
    if !p.state_columns.is_empty() {
        c.execute_batch(&format!("CREATE TABLE {} ({})", state_table("v"), typed(&p.state_columns))).unwrap();
    }
    let windowed = p.window.is_some();
    // Event time is the row number in seconds and the watermark the newest event time, so no row is late. A
    // retracting source changes rows of earlier epochs, so its windows stay open until the last epoch.
    let watermark = |e: usize| {
        if windowed && e + 1 == epochs.len() {
            "TIMESTAMP '9999-01-01'".to_string()
        } else if windowed && p.retracts() {
            "NULL::TIMESTAMP".to_string()
        } else {
            format!("TIMESTAMP '2026-01-01' + to_seconds({})", epochs[e].1 - 1)
        }
    };
    let ctx = |e: usize| EpochCtx {
        job: "v",
        plan: &p,
        sink: quote_table(SINK).unwrap(),
        late_table: None,
        sources: names_of
            .iter()
            .zip(&cols)
            .enumerate()
            .map(|(k, (name, columns))| SourceInput { name, columns, typed: staging("typed", &format!("g{k}")) })
            .collect(),
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
        for (k, name) in names_of.iter().enumerate() {
            c.execute_batch(&format!("INSERT INTO {} SELECT {}, __w FROM ({})", seen(k), names(&cols[k]), rows(name, epochs, e))).unwrap();
        }
    };
    let check = |committed: usize| {
        if windowed && committed < epochs.len() {
            return;
        }
        // Each staging table the batch query reads, filled with its source's rows.
        let inputs: Vec<(String, &str)> = if p.union.is_empty() {
            vec![(p.input_table.clone(), p.source.as_str())]
        } else {
            p.union.iter().map(|b| (b.input_table.clone(), b.source.as_str())).collect()
        };
        for (table, source) in inputs {
            let k = names_of.iter().position(|n| n == source).unwrap();
            let input = if windowed {
                format!("SELECT {}, window_start, window_end, 1::INTEGER AS __w FROM ({})", names(&cols[k]), p.window_sql(&net(k)))
            } else {
                format!("SELECT *, 1::INTEGER AS __w FROM {}", net(k))
            };
            c.execute_batch(&format!("CREATE OR REPLACE TABLE {table} AS {input}")).unwrap();
        }
        c.execute_batch(&format!("CREATE OR REPLACE TABLE memory.main.expected AS {}", p.batch_sql)).unwrap();
        // DOUBLE results (avg) to 9 decimals: merged partials add in another order than one batch query.
        let cols: Vec<String> = p
            .sink_columns
            .iter()
            .map(|(n, t)| if t == "DOUBLE" || t == "FLOAT" { format!("round({0}, 9) AS {0}", ident(n)) } else { ident(n) })
            .collect();
        let cols = cols.join(", ");
        // A counted sink holds each row once with its multiplicity.
        let sink = if circuit.counts_rows() {
            format!("(SELECT * FROM {}, range({}))", quote_table(SINK).unwrap(), crate::ops::COUNT_COLUMN)
        } else {
            quote_table(SINK).unwrap()
        };
        let diff: i64 = c
            .query_row(
                &format!(
                    "SELECT (SELECT count(*) FROM (SELECT {cols} FROM memory.main.expected EXCEPT ALL SELECT {cols} FROM {sink}))
                          + (SELECT count(*) FROM (SELECT {cols} FROM {sink} EXCEPT ALL SELECT {cols} FROM memory.main.expected))"
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
        for (k, name) in names_of.iter().enumerate() {
            c.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {} AS SELECT 0::INTEGER AS _partition, _offset, {}, __w, NULL::VARCHAR AS _err FROM ({})",
                staging("typed", &format!("g{k}")),
                names(&cols[k]),
                rows(name, epochs, e)
            ))
            .unwrap();
        }
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
