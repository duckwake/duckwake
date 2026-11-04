//! The circuits of today's three view kinds, compiled from SQL.
use crate::frontend::SourceSchema;
use crate::plan::{self, GlobalTables, Plan};
use crate::sql::{quote_table, state_table};
use crate::ops::{Differentiate, Integrate, Source};
use crate::{Circuit, EpochCtx, Op, Operator, View};
use duckdb::Connection;
use std::collections::HashMap;

const SINK: &str = "lake.analytics.v";

/// The `clicks` source of the tests.
pub(crate) struct TestSource {
    pub columns: Vec<(String, String)>,
}

impl SourceSchema for TestSource {
    fn event_time(&self) -> &str {
        "ts"
    }
    fn columns(&self) -> &[(String, String)] {
        &self.columns
    }
}

/// A connection with the lookup table the tests join, and the `clicks` source.
pub(crate) fn setup() -> (Connection, HashMap<String, TestSource>) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE games (game_id VARCHAR, studio VARCHAR)").unwrap();
    let columns = [("ts", "TIMESTAMP"), ("game_id", "VARCHAR"), ("user_id", "VARCHAR"), ("amount", "DECIMAL(18,2)")];
    let columns = columns.iter().map(|(c, t)| (c.to_string(), t.to_string())).collect();
    (conn, HashMap::from([("clicks".to_string(), TestSource { columns })]))
}

struct Def {
    plan: Plan,
    late: bool,
}

impl Def {
    fn view(&self) -> View<'_> {
        View { id: "v", sink: SINK, late_data: self.late.then_some("lake.analytics.v_late"), plan: &self.plan }
    }
}

fn def_for(sql: &str, late: bool) -> Def {
    let (conn, sources) = setup();
    Def { plan: plan::compile(&conn, "v", sql, &sources).unwrap(), late }
}

fn ops(c: &Circuit) -> Vec<String> {
    c.nodes.iter().map(|n| n.op.name()).collect()
}

#[test]
fn stateless_view_needs_no_state() {
    let def = def_for("SELECT ts, amount FROM clicks WHERE amount > 10", false);
    let c = Circuit::compile(&def.view()).unwrap();
    assert_eq!(ops(&c), ["Source(clicks)", "Map(view)", "Sink(append)"]);
    assert!(c.trace.iter().any(|t| t.contains("D ∘ I = id")), "{:?}", c.trace);
}

#[test]
fn windowed_aggregate_keeps_state_and_routes_late_rows() {
    let def = def_for("SELECT window_start, game_id, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL", true);
    let c = Circuit::compile(&def.view()).unwrap();
    assert_eq!(
        ops(&c),
        [
            "Source(clicks)",
            "Window(TUMBLE ts, 10s)",
            "LateSplit(on time | late)",
            "AggregateΔ(windowed, emit on close) [state table]",
            "Sink(append)",
            "Sink(late data)"
        ]
    );
    assert!(!c.has(&Op::Integrate(Integrate)) && !c.has(&Op::Differentiate(Differentiate)));
    assert!(c.emits_final());
}

#[test]
fn global_aggregate_becomes_an_incremental_aggregate_between_linear_maps() {
    let def = def_for("SELECT game_id, count(*) AS n, sum(amount) AS s FROM clicks GROUP BY ALL HAVING count(*) > 1", false);
    let c = Circuit::compile(&def.view()).unwrap();
    assert_eq!(
        ops(&c),
        ["Source(clicks)", "Map(pre-aggregate)", "AggregateΔ(global) [state table]", "Map(post-aggregate)", "Sink(upsert)"]
    );
    assert!(c.upserts());
    // Append-only Kafka rows in, a stream that replaces group rows out.
    let append_only: Vec<bool> = c.nodes.iter().map(|n| n.props.append_only).collect();
    assert_eq!(append_only, [true, true, false, false, false]);
    // The generated SQL is the same set of statements the engine ran before the circuit existed.
    let cols = vec![("ts".to_string(), "TIMESTAMP".to_string())];
    let x = EpochCtx::example(def.view(), &cols).unwrap();
    let g = def.plan.global.as_ref().unwrap();
    let t = GlobalTables::new("v", state_table("v"), quote_table(SINK).unwrap(), &def.plan, 1);
    let stage: Vec<String> = c.stage_steps(&x).unwrap().into_iter().map(|s| s.sql).skip(1).collect();
    assert_eq!(stage, g.stage_sql(&t, &def.plan.state_columns));
    let commit: Vec<String> = c.commit_steps(&x).unwrap().into_iter().map(|s| s.sql).collect();
    assert_eq!(commit, [g.commit_state_sql(&t, &def.plan.state_columns), g.commit_sink_sql(&t, "<epoch>")].concat());
}

#[test]
fn retracting_input_is_refused_where_an_operator_assumes_append_only() {
    let refused = [
        "SELECT window_start, game_id, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL",
        "SELECT game_id, min(amount) AS lo FROM clicks GROUP BY ALL",
        "SELECT ts, amount FROM clicks",
    ];
    let allowed = ["SELECT game_id, count(*) AS n, sum(amount) AS s, avg(amount) AS a FROM clicks GROUP BY ALL"];
    for sql in refused.iter().chain(&allowed) {
        let def = def_for(sql, false);
        let mut q = Circuit::query(&def.view());
        assert!(q.check(&def.plan).is_ok(), "{sql}: Kafka input is append-only");
        // The same view over a source that can retract (CDC).
        q.nodes[0].op = Op::Source(Source { name: "clicks".into(), append_only: false });
        q.analyze();
        assert_eq!(q.check(&def.plan).is_err(), refused.contains(sql), "{sql}");
    }
}
