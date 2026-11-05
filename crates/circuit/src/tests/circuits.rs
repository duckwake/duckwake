//! The circuits of today's three view kinds, compiled from SQL.
use super::def_for;
use crate::ops::{Differentiate, Integrate, Source};
use crate::{Circuit, Op, Operator};

fn ops(c: &Circuit) -> Vec<String> {
    c.nodes.iter().map(|n| n.op.name()).collect()
}

#[test]
fn stateless_view_needs_no_state() {
    let def = def_for("SELECT ts, amount FROM clicks WHERE amount > 10", false);
    let c = Circuit::compile(&def.view()).unwrap();
    assert_eq!(ops(&c), ["Source(clicks)", "From(clicks)", "Filter((amount > 10))", "Map(select)", "Sink(append)"]);
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
            "From(tumble)",
            "Map(aggregate input)",
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
        [
            "Source(clicks)",
            "From(clicks)",
            "Map(aggregate input)",
            "AggregateΔ(global) [state table]",
            "Filter((__a0 > 1))",
            "Map(select)",
            "Sink(upsert)"
        ]
    );
    assert!(c.upserts());
    // Append-only Kafka rows in, a stream that replaces group rows out.
    let append_only: Vec<bool> = c.nodes.iter().map(|n| n.props.append_only).collect();
    assert_eq!(append_only, [true, true, true, false, false, false, false]);
}

#[test]
fn retracting_input_is_refused_where_an_operator_assumes_append_only() {
    let refused = [
        "SELECT window_start, game_id, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL",
        "SELECT game_id, min(amount) AS lo FROM clicks GROUP BY ALL",
        "SELECT ts, amount FROM clicks",
        "SELECT DISTINCT game_id FROM clicks",
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
