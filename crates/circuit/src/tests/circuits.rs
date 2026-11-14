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
        q.nodes[0].op = Op::Source(Source { name: "clicks".into(), append_only: false, change_feed: false });
        q.analyze();
        assert_eq!(q.check(&def.plan).is_err(), refused.contains(sql), "{sql}");
    }
}

#[test]
fn retracting_sources_pick_their_sinks_and_refuse_what_is_not_supported_yet() {
    let sinks = |sql: &str| ops(&Circuit::compile(&def_for(sql, false).view()).unwrap()).pop().unwrap();
    assert_eq!(sinks("SELECT ts, amount FROM changes"), "Sink(counted rows)");
    assert_eq!(sinks("SELECT DISTINCT game_id FROM changes"), "Sink(upsert)");
    assert_eq!(sinks("SELECT DISTINCT game_id FROM clicks"), "Sink(append)");
    assert_eq!(sinks("SELECT ts FROM clicks UNION ALL SELECT ts FROM changes"), "Sink(counted rows)");
    // A window is emitted once and never retracted, so its sink stays an append sink.
    assert_eq!(sinks("SELECT window_start, count(*) AS n FROM TUMBLE(changes, ts, INTERVAL 1 MINUTE) GROUP BY ALL"), "Sink(append)");
    let late = Circuit::compile(&def_for("SELECT window_start, count(*) AS n FROM TUMBLE(changes, ts, INTERVAL 1 MINUTE) GROUP BY ALL", true).view());
    assert!(late.unwrap_err().to_string().contains("late-data table"));
    let (conn, sources) = super::setup();
    let window_min = "SELECT window_start, min(amount) AS lo FROM TUMBLE(changes, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    assert!(crate::plan::compile(&conn, "v", window_min, &sources).unwrap_err().to_string().contains("count per value"));
}

#[test]
fn lake_inputs_are_change_feeds_alone_and_lookups_next_to_kafka() {
    let (conn, sources) = super::setup();
    let compile = |sql: &str| crate::plan::compile(&conn, "v", sql, &sources);
    let first_op = |p: &crate::plan::Plan| ops(&Circuit::compile(&crate::View { id: "v", sink: super::SINK, late_data: None, plan: p }).unwrap()).remove(0);

    // Alone, a lake table is the view's stream, read through its change feed.
    let p = compile("SELECT id, amount FROM shop.orders WHERE amount > 5").unwrap();
    assert_eq!(p.sources(), ["shop.orders"]);
    assert_eq!(p.change_feeds, ["shop.orders"]);
    assert_eq!(first_op(&p), "ChangeFeed(shop.orders)");

    // Joined to a Kafka source it is a lookup: the Kafka source is the stream.
    let p = compile("SELECT c.ts, o.amount FROM clicks c JOIN shop.orders o ON o.game_id = c.game_id").unwrap();
    assert_eq!(p.sources(), ["clicks"]);
    assert!(p.change_feeds.is_empty());
    assert_eq!(first_op(&p), "Source(clicks)");

    // Windows over a lake input use its commit time, or a windowed view's window columns.
    compile("SELECT window_start, count(*) AS n FROM TUMBLE(shop.orders, _commit_ts, INTERVAL 1 MINUTE) GROUP BY ALL").unwrap();
    let err = compile("SELECT window_start, count(*) AS n FROM TUMBLE(per_min, game_id, INTERVAL 1 HOUR) GROUP BY ALL").unwrap_err();
    assert!(err.to_string().contains("must use"), "{err}");
    // A window over a windowed view's table replaces that view's window columns with its own.
    let p = compile("SELECT window_start, game_id, sum(n) AS n FROM TUMBLE(per_min, window_end, INTERVAL 1 HOUR) GROUP BY ALL").unwrap();
    assert_eq!(p.query.columns.iter().filter(|c| c.as_str() == "window_start").count(), 1);

    // A lake table read with its updates and deletes retracts: keyless rows are counted, the rest upserted.
    let last_op = |sql: &str| ops(&Circuit::compile(&crate::View { id: "v", sink: super::SINK, late_data: None, plan: &compile(sql).unwrap() }).unwrap()).pop().unwrap();
    let p = compile("SELECT id, amount, _commit_ts FROM shop.accounts WHERE amount > 5").unwrap();
    assert_eq!(p.retracting, ["shop.accounts"]);
    assert_eq!(first_op(&p), "ChangeFeed(shop.accounts)");
    assert_eq!(last_op("SELECT id, amount FROM shop.accounts"), "Sink(counted rows)");
    assert_eq!(last_op("SELECT DISTINCT game_id FROM shop.accounts"), "Sink(upsert)");
    let p = compile("SELECT game_id, count(*) AS n, min(amount) AS lo FROM shop.accounts GROUP BY game_id").unwrap();
    assert!(p.global.as_ref().unwrap().counted_values);
    // UNION ALL over a lake table and a view's table: two change feeds in one view, counted.
    let p = compile("SELECT game_id, amount FROM shop.accounts WHERE amount > 90 UNION ALL SELECT game_id, n FROM per_min").unwrap();
    assert_eq!(p.change_feeds, ["shop.accounts", "per_min"]);
    assert_eq!(p.retracting, ["shop.accounts"]);
    // Time buckets kept up to date: a group per bucket of the commit time, which retractions also carry.
    let p = compile("SELECT time_bucket(INTERVAL 1 HOUR, _commit_ts) AS hour, count(*) AS n FROM shop.accounts GROUP BY ALL").unwrap();
    assert_eq!(p.global.as_ref().unwrap().sink_keys, ["hour"]);

    // A join of two lake inputs is not supported yet.
    let err = compile("SELECT o.id FROM shop.orders o JOIN per_min m ON m.game_id = o.game_id").unwrap_err();
    assert!(err.to_string().contains("joins of two"), "{err}");
}
