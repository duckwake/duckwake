//! The compiled plan as Q, a graph of operators over whole collections.
use crate::ir::{Circuit, Port, View};
use crate::ops::{Aggregate, Distinct, Filter, From, Grouping, LateSplit, Map, Op, Sink, SinkKind, Source, Window};

impl Circuit {
    /// Q: the view as a query over whole collections, built from the compiled plan.
    ///
    /// - stateless: Source → From → Filter → Map → Sink(append), with a Distinct before the sink for DISTINCT
    /// - windowed: Source → Window → LateSplit → From → Filter → Map → Aggregate → Sink(append), late rows to
    ///   Sink(late data)
    /// - global aggregate: Source → From → Filter → Map → Aggregate → Filter(HAVING) → Map → Sink(upsert)
    pub fn query(view: &View) -> Circuit {
        let (plan, q) = (view.plan, &view.plan.query);
        let mut c = Circuit::default();
        // Kafka without CDC: every message is an insert.
        let src = c.add(Op::Source(Source { name: plan.source.clone(), append_only: true }), vec![]);
        let (stream, split) = match &plan.window {
            Some(w) => {
                let win = c.add(Op::Window(Window(w.clone())), vec![src]);
                let split = c.add(Op::LateSplit(LateSplit), vec![win]).node;
                (Port { node: split, port: 0 }, Some(split))
            }
            None => (src, None),
        };
        let from = Op::From(From { alias: q.alias.clone(), from: q.from.clone(), columns: q.columns.clone(), lookups: q.lookups.clone() });
        let mut last = c.add(from, vec![stream]);
        if let Some(pred) = &q.filter {
            last = c.add(Op::Filter(Filter { pred: pred.clone() }), vec![last]);
        }
        let label = if plan.window.is_some() || plan.global.is_some() { "aggregate input" } else { "select" };
        last = c.add(Op::Map(Map { label: label.into(), items: q.project.clone() }), vec![last]);
        if let Some(split) = split {
            let agg = c.add(Op::Aggregate(Aggregate { grouping: Grouping::Windowed }), vec![last]);
            c.add(Op::Sink(Sink { kind: SinkKind::Append }), vec![agg]);
            if view.late_data.is_some() {
                c.add(Op::Sink(Sink { kind: SinkKind::Late }), vec![Port { node: split, port: 1 }]);
            }
        } else if plan.global.is_some() {
            last = c.add(Op::Aggregate(Aggregate { grouping: Grouping::Global }), vec![last]);
            if let Some(pred) = &q.having {
                last = c.add(Op::Filter(Filter { pred: pred.clone() }), vec![last]);
            }
            // The output change carries its weights to the upsert sink.
            let items = q.finish.iter().cloned().chain(["__w".to_string()]).collect();
            last = c.add(Op::Map(Map { label: "select".into(), items }), vec![last]);
            c.add(Op::Sink(Sink { kind: SinkKind::Upsert }), vec![last]);
        } else {
            if plan.distinct {
                last = c.add(Op::Distinct(Distinct), vec![last]);
            }
            c.add(Op::Sink(Sink { kind: SinkKind::Append }), vec![last]);
        }
        c.analyze();
        c
    }
}
