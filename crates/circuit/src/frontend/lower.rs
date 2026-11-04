//! The compiled plan as Q, a graph of operators over whole collections.
use crate::ir::{Circuit, Port, View};
use crate::ops::{Aggregate, Fragment, Grouping, LateSplit, Op, Sink, SinkKind, Source, SqlFragment, Window};

impl Circuit {
    /// Q: the view as a query over whole collections, built from the compiled plan.
    pub fn query(view: &View) -> Circuit {
        let plan = view.plan;
        let mut c = Circuit::default();
        // Kafka without CDC: every message is an insert.
        let src = c.add(Op::Source(Source { name: plan.source.clone(), append_only: true }), vec![]);
        if let Some(w) = &plan.window {
            let win = c.add(Op::Window(Window(w.clone())), vec![src]);
            let split = c.add(Op::LateSplit(LateSplit), vec![win]).node;
            let agg = c.add(Op::Aggregate(Aggregate { grouping: Grouping::Windowed }), vec![Port { node: split, port: 0 }]);
            c.add(Op::Sink(Sink { kind: SinkKind::Append }), vec![agg]);
            if view.late_data.is_some() {
                c.add(Op::Sink(Sink { kind: SinkKind::Late }), vec![Port { node: split, port: 1 }]);
            }
        } else if plan.global.is_some() {
            let pre = c.add(Op::SqlFragment(SqlFragment(Fragment::PreAggregate)), vec![src]);
            let agg = c.add(Op::Aggregate(Aggregate { grouping: Grouping::Global }), vec![pre]);
            let post = c.add(Op::SqlFragment(SqlFragment(Fragment::PostAggregate)), vec![agg]);
            c.add(Op::Sink(Sink { kind: SinkKind::Upsert }), vec![post]);
        } else {
            let view = c.add(Op::SqlFragment(SqlFragment(Fragment::View)), vec![src]);
            c.add(Op::Sink(Sink { kind: SinkKind::Append }), vec![view]);
        }
        c.analyze();
        c
    }
}
