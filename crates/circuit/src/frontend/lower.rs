//! The compiled plan as Q, a graph of operators over whole collections.
use crate::ir::{Circuit, Port, View};
use crate::ops::{Aggregate, Distinct, Filter, From, Grouping, LateSplit, Map, Op, Sink, SinkKind, Source, Union, Window};
use crate::plan::{Plan, Query};

impl Circuit {
    /// Q: the view as a query over whole collections, built from the compiled plan.
    ///
    /// - stateless: Source → From → Filter → Map → Sink(append), with a Distinct before the sink for DISTINCT
    /// - windowed: Source → Window → LateSplit → From → Filter → Map → Aggregate → Sink(append), late rows to
    ///   Sink(late data)
    /// - global aggregate: Source → From → Filter → Map → Aggregate → Filter(HAVING) → Map → Sink(upsert)
    /// - UNION ALL: a stateless chain per branch, each from its own source, into Union → Sink(append)
    ///
    /// Over a source that can retract, a stateless view's sink keeps a count per row, and DISTINCT's an
    /// upsert by the whole row.
    pub fn query(view: &View) -> Circuit {
        let (plan, q) = (view.plan, &view.plan.query);
        let mut c = Circuit::default();
        let rows_sink = Op::Sink(Sink { kind: if plan.retracts() { SinkKind::Counted } else { SinkKind::Append } });
        if !plan.union.is_empty() {
            // UNION ALL: Source → From → Filter → Map per branch, then Union → Sink.
            let branches: Vec<Port> = plan.union.iter().map(|b| c.branch(plan, &b.source, &b.query, "select")).collect();
            let union = c.add(Op::Union(Union), branches);
            c.add(rows_sink, vec![union]);
            c.analyze();
            return c;
        }
        let src = c.add(source_op(plan, &plan.source), vec![]);
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
        } else if plan.distinct {
            last = c.add(Op::Distinct(Distinct), vec![last]);
            let kind = if plan.retracts() { SinkKind::Upsert } else { SinkKind::Append };
            c.add(Op::Sink(Sink { kind }), vec![last]);
        } else {
            c.add(rows_sink, vec![last]);
        }
        c.analyze();
        c
    }

    /// A plain view over one source: Source → From → Filter → Map.
    fn branch(&mut self, plan: &Plan, source: &str, q: &Query, label: &str) -> Port {
        let src = self.add(source_op(plan, source), vec![]);
        let from = Op::From(From { alias: q.alias.clone(), from: q.from.clone(), columns: q.columns.clone(), lookups: q.lookups.clone() });
        let mut last = self.add(from, vec![src]);
        if let Some(pred) = &q.filter {
            last = self.add(Op::Filter(Filter { pred: pred.clone() }), vec![last]);
        }
        self.add(Op::Map(Map { label: label.into(), items: q.project.clone() }), vec![last])
    }
}

/// A source of the view: Kafka without CDC only ever inserts.
fn source_op(plan: &Plan, name: &str) -> Op {
    Op::Source(Source {
        name: name.into(),
        append_only: !plan.retracting.iter().any(|s| s == name),
        change_feed: plan.change_feeds.iter().any(|s| s == name),
    })
}
