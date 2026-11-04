//! The circuit: operators, ports, nodes and their properties.
mod display;

pub use crate::ops::{Aggregate, Grouping, Op, Operator, Sink, SinkKind, Source};
use crate::plan::Plan;

/// A view as the circuit needs it.
#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    pub id: &'a str,
    /// The DuckLake table it maintains, and its late-data table (`schema.table` or `lake.schema.table`).
    pub sink: &'a str,
    pub late_data: Option<&'a str>,
    pub plan: &'a Plan,
}

/// An output port of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Port {
    pub node: usize,
    pub port: usize,
}

/// What the analyses know about a node's output.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Props {
    /// No row with weight −1 can reach it: rows are only ever added.
    pub append_only: bool,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub op: Op,
    pub inputs: Vec<Port>,
    pub props: Props,
}

#[derive(Debug, Clone, Default)]
pub struct Circuit {
    /// In topological order: a node reads only earlier nodes.
    pub nodes: Vec<Node>,
    /// The rewrites `incrementalize` applied, in order.
    pub trace: Vec<String>,
    /// True once the circuit runs on changes.
    pub incremental: bool,
}

impl Circuit {
    /// Adds a node; until `analyze` runs, it has its first input's properties.
    pub(crate) fn add(&mut self, op: Op, inputs: Vec<Port>) -> Port {
        let props = inputs.first().map(|p| self.nodes[p.node].props).unwrap_or_default();
        self.nodes.push(Node { op, inputs, props });
        Port { node: self.nodes.len() - 1, port: 0 }
    }

    /// Readers of `p`: (node, input slot).
    pub(crate) fn readers(&self, p: Port, dead: &[bool]) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for (n, node) in self.nodes.iter().enumerate() {
            if dead[n] {
                continue;
            }
            for (slot, i) in node.inputs.iter().enumerate() {
                if *i == p {
                    out.push((n, slot));
                }
            }
        }
        out
    }

    pub(crate) fn redirect(&mut self, from: Port, to: Port, except: Option<usize>, dead: &[bool]) {
        for (n, slot) in self.readers(from, dead) {
            if Some(n) != except {
                self.nodes[n].inputs[slot] = to;
            }
        }
    }

    /// Drops dead nodes and renumbers the rest in topological order.
    pub(crate) fn compact(&self, dead: &[bool]) -> Circuit {
        let live: Vec<usize> = (0..self.nodes.len()).filter(|n| !dead[*n]).collect();
        let mut order = Vec::new();
        let mut placed = vec![false; self.nodes.len()];
        while order.len() < live.len() {
            for &n in &live {
                if !placed[n] && self.nodes[n].inputs.iter().all(|p| placed[p.node]) {
                    placed[n] = true;
                    order.push(n);
                }
            }
        }
        let new_id = |old: usize| order.iter().position(|n| *n == old).unwrap();
        let nodes = order
            .iter()
            .map(|&n| Node {
                op: self.nodes[n].op.clone(),
                inputs: self.nodes[n].inputs.iter().map(|p| Port { node: new_id(p.node), port: p.port }).collect(),
                props: self.nodes[n].props,
            })
            .collect();
        Circuit { nodes, trace: Vec::new(), incremental: false }
    }

    pub(crate) fn has(&self, op: &Op) -> bool {
        self.nodes.iter().any(|n| &n.op == op)
    }

    /// Windowed aggregates emit a window only once the watermark closes it.
    pub fn emits_final(&self) -> bool {
        self.has(&Op::Delta(Box::new(Op::Aggregate(Aggregate { grouping: Grouping::Windowed }))))
    }

    /// Whether the output rows are the changed groups (an upsert sink) rather than rows inserted.
    pub fn upserts(&self) -> bool {
        self.has(&Op::Sink(Sink { kind: SinkKind::Upsert }))
    }
}
