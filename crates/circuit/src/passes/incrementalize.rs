//! D ∘ Q ∘ I and Feldera's rules (see the crate docs).
use crate::ir::{Circuit, Op, Operator, Port};
use crate::ops::{Class, Differentiate, Integrate};

impl Circuit {
    /// D ∘ Q ∘ I, rewritten until no integrator or differentiator is left (see the module docs).
    pub fn incrementalize(mut self) -> Circuit {
        let mut dead = vec![false; self.nodes.len()];
        let mut trace = Vec::new();
        // Wrap: an integrator after every source, a differentiator before every sink.
        for n in 0..self.nodes.len() {
            if self.nodes[n].op.class() == Class::Source {
                let out = Port { node: n, port: 0 };
                let i = self.add(Op::Integrate(Integrate), vec![out]);
                dead.push(false);
                self.redirect(out, i, Some(i.node), &dead);
                trace.push(format!("wrap: {} now emits the epoch's changes; I integrates them", self.nodes[n].op.name()));
            }
        }
        for n in 0..self.nodes.len() {
            if self.nodes[n].op.class() == Class::Sink {
                let input = self.nodes[n].inputs[0];
                let d = self.add(Op::Differentiate(Differentiate), vec![input]);
                dead.push(false);
                self.nodes[n].inputs[0] = d;
                trace.push(format!("wrap: D before {}", self.nodes[n].op.name()));
            }
        }
        // Push every integrator forward, as Feldera's OptimizeIncrementalVisitor does, until it meets a
        // differentiator: an operator whose input is integrated runs on the changes instead, followed by an
        // integrator on each of its outputs.
        loop {
            let mut changed = false;
            for n in 0..self.nodes.len() {
                if dead[n] || self.nodes[n].inputs.is_empty() {
                    continue;
                }
                let input = self.nodes[n].inputs[0];
                if self.nodes[input.node].op != Op::Integrate(Integrate) {
                    continue;
                }
                let op = self.nodes[n].op.clone();
                let rule = match op.class() {
                    // D ∘ I = id.
                    Class::Rewrite if op == Op::Differentiate(Differentiate) => {
                        let x = self.nodes[input.node].inputs[0];
                        let sinks: Vec<String> = self.readers(Port { node: n, port: 0 }, &dead).iter().map(|(r, _)| self.nodes[*r].op.name()).collect();
                        self.redirect(Port { node: n, port: 0 }, x, None, &dead);
                        dead[n] = true;
                        format!("D ∘ I = id before {}", sinks.join(", "))
                    }
                    // L ∘ I = I ∘ L.
                    Class::Linear => format!("{} ∘ I = I ∘ {0}: it is linear", op.name()),
                    // S ∘ I = I ∘ SΔ: the operator's incremental form, by definition SΔ = D ∘ S ∘ I.
                    Class::NonLinear => {
                        self.nodes[n].op = Op::Delta(Box::new(op.clone()));
                        format!("{} ∘ I = I ∘ {}: state in a DuckLake table", op.name(), self.nodes[n].op.name().replace(" [state table]", ""))
                    }
                    _ => continue,
                };
                if op != Op::Differentiate(Differentiate) {
                    self.nodes[n].inputs[0] = self.nodes[input.node].inputs[0];
                    for port in 0..op.outputs() {
                        let out = Port { node: n, port };
                        let i = self.add(Op::Integrate(Integrate), vec![out]);
                        dead.push(false);
                        self.redirect(out, i, Some(i.node), &dead);
                    }
                }
                if self.readers(input, &dead).is_empty() {
                    dead[input.node] = true;
                }
                trace.push(rule);
                changed = true;
                break;
            }
            if !changed {
                break;
            }
        }
        // Like Feldera's NoIntegralVisitor: anything left needs an operator the rules do not know.
        if let Some(n) = (0..self.nodes.len()).find(|n| !dead[*n] && self.nodes[*n].op.class() == Class::Rewrite) {
            trace.push(format!("left over: {} at n{n}", self.nodes[n].op.name()));
        }
        let mut out = self.compact(&dead);
        out.trace = trace;
        out.incremental = true;
        out
    }
}
