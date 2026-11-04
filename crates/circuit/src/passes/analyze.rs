//! Node properties, and the operators that need them.
use crate::ir::{Circuit, Operator, Props};
use crate::plan::Plan;
use anyhow::Result;

impl Circuit {
    /// Fills in each node's properties from its inputs', in topological order.
    pub fn analyze(&mut self) {
        for n in 0..self.nodes.len() {
            let inputs: Vec<Props> = self.nodes[n].inputs.iter().map(|p| self.nodes[p.node].props).collect();
            self.nodes[n].props = self.nodes[n].op.props(&inputs);
        }
    }

    /// Refuses a circuit where an operator implemented for append-only input reads a stream that can retract.
    pub fn check(&self, plan: &Plan) -> Result<()> {
        for node in &self.nodes {
            let inputs: Vec<Props> = node.inputs.iter().map(|p| self.nodes[p.node].props).collect();
            node.op.check(&inputs, plan)?;
        }
        Ok(())
    }
}
