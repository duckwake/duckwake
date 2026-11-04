use super::{Class, Operator};
use crate::codegen::{Cx, Rel};
use anyhow::Result;

/// Keeps the rows a predicate holds for: WHERE, or HAVING over an aggregate's output.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub pred: String,
}

impl Operator for Filter {
    fn name(&self) -> String {
        let pred = self.pred.split_whitespace().collect::<Vec<_>>().join(" ");
        let short: String = pred.chars().take(40).collect();
        format!("Filter({short}{})", if short.len() < pred.len() { "…" } else { "" })
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn stage(&self, _cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let (from, mut filter) = inputs[0].clone().open();
        filter.push(format!("({})", self.pred));
        Ok(vec![Rel::Block { from, filter, select: None }])
    }
}
