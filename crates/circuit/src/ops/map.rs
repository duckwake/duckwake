use super::{Class, Operator};
use crate::codegen::{Cx, Rel};
use anyhow::Result;

/// A projection: one output row per input row, each column an expression over the input's.
#[derive(Debug, Clone, PartialEq)]
pub struct Map {
    /// What the projection is for, for `explain`.
    pub label: String,
    /// Select items, `expr AS name`.
    pub items: Vec<String>,
}

impl Operator for Map {
    fn name(&self) -> String {
        format!("Map({})", self.label)
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn stage(&self, _cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let (from, filter) = inputs[0].clone().open();
        Ok(vec![Rel::Block { from, filter, select: Some(self.items.join(", ")) }])
    }
}
