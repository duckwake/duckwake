use super::{Class, Operator};
use crate::codegen::{Cx, Rel};
use anyhow::Result;

/// The stream's rows under the view's alias, joined with the lake tables the view looks up. A lookup table is
/// read whole every epoch, as a constant: the join is linear in the stream, and a change to the table reaches
/// only rows that arrive after it.
#[derive(Debug, Clone, PartialEq)]
pub struct From {
    pub alias: String,
    /// The FROM clause, with `{input}` where the stream's rows go, and the columns they expose.
    pub from: String,
    pub columns: Vec<String>,
    pub lookups: Vec<String>,
}

impl Operator for From {
    fn name(&self) -> String {
        if self.lookups.is_empty() {
            format!("From({})", self.alias)
        } else {
            format!("From({} ⋈ {})", self.alias, self.lookups.join(", "))
        }
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn stage(&self, _cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let from = self.from.replace("{input}", &format!("(SELECT {} FROM {})", self.columns.join(", "), inputs[0].sql()));
        Ok(vec![Rel::Block { from, filter: Vec::new(), select: None }])
    }
}
