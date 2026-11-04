use super::{Class, Operator};
use crate::codegen::{Cx, Rel};
use crate::ir::Props;
use anyhow::Result;

/// The source's rows, each with its weight `__w`. In Q the whole collection; after incrementalization the epoch's
/// new rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub name: String,
    /// Whether its rows are only ever inserted (Kafka without CDC).
    pub append_only: bool,
}

impl Operator for Source {
    fn name(&self) -> String {
        format!("Source({})", self.name)
    }

    fn class(&self) -> Class {
        Class::Source
    }

    fn props(&self, _inputs: &[Props]) -> Props {
        Props { append_only: self.append_only }
    }

    fn stage(&self, cx: &mut Cx, _inputs: &[Rel]) -> Result<Vec<Rel>> {
        // Kafka rows are inserts: weight +1.
        let (cols, typed) = (&cx.cols, &cx.x.typed);
        Ok(vec![Rel::Sql(format!("(SELECT _partition, _offset, {cols}, 1::INTEGER AS __w FROM {typed} WHERE _err IS NULL)"))])
    }
}
