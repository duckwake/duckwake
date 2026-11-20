use super::{Class, Operator};
use crate::codegen::{Cx, Rel};
use crate::ir::Props;
use crate::sql::{ident, lit, TOMBSTONE};
use anyhow::Result;

/// The source's rows, each with its weight `__w`. In Q the whole collection; after incrementalization the epoch's
/// new rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    pub name: String,
    /// Whether its rows are only ever inserted (Kafka without CDC). Otherwise each staged row has its weight.
    pub append_only: bool,
    /// Read through a DuckLake change feed (a lake table or view) rather than from Kafka.
    pub change_feed: bool,
}

impl Operator for Source {
    fn name(&self) -> String {
        if self.change_feed { format!("ChangeFeed({})", self.name) } else { format!("Source({})", self.name) }
    }

    fn class(&self) -> Class {
        Class::Source
    }

    fn props(&self, _inputs: &[Props]) -> Props {
        Props { append_only: self.append_only }
    }

    fn stage(&self, cx: &mut Cx, _inputs: &[Rel]) -> Result<Vec<Rel>> {
        // Kafka rows are inserts: weight +1. A retracting source stages each row's weight.
        let input = cx.x.source(&self.name)?;
        let cols = input.columns.iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", ");
        if cx.plan.upsert.is_some() {
            // Read as upserts: every message with its key, tombstones too (`__del`), for the Upsert operator.
            return Ok(vec![Rel::Sql(format!(
                "(SELECT _partition, _offset, _key, _err IS NOT NULL AS __del, {cols}, 1::INTEGER AS __w FROM {}
                  WHERE _err IS NULL OR _err = {})",
                input.typed,
                lit(TOMBSTONE)
            ))]);
        }
        let w = if self.append_only { "1::INTEGER" } else { "__w::INTEGER" };
        Ok(vec![Rel::Sql(format!("(SELECT _partition, _offset, {cols}, {w} AS __w FROM {} WHERE _err IS NULL)", input.typed))])
    }
}
