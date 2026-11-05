use super::{Class, Operator};
use crate::codegen::{Cx, Rel};
use anyhow::Result;

/// `UNION ALL`: the sum of its inputs' Z-sets. Linear in each input, so it runs on changes as it is once every
/// input is integrated (Feldera's `DBSPSumOperator`).
#[derive(Debug, Clone, PartialEq)]
pub struct Union;

impl Operator for Union {
    fn name(&self) -> String {
        "Union".into()
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn stage(&self, _cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let parts: Vec<String> = inputs.iter().map(|r| format!("({})", r.query())).collect();
        Ok(vec![Rel::Sql(format!("({})", parts.join(" UNION ALL ")))])
    }
}
