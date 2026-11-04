use super::{not_incremental, Class, Operator};
use crate::codegen::Cx;
use anyhow::Result;

/// D: collections in, what changed since the last step out. Only while the circuit is rewritten.
#[derive(Debug, Clone, PartialEq)]
pub struct Differentiate;

impl Operator for Differentiate {
    fn name(&self) -> String {
        "D".into()
    }

    fn class(&self) -> Class {
        Class::Rewrite
    }

    fn stage(&self, _cx: &mut Cx, _inputs: &[String]) -> Result<Vec<String>> {
        Err(not_incremental(&self.name()))
    }
}
