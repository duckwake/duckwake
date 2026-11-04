use super::{not_incremental, Class, Operator};
use crate::codegen::{Cx, Rel};
use anyhow::Result;

/// I: changes in, the collection so far out. Only while the circuit is rewritten.
#[derive(Debug, Clone, PartialEq)]
pub struct Integrate;

impl Operator for Integrate {
    fn name(&self) -> String {
        "I".into()
    }

    fn class(&self) -> Class {
        Class::Rewrite
    }

    fn stage(&self, _cx: &mut Cx, _inputs: &[Rel]) -> Result<Vec<Rel>> {
        Err(not_incremental(&self.name()))
    }
}
