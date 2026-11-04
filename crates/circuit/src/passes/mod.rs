//! The passes from Q to the circuit that runs every epoch.
pub mod analyze;
pub mod incrementalize;

use crate::ir::{Circuit, View};
use anyhow::Result;

impl Circuit {
    /// The circuit of `plan` that runs every epoch.
    pub fn compile(view: &View) -> Result<Circuit> {
        let q = Circuit::query(view);
        q.check(view.plan)?;
        Ok(q.incrementalize())
    }
}
