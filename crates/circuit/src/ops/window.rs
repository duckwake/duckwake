use super::{Class, Operator};
use crate::codegen::{staging, Cx, Rel, Step};
use crate::plan::{WindowKind, WindowSpec};
use anyhow::Result;

/// TUMBLE or HOP: one row per window a row falls in, with `window_start` and `window_end`.
#[derive(Debug, Clone, PartialEq)]
pub struct Window(pub WindowSpec);

impl Operator for Window {
    fn name(&self) -> String {
        let w = &self.0;
        let secs = |us: i64| format!("{}s", us / 1_000_000);
        match w.kind {
            WindowKind::Tumble => format!("Window(TUMBLE {}, {})", w.time_col, secs(w.size_us)),
            WindowKind::Hop => format!("Window(HOP {}, {} every {})", w.time_col, secs(w.size_us), secs(w.slide_us)),
        }
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn stage(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        // Read twice by the late split, so materialized.
        let win = staging("win", cx.x.job);
        cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {win} AS {}", cx.plan.window_sql(&inputs[0].sql()))));
        Ok(vec![Rel::Sql(win)])
    }
}
