use super::{Class, Operator};
use crate::codegen::{staging, Counter, Cx, Rel, Step};
use anyhow::Result;

/// Port 0: rows of windows still open at the previous watermark. Port 1: late rows of windows already closed.
#[derive(Debug, Clone, PartialEq)]
pub struct LateSplit;

impl Operator for LateSplit {
    fn name(&self) -> String {
        "LateSplit(on time | late)".into()
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn outputs(&self) -> usize {
        2
    }

    fn stage(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let (late, prev, cols, input) = (staging("late", &cx.set), &cx.x.prev_watermark, &cx.cols, inputs[0].sql());
        cx.steps.push(
            Step::stage(format!("CREATE OR REPLACE TABLE {late} AS SELECT * FROM {input} WHERE window_end <= {prev}"))
                .counting(Counter::Late, &late),
        );
        let on_time = format!("(SELECT {cols}, __w, window_start, window_end FROM {input} WHERE {prev} IS NULL OR window_end > {prev})");
        Ok(vec![Rel::Sql(on_time), Rel::Sql(late)])
    }
}
