use super::{not_incremental, Class, Operator, Stateful};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::{windowed_partials_sql, AggKind, GlobalTables, Plan};
use crate::sql::state_table;
use anyhow::{bail, Result};

/// Grouped aggregation. Windowed: partials per window and group, emitted once the watermark closes the window.
/// Global: one row per group, replaced when it changes.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub grouping: Grouping,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Grouping {
    Windowed,
    Global,
}

impl Operator for Aggregate {
    fn name(&self) -> String {
        match self.grouping {
            Grouping::Windowed => "Aggregate(windowed, emit on close)".into(),
            Grouping::Global => "Aggregate(global)".into(),
        }
    }

    fn class(&self) -> Class {
        Class::NonLinear
    }

    fn props(&self, inputs: &[Props]) -> Props {
        match self.grouping {
            // A window is emitted once, when it closes, so the output retracts only if the input does.
            Grouping::Windowed => Props { append_only: inputs.iter().all(|p| p.append_only) },
            // A group's row is replaced when the group changes: the old one is retracted.
            Grouping::Global => Props { append_only: false },
        }
    }

    fn check(&self, inputs: &[Props], plan: &Plan) -> Result<()> {
        if inputs.iter().all(|p| p.append_only) {
            return Ok(());
        }
        let min_max = plan.global.as_ref().is_some_and(|g| g.aggs.iter().any(|a| matches!(a.kind, AggKind::Min | AggKind::Max)));
        let why = match self.grouping {
            Grouping::Windowed => "a windowed aggregate's partials ignore weights",
            Grouping::Global if min_max => "MIN and MAX keep one value per group",
            Grouping::Global => return Ok(()),
        };
        bail!("{} reads a stream that can retract rows, but {why}: not supported yet", self.name())
    }

    fn stage(&self, _cx: &mut Cx, _inputs: &[Rel]) -> Result<Vec<Rel>> {
        Err(not_incremental(&self.name()))
    }

    fn stateful(&self) -> Option<&dyn Stateful> {
        Some(self)
    }
}

impl Stateful for Aggregate {
    fn stage_delta(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let (plan, x) = (cx.plan, cx.x);
        match self.grouping {
            Grouping::Windowed => {
                // The epoch's partials per window and key, merged into the state at commit.
                let out = staging("out", &cx.set);
                let sql = format!("CREATE OR REPLACE TABLE {out} AS {}", windowed_partials_sql(plan, &inputs[0].sql()));
                cx.steps.push(Step::stage(sql).counting(Counter::Staged, &out));
                Ok(vec![Rel::Sql(out)])
            }
            Grouping::Global => {
                let g = plan.global.as_ref().unwrap();
                let mut t = x.global_tables()?;
                cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {} AS {}", t.pre, inputs[0].query())));
                if let Some(prev) = x.overlay {
                    // The previous epoch may still be committing: read its new state for the groups it touched.
                    let prev_new = GlobalTables::new(x.job, state_table(x.job), String::new(), plan, prev).new_state;
                    t.state = g.overlay_sql(&t.state, &prev_new, &plan.state_columns);
                }
                for (k, s) in g.aggregate_stmts(&t, &plan.state_columns).into_iter().enumerate() {
                    let step = Step::stage(s);
                    cx.steps.push(if k == 0 { step.counting(Counter::Staged, &t.delta) } else { step });
                }
                // The output change: the touched groups' old rows retracted, their new rows inserted.
                Ok(vec![Rel::Sql(g.change_sql(&t))])
            }
        }
    }

    fn commit_delta(&self, cx: &Cx) -> Result<Vec<Step>> {
        let (plan, x) = (cx.plan, cx.x);
        let mut steps = Vec::new();
        match self.grouping {
            Grouping::Windowed => {
                let st = state_table(x.job);
                steps.push(Step::commit(plan.merge_sql(&st, &staging("out", &cx.set)), Cost::State).skip_if_none(Counter::Staged).crash_after());
                if let Some(wm) = &x.watermark {
                    // Emit the windows the watermark closed, then drop them from the state.
                    let sql = format!(
                        "INSERT INTO {} ({}) SELECT {}, {} FROM {st} WHERE __we <= {wm}",
                        x.sink,
                        cx.sink_cols(),
                        plan.final_select,
                        x.epoch
                    );
                    steps.push(Step::commit(sql, Cost::Sink).output_rows().needs_close());
                    steps.push(Step::commit(format!("DELETE FROM {st} WHERE __we <= {wm}"), Cost::State).needs_close());
                }
            }
            Grouping::Global => {
                let t = x.global_tables()?;
                let stmts = plan.global.as_ref().unwrap().commit_state_sql(&t, &plan.state_columns);
                let last = stmts.len() - 1;
                for (k, s) in stmts.into_iter().enumerate() {
                    let step = Step::commit(s, Cost::State).skip_if_none(Counter::Staged);
                    steps.push(if k == last { step.crash_after() } else { step });
                }
            }
        }
        Ok(steps)
    }
}
