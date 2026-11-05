use super::{not_incremental, Class, Operator, Stateful};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::{windowed_partials_sql, AggKind, GlobalTables, Plan, WINDOW_WEIGHT};
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

    fn props(&self, _inputs: &[Props]) -> Props {
        match self.grouping {
            // A window is emitted once, when it closes, and never retracted: a later change to it is late.
            Grouping::Windowed => Props { append_only: true },
            // A group's row is replaced when the group changes: the old one is retracted.
            Grouping::Global => Props { append_only: false },
        }
    }

    fn check(&self, inputs: &[Props], plan: &Plan) -> Result<()> {
        if inputs.iter().all(|p| p.append_only) {
            return Ok(());
        }
        // The plan counts values for MIN and MAX when a source it reads can retract.
        let min_max = plan
            .global
            .as_ref()
            .is_some_and(|g| !g.counted_values && g.aggs.iter().any(|a| matches!(a.kind, AggKind::Min | AggKind::Max)));
        let why = match self.grouping {
            // The plan sums weights in its partials when a source it reads can retract.
            Grouping::Windowed if !plan.retracts() => "a windowed aggregate's partials ignore weights",
            Grouping::Windowed => return Ok(()),
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
                let groups = g.group_columns(&plan.state_columns);
                if g.counted_values {
                    // Group rows and value rows share the state table; each read takes its own.
                    let names = |cols: Vec<(String, String)>| cols.into_iter().map(|(c, _)| c).collect::<Vec<_>>().join(", ");
                    let st = state_table(x.job);
                    t.state = format!("(SELECT {} FROM {st} WHERE __i IS NULL)", names(groups.clone()));
                    t.state_values = format!("(SELECT {} FROM {st} WHERE __i IS NOT NULL)", names(g.value_columns(&plan.state_columns)));
                }
                if let Some(prev) = x.overlay {
                    // The previous epoch may still be committing: read its new state for the groups it touched.
                    let prev = GlobalTables::new(x.job, state_table(x.job), String::new(), plan, prev);
                    t.state = g.overlay_sql(&t.state, &prev.new_state, &groups);
                    if g.counted_values {
                        t.state_values = g.values_overlay_sql(&t.state_values, &prev.vnew, &plan.state_columns);
                    }
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
                    // Emit the windows the watermark closed, then drop them from the state. Over retractions a
                    // window and group whose rows were all retracted has a weight of 0 and no row.
                    let live = if plan.retracts() { format!(" AND {WINDOW_WEIGHT} <> 0") } else { String::new() };
                    let sql = format!(
                        "INSERT INTO {} ({}) SELECT {}, {} FROM {st} WHERE __we <= {wm}{live}",
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
