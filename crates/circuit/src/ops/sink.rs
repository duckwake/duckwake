use super::{Class, Operator};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::Plan;
use crate::sql::ident;
use anyhow::{bail, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct Sink {
    pub kind: SinkKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SinkKind {
    /// The view's table, rows only ever added.
    Append,
    /// The view's table, one row per group, replaced when it changes.
    Upsert,
    /// The late-data table.
    Late,
}

impl Operator for Sink {
    fn name(&self) -> String {
        format!(
            "Sink({})",
            match self.kind {
                SinkKind::Append => "append",
                SinkKind::Upsert => "upsert",
                SinkKind::Late => "late data",
            }
        )
    }

    fn class(&self) -> Class {
        Class::Sink
    }

    fn outputs(&self) -> usize {
        0
    }

    fn check(&self, inputs: &[Props], _plan: &Plan) -> Result<()> {
        if self.kind != SinkKind::Upsert && !inputs.iter().all(|p| p.append_only) {
            bail!("{} reads a stream that can retract rows, but an append sink only inserts: not supported yet", self.name());
        }
        Ok(())
    }

    fn stage(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        match self.kind {
            SinkKind::Upsert => {
                let t = cx.x.global_tables()?;
                let stmts = cx.plan.global.as_ref().unwrap().upsert_stmts(&t, &inputs[0].sql());
                let last = stmts.len() - 1;
                for (k, s) in stmts.into_iter().enumerate() {
                    let step = Step::stage(s);
                    cx.steps.push(if k == last { step.counting(Counter::Changed, &t.changed) } else { step });
                }
            }
            // The epoch's rows, written at commit. A windowed aggregate writes closed windows itself.
            SinkKind::Append if !cx.emits_final => {
                let out = staging("out", &cx.set);
                cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {out} AS {}", inputs[0].query())).counting(Counter::Changed, &out));
            }
            SinkKind::Append | SinkKind::Late => {}
        }
        Ok(vec![])
    }

    fn commit(&self, cx: &Cx) -> Result<Vec<Step>> {
        let x = cx.x;
        let mut steps = Vec::new();
        match self.kind {
            SinkKind::Upsert => {
                let t = x.global_tables()?;
                for s in cx.plan.global.as_ref().unwrap().commit_sink_sql(&t, &x.epoch) {
                    steps.push(Step::commit(s, Cost::Sink).skip_if_none(Counter::Changed));
                }
            }
            // A windowed aggregate writes closed windows to the sink itself, from its state after the merge.
            SinkKind::Append if cx.emits_final => {}
            SinkKind::Append => {
                let sql = format!("INSERT INTO {} ({}) SELECT *, {} FROM {}", x.sink, cx.sink_cols(), x.epoch, staging("out", &cx.set));
                steps.push(Step::commit(sql, Cost::Sink).skip_if_none(Counter::Changed).output_rows().crash_after());
            }
            SinkKind::Late => {
                let Some(late) = &x.late_table else { return Ok(steps) };
                let cols = x.source_columns.iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", ");
                let sql = format!(
                    "INSERT INTO {late} ({cols}, window_start, window_end, _source_partition, _source_offset, _duckwake_epoch)
                                 SELECT {cols}, window_start, window_end, _partition, _offset, {} FROM {}",
                    x.epoch,
                    staging("late", &cx.set)
                );
                steps.push(Step::commit(sql, Cost::Late).skip_if_none(Counter::Late));
            }
        }
        Ok(steps)
    }
}
