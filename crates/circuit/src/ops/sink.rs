use super::{Class, Operator};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::{GlobalTables, Plan};
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
    /// The view's table, one row per key, replaced when it changes: the group keys of a global aggregate, the
    /// whole row of `DISTINCT`.
    Upsert,
    /// The view's table, one row per distinct row with its multiplicity in `_duckwake_count`: a view that can
    /// retract and has no key.
    Counted,
    /// The late-data table.
    Late,
}

/// The column a counted sink keeps each row's multiplicity in.
pub const COUNT_COLUMN: &str = "_duckwake_count";

impl Operator for Sink {
    fn name(&self) -> String {
        format!(
            "Sink({})",
            match self.kind {
                SinkKind::Append => "append",
                SinkKind::Upsert => "upsert",
                SinkKind::Counted => "counted rows",
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
        if inputs.iter().all(|p| p.append_only) {
            return Ok(());
        }
        match self.kind {
            SinkKind::Append => bail!("{} reads a stream that can retract rows, but an append sink only inserts: not supported yet", self.name()),
            SinkKind::Late => bail!("late rows of a source that can retract need their weights in the late-data table: not supported yet (drop the late-data table)"),
            SinkKind::Upsert | SinkKind::Counted => Ok(()),
        }
    }

    fn stage(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        match self.kind {
            SinkKind::Upsert | SinkKind::Counted => {
                let t = cx.x.global_tables()?;
                let mut stmts = vec![consolidate_sql(&t, &inputs[0].sql())];
                if self.kind == SinkKind::Upsert {
                    stmts.push(changed_keys_sql(&t, &upsert_keys(cx.plan)));
                }
                let (last, counted) = (stmts.len() - 1, if self.kind == SinkKind::Upsert { &t.changed } else { &t.changes });
                for (k, s) in stmts.into_iter().enumerate() {
                    let step = Step::stage(s);
                    cx.steps.push(if k == last { step.counting(Counter::Changed, counted) } else { step });
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
                for s in commit_upsert_sql(&t, &upsert_keys(cx.plan), &x.epoch) {
                    steps.push(Step::commit(s, Cost::Sink).skip_if_none(Counter::Changed));
                }
            }
            SinkKind::Counted => {
                let t = x.global_tables()?;
                let stmts = commit_counted_sql(&t, &x.epoch);
                let last = stmts.len() - 1;
                for (k, s) in stmts.into_iter().enumerate() {
                    let step = Step::commit(s, Cost::Sink).skip_if_none(Counter::Changed);
                    steps.push(if k == last { step.crash_after() } else { step });
                }
            }
            // A windowed aggregate writes closed windows to the sink itself, from its state after the merge.
            SinkKind::Append if cx.emits_final => {}
            SinkKind::Append => {
                let sql = format!(
                    "INSERT INTO {} ({}) SELECT * EXCLUDE (__w), {} FROM {}",
                    x.sink,
                    cx.sink_cols(),
                    x.epoch,
                    staging("out", &cx.set)
                );
                steps.push(Step::commit(sql, Cost::Sink).skip_if_none(Counter::Changed).output_rows().crash_after());
            }
            SinkKind::Late => {
                let Some(late) = &x.late_table else { return Ok(steps) };
                let cols = x.stream_columns().iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", ");
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

/// The columns an upsert sink replaces rows by: an upsert view's keys, a global aggregate's group keys, or every
/// column.
fn upsert_keys(plan: &Plan) -> Vec<String> {
    if let Some(u) = &plan.upsert {
        return u.key.clone();
    }
    match &plan.global {
        Some(g) => g.sink_keys.clone(),
        None => plan.sink_columns.iter().map(|(c, _)| c.clone()).collect(),
    }
}

/// Null-safe equality of `cols` between aliases `a` and `b`; TRUE for no columns.
fn cols_match(cols: &[String], a: &str, b: &str) -> String {
    let eq: Vec<String> = cols.iter().map(|c| format!("{a}.{c} IS NOT DISTINCT FROM {b}.{c}", c = ident(c))).collect();
    if eq.is_empty() { "TRUE".into() } else { eq.join(" AND ") }
}

/// The sink's input change, consolidated: a row whose weights cancel out is not a change.
fn consolidate_sql(t: &GlobalTables, input: &str) -> String {
    let cols: Vec<String> = t.sink_columns.iter().map(|c| ident(c)).collect();
    format!(
        "CREATE OR REPLACE TABLE {} AS SELECT {}, CAST(sum(__w) AS BIGINT) AS __w FROM {input} GROUP BY ALL HAVING sum(__w) <> 0",
        t.changes,
        cols.join(", ")
    )
}

/// The keys of the rows the change touches.
fn changed_keys_sql(t: &GlobalTables, keys: &[String]) -> String {
    let keys: Vec<String> = keys.iter().map(|k| ident(k)).collect();
    let keys = if keys.is_empty() { "1 AS __one".to_string() } else { keys.join(", ") };
    format!("CREATE OR REPLACE TABLE {} AS SELECT DISTINCT {keys} FROM {}", t.changed, t.changes)
}

/// An upsert sink's writes at commit: the changed keys' rows replaced by their new ones, if any.
fn commit_upsert_sql(t: &GlobalTables, keys: &[String], epoch: &str) -> Vec<String> {
    let cols: Vec<String> = t.sink_columns.iter().map(|c| ident(c)).collect();
    vec![
        format!("DELETE FROM {} s WHERE EXISTS (SELECT 1 FROM {} c WHERE {})", t.sink, t.changed, cols_match(keys, "s", "c")),
        format!("INSERT INTO {} ({c}, _duckwake_epoch) SELECT {c}, {epoch} FROM {} WHERE __w > 0", t.sink, t.changes, c = cols.join(", ")),
    ]
}

/// A counted sink's writes at commit: each changed row's count moves by its weight; a new row is inserted with
/// its weight, and a row whose count reaches 0 is deleted. (A DuckLake MERGE allows one UPDATE or DELETE action.)
fn commit_counted_sql(t: &GlobalTables, epoch: &str) -> Vec<String> {
    let cols: Vec<String> = t.sink_columns.iter().map(|c| ident(c)).collect();
    let (on, n) = (cols_match(&t.sink_columns, "s", "c"), COUNT_COLUMN);
    vec![
        format!("UPDATE {} s SET {n} = s.{n} + c.__w, _duckwake_epoch = {epoch} FROM {} c WHERE {on}", t.sink, t.changes),
        format!(
            "INSERT INTO {sink} ({c}, {n}, _duckwake_epoch) SELECT {c}, __w, {epoch} FROM {} c
               WHERE NOT EXISTS (SELECT 1 FROM {sink} s WHERE {on})",
            t.changes,
            sink = t.sink,
            c = cols.join(", ")
        ),
        format!("DELETE FROM {} s WHERE s.{n} <= 0 AND EXISTS (SELECT 1 FROM {} c WHERE {on})", t.sink, t.changes),
    ]
}
