use super::{not_incremental, Class, Operator, Stateful};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::UpsertSpec;
use crate::sql::{ident, state_table};
use anyhow::{Context, Result};

/// A Kafka source read as upserts (`Plan::upsert`): each key's current row, as a collection. Its incremental
/// form keeps every key's current row in the state table, the source's columns and nothing else, and outputs
/// each epoch's change: a changed key's old row −1 and its new row +1.
///
/// Per epoch, each key the epoch touches gets its best message after its last tombstone in the epoch
/// (`UpsertSpec::best_sql`). With a tombstone in the epoch, that message, or nothing, is the key's new row. Without
/// one, the message replaces the stored row when it is newer (`UpsertSpec::newer`): its messages come after the
/// stored row's, since a key's messages stay in one partition. This is the batch query `UpsertSpec::batch_sql`
/// taken one epoch at a time.
#[derive(Debug, Clone, PartialEq)]
pub struct Upsert;

impl Operator for Upsert {
    fn name(&self) -> String {
        "Upsert".into()
    }

    fn class(&self) -> Class {
        Class::NonLinear
    }

    fn props(&self, _inputs: &[Props]) -> Props {
        Props { append_only: false }
    }

    fn stage(&self, _cx: &mut Cx, _inputs: &[Rel]) -> Result<Vec<Rel>> {
        Err(not_incremental(&self.name()))
    }

    fn stateful(&self) -> Option<&dyn Stateful> {
        Some(self)
    }
}

/// The epoch's tables: each touched key's best message, its stored row, and the changed keys' new rows (`__del`
/// for a deleted key).
struct Tables {
    best: String,
    old: String,
    new: String,
}

impl Tables {
    fn of(cx: &Cx, parity: u64) -> Tables {
        let set = format!("{}_{parity}", cx.x.job);
        Tables { best: staging("ubest", &set), old: staging("uold", cx.x.job), new: staging("unew", &set) }
    }
}

fn spec<'a>(cx: &'a Cx) -> Result<&'a UpsertSpec> {
    cx.plan.upsert.as_ref().context("an Upsert operator in a view without upsert_key")
}

/// The source's columns, quoted: the state's columns.
fn source_cols(cx: &Cx) -> Vec<String> {
    cx.x.primary().columns.iter().map(|(c, _)| ident(c)).collect()
}

impl Stateful for Upsert {
    fn stage_delta(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let u = spec(cx)?.clone();
        let t = Tables::of(cx, cx.x.parity);
        let columns: Vec<(String, String)> = cx.x.primary().columns.to_vec();
        let cols = source_cols(cx);
        let all = cols.join(", ");
        let messages = format!("({})", u.messages_sql(&columns, &inputs[0].sql()));
        cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {} AS {}", t.best, u.best_sql(&messages))).counting(Counter::Staged, &t.best));

        // The previous epoch may still be committing: read its new rows for the keys it changed.
        let mut state = state_table(cx.x.job);
        if let Some(prev) = cx.x.overlay {
            let prev_new = Tables::of(cx, prev).new;
            state = format!(
                "(SELECT {all} FROM {prev_new} WHERE NOT __del
                  UNION ALL SELECT {all} FROM {state} s WHERE NOT EXISTS (SELECT 1 FROM {prev_new} p WHERE {}))",
                u.keys_match("s", "p")
            );
        }
        cx.steps.push(Step::stage(format!(
            "CREATE OR REPLACE TABLE {} AS SELECT s.* FROM {state} s WHERE EXISTS (SELECT 1 FROM {} b WHERE {})",
            t.old,
            t.best,
            u.keys_match("s", "b")
        )));

        // A key changes when the epoch has a tombstone for it, when it has no stored row, or when its message is
        // newer than the stored row; its new row is then the epoch's message, or none (`__del`).
        let o_absent = format!("NOT EXISTS (SELECT 1 FROM {} x WHERE {})", t.old, u.keys_match("x", "b"));
        cx.steps.push(Step::stage(format!(
            "CREATE OR REPLACE TABLE {new} AS
               SELECT {pick}, b.__del FROM {best} b
               WHERE b.__cut OR {o_absent} OR EXISTS (SELECT 1 FROM {old} o WHERE {on} AND {newer})",
            new = t.new,
            pick = cols.iter().map(|c| format!("b.{c}")).collect::<Vec<_>>().join(", "),
            best = t.best,
            old = t.old,
            on = u.keys_match("o", "b"),
            newer = u.newer("b", "o"),
        )));

        // The change: each changed key's old row out, if it had one, and its new row in, unless it is deleted.
        let from = |a: &str| cols.iter().map(|c| format!("{a}.{c}")).collect::<Vec<_>>().join(", ");
        Ok(vec![Rel::Sql(format!(
            "(SELECT {}, (-1)::INTEGER AS __w FROM {old} o JOIN {new} n ON {on}
              UNION ALL SELECT {}, 1::INTEGER AS __w FROM {new} n WHERE NOT n.__del)",
            from("o"),
            from("n"),
            old = t.old,
            new = t.new,
            on = u.keys_match("o", "n"),
        ))])
    }

    fn commit_delta(&self, cx: &Cx) -> Result<Vec<Step>> {
        let u = spec(cx)?;
        let t = Tables::of(cx, cx.x.parity);
        let all = source_cols(cx).join(", ");
        let state = state_table(cx.x.job);
        Ok(vec![
            Step::commit(format!("DELETE FROM {state} s WHERE EXISTS (SELECT 1 FROM {} n WHERE {})", t.new, u.keys_match("s", "n")), Cost::State)
                .skip_if_none(Counter::Staged),
            Step::commit(format!("INSERT INTO {state} ({all}) SELECT {all} FROM {} WHERE NOT __del", t.new), Cost::State)
                .skip_if_none(Counter::Staged)
                .crash_after(),
        ])
    }
}
