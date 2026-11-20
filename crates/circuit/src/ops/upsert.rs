use super::{not_incremental, Class, Operator, Stateful};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::{UpsertSpec, TOMBSTONE_RETENTION, UPSERT_STATE};
use crate::sql::{ident, state_table};
use anyhow::{Context, Result};

/// A Kafka source read as upserts (`Plan::upsert`): each key's current row, as a collection. Its incremental
/// form keeps every key's current row in the state table (with `__del` for a deleted key kept as a tombstone,
/// its message's position `__p`, `__o`, and `__at` when it was deleted) and outputs each epoch's change: a
/// replaced key's old row −1 and its new row +1, a deleted key's row −1.
///
/// Per epoch: of the epoch's messages, each key's best row and last tombstone (`UpsertSpec::best_sql`); a
/// key's best row replaces its current one when newer (`UpsertSpec::newer`), and is deleted when a tombstone
/// follows it in the epoch; a current row that is kept is deleted by any tombstone of the epoch, which follows it.
/// This is the batch query `UpsertSpec::batch_sql` taken one epoch at a time.
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

/// The epoch's tables: each key's best message and last tombstone, the touched keys' stored rows, and the
/// changed keys' new rows.
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

/// The state's columns: the source's, then `UPSERT_STATE`.
fn state_cols(cx: &Cx) -> (Vec<String>, String) {
    let source: Vec<String> = cx.x.primary().columns.iter().map(|(c, _)| ident(c)).collect();
    let all: Vec<String> = source.iter().cloned().chain(UPSERT_STATE.iter().map(|(c, _)| c.to_string())).collect();
    (source, all.join(", "))
}

impl Stateful for Upsert {
    fn stage_delta(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let u = spec(cx)?.clone();
        let t = Tables::of(cx, cx.x.parity);
        let columns: Vec<(String, String)> = cx.x.primary().columns.to_vec();
        let (source, all) = state_cols(cx);
        let messages = format!("({})", u.messages_sql(&columns, &inputs[0].sql()));
        cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {} AS {}", t.best, u.best_sql(&columns, &messages))).counting(Counter::Staged, &t.best));

        // The previous epoch may still be committing: read its new rows for the keys it touched.
        let mut state = state_table(cx.x.job);
        if let Some(prev) = cx.x.overlay {
            let prev_new = Tables::of(cx, prev).new;
            let kept = if u.version.is_some() { "TRUE" } else { "NOT __del" };
            state = format!(
                "(SELECT {all} FROM {prev_new} WHERE {kept}
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

        // The changed keys' new rows: the epoch's best row when it replaces the current one (deleted if a
        // tombstone follows it), else the current row deleted when the epoch has a tombstone for it.
        let take = format!("(b.__p IS NOT NULL AND (o.__p IS NULL OR {}))", u.newer("b", "o"));
        let pick: Vec<String> = source.iter().map(|c| format!("CASE WHEN {take} THEN b.{c} ELSE o.{c} END AS {c}")).collect();
        cx.steps.push(Step::stage(format!(
            "CREATE OR REPLACE TABLE {new} AS
               SELECT {pick},
                      CASE WHEN {take} THEN {after} ELSE true END AS __del,
                      CASE WHEN {take} THEN b.__p ELSE o.__p END AS __p,
                      CASE WHEN {take} THEN b.__o ELSE o.__o END AS __o,
                      CASE WHEN (CASE WHEN {take} THEN {after} ELSE true END) THEN now()::TIMESTAMP END AS __at
               FROM {best} b LEFT JOIN {old} o ON {on}
               WHERE {take} OR (o.__p IS NOT NULL AND NOT o.__del AND b.__tp IS NOT NULL)",
            new = t.new,
            pick = pick.join(", "),
            after = UpsertSpec::deleted_after("b"),
            best = t.best,
            old = t.old,
            on = u.keys_match("b", "o"),
        )));

        // The change: each changed key's old row out, if it had one, and its new row in, if it is not deleted.
        let from = |a: &str| source.iter().map(|c| format!("{a}.{c}")).collect::<Vec<_>>().join(", ");
        Ok(vec![Rel::Sql(format!(
            "(SELECT {}, (-1)::INTEGER AS __w FROM {old} o JOIN {new} n ON {on} WHERE NOT o.__del
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
        let (_, all) = state_cols(cx);
        let state = state_table(cx.x.job);
        // Without a version a deleted key keeps nothing; with one it keeps a tombstone for the retention.
        let kept = if u.version.is_some() { "TRUE" } else { "NOT __del" };
        let mut steps = vec![
            Step::commit(format!("DELETE FROM {state} s WHERE EXISTS (SELECT 1 FROM {} n WHERE {})", t.new, u.keys_match("s", "n")), Cost::State)
                .skip_if_none(Counter::Staged),
            Step::commit(format!("INSERT INTO {state} ({all}) SELECT {all} FROM {} WHERE {kept}", t.new), Cost::State)
                .skip_if_none(Counter::Staged)
                .crash_after(),
        ];
        if u.version.is_some() {
            let retention = u.tombstone_retention.as_deref().unwrap_or(TOMBSTONE_RETENTION).replace('\'', "''");
            // Tombstones past the retention go every 60th epoch, not every epoch: it reads the whole state.
            steps.push(Step::commit(
                format!(
                    "DELETE FROM {state} WHERE ({epoch}) % 60 = 0 AND __del AND __at < now()::TIMESTAMP - INTERVAL '{retention}'",
                    epoch = cx.x.epoch
                ),
                Cost::State,
            ));
        }
        Ok(steps)
    }
}
