use super::{not_incremental, Class, Operator, Stateful};
use crate::codegen::{staging, Cost, Counter, Cx, Rel, Step};
use crate::sql::{ident, state_table};
use anyhow::Result;

/// `SELECT DISTINCT`: a row is in the output while its total weight is positive.
///
/// Its incremental form keeps each row's total weight in the state table, and a row enters the output when an
/// epoch takes its weight above 0 and leaves it when one brings it back to 0 (DBSP's `distinct`).
#[derive(Debug, Clone, PartialEq)]
pub struct Distinct;

impl Operator for Distinct {
    fn name(&self) -> String {
        "Distinct".into()
    }

    fn class(&self) -> Class {
        Class::NonLinear
    }

    // On append-only input a row's weight only grows: it enters once and never leaves, so the output is
    // append-only too; with retractions it is not. The default (the input's) says exactly that.

    fn stage(&self, _cx: &mut Cx, _inputs: &[Rel]) -> Result<Vec<Rel>> {
        Err(not_incremental(&self.name()))
    }

    fn stateful(&self) -> Option<&dyn Stateful> {
        Some(self)
    }
}

/// The epoch's tables: the rows' weight changes, the touched rows' stored weights, and their new weights.
struct Tables {
    delta: String,
    old: String,
    new: String,
}

impl Tables {
    fn of(cx: &Cx, parity: u64) -> Tables {
        let set = format!("{}_{parity}", cx.x.job);
        Tables { delta: staging("ddelta", &set), old: staging("dold", cx.x.job), new: staging("dnew", &set) }
    }
}

/// The view's columns, quoted, and a null-safe match of all of them between two aliases.
fn columns(cx: &Cx) -> (Vec<String>, impl Fn(&str, &str) -> String) {
    let cols: Vec<String> = cx.plan.sink_columns.iter().map(|(c, _)| ident(c)).collect();
    let on = cols.clone();
    let matches = move |a: &str, b: &str| on.iter().map(|c| format!("{a}.{c} IS NOT DISTINCT FROM {b}.{c}")).collect::<Vec<_>>().join(" AND ");
    (cols, matches)
}

impl Stateful for Distinct {
    fn stage_delta(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        let t = Tables::of(cx, cx.x.parity);
        let (cols, m) = columns(cx);
        let list = cols.join(", ");
        let prefixed = |a: &str| cols.iter().map(|c| format!("{a}.{c}")).collect::<Vec<_>>().join(", ");
        // Each row's weight change; rows whose weights cancel out within the epoch are not a change.
        let delta = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT {list}, sum(__w)::BIGINT AS __w FROM {} GROUP BY ALL HAVING sum(__w) <> 0",
            t.delta,
            inputs[0].sql()
        );
        cx.steps.push(Step::stage(delta).counting(Counter::Staged, &t.delta));
        // The previous epoch may still be committing: read its new weights for the rows it touched.
        let mut state = state_table(cx.x.job);
        if let Some(prev) = cx.x.overlay {
            let prev_new = Tables::of(cx, prev).new;
            state = format!(
                "(SELECT {list}, __cnt FROM {prev_new} WHERE __cnt <> 0
                  UNION ALL SELECT {list}, __cnt FROM {state} s WHERE NOT EXISTS (SELECT 1 FROM {prev_new} p WHERE {}))",
                m("s", "p")
            );
        }
        cx.steps.push(Step::stage(format!(
            "CREATE OR REPLACE TABLE {} AS SELECT s.* FROM {state} s WHERE EXISTS (SELECT 1 FROM {} d WHERE {})",
            t.old,
            t.delta,
            m("s", "d")
        )));
        cx.steps.push(Step::stage(format!(
            "CREATE OR REPLACE TABLE {} AS SELECT {}, CAST(coalesce(o.__cnt, 0) + d.__w AS BIGINT) AS __cnt FROM {} d LEFT JOIN {} o ON {}",
            t.new,
            prefixed("d"),
            t.delta,
            t.old,
            m("d", "o")
        )));
        // The rows whose weight became positive this epoch enter (+1), those whose weight fell to 0 leave (−1).
        // On append-only input none leaves. `new` has a row for every touched row, so every left one is found.
        Ok(vec![Rel::Sql(format!(
            "(SELECT {}, 1::INTEGER AS __w FROM {new} n LEFT JOIN {old} o ON {nm} WHERE n.__cnt > 0 AND coalesce(o.__cnt, 0) <= 0
              UNION ALL SELECT {}, (-1)::INTEGER AS __w FROM {old} o JOIN {new} n ON {nm} WHERE o.__cnt > 0 AND n.__cnt <= 0)",
            prefixed("n"),
            prefixed("o"),
            new = t.new,
            old = t.old,
            nm = m("n", "o")
        ))])
    }

    fn commit_delta(&self, cx: &Cx) -> Result<Vec<Step>> {
        let t = Tables::of(cx, cx.x.parity);
        let (cols, m) = columns(cx);
        let (state, list) = (state_table(cx.x.job), cols.join(", "));
        Ok(vec![
            Step::commit(format!("DELETE FROM {state} s WHERE EXISTS (SELECT 1 FROM {} d WHERE {})", t.delta, m("s", "d")), Cost::State)
                .skip_if_none(Counter::Staged),
            Step::commit(format!("INSERT INTO {state} ({list}, __cnt) SELECT {list}, __cnt FROM {} WHERE __cnt <> 0", t.new), Cost::State)
                .skip_if_none(Counter::Staged)
                .crash_after(),
        ])
    }
}
