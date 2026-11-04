use super::{Class, Operator};
use crate::codegen::{staging, Counter, Cx, Step};
use anyhow::Result;

/// A linear piece of the view as opaque SQL from the view compiler, which reads its input under a fixed table
/// name. Transitional: the rewrite rules cannot see inside it, so it goes once SQL lowers to `Map`, `Filter`
/// and `Join`.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlFragment(pub Fragment);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Fragment {
    /// The whole view of a stateless view.
    View,
    /// A global aggregate's FROM/WHERE: group keys, aggregate arguments and weights.
    PreAggregate,
    /// A global aggregate's HAVING and select list over the finalized groups.
    PostAggregate,
}

impl Operator for SqlFragment {
    fn name(&self) -> String {
        format!(
            "Map({})",
            match self.0 {
                Fragment::View => "view",
                Fragment::PreAggregate => "pre-aggregate",
                Fragment::PostAggregate => "post-aggregate",
            }
        )
    }

    fn class(&self) -> Class {
        Class::Linear
    }

    fn stage(&self, cx: &mut Cx, inputs: &[String]) -> Result<Vec<String>> {
        let plan = cx.plan;
        if self.0 == Fragment::PostAggregate {
            let t = cx.x.global_tables()?;
            cx.steps.extend(plan.global.as_ref().unwrap().post_stmts(&t).into_iter().map(Step::stage));
            return Ok(vec![t.new_out.clone()]);
        }
        // The fragment reads the view's input table: materialize this operator's input under that name, with the
        // source's columns only (and the weights, which the pre-aggregate sums). An append sink writes the view's
        // rows as they are: `check` makes sure none of them is a retraction.
        let keep = if self.0 == Fragment::View { cx.cols.clone() } else { format!("{}, __w", cx.cols) };
        cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {} AS SELECT {keep} FROM {}", plan.input_table, inputs[0])));
        if self.0 == Fragment::View {
            let out = staging("out", &cx.set);
            cx.steps.push(Step::stage(format!("CREATE OR REPLACE TABLE {out} AS {}", plan.delta_sql)).counting(Counter::Staged, &out));
            Ok(vec![out])
        } else {
            let t = cx.x.global_tables()?;
            cx.steps.push(Step::stage(plan.global.as_ref().unwrap().pre_stmt(&t)));
            Ok(vec![t.pre.clone()])
        }
    }
}
