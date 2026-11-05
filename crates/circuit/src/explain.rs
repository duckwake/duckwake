use crate::codegen::EpochCtx;
use crate::ir::{Circuit, View};
use anyhow::Result;
use std::fmt::Write;

/// The circuit, how it was derived, and one epoch's SQL, as text (`duckwake explain`, `duckwake_explain`).
/// `sources` holds the columns of each source the view reads.
pub fn explain(view: View, sources: &[(String, Vec<(String, String)>)]) -> Result<String> {
    let q = Circuit::query(&view);
    q.check(view.plan)?;
    let c = q.clone().incrementalize();
    let x = EpochCtx::example(view, sources)?;
    let mut out = String::new();
    let _ = writeln!(out, "view {} ({}), table {}\n", view.id, view.plan.kind_name(), view.sink);
    let _ = writeln!(out, "Q: the view over whole collections\n{q}");
    let _ = writeln!(out, "Incrementalization: D ∘ Q ∘ I, rewritten");
    for (k, t) in c.trace.iter().enumerate() {
        let _ = writeln!(out, "  {:>2}. {t}", k + 1);
    }
    let _ = writeln!(out, "\nThe circuit that runs every epoch (inputs and outputs are changes)\n{c}");
    let _ = writeln!(out, "One epoch's SQL (staging in memory, then the writes in the epoch transaction)");
    for (k, s) in c.stage_steps(&x)?.iter().enumerate() {
        let _ = writeln!(out, "  stage {}: {}", k + 1, squash(&s.sql));
    }
    for (k, s) in c.commit_steps(&x)?.iter().enumerate() {
        let _ = writeln!(out, "  commit {}: {}", k + 1, squash(&s.sql));
    }
    Ok(out)
}

fn squash(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}
