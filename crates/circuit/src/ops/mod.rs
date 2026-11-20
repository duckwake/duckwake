//! The operators, one file each. Every operator implements `Operator`; `Op` is the enum the graph holds.
//!
//! Adding an operator: its file here with the struct and its `Operator` impl (plus `Stateful` when it is not
//! linear), a variant in `Op` and a line in `dispatch!`, its lowering in `frontend`, and a test. The rewrite
//! rules look only at `class()` and the engine runs only steps, so neither changes.
mod aggregate;
mod differentiate;
mod distinct;
mod filter;
mod from;
mod integrate;
mod late_split;
mod map;
mod sink;
mod source;
mod union;
mod upsert;
mod window;

pub use aggregate::{Aggregate, Grouping};
pub use differentiate::Differentiate;
pub use distinct::Distinct;
pub use filter::Filter;
pub use from::From;
pub use integrate::Integrate;
pub use late_split::LateSplit;
pub use map::Map;
pub use sink::{Sink, SinkKind, COUNT_COLUMN};
pub use source::Source;
pub use union::Union;
pub use upsert::Upsert;
pub use window::Window;

use crate::codegen::{Cx, Rel, Step};
use crate::ir::Props;
use crate::plan::Plan;
use anyhow::{bail, Result};

/// How an operator behaves when the circuit is made incremental.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Source,
    Sink,
    /// Commutes with I: runs on changes as it is.
    Linear,
    /// Becomes its incremental form: S ∘ I = I ∘ SΔ.
    NonLinear,
    /// An incremental form, SΔ: runs on changes, keeps state.
    Incremental,
    /// I and D: only while the circuit is rewritten.
    Rewrite,
}

pub trait Operator {
    fn name(&self) -> String;
    fn class(&self) -> Class;
    /// Number of output ports.
    fn outputs(&self) -> usize {
        1
    }
    /// The output's properties, from the inputs'. By default an operator keeps retractions as they are.
    fn props(&self, inputs: &[Props]) -> Props {
        Props { append_only: inputs.iter().all(|p| p.append_only) }
    }
    /// Refuses inputs the operator's implementation cannot handle.
    fn check(&self, _inputs: &[Props], _plan: &Plan) -> Result<()> {
        Ok(())
    }
    /// Staging: pushes its statements onto `cx` and returns the relation each output port reads as (a subquery
    /// when it fuses into its reader, else a table).
    fn stage(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>>;
    /// Its writes in the epoch transaction.
    fn commit(&self, _cx: &Cx) -> Result<Vec<Step>> {
        Ok(vec![])
    }
    /// Its incremental form, for a non-linear operator.
    fn stateful(&self) -> Option<&dyn Stateful> {
        None
    }
}

/// The incremental form SΔ = D ∘ S ∘ I of a non-linear operator, with its state in a DuckLake table.
pub trait Stateful {
    fn stage_delta(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>>;
    fn commit_delta(&self, cx: &Cx) -> Result<Vec<Step>>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Source(Source),
    Window(Window),
    LateSplit(LateSplit),
    From(From),
    Filter(Filter),
    Map(Map),
    Aggregate(Aggregate),
    Distinct(Distinct),
    Upsert(Upsert),
    Union(Union),
    Sink(Sink),
    Integrate(Integrate),
    Differentiate(Differentiate),
    /// The incremental form of a non-linear operator.
    Delta(Box<Op>),
}

/// Runs `$e` with `$o` bound to the operator inside any variant but `Delta`.
macro_rules! dispatch {
    ($op:expr, $o:ident => $e:expr) => {
        match $op {
            Op::Source($o) => $e,
            Op::Window($o) => $e,
            Op::LateSplit($o) => $e,
            Op::From($o) => $e,
            Op::Filter($o) => $e,
            Op::Map($o) => $e,
            Op::Aggregate($o) => $e,
            Op::Distinct($o) => $e,
            Op::Upsert($o) => $e,
            Op::Union($o) => $e,
            Op::Sink($o) => $e,
            Op::Integrate($o) => $e,
            Op::Differentiate($o) => $e,
            Op::Delta(_) => unreachable!("Delta is handled by the caller"),
        }
    };
}

impl Op {
    /// The incremental form of a non-linear operator.
    fn delta(&self) -> Result<&dyn Stateful> {
        match self {
            Op::Delta(inner) => inner.stateful().ok_or_else(|| anyhow::anyhow!("{} has no incremental form", inner.name())),
            _ => bail!("{} is not an incremental operator", self.name()),
        }
    }
}

impl Operator for Op {
    fn name(&self) -> String {
        match self {
            // Aggregate(global) -> AggregateΔ(global) [state table]
            Op::Delta(inner) => {
                let name = inner.name();
                let (head, tail) = name.split_at(name.find('(').unwrap_or(name.len()));
                format!("{head}Δ{tail} [state table]")
            }
            _ => dispatch!(self, o => o.name()),
        }
    }

    fn class(&self) -> Class {
        match self {
            Op::Delta(_) => Class::Incremental,
            _ => dispatch!(self, o => o.class()),
        }
    }

    fn outputs(&self) -> usize {
        match self {
            Op::Delta(inner) => inner.outputs(),
            _ => dispatch!(self, o => o.outputs()),
        }
    }

    fn props(&self, inputs: &[Props]) -> Props {
        match self {
            Op::Delta(inner) => inner.props(inputs),
            _ => dispatch!(self, o => o.props(inputs)),
        }
    }

    fn check(&self, inputs: &[Props], plan: &Plan) -> Result<()> {
        match self {
            Op::Delta(inner) => inner.check(inputs, plan),
            _ => dispatch!(self, o => o.check(inputs, plan)),
        }
    }

    fn stage(&self, cx: &mut Cx, inputs: &[Rel]) -> Result<Vec<Rel>> {
        match self {
            Op::Delta(_) => self.delta()?.stage_delta(cx, inputs),
            _ => dispatch!(self, o => o.stage(cx, inputs)),
        }
    }

    fn commit(&self, cx: &Cx) -> Result<Vec<Step>> {
        match self {
            Op::Delta(_) => self.delta()?.commit_delta(cx),
            _ => dispatch!(self, o => o.commit(cx)),
        }
    }

    fn stateful(&self) -> Option<&dyn Stateful> {
        match self {
            Op::Delta(inner) => inner.stateful(),
            _ => dispatch!(self, o => o.stateful()),
        }
    }
}

/// The error of an operator that cannot run in the epoch circuit as it is.
fn not_incremental(name: &str) -> anyhow::Error {
    anyhow::anyhow!("{name} left in the epoch circuit: incrementalize it first")
}
