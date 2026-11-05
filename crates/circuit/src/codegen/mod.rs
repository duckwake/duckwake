//! The SQL of one epoch: staging statements, then the writes of the epoch transaction.
use crate::ir::{Circuit, Operator, View};
use crate::plan::{GlobalTables, Plan};
use crate::sql::{ident, quote_table, state_table};
use anyhow::Result;

impl Circuit {
    /// The SQL of one epoch's staging, in order: in-memory tables only, nothing touches the lake.
    pub fn stage_steps(&self, x: &EpochCtx) -> Result<Vec<Step>> {
        let mut cx = Cx::new(x, self.emits_final());
        // The relation each node's output port reads as (a subquery when fused, else a table).
        let mut rel: Vec<Vec<Rel>> = vec![Vec::new(); self.nodes.len()];
        for (n, node) in self.nodes.iter().enumerate() {
            let inputs: Vec<Rel> = node.inputs.iter().map(|p| rel[p.node][p.port].clone()).collect();
            rel[n] = node.op.stage(&mut cx, &inputs)?;
        }
        Ok(cx.steps)
    }

    /// The SQL of one epoch's writes, in the epoch transaction, in order.
    pub fn commit_steps(&self, x: &EpochCtx) -> Result<Vec<Step>> {
        let cx = Cx::new(x, self.emits_final());
        let mut steps = Vec::new();
        for node in &self.nodes {
            steps.extend(node.op.commit(&cx)?);
        }
        Ok(steps)
    }
}

/// What an operator's SQL is generated from, and the staging statements generated so far.
pub struct Cx<'a> {
    pub x: &'a EpochCtx<'a>,
    pub plan: &'a Plan,
    /// The source's columns, quoted and comma-separated.
    pub cols: String,
    /// The key of this epoch's table set: `<job>_<parity>`.
    pub set: String,
    /// Whether a windowed aggregate writes the view's table itself.
    pub emits_final: bool,
    pub steps: Vec<Step>,
}

impl<'a> Cx<'a> {
    fn new(x: &'a EpochCtx<'a>, emits_final: bool) -> Cx<'a> {
        Cx {
            x,
            plan: x.plan,
            cols: x.source_columns.iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", "),
            set: format!("{}_{}", x.job, x.parity),
            emits_final,
            steps: Vec::new(),
        }
    }

    /// The view table's columns, quoted, with the epoch column.
    pub fn sink_cols(&self) -> String {
        let cols: Vec<String> = self.plan.sink_columns.iter().map(|(c, _)| ident(c)).chain(["_duckwake_epoch".into()]).collect();
        cols.join(", ")
    }
}

/// What one epoch's SQL needs to know.
pub struct EpochCtx<'a> {
    pub job: &'a str,
    pub plan: &'a Plan,
    /// The view's table and its late-data table, quoted.
    pub sink: String,
    pub late_table: Option<String>,
    pub source_columns: &'a [(String, String)],
    /// The epoch's decoded rows of the view's source.
    pub typed: String,
    /// Which of the two table sets this epoch stages into, and the previous epoch's set when a global aggregate
    /// reads that epoch's new state before it has committed.
    pub parity: u64,
    pub overlay: Option<u64>,
    /// SQL expressions: the watermark before and after this epoch (NULL / None before the first), the epoch.
    pub prev_watermark: String,
    pub watermark: Option<String>,
    pub epoch: String,
}

impl EpochCtx<'_> {
    pub(crate) fn global_tables(&self) -> Result<GlobalTables> {
        Ok(GlobalTables::new(self.job, state_table(self.job), self.sink.clone(), self.plan, self.parity))
    }

    /// A context with placeholders instead of values, for showing the SQL.
    pub fn example<'a>(view: View<'a>, source_columns: &'a [(String, String)]) -> Result<EpochCtx<'a>> {
        Ok(EpochCtx {
            job: view.id,
            plan: view.plan,
            sink: quote_table(view.sink)?,
            late_table: view.late_data.map(quote_table).transpose()?,
            source_columns,
            typed: staging("typed", "g0"),
            parity: 1,
            overlay: None,
            prev_watermark: "<previous watermark>".into(),
            watermark: Some("<watermark>".into()),
            epoch: "<epoch>".into(),
        })
    }
}

/// What an operator's output port is read as.
#[derive(Debug, Clone)]
pub enum Rel {
    /// A table, or a parenthesized subquery.
    Sql(String),
    /// A SELECT still open: linear operators add to it instead of wrapping it, so a chain of them is one
    /// statement. `from` keeps the view's aliases, which its WHERE and select list refer to.
    Block { from: String, filter: Vec<String>, select: Option<String> },
}

impl Rel {
    /// As an item of a FROM clause.
    pub fn sql(&self) -> String {
        match self {
            Rel::Sql(s) => s.clone(),
            Rel::Block { .. } => format!("({})", self.query()),
        }
    }

    /// As a query.
    pub fn query(&self) -> String {
        match self {
            Rel::Sql(s) => match s.strip_prefix('(').and_then(|r| r.strip_suffix(')')) {
                Some(q) => q.to_string(),
                None => format!("SELECT * FROM {s}"),
            },
            Rel::Block { from, filter, select } => {
                let mut q = format!("SELECT {} FROM {from}", select.as_deref().unwrap_or("*"));
                if !filter.is_empty() {
                    q.push_str(" WHERE ");
                    q.push_str(&filter.join(" AND "));
                }
                q
            }
        }
    }

    /// The open block to add a filter or a select list to: this one while its select list is not set, else a
    /// new one over this relation.
    pub fn open(self) -> (String, Vec<String>) {
        match self {
            Rel::Block { from, filter, select: None } => (from, filter),
            r => (format!("{} AS __in", r.sql()), Vec::new()),
        }
    }
}

/// An in-memory staging table.
pub fn staging(name: &str, key: &str) -> String {
    format!("memory.main.{}", ident(&format!("{name}_{key}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counter {
    /// Late rows routed to the late-data table.
    Late,
    /// Rows a stateful operator merges into its state: windowed partials, touched groups or rows.
    Staged,
    /// Rows the view's table changes by: an append sink's rows, an upsert sink's changed groups.
    Changed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cost {
    Stage,
    State,
    Sink,
    Late,
}

/// One SQL statement of an epoch, with what the engine does around it.
#[derive(Debug, Clone)]
pub struct Step {
    pub sql: String,
    pub cost: Cost,
    /// Count this table's rows after the step.
    pub count: Option<(Counter, String)>,
    /// With a lean commit, skip the step when this count is 0.
    pub skip_if_none: Option<Counter>,
    /// With a lean commit, skip the step unless a window closes this epoch.
    pub needs_close: bool,
    /// The statement's row count is the view's output rows.
    pub output_rows: bool,
    /// The crash tests' `commit_mid` point follows this step.
    pub crash_after: bool,
}

impl Step {
    pub(crate) fn stage(sql: String) -> Step {
        Step { sql, cost: Cost::Stage, count: None, skip_if_none: None, needs_close: false, output_rows: false, crash_after: false }
    }
    pub(crate) fn commit(sql: String, cost: Cost) -> Step {
        Step { cost, ..Step::stage(sql) }
    }
    pub(crate) fn counting(mut self, c: Counter, table: &str) -> Step {
        self.count = Some((c, table.to_string()));
        self
    }
    pub(crate) fn skip_if_none(mut self, c: Counter) -> Step {
        self.skip_if_none = Some(c);
        self
    }
    pub(crate) fn needs_close(mut self) -> Step {
        self.needs_close = true;
        self
    }
    pub(crate) fn output_rows(mut self) -> Step {
        self.output_rows = true;
        self
    }
    pub(crate) fn crash_after(mut self) -> Step {
        self.crash_after = true;
        self
    }
}
