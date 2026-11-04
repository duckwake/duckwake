//! View compiler: turns a continuous view's SQL into an incremental plan.
//!
//! DuckDB parses the view (`json_serialize_sql`); we check the AST against the v1 operator matrix, swap the
//! stream reference for the epoch's staging table, and for windowed aggregates rewrite the select list into
//! mergeable partials. `json_deserialize_sql` turns the rewritten AST back into SQL, so every expression the
//! user wrote is executed by DuckDB exactly as written. Anything outside the matrix is rejected here, at
//! create time, never recomputed silently.
use crate::lake::{describe, ident, lit};
use crate::model::SourceDef;
use anyhow::{anyhow, bail, Context, Result};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowKind {
    Tumble,
    Hop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub time_col: String,
    pub size_us: i64,
    pub slide_us: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeOp {
    /// count: partials are never NULL.
    Add,
    /// sum: NULL means "no non-NULL input yet".
    AddNullable,
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub source: String,
    pub window: Option<WindowSpec>,
    /// Staging table the rewritten SQL reads (`memory.main.in_<job>`).
    pub input_table: String,
    /// Stateless: the sink rows for one epoch. Windowed: partial aggregates for one epoch.
    pub delta_sql: String,
    /// The view as a batch query over `input_table`; used for the sink schema and differential checks.
    pub batch_sql: String,
    pub sink_columns: Vec<(String, String)>,
    /// Windowed only: `__ws`, `__we`, keys `__k<j>`, partials `__p<i>`.
    pub state_columns: Vec<(String, String)>,
    pub key_columns: Vec<String>,
    pub merge_ops: Vec<(String, MergeOp)>,
    /// Windowed only: projection from state rows to sink columns.
    pub final_select: String,
    /// Global (non-windowed) aggregate: the operator chain pre-aggregate -> aggregate -> post.
    #[serde(default)]
    pub global: Option<GlobalPlan>,
}

/// How a view's sink is maintained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    /// Rows are only ever added (stateless views, closed windows).
    #[default]
    Append,
    /// One row per group, replaced when the group's result changes (global aggregates).
    Upsert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggKind {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggSpec {
    pub kind: AggKind,
    /// Index of the argument column `__x<arg>` in the pre-aggregate output; None for count(*).
    pub arg: Option<usize>,
}

/// A global aggregate compiled into three operators. Every row between operators carries a weight
/// `__w` (+1 insert, -1 retraction), so the same plan stays correct once inputs can retract.
///
/// 1. pre-aggregate (stateless): the view's FROM/WHERE producing group keys `__g<j>`, aggregate
///    arguments `__x<k>` and `__w`;
/// 2. aggregate (stateful): weighted partials per group, merged into a DuckLake state table;
///    `__cnt` is the group's total weight and the group is dropped when it reaches 0;
/// 3. post (stateless): HAVING and the select list over finalized values `__g<j>`, `__a<i>`.
///
/// count, sum and avg are invertible (sums of weights). min and max are only mergeable, which is
/// correct while sources are append-only; retracting inputs will need value counts (milestone 3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPlan {
    pub pre_sql: String,
    pub keys: usize,
    pub aggs: Vec<AggSpec>,
    /// Table the post SQL reads (`memory.main.agg_<job>`): columns `__g<j>`, `__a<i>`.
    pub agg_table: String,
    /// HAVING + select list, cast to the sink's types.
    pub post_sql: String,
    /// Sink column holding group key j.
    pub sink_keys: Vec<String>,
}

const SUPPORTED_AGGS: [&str; 6] = ["count", "count_star", "sum", "min", "max", "avg"];

impl Plan {
    pub fn is_windowed(&self) -> bool {
        self.window.is_some()
    }

    pub fn is_global(&self) -> bool {
        self.global.is_some()
    }

    /// Whether the plan keeps a state table in `__duckwake`.
    pub fn has_state(&self) -> bool {
        self.is_windowed() || self.is_global()
    }

    pub fn sink_mode(&self) -> SinkMode {
        if self.is_global() { SinkMode::Upsert } else { SinkMode::Append }
    }

    pub fn kind_name(&self) -> &'static str {
        if self.is_windowed() {
            "windowed"
        } else if self.is_global() {
            "aggregate"
        } else {
            "stateless"
        }
    }

    /// Adds `window_start` / `window_end` to every row of `input` (a query or table).
    pub fn window_sql(&self, input: &str) -> String {
        let w = self.window.as_ref().expect("windowed plan");
        let tc = ident(&w.time_col);
        let size = format!("to_microseconds({})", w.size_us);
        match w.kind {
            WindowKind::Tumble => format!(
                "SELECT *, time_bucket({size}, {tc}) AS window_start, time_bucket({size}, {tc}) + {size} AS window_end FROM {input}"
            ),
            WindowKind::Hop => {
                let slide = format!("to_microseconds({})", w.slide_us);
                format!(
                    "SELECT *, window_start + {size} AS window_end FROM (
                       SELECT *, unnest(generate_series(time_bucket({slide}, {tc}) - {size} + {slide},
                                                        time_bucket({slide}, {tc}), {slide})) AS window_start
                       FROM {input})"
                )
            }
        }
    }

    /// `MERGE` of one epoch's partials into the state table.
    pub fn merge_sql(&self, state: &str, partial: &str) -> String {
        let mut on = vec!["s.__ws = p.__ws".to_string()];
        on.extend(self.key_columns.iter().map(|k| format!("s.{0} IS NOT DISTINCT FROM p.{0}", ident(k))));
        let set: Vec<String> = self
            .merge_ops
            .iter()
            .map(|(c, op)| {
                let c = ident(c);
                let e = match op {
                    MergeOp::Add => format!("s.{c} + p.{c}"),
                    MergeOp::AddNullable => format!("coalesce(s.{c} + p.{c}, s.{c}, p.{c})"),
                    MergeOp::Min => format!("least(s.{c}, p.{c})"),
                    MergeOp::Max => format!("greatest(s.{c}, p.{c})"),
                };
                format!("{c} = {e}")
            })
            .collect();
        let cols: Vec<String> = self.state_columns.iter().map(|(c, _)| ident(c)).collect();
        let vals: Vec<String> = self.state_columns.iter().map(|(c, _)| format!("p.{}", ident(c))).collect();
        format!(
            "MERGE INTO {state} s USING {partial} p ON {}
             WHEN MATCHED THEN UPDATE SET {}
             WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            on.join(" AND "),
            set.join(", "),
            cols.join(", "),
            vals.join(", ")
        )
    }
}

impl GlobalPlan {
    fn key_cols(&self) -> Vec<String> {
        (0..self.keys).map(|j| format!("__g{j}")).collect()
    }

    /// Null-safe equality of the group keys of `a` and `b`; TRUE for a view without GROUP BY.
    pub fn keys_match(&self, a: &str, b: &str) -> String {
        let eq: Vec<String> = self.key_cols().iter().map(|k| format!("{a}.{k} IS NOT DISTINCT FROM {b}.{k}")).collect();
        if eq.is_empty() { "TRUE".into() } else { eq.join(" AND ") }
    }

    /// The same over the sink's key columns.
    pub fn sink_keys_match(&self, a: &str, b: &str) -> String {
        let eq: Vec<String> =
            self.sink_keys.iter().map(|k| format!("{a}.{k} IS NOT DISTINCT FROM {b}.{k}", k = ident(k))).collect();
        if eq.is_empty() { "TRUE".into() } else { eq.join(" AND ") }
    }

    /// Weighted partials per group for one epoch's pre-aggregate rows.
    pub fn delta_sql(&self, pre: &str) -> String {
        let signed = |k: usize| format!("CASE WHEN __w = 1 THEN __x{k} WHEN __w = -1 THEN -__x{k} ELSE __x{k} * __w END");
        let nonnull = |k: usize| format!("CAST(sum(CASE WHEN __x{k} IS NOT NULL THEN __w ELSE 0 END) AS BIGINT)");
        let mut cols = self.key_cols();
        cols.push("CAST(sum(__w) AS BIGINT) AS __cnt".into());
        for (i, a) in self.aggs.iter().enumerate() {
            let k = a.arg.unwrap_or(0);
            match a.kind {
                AggKind::CountStar => cols.push(format!("CAST(sum(__w) AS BIGINT) AS __a{i}")),
                AggKind::Count => cols.push(format!("{} AS __a{i}", nonnull(k))),
                AggKind::Sum | AggKind::Avg => {
                    cols.push(format!("sum({}) AS __a{i}_s", signed(k)));
                    cols.push(format!("{} AS __a{i}_n", nonnull(k)));
                }
                AggKind::Min => cols.push(format!("min(__x{k}) AS __a{i}")),
                AggKind::Max => cols.push(format!("max(__x{k}) AS __a{i}")),
            }
        }
        let group = if self.keys == 0 { "HAVING count(*) > 0".to_string() } else { "GROUP BY ALL".to_string() };
        format!("SELECT {} FROM {pre} {group}", cols.join(", "))
    }

    /// The touched groups' stored state.
    pub fn old_sql(&self, state: &str, delta: &str) -> String {
        format!("SELECT s.* FROM {state} s WHERE EXISTS (SELECT 1 FROM {delta} d WHERE {})", self.keys_match("s", "d"))
    }

    /// New state of every touched group: stored state plus this epoch's partials.
    pub fn merge_sql(&self, delta: &str, old: &str, state_columns: &[(String, String)]) -> String {
        let ty = |c: &str| state_columns.iter().find(|(n, _)| n == c).map(|(_, t)| t.clone()).unwrap();
        let add = |c: &str| format!("CAST(coalesce(o.{c}, 0) + coalesce(d.{c}, 0) AS {}) AS {c}", ty(c));
        let mut cols: Vec<String> = self.key_cols().iter().map(|k| format!("d.{k}")).collect();
        cols.push(add("__cnt"));
        for (i, a) in self.aggs.iter().enumerate() {
            match a.kind {
                AggKind::CountStar | AggKind::Count => cols.push(add(&format!("__a{i}"))),
                AggKind::Sum | AggKind::Avg => {
                    cols.push(add(&format!("__a{i}_s")));
                    cols.push(add(&format!("__a{i}_n")));
                }
                AggKind::Min => cols.push(format!("least(o.__a{i}, d.__a{i}) AS __a{i}")),
                AggKind::Max => cols.push(format!("greatest(o.__a{i}, d.__a{i}) AS __a{i}")),
            }
        }
        format!("SELECT {} FROM {delta} d LEFT JOIN {old} o ON {}", cols.join(", "), self.keys_match("d", "o"))
    }

    /// One epoch's staging statements (in-memory tables only). `t` names the tables; `state` is read.
    pub fn stage_sql(&self, t: &GlobalTables, state_columns: &[(String, String)]) -> Vec<String> {
        let sink_cols: Vec<String> = t.sink_columns.iter().map(|c| ident(c)).collect();
        let row = |a: &str| format!("({})", sink_cols.iter().map(|c| format!("{a}.{c}")).collect::<Vec<_>>().join(", "));
        let keys: Vec<String> = self.sink_keys.iter().map(|k| format!("coalesce(o.{k}, n.{k}) AS {k}", k = ident(k))).collect();
        let keys = if keys.is_empty() { "1 AS __one".to_string() } else { keys.join(", ") };
        let changed_match = if self.sink_keys.is_empty() { "TRUE".to_string() } else { self.sink_keys_match("n", "c") };
        vec![
            format!("CREATE OR REPLACE TABLE {} AS {}", t.pre, self.pre_sql),
            format!("CREATE OR REPLACE TABLE {} AS {}", t.delta, self.delta_sql(&t.pre)),
            format!("CREATE OR REPLACE TABLE {} AS {}", t.old, self.old_sql(&t.state, &t.delta)),
            format!("CREATE OR REPLACE TABLE {} AS {}", t.new_state, self.merge_sql(&t.delta, &t.old, state_columns)),
            format!("CREATE OR REPLACE TABLE {} AS {}", self.agg_table, self.finalize_sql(&t.old)),
            format!("CREATE OR REPLACE TABLE {} AS SELECT *, true AS __p FROM ({})", t.old_out, self.post_sql),
            format!("CREATE OR REPLACE TABLE {} AS {}", self.agg_table, self.finalize_sql(&t.new_state)),
            format!("CREATE OR REPLACE TABLE {} AS SELECT *, true AS __p FROM ({})", t.new_out, self.post_sql),
            // A group's sink row changes when it appears, disappears (HAVING, or emptied) or differs.
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT {keys} FROM {} o FULL OUTER JOIN {} n ON {}
                 WHERE o.__p IS DISTINCT FROM n.__p OR {} IS DISTINCT FROM {}",
                t.changed, t.old_out, t.new_out, self.sink_keys_match("o", "n"), row("o"), row("n")
            ),
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT {} FROM {} n WHERE EXISTS (SELECT 1 FROM {} c WHERE {changed_match})",
                t.upserts, sink_cols.iter().map(|c| format!("n.{c}")).collect::<Vec<_>>().join(", "), t.new_out, t.changed
            ),
        ]
    }

    /// The epoch's writes to the lake: replace touched groups' state and changed groups' sink rows.
    pub fn commit_sql(&self, t: &GlobalTables, state_columns: &[(String, String)], epoch: i64) -> Vec<String> {
        let st_cols: Vec<String> = state_columns.iter().map(|(c, _)| ident(c)).collect();
        let sink_cols: Vec<String> = t.sink_columns.iter().map(|c| ident(c)).collect();
        let sink_match = if self.sink_keys.is_empty() { "TRUE".to_string() } else { self.sink_keys_match("s", "c") };
        vec![
            format!("DELETE FROM {} s WHERE EXISTS (SELECT 1 FROM {} d WHERE {})", t.state, t.delta, self.keys_match("s", "d")),
            format!("INSERT INTO {} ({c}) SELECT {c} FROM {} WHERE __cnt <> 0", t.state, t.new_state, c = st_cols.join(", ")),
            format!("DELETE FROM {} s WHERE EXISTS (SELECT 1 FROM {} c WHERE {sink_match})", t.sink, t.changed),
            format!(
                "INSERT INTO {} ({c}, _duckwake_epoch) SELECT {c}, {epoch} FROM {}",
                t.sink, t.upserts, c = sink_cols.join(", ")
            ),
        ]
    }

    /// The state as of the previous epoch while that epoch may still be committing: its new state for the
    /// groups it touched (`prev_new`, groups it emptied left out) and the stored state for every other group.
    /// Exact because the worker holding the lease is the state table's only writer, and correct whether or
    /// not that commit has landed, since it changes exactly the groups `prev_new` holds.
    pub fn overlay_sql(&self, state: &str, prev_new: &str, state_columns: &[(String, String)]) -> String {
        let cast: Vec<String> = state_columns.iter().map(|(c, t)| format!("CAST({c} AS {t}) AS {c}", c = ident(c))).collect();
        let cols: Vec<String> = state_columns.iter().map(|(c, _)| ident(c)).collect();
        format!(
            "(SELECT {} FROM {prev_new} WHERE __cnt <> 0
              UNION ALL SELECT {} FROM {state} s WHERE NOT EXISTS (SELECT 1 FROM {prev_new} p WHERE {}))",
            cast.join(", "),
            cols.join(", "),
            self.keys_match("s", "p")
        )
    }

    /// Finalized values (`__g<j>`, `__a<i>`) of the live groups in a state-shaped table.
    pub fn finalize_sql(&self, state: &str) -> String {
        let mut cols = self.key_cols();
        for (i, a) in self.aggs.iter().enumerate() {
            cols.push(match a.kind {
                AggKind::CountStar | AggKind::Count | AggKind::Min | AggKind::Max => format!("__a{i}"),
                AggKind::Sum => format!("CASE WHEN __a{i}_n = 0 THEN NULL ELSE __a{i}_s END AS __a{i}"),
                AggKind::Avg => format!("CASE WHEN __a{i}_n = 0 THEN NULL ELSE CAST(__a{i}_s AS DOUBLE) / __a{i}_n END AS __a{i}"),
            });
        }
        format!("SELECT {} FROM {state} WHERE __cnt > 0", cols.join(", "))
    }
}

/// Table names one epoch of a global aggregate uses. Staging tables live in `memory.main`; the ones the commit
/// reads (`delta`, `new_state`, `changed`, `upserts`) come in two sets, by epoch parity, so the next epoch can
/// be staged while this one commits.
#[derive(Debug, Clone)]
pub struct GlobalTables {
    pub pre: String,
    pub delta: String,
    pub old: String,
    pub new_state: String,
    pub old_out: String,
    pub new_out: String,
    pub changed: String,
    pub upserts: String,
    /// The DuckLake state table, written at commit. While staging it is what `old` reads: the table itself, or
    /// `GlobalPlan::overlay_sql` over it when the previous epoch may not have committed yet.
    pub state: String,
    /// The sink table and its user-visible columns.
    pub sink: String,
    pub sink_columns: Vec<String>,
}

impl GlobalTables {
    pub fn new(job: &str, state: String, sink: String, plan: &Plan, parity: u64) -> Self {
        let mem = |n: &str| format!("memory.main.{}", ident(&format!("{n}_{job}")));
        let set = |n: &str| format!("memory.main.{}", ident(&format!("{n}_{job}_{parity}")));
        GlobalTables {
            pre: mem("pre"),
            delta: set("gdelta"),
            old: mem("gold"),
            new_state: set("gnew"),
            old_out: mem("oldout"),
            new_out: mem("newout"),
            changed: set("changed"),
            upserts: set("upserts"),
            state,
            sink,
            sink_columns: plan.sink_columns.iter().map(|(c, _)| c.clone()).collect(),
        }
    }
}

/// True for payloads that parse as a JSON object. `json_type` raises on malformed input and DuckDB does not
/// short-circuit AND, so the CASE keeps it away from rows that are not valid JSON.
pub const IS_JSON_OBJECT: &str = "CASE WHEN json_valid(payload) THEN json_type(payload) = 'OBJECT' ELSE false END";

/// Builds the decode query for raw Kafka rows `(_partition, _offset, payload)` already known to be JSON
/// objects. Adds the typed source columns and `_err` (NULL when the row is usable).
pub fn decode_sql(src: &SourceDef, ok_table: &str) -> String {
    let o = &src.options;
    let field = |c: &str| format!("json_extract_string(payload, {})", lit(&format!("$.\"{c}\"")));
    let cols: Vec<String> = o
        .columns
        .iter()
        .map(|(c, t)| format!("TRY_CAST({} AS {t}) AS {}", field(c), ident(c)))
        .collect();
    let mut checks = vec![format!(
        "WHEN TRY_CAST({} AS TIMESTAMP) IS NULL THEN 'missing or invalid event time {}'",
        field(&o.event_time),
        o.event_time
    )];
    for (c, t) in &o.columns {
        checks.push(format!(
            "WHEN {f} IS NOT NULL AND TRY_CAST({f} AS {t}) IS NULL THEN {}",
            lit(&format!("type conflict: {c} is not {t}")),
            f = field(c)
        ));
    }
    format!(
        "SELECT _partition, _offset, payload, {}, CASE {} ELSE NULL END AS _err FROM {ok_table}",
        cols.join(", "),
        checks.join(" ")
    )
}

/// Compiles `sql` for job `job`. Creates the (empty) staging table so DuckDB can bind and DESCRIBE the
/// rewritten queries, which also validates lookup-join tables and column names.
pub fn compile(conn: &Connection, job: &str, sql: &str, sources: &HashMap<String, SourceDef>) -> Result<Plan> {
    let ast = serialize(conn, sql)?;
    let stmts = ast["statements"].as_array().ok_or_else(|| anyhow!("no statements"))?;
    if stmts.len() != 1 {
        bail!("a continuous view must be exactly one SELECT statement");
    }
    let mut stmt = stmts[0].clone();
    let node = &mut stmt["node"];
    if node["type"] != "SELECT_NODE" {
        bail!("only plain SELECT is supported (got {}): UNION, VALUES and set operations are rejected", node["type"]);
    }
    reject_unsupported(node)?;
    let aggs = aggregate_names(conn)?;

    // Find the single stream reference in FROM and point it at the staging table.
    let input_table = format!("memory.main.{}", ident(&format!("in_{job}")));
    let mut found = Vec::new();
    rewrite_from(&mut node["from_table"], sources, &mut found)?;
    let stream = match found.as_slice() {
        [one] => one.clone(),
        [] => bail!("the view must read from a registered source (FROM <source> or FROM TUMBLE/HOP(<source>, ...))"),
        _ => bail!("only one stream reference per view is supported (stream-stream joins are phase 3)"),
    };
    let src = &sources[&stream.source];
    let window = match &stream.window {
        None => None,
        Some((kind, time_col, size, slide)) => {
            if time_col != &src.options.event_time {
                bail!("windows must use the source's event_time column {:?}, not {time_col:?}", src.options.event_time);
            }
            let size_us = interval_us(conn, size)?;
            let slide_us = match slide {
                Some(s) => interval_us(conn, s)?,
                None => size_us,
            };
            if slide_us <= 0 || size_us <= 0 || size_us % slide_us != 0 {
                bail!("HOP size must be a positive multiple of slide");
            }
            Some(WindowSpec { kind: *kind, time_col: time_col.clone(), size_us, slide_us })
        }
    };
    let table_ref = base_table_template(conn, &input_table, &stream.alias)?;
    *stream_slot(&mut node["from_table"]).ok_or_else(|| anyhow!("lost stream reference"))? = table_ref;

    // Staging table with the source columns (+ window columns) so the rewritten SQL binds.
    let mut cols: Vec<String> = src.options.columns.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
    if window.is_some() {
        cols.push("window_start TIMESTAMP".into());
        cols.push("window_end TIMESTAMP".into());
    }
    conn.execute_batch(&format!("CREATE OR REPLACE TABLE {input_table} ({})", cols.join(", ")))?;

    let batch_sql = deserialize(conn, &stmt)?;
    let sink_columns = describe(conn, &batch_sql).context("binding the view")?;
    let mut plan = Plan {
        source: stream.source.clone(),
        window,
        input_table,
        delta_sql: String::new(),
        batch_sql,
        sink_columns,
        state_columns: vec![],
        key_columns: vec![],
        merge_ops: vec![],
        final_select: String::new(),
        global: None,
    };

    let node = &mut stmt["node"];
    let has_group = node["aggregate_handling"] == "FORCE_AGGREGATES"
        || node["group_expressions"].as_array().is_some_and(|g| !g.is_empty());
    let has_agg = node["select_list"].as_array().unwrap().iter().any(|e| contains_fn(e, &aggs));
    if plan.window.is_none() {
        if has_group || has_agg || !node["having"].is_null() {
            compile_global(conn, job, &mut stmt, &stream.alias, &aggs, &mut plan)?;
            return Ok(plan);
        }
        plan.delta_sql = plan.batch_sql.clone();
        return Ok(plan);
    }
    if !node["having"].is_null() {
        bail!("HAVING is not supported in windowed views yet; it is supported in non-windowed GROUP BY views");
    }
    if !has_group {
        bail!("a windowed view must GROUP BY window_start (or GROUP BY ALL with window_start selected)");
    }
    compile_windowed(conn, &mut stmt, &stream.alias, &aggs, &mut plan)?;
    Ok(plan)
}

fn compile_windowed(
    conn: &Connection,
    stmt: &mut Value,
    alias: &str,
    aggs: &HashSet<String>,
    plan: &mut Plan,
) -> Result<()> {
    let node = &mut stmt["node"];
    let group_all = node["aggregate_handling"] == "FORCE_AGGREGATES";
    let groups_by_window = if group_all {
        node["select_list"].as_array().unwrap().iter().any(is_window_col)
    } else {
        node["group_expressions"].as_array().unwrap().iter().any(is_window_col)
    };
    if !groups_by_window {
        bail!("a windowed view must group by window_start or window_end");
    }
    if node["group_sets"].as_array().is_some_and(|s| s.len() > 1) {
        bail!("GROUPING SETS / ROLLUP / CUBE are not supported");
    }

    let items = node["select_list"].as_array().unwrap().clone();
    if plan.sink_columns.len() != items.len() {
        bail!("SELECT * is not supported in windowed views; list the columns");
    }
    let (ws_ref, we_ref) = window_refs(conn, alias)?;
    let mut partial_items = Vec::new();
    let mut finals = Vec::new();
    let mut nkeys = 0;
    for (i, item) in items.iter().enumerate() {
        let (sink_name, sink_type) = &plan.sink_columns[i];
        let is_agg = item["class"] == "FUNCTION" && aggs.contains(item["function_name"].as_str().unwrap_or(""));
        if !is_agg {
            if contains_fn(item, aggs) {
                bail!("aggregates must be top-level select items (column {sink_name:?}); nested expressions over aggregates are not supported");
            }
            let k = format!("__k{nkeys}");
            nkeys += 1;
            let mut e = item.clone();
            e["alias"] = json!(k);
            partial_items.push(e);
            plan.key_columns.push(k.clone());
            finals.push(format!("CAST({} AS {sink_type}) AS {}", ident(&k), ident(sink_name)));
            continue;
        }
        let fname = item["function_name"].as_str().unwrap().to_ascii_lowercase();
        if !SUPPORTED_AGGS.contains(&fname.as_str()) {
            bail!("aggregate {fname}() is not supported in v1 (supported: count, sum, min, max, avg)");
        }
        if item["distinct"] == true
            || !item["filter"].is_null()
            || item["order_bys"]["orders"].as_array().is_some_and(|o| !o.is_empty())
        {
            bail!("DISTINCT, FILTER and ORDER BY inside aggregates are not supported ({fname})");
        }
        if item["arguments"].as_array().unwrap().iter().any(|a| contains_fn(a, aggs)) {
            bail!("nested aggregates are not supported ({fname})");
        }
        let p = format!("__p{i}");
        let mut e = item.clone();
        e["alias"] = json!(p);
        let finalize = match fname.as_str() {
            "count" | "count_star" => {
                plan.merge_ops.push((p.clone(), MergeOp::Add));
                ident(&p)
            }
            "sum" => {
                plan.merge_ops.push((p.clone(), MergeOp::AddNullable));
                ident(&p)
            }
            "min" => {
                plan.merge_ops.push((p.clone(), MergeOp::Min));
                ident(&p)
            }
            "max" => {
                plan.merge_ops.push((p.clone(), MergeOp::Max));
                ident(&p)
            }
            "avg" => {
                let (s, c) = (format!("{p}s"), format!("{p}c"));
                let mut es = item.clone();
                set_fn_name(&mut es, "sum", &s);
                let mut ec = item.clone();
                set_fn_name(&mut ec, "count", &c);
                partial_items.push(es);
                plan.merge_ops.push((s.clone(), MergeOp::AddNullable));
                e = ec;
                plan.merge_ops.push((c.clone(), MergeOp::Add));
                format!("CAST({} AS DOUBLE) / NULLIF({}, 0)", ident(&s), ident(&c))
            }
            _ => unreachable!(),
        };
        partial_items.push(e);
        finals.push(format!("CAST({finalize} AS {sink_type}) AS {}", ident(sink_name)));
    }
    partial_items.push(ws_ref.clone());
    partial_items.push(we_ref.clone());
    node["select_list"] = Value::Array(partial_items);
    if !group_all {
        let groups = node["group_expressions"].as_array_mut().unwrap();
        let base = groups.len();
        for r in [&ws_ref, &we_ref] {
            let mut g = r.clone();
            g["alias"] = json!("");
            groups.push(g);
        }
        if let Some(set) = node["group_sets"].get_mut(0).and_then(|s| s.as_array_mut()) {
            set.push(json!(base));
            set.push(json!(base + 1));
        }
    }
    plan.delta_sql = deserialize(conn, stmt)?;
    let partial_cols = describe(conn, &plan.delta_sql).context("binding the partial aggregate")?;
    // State column order: window, keys, partials.
    let mut state = vec![];
    for name in ["__ws", "__we"].iter().map(|s| s.to_string()).chain(plan.key_columns.clone()) {
        state.push(partial_cols.iter().find(|(c, _)| *c == name).cloned().unwrap());
    }
    for (c, _) in &plan.merge_ops {
        state.push(partial_cols.iter().find(|(n, _)| n == c).cloned().unwrap());
    }
    plan.state_columns = state;
    plan.final_select = finals.join(", ");
    Ok(())
}

fn compile_global(
    conn: &Connection,
    job: &str,
    stmt: &mut Value,
    alias: &str,
    aggs: &HashSet<String>,
    plan: &mut Plan,
) -> Result<()> {
    let node = stmt["node"].clone();
    if node["group_sets"].as_array().is_some_and(|s| s.len() > 1) {
        bail!("GROUPING SETS / ROLLUP / CUBE are not supported");
    }
    let items: Vec<Value> = node["select_list"].as_array().unwrap().clone();
    if plan.sink_columns.len() != items.len() {
        bail!("SELECT * is not supported in aggregate views; list the columns");
    }
    let stripped = |e: &Value| {
        let mut e = e.clone();
        e["alias"] = json!("");
        e
    };

    // Group keys: GROUP BY ALL = the select items without aggregates; explicit keys may name a select
    // alias or position, which DuckDB allows, so resolve those to the item's expression.
    let group_all = node["aggregate_handling"] == "FORCE_AGGREGATES";
    let mut groups: Vec<Value> = Vec::new();
    if group_all {
        groups.extend(items.iter().filter(|e| !contains_fn(e, aggs)).map(stripped));
    } else {
        for g in node["group_expressions"].as_array().into_iter().flatten() {
            let by_alias = (g["class"] == "COLUMN_REF" && g["column_names"].as_array().is_some_and(|n| n.len() == 1))
                .then(|| g["column_names"][0].as_str().unwrap_or(""))
                .and_then(|name| items.iter().find(|e| e["alias"] == name));
            let by_position = (g["class"] == "CONSTANT")
                .then(|| g["literal"]["text"].as_str().and_then(|t| t.parse::<usize>().ok()))
                .flatten()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| items.get(i));
            groups.push(stripped(by_alias.or(by_position).unwrap_or(g)));
        }
    }
    if groups.iter().any(|g| contains_fn(g, aggs)) {
        bail!("aggregates cannot be used as GROUP BY keys");
    }

    // Aggregates in the select list and HAVING, deduplicated; each becomes a column __a<i>.
    let mut specs: Vec<(Value, AggSpec)> = Vec::new();
    let mut args: Vec<Value> = Vec::new();
    let mut extract = |e: &mut Value| -> Result<()> { replace_aggs(e, aggs, &mut specs, &mut args) };
    let mut post_items = items.clone();
    for e in post_items.iter_mut() {
        extract(e)?;
    }
    let mut having = node["having"].clone();
    if !having.is_null() {
        extract(&mut having)?;
    }
    // Remaining references to the group keys become __g<j>.
    for e in post_items.iter_mut() {
        replace_groups(e, &groups);
    }
    if !having.is_null() {
        replace_groups(&mut having, &groups);
    }

    // Every key must be selected, so the sink can be upserted by it.
    let mut sink_keys = Vec::new();
    for (j, g) in groups.iter().enumerate() {
        let pos = items.iter().position(|e| norm(&stripped(e)) == norm(g)).ok_or_else(|| {
            anyhow!("select every GROUP BY key (key {} is missing) so the sink can be updated per group", j + 1)
        })?;
        sink_keys.push(plan.sink_columns[pos].0.clone());
    }

    // Operator 1, pre-aggregate: the view's FROM/WHERE producing keys, arguments and the weight.
    conn.execute_batch(&format!("ALTER TABLE {} ADD COLUMN __w INTEGER", plan.input_table))?;
    let weight = serialize(conn, &format!("SELECT {}.__w AS __w", ident(alias)))?["statements"][0]["node"]["select_list"][0].clone();
    let mut pre = stmt.clone();
    let mut list: Vec<Value> = Vec::new();
    for (j, g) in groups.iter().enumerate() {
        let mut g = g.clone();
        g["alias"] = json!(format!("__g{j}"));
        list.push(g);
    }
    for (k, a) in args.iter().enumerate() {
        let mut a = a.clone();
        a["alias"] = json!(format!("__x{k}"));
        list.push(a);
    }
    list.push(weight);
    let pn = &mut pre["node"];
    pn["select_list"] = Value::Array(list);
    pn["group_expressions"] = json!([]);
    pn["group_sets"] = json!([]);
    pn["aggregate_handling"] = json!("STANDARD_HANDLING");
    pn["having"] = Value::Null;
    let pre_sql = deserialize(conn, &pre)?;
    let pre_cols = describe(conn, &pre_sql).context("binding the pre-aggregate")?;
    let col_type = |name: &str| pre_cols.iter().find(|(c, _)| c == name).map(|(_, t)| t.clone()).unwrap();

    // Operator 2, aggregate: state columns and the finalized-value table the post step reads.
    let sum_type = |k: usize| -> Result<String> {
        Ok(describe(conn, &format!("SELECT sum(__x{k}) FROM ({pre_sql})"))?.remove(0).1)
    };
    let mut state: Vec<(String, String)> = (0..groups.len()).map(|j| (format!("__g{j}"), col_type(&format!("__g{j}")))).collect();
    state.push(("__cnt".into(), "BIGINT".into()));
    let mut agg_cols: Vec<String> = (0..groups.len()).map(|j| format!("__g{j} {}", col_type(&format!("__g{j}")))).collect();
    for (i, (_, a)) in specs.iter().enumerate() {
        let x = a.arg.map(|k| col_type(&format!("__x{k}")));
        let out_type = match a.kind {
            AggKind::CountStar | AggKind::Count => {
                state.push((format!("__a{i}"), "BIGINT".into()));
                "BIGINT".to_string()
            }
            AggKind::Sum | AggKind::Avg => {
                let st = sum_type(a.arg.unwrap())?;
                state.push((format!("__a{i}_s"), st.clone()));
                state.push((format!("__a{i}_n"), "BIGINT".into()));
                if a.kind == AggKind::Sum { st } else { "DOUBLE".into() }
            }
            AggKind::Min | AggKind::Max => {
                state.push((format!("__a{i}"), x.clone().unwrap()));
                x.unwrap()
            }
        };
        agg_cols.push(format!("__a{i} {out_type}"));
    }
    let agg_table = format!("memory.main.{}", ident(&format!("agg_{job}")));
    conn.execute_batch(&format!("CREATE OR REPLACE TABLE {agg_table} ({})", agg_cols.join(", ")))?;

    // Operator 3, post: HAVING and the select list over the finalized values, named like the sink.
    let mut post = serialize(conn, &format!("SELECT 1 FROM {agg_table}"))?["statements"][0].clone();
    for (e, (name, _)) in post_items.iter_mut().zip(&plan.sink_columns) {
        e["alias"] = json!(name);
    }
    post["node"]["select_list"] = Value::Array(post_items);
    post["node"]["where_clause"] = having;
    let post_raw = deserialize(conn, &post)?;
    let casts: Vec<String> = plan.sink_columns.iter().map(|(c, t)| format!("CAST({} AS {t}) AS {}", ident(c), ident(c))).collect();
    let post_sql = format!("SELECT {} FROM ({post_raw})", casts.join(", "));
    describe(conn, &post_sql).context("binding the select list over the aggregates (every non-aggregate column must be a GROUP BY key)")?;

    plan.state_columns = state;
    plan.delta_sql = pre_sql.clone();
    plan.global = Some(GlobalPlan {
        pre_sql,
        keys: groups.len(),
        aggs: specs.into_iter().map(|(_, a)| a).collect(),
        agg_table,
        post_sql,
        sink_keys,
    });
    Ok(())
}

/// Structural equality key for an expression: its JSON without source positions or alias.
fn norm(e: &Value) -> String {
    fn strip(v: &Value) -> Value {
        match v {
            Value::Object(o) => Value::Object(
                o.iter()
                    .filter(|(k, _)| !k.starts_with("query_location") && k.as_str() != "alias")
                    .map(|(k, v)| (k.clone(), strip(v)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(strip).collect()),
            v => v.clone(),
        }
    }
    strip(e).to_string()
}

fn column_ref(name: &str, alias: &str) -> Value {
    json!({"class": "COLUMN_REF", "type": "COLUMN_REF", "alias": alias, "query_location": 18446744073709551615u64,
           "query_location_length": 0, "column_names": [name]})
}

/// Replaces each aggregate call in `e` by `__a<i>`, recording its spec and argument.
fn replace_aggs(e: &mut Value, aggs: &HashSet<String>, specs: &mut Vec<(Value, AggSpec)>, args: &mut Vec<Value>) -> Result<()> {
    if e["class"] == "FUNCTION" && aggs.contains(e["function_name"].as_str().unwrap_or("")) {
        let fname = e["function_name"].as_str().unwrap().to_ascii_lowercase();
        let kind = match fname.as_str() {
            "count_star" => AggKind::CountStar,
            "count" => AggKind::Count,
            "sum" => AggKind::Sum,
            "avg" | "mean" => AggKind::Avg,
            "min" => AggKind::Min,
            "max" => AggKind::Max,
            _ => bail!("aggregate {fname}() is not supported in aggregate views yet (supported: count, sum, avg, min, max)"),
        };
        if e["distinct"] == true || !e["filter"].is_null() || e["order_bys"]["orders"].as_array().is_some_and(|o| !o.is_empty()) {
            bail!("DISTINCT, FILTER and ORDER BY inside aggregates are not supported yet ({fname})");
        }
        let fn_args: Vec<Value> = e["arguments"].as_array().cloned().unwrap_or_default().iter()
            .map(|a| a.get("expression").cloned().unwrap_or_else(|| a.clone())).collect();
        if fn_args.iter().any(|a| contains_fn(a, aggs)) {
            bail!("nested aggregates are not supported ({fname})");
        }
        let arg = match kind {
            AggKind::CountStar => None,
            _ => {
                let [a] = fn_args.as_slice() else { bail!("{fname}() takes exactly one argument here") };
                let key = norm(a);
                Some(args.iter().position(|x| norm(x) == key).unwrap_or_else(|| {
                    args.push(a.clone());
                    args.len() - 1
                }))
            }
        };
        let key = norm(e);
        let i = specs.iter().position(|(n, _)| norm(n) == key).unwrap_or_else(|| {
            specs.push((e.clone(), AggSpec { kind, arg }));
            specs.len() - 1
        });
        let alias = e["alias"].as_str().unwrap_or("").to_string();
        *e = column_ref(&format!("__a{i}"), &alias);
        return Ok(());
    }
    match e {
        Value::Array(a) => a.iter_mut().try_for_each(|x| replace_aggs(x, aggs, specs, args)),
        Value::Object(o) => o.values_mut().try_for_each(|x| replace_aggs(x, aggs, specs, args)),
        _ => Ok(()),
    }
}

/// Replaces each subexpression equal to a group key by `__g<j>`.
fn replace_groups(e: &mut Value, groups: &[Value]) {
    if e.get("class").is_some() {
        let key = norm(e);
        if let Some(j) = groups.iter().position(|g| norm(g) == key) {
            let alias = e["alias"].as_str().unwrap_or("").to_string();
            *e = column_ref(&format!("__g{j}"), &alias);
            return;
        }
    }
    match e {
        Value::Array(a) => a.iter_mut().for_each(|x| replace_groups(x, groups)),
        Value::Object(o) => o.values_mut().for_each(|x| replace_groups(x, groups)),
        _ => {}
    }
}

#[derive(Clone)]
struct StreamRef {
    source: String,
    alias: String,
    window: Option<(WindowKind, String, Value, Option<Value>)>,
}

fn reject_unsupported(node: &Value) -> Result<()> {
    if let Some(m) = node["modifiers"].as_array().and_then(|m| m.first()) {
        let what = match m["type"].as_str().unwrap_or("") {
            "ORDER_MODIFIER" => "ORDER BY",
            "DISTINCT_MODIFIER" => "DISTINCT",
            "LIMIT_MODIFIER" | "LIMIT_PERCENT_MODIFIER" => "LIMIT",
            other => other,
        };
        bail!("{what} is not supported in continuous views");
    }
    if node["cte_map"]["map"].as_array().is_some_and(|m| !m.is_empty()) {
        bail!("WITH (CTEs) is not supported in continuous views");
    }
    for (key, what) in [("qualify", "QUALIFY"), ("sample", "USING SAMPLE")] {
        if !node[key].is_null() {
            bail!("{what} is not supported in continuous views");
        }
    }
    let mut bad = None;
    walk(node, &mut |v| match v["class"].as_str() {
        Some("WINDOW") => bad = Some("window functions (OVER)"),
        Some("SUBQUERY") => bad = Some("subqueries"),
        _ => {}
    });
    if let Some(what) = bad {
        bail!("{what} are not supported in continuous views");
    }
    Ok(())
}

fn rewrite_from(t: &mut Value, sources: &HashMap<String, SourceDef>, found: &mut Vec<StreamRef>) -> Result<()> {
    match t["type"].as_str().unwrap_or("") {
        "BASE_TABLE" => {
            let name = t["table_name"].as_str().unwrap_or("");
            let unqualified = t["schema_name"] == "" && t["catalog_name"] == "";
            if unqualified && sources.contains_key(name) {
                let alias = t["alias"].as_str().filter(|a| !a.is_empty()).unwrap_or(name).to_string();
                found.push(StreamRef { source: name.to_string(), alias, window: None });
                t["__stream"] = json!(true);
            }
            Ok(())
        }
        "TABLE_FUNCTION" => {
            let f = &t["function"];
            let fname = f["function_name"].as_str().unwrap_or("").to_ascii_lowercase();
            let kind = match fname.as_str() {
                "tumble" => WindowKind::Tumble,
                "hop" => WindowKind::Hop,
                _ => return Ok(()),
            };
            let args: Vec<Value> = f["arguments"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| a.get("expression").cloned().unwrap_or_else(|| a.clone()))
                .collect();
            let want = if kind == WindowKind::Tumble { 3 } else { 4 };
            if args.len() != want {
                bail!("{fname}() takes {} arguments", if want == 3 { "(source, time_col, size)" } else { "(source, time_col, size, slide)" });
            }
            let col = |v: &Value| -> Result<String> {
                (v["class"] == "COLUMN_REF")
                    .then(|| v["column_names"].as_array().and_then(|n| n.last()).and_then(|n| n.as_str()).map(String::from))
                    .flatten()
                    .ok_or_else(|| anyhow!("{fname}(): first two arguments must be a source name and a column"))
            };
            let source = col(&args[0])?;
            if !sources.contains_key(&source) {
                bail!("{fname}(): unknown source {source:?}");
            }
            let time_col = col(&args[1])?;
            let alias = t["alias"].as_str().filter(|a| !a.is_empty()).unwrap_or(&fname).to_string();
            let slide = (kind == WindowKind::Hop).then(|| args[3].clone());
            found.push(StreamRef { source, alias, window: Some((kind, time_col, args[2].clone(), slide)) });
            t["__stream"] = json!(true);
            Ok(())
        }
        "JOIN" => {
            rewrite_from(&mut t["left"], sources, found)?;
            rewrite_from(&mut t["right"], sources, found)
        }
        "SUBQUERY" => bail!("subqueries in FROM are not supported in continuous views"),
        "EMPTY" => Ok(()),
        other => bail!("unsupported FROM item {other}"),
    }
}

/// The table ref marked by `rewrite_from`.
fn stream_slot(t: &mut Value) -> Option<&mut Value> {
    if t.get("__stream").is_some() {
        return Some(t);
    }
    if t["type"] == "JOIN" {
        if t["left"].to_string().contains("\"__stream\"") {
            return stream_slot(&mut t["left"]);
        }
        return stream_slot(&mut t["right"]);
    }
    None
}

fn is_window_col(e: &Value) -> bool {
    e["class"] == "COLUMN_REF"
        && e["column_names"]
            .as_array()
            .and_then(|n| n.last())
            .and_then(|n| n.as_str())
            .is_some_and(|n| n.eq_ignore_ascii_case("window_start") || n.eq_ignore_ascii_case("window_end"))
}

fn set_fn_name(e: &mut Value, name: &str, alias: &str) {
    e["function_name"] = json!(name);
    e["qualified_name"] = json!({ "path": [name] });
    e["alias"] = json!(alias);
}

fn walk(v: &Value, f: &mut dyn FnMut(&Value)) {
    f(v);
    match v {
        Value::Array(a) => a.iter().for_each(|x| walk(x, f)),
        Value::Object(o) => o.values().for_each(|x| walk(x, f)),
        _ => {}
    }
}

fn contains_fn(e: &Value, names: &HashSet<String>) -> bool {
    let mut hit = false;
    walk(e, &mut |v| {
        if v["class"] == "FUNCTION" && names.contains(v["function_name"].as_str().unwrap_or("")) {
            hit = true;
        }
    });
    hit
}

fn aggregate_names(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'aggregate'")?;
    let names = stmt.query_map([], |r| r.get::<_, String>(0))?.collect::<duckdb::Result<HashSet<_>>>()?;
    Ok(names)
}

fn serialize(conn: &Connection, sql: &str) -> Result<Value> {
    let s: String = conn.query_row("SELECT json_serialize_sql(?)::VARCHAR", [sql], |r| r.get(0))?;
    let v: Value = serde_json::from_str(&s)?;
    if v["error"] == true {
        bail!("{}", v["error_message"].as_str().unwrap_or("parse error"));
    }
    Ok(v)
}

fn deserialize(conn: &Connection, stmt: &Value) -> Result<String> {
    let doc = json!({ "error": false, "statements": [stmt] });
    let sql: String = conn.query_row("SELECT json_deserialize_sql(?::JSON)", [doc.to_string()], |r| r.get(0))?;
    Ok(sql)
}

fn base_table_template(conn: &Connection, table: &str, alias: &str) -> Result<Value> {
    let v = serialize(conn, &format!("SELECT 1 FROM {table} AS {}", ident(alias)))?;
    Ok(v["statements"][0]["node"]["from_table"].clone())
}

fn window_refs(conn: &Connection, alias: &str) -> Result<(Value, Value)> {
    let a = ident(alias);
    let v = serialize(conn, &format!("SELECT {a}.window_start AS __ws, {a}.window_end AS __we"))?;
    let l = &v["statements"][0]["node"]["select_list"];
    Ok((l[0].clone(), l[1].clone()))
}

/// Evaluates a size/slide expression (e.g. `INTERVAL 1 MINUTE`) to microseconds; month-based sizes are rejected.
fn interval_us(conn: &Connection, expr: &Value) -> Result<i64> {
    let mut sel = serialize(conn, "SELECT 1")?;
    sel["statements"][0]["node"]["select_list"] = json!([expr]);
    let e = deserialize(conn, &sel["statements"][0])?;
    let e = e.strip_prefix("SELECT ").unwrap_or(&e).to_string();
    let (a, b): (i64, i64) = conn
        .query_row(
            &format!(
                "SELECT epoch_us(TIMESTAMP '1970-01-01' + ({e})::INTERVAL),
                        epoch_us(TIMESTAMP '1970-02-01' + ({e})::INTERVAL) - epoch_us(TIMESTAMP '1970-02-01')"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .with_context(|| format!("window size {e} is not an interval"))?;
    if a != b {
        bail!("month-based window sizes are not supported");
    }
    Ok(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SourceOptions;

    fn setup() -> (Connection, HashMap<String, SourceDef>) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE games (game_id VARCHAR, studio VARCHAR)").unwrap();
        let opts = SourceOptions::from_json(&json!({
            "brokers": "x", "topic": "clicks", "event_time": "ts",
            "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR", "user_id": "VARCHAR", "amount": "DECIMAL(18,2)"}
        }))
        .unwrap();
        let src = SourceDef { name: "clicks".into(), kind: "kafka".into(), options: opts };
        (conn, HashMap::from([("clicks".to_string(), src)]))
    }

    fn err(sql: &str) -> String {
        let (c, s) = setup();
        format!("{:#}", compile(&c, "j", sql, &s).unwrap_err())
    }

    #[test]
    fn tumble_group_by_all() {
        let (c, s) = setup();
        let p = compile(&c, "j", "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS a, max(ts) AS last_ts FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL", &s).unwrap();
        assert_eq!(p.window.as_ref().unwrap().size_us, 60_000_000);
        let names: Vec<_> = p.sink_columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["window_start", "game_id", "n", "revenue", "a", "last_ts"]);
        let state: Vec<_> = p.state_columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(state, ["__ws", "__we", "__k0", "__k1", "__p2", "__p3", "__p4s", "__p4c", "__p5"]);
        assert!(p.delta_sql.contains("in_j"), "{}", p.delta_sql);
        // The partial query must run.
        c.execute_batch(&format!("CREATE TABLE part AS {}", p.delta_sql)).unwrap();
    }

    #[test]
    fn hop_explicit_group_by_with_lookup_join() {
        let (c, s) = setup();
        let p = compile(&c, "j", "SELECT c.window_end, g.studio, count(*) AS n FROM HOP(clicks, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND) c JOIN games g ON g.game_id = c.game_id WHERE c.amount > 0 GROUP BY c.window_end, g.studio", &s).unwrap();
        let w = p.window.unwrap();
        assert_eq!((w.kind, w.size_us, w.slide_us), (WindowKind::Hop, 60_000_000, 20_000_000));
        assert_eq!(p.key_columns, ["__k0", "__k1"]);
    }

    #[test]
    fn stateless_filter() {
        let (c, s) = setup();
        let p = compile(&c, "j", "SELECT ts, upper(game_id) AS g, amount * 2 AS a2 FROM clicks WHERE amount > 10", &s).unwrap();
        assert!(p.window.is_none());
        assert_eq!(p.sink_columns.len(), 3);
    }

    /// Runs a global aggregate epoch by epoch with the engine's own statements and checks the upsert
    /// sink against the view run as one batch query after every epoch.
    fn incremental_equals_batch(sql: &str, epochs: &[(i64, i64)]) {
        let (c, s) = setup();
        c.execute_batch("INSERT INTO games SELECT 'g' || i, ['Aurora', 'Basalt'][i % 2 + 1] FROM range(4) t(i)").unwrap();
        let p = compile(&c, "g", sql, &s).unwrap();
        let g = p.global.as_ref().expect("global plan");
        let cols: Vec<String> = p.state_columns.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect();
        let sink: Vec<String> = p.sink_columns.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect();
        c.execute_batch(&format!(
            "CREATE TABLE memory.main.st ({}); CREATE TABLE memory.main.sink ({}, _duckwake_epoch BIGINT);
             CREATE TABLE memory.main.seen (ts TIMESTAMP, game_id VARCHAR, user_id VARCHAR, amount DECIMAL(18,2));",
            cols.join(", "), sink.join(", ")
        )).unwrap();
        let t = GlobalTables::new("g", "memory.main.st".into(), "memory.main.sink".into(), &p, 0);
        let names: Vec<String> = p.sink_columns.iter().map(|(n, _)| ident(n)).collect();
        let names = names.join(", ");
        for (epoch, (from, to)) in epochs.iter().enumerate() {
            let rows = format!(
                "SELECT TIMESTAMP '2026-01-01' + to_seconds(i) AS ts,
                        CASE WHEN i % 17 = 0 THEN NULL ELSE 'g' || (i * 7 % 5) END AS game_id, 'u' || (i % 9) AS user_id,
                        CASE WHEN i % 11 = 0 THEN NULL ELSE ((i * 37) % 13)::DECIMAL(18,2) END AS amount
                 FROM range({from}, {to}) t(i)"
            );
            c.execute_batch(&format!(
                "INSERT INTO memory.main.seen {rows};
                 CREATE OR REPLACE TABLE {} AS SELECT *, 1::INTEGER AS __w FROM ({rows});",
                p.input_table
            )).unwrap();
            for stmt in g.stage_sql(&t, &p.state_columns).into_iter().chain(g.commit_sql(&t, &p.state_columns, epoch as i64 + 1)) {
                c.execute_batch(&stmt).unwrap_or_else(|e| panic!("{e}\n{stmt}"));
            }
            c.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {} AS SELECT *, 1::INTEGER AS __w FROM memory.main.seen;
                 CREATE OR REPLACE TABLE memory.main.expected AS {};",
                p.input_table, p.batch_sql
            )).unwrap();
            let diff: i64 = c.query_row(&format!(
                "SELECT (SELECT count(*) FROM (SELECT * FROM memory.main.expected EXCEPT ALL SELECT {names} FROM memory.main.sink))
                      + (SELECT count(*) FROM (SELECT {names} FROM memory.main.sink EXCEPT ALL SELECT * FROM memory.main.expected))"
            ), [], |r| r.get(0)).unwrap();
            assert_eq!(diff, 0, "epoch {} of {sql}", epoch + 1);
        }
        let n: i64 = c.query_row("SELECT count(*) FROM memory.main.sink", [], |r| r.get(0)).unwrap();
        assert!(n > 0, "sink empty for {sql}");
    }

    /// The same with each epoch staged before the previous one commits, reading that epoch's new state through
    /// `overlay_sql` as a pipelined worker does. The previous commit lands before the staging on even epochs
    /// and after it on odd ones; either way the result must equal the batch query.
    fn pipelined_equals_batch(sql: &str, epochs: &[(i64, i64)]) {
        let (c, s) = setup();
        c.execute_batch("INSERT INTO games SELECT 'g' || i, ['Aurora', 'Basalt'][i % 2 + 1] FROM range(4) t(i)").unwrap();
        let p = compile(&c, "g", sql, &s).unwrap();
        let g = p.global.as_ref().expect("global plan");
        let cols: Vec<String> = p.state_columns.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect();
        let sink: Vec<String> = p.sink_columns.iter().map(|(n, t)| format!("{} {t}", ident(n))).collect();
        c.execute_batch(&format!(
            "CREATE TABLE memory.main.st ({}); CREATE TABLE memory.main.sink ({}, _duckwake_epoch BIGINT);
             CREATE TABLE memory.main.seen (ts TIMESTAMP, game_id VARCHAR, user_id VARCHAR, amount DECIMAL(18,2));",
            cols.join(", "), sink.join(", ")
        )).unwrap();
        let tables = |e: usize| GlobalTables::new("g", "memory.main.st".into(), "memory.main.sink".into(), &p, e as u64 % 2);
        let rows = |e: usize| {
            let (from, to) = epochs[e];
            format!(
                "SELECT TIMESTAMP '2026-01-01' + to_seconds(i) AS ts,
                        CASE WHEN i % 17 = 0 THEN NULL ELSE 'g' || (i * 7 % 5) END AS game_id, 'u' || (i % 9) AS user_id,
                        CASE WHEN i % 11 = 0 THEN NULL ELSE ((i * 37) % 13)::DECIMAL(18,2) END AS amount
                 FROM range({from}, {to}) t(i)"
            )
        };
        let names: Vec<String> = p.sink_columns.iter().map(|(n, _)| ident(n)).collect();
        let names = names.join(", ");
        let commit = |e: usize| {
            for stmt in g.commit_sql(&tables(e), &p.state_columns, e as i64 + 1) {
                c.execute_batch(&stmt).unwrap_or_else(|err| panic!("{err}\n{stmt}"));
            }
            c.execute_batch(&format!("INSERT INTO memory.main.seen {}", rows(e))).unwrap();
        };
        let check = |committed: usize| {
            c.execute_batch(&format!(
                "CREATE OR REPLACE TABLE {} AS SELECT *, 1::INTEGER AS __w FROM memory.main.seen;
                 CREATE OR REPLACE TABLE memory.main.expected AS {};",
                p.input_table, p.batch_sql
            )).unwrap();
            let diff: i64 = c.query_row(&format!(
                "SELECT (SELECT count(*) FROM (SELECT * FROM memory.main.expected EXCEPT ALL SELECT {names} FROM memory.main.sink))
                      + (SELECT count(*) FROM (SELECT {names} FROM memory.main.sink EXCEPT ALL SELECT * FROM memory.main.expected))"
            ), [], |r| r.get(0)).unwrap();
            assert_eq!(diff, 0, "after committing {committed} epochs of {sql}");
        };
        for e in 0..epochs.len() {
            if e > 0 && e % 2 == 0 {
                commit(e - 1);
            }
            let mut t = tables(e);
            if e > 0 {
                t.state = g.overlay_sql(&t.state, &tables(e - 1).new_state, &p.state_columns);
            }
            c.execute_batch(&format!("CREATE OR REPLACE TABLE {} AS SELECT *, 1::INTEGER AS __w FROM ({})", p.input_table, rows(e))).unwrap();
            for stmt in g.stage_sql(&t, &p.state_columns) {
                c.execute_batch(&stmt).unwrap_or_else(|err| panic!("{err}\n{stmt}"));
            }
            if e > 0 && e % 2 == 1 {
                commit(e - 1);
            }
            if e > 0 {
                check(e);
            }
        }
        commit(epochs.len() - 1);
        check(epochs.len());
    }

    const EPOCHS: [(i64, i64); 5] = [(0, 7), (7, 40), (40, 41), (41, 200), (200, 260)];

    #[test]
    fn global_pipelined_through_the_overlay() {
        pipelined_equals_batch(
            "SELECT game_id, count(*) AS n, count(amount) AS n_amount, sum(amount) AS total, avg(amount) AS mean,
                    min(amount) AS lo, max(amount) AS hi
             FROM clicks GROUP BY ALL HAVING avg(amount) > 5.5",
            &EPOCHS,
        );
        // Without GROUP BY the single group is touched by every epoch; with a lookup join and HAVING groups
        // leave and re-enter the sink.
        pipelined_equals_batch("SELECT count(*) AS n, sum(amount) AS s, avg(amount) AS a FROM clicks WHERE amount > 5", &EPOCHS);
        pipelined_equals_batch(
            "SELECT g.studio, count(*) AS n, max(c.amount) AS top FROM clicks c JOIN games g ON g.game_id = c.game_id
             GROUP BY 1 HAVING count(*) % 2 = 0",
            &EPOCHS,
        );
    }

    #[test]
    fn global_group_by_all_with_having() {
        incremental_equals_batch(
            "SELECT game_id, count(*) AS n, count(amount) AS n_amount, sum(amount) AS total, avg(amount) AS mean,
                    min(amount) AS lo, max(amount) AS hi
             FROM clicks GROUP BY ALL HAVING avg(amount) > 5.5",
            &EPOCHS,
        );
    }

    #[test]
    fn global_alias_key_and_expressions_over_aggregates() {
        incremental_equals_batch(
            "SELECT upper(game_id) AS g, sum(amount) / count(*) AS per_click, count(*) + 1 AS n1
             FROM clicks WHERE user_id <> 'u3' GROUP BY g HAVING sum(amount) > 50",
            &EPOCHS,
        );
    }

    #[test]
    fn global_positional_key_with_lookup_join() {
        incremental_equals_batch(
            "SELECT g.studio, count(*) AS n, max(c.amount) AS top FROM clicks c JOIN games g ON g.game_id = c.game_id GROUP BY 1",
            &EPOCHS,
        );
    }

    #[test]
    fn global_without_group_by() {
        incremental_equals_batch("SELECT count(*) AS n, sum(amount) AS s, avg(amount) AS a FROM clicks WHERE amount > 5", &EPOCHS);
    }

    #[test]
    fn global_rejections() {
        assert!(err("SELECT count(*) FROM clicks GROUP BY game_id").contains("select every GROUP BY key"));
        assert!(err("SELECT game_id, count(DISTINCT user_id) FROM clicks GROUP BY ALL").contains("DISTINCT"));
        assert!(err("SELECT game_id, string_agg(user_id) FROM clicks GROUP BY ALL").contains("not supported in aggregate views"));
        assert!(err("SELECT game_id, count(*) FROM clicks GROUP BY ROLLUP (game_id)").contains("ROLLUP"));
    }

    #[test]
    fn rejections() {
        assert!(err("SELECT * FROM clicks ORDER BY ts").contains("ORDER BY"));
        assert!(err("SELECT * FROM clicks LIMIT 5").contains("LIMIT"));
        assert!(err("SELECT DISTINCT game_id FROM clicks").contains("DISTINCT"));
        assert!(err("SELECT ts, row_number() OVER () FROM clicks").contains("window functions"));
        assert!(err("SELECT window_start, approx_count_distinct(user_id) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("not supported in v1"));
        assert!(err("SELECT window_start, count(DISTINCT user_id) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("DISTINCT"));
        assert!(err("SELECT game_id, count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("group by window"));
        assert!(err("SELECT window_start, sum(amount) / count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("top-level"));
        assert!(err("SELECT window_start, count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL HAVING count(*) > 1").contains("HAVING"));
        assert!(err("SELECT * FROM nope").contains("registered source"));
        assert!(err("SELECT window_start, count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MONTH) GROUP BY ALL").contains("month"));
    }
}

