//! View compiler: turns a continuous view's SQL into an incremental plan.
//!
//! DuckDB parses the view (`json_serialize_sql`); we check the AST against the v1 operator matrix, swap the
//! stream reference for the epoch's staging table, and for windowed aggregates rewrite the select list into
//! mergeable partials. `json_deserialize_sql` turns the rewritten AST back into SQL, so every expression the
//! user wrote is executed by DuckDB exactly as written. Anything outside the matrix is rejected here, at
//! create time, never recomputed silently.
use super::SourceSchema;
use crate::sql::{describe, ident};
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Staging table the view's SQL binds against (`memory.main.in_<job>`), and that `verify` fills.
    pub input_table: String,
    /// The view as a batch query over `input_table`; used for the sink schema and differential checks.
    pub batch_sql: String,
    pub sink_columns: Vec<(String, String)>,
    /// Windowed only: `__ws`, `__we`, keys `__k<j>`, partials `__p<i>`.
    pub state_columns: Vec<(String, String)>,
    pub key_columns: Vec<String>,
    pub merge_ops: Vec<(String, MergeOp)>,
    /// Windowed only: projection from state rows to sink columns.
    pub final_select: String,
    /// Windowed only: the partial aggregates over the aggregate's input, named like `merge_ops`.
    #[serde(default)]
    pub partials: Vec<String>,
    /// `SELECT DISTINCT`: the state counts each distinct row, in `state_columns` (the view's columns and `__cnt`).
    #[serde(default)]
    pub distinct: bool,
    /// The view in pieces, which the operator graph is built from.
    #[serde(default)]
    pub query: Query,
    /// `UNION ALL` views: one branch per `SELECT`, each a plain view over one source. `source`, `input_table`
    /// and `query` are then the first branch's.
    #[serde(default)]
    pub union: Vec<Branch>,
    /// Global (non-windowed) aggregate: the operator chain pre-aggregate -> aggregate -> post.
    #[serde(default)]
    pub global: Option<GlobalPlan>,
    /// The sources the view reads that can retract rows: their staged rows carry a weight `__w`.
    #[serde(default)]
    pub retracting: Vec<String>,
}

/// One branch of a `UNION ALL` view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Branch {
    pub source: String,
    /// The staging table the branch's SQL binds against, and that `verify` fills.
    pub input_table: String,
    pub query: Query,
}

/// The view in pieces, each SQL text in the view's own names.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Query {
    /// The stream's alias in the view.
    pub alias: String,
    /// The columns the stream exposes under that alias, quoted: the source's, the window's, and `__w` when
    /// the view aggregates.
    pub columns: Vec<String>,
    /// The FROM clause, with `{input}` where the stream's rows go.
    pub from: String,
    /// Lake tables the FROM joins, read whole every epoch.
    pub lookups: Vec<String>,
    /// WHERE.
    pub filter: Option<String>,
    /// The select list over the FROM: the view's rows, or the aggregate's input (keys, arguments, `__w`).
    pub project: Vec<String>,
    /// Global aggregates: HAVING, and the select list cast to the sink's types, both over the aggregate's
    /// output `__g<j>`, `__a<i>`.
    pub having: Option<String>,
    pub finish: Vec<String>,
}

impl Query {
    /// The FROM clause over a relation holding the stream's rows.
    pub fn from_over(&self, input: &str) -> String {
        self.from.replace("{input}", &format!("(SELECT {} FROM {input})", self.columns.join(", ")))
    }

    /// The select list over the FROM and WHERE, as one query over `input`.
    pub fn project_over(&self, input: &str) -> String {
        let filter = self.filter.as_ref().map(|f| format!(" WHERE {f}")).unwrap_or_default();
        format!("SELECT {} FROM {}{filter}", self.project.join(", "), self.from_over(input))
    }
}

/// How a view's sink is maintained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    /// Rows are only ever added (stateless views, closed windows).
    #[default]
    Append,
    /// Rows replaced or removed as they change: a global aggregate's groups, or any view but a windowed one
    /// over a source that can retract.
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
    pub keys: usize,
    pub aggs: Vec<AggSpec>,
    /// Sink column holding group key j.
    pub sink_keys: Vec<String>,
    /// MIN and MAX over a source that can retract: the state also counts each group's values, one row per
    /// (group, aggregate `__i`, value `__v<i>`) with its count `__n`, so a retracted extreme gives way to the
    /// next value still present (spike S4). Group rows have `__i` NULL.
    #[serde(default)]
    pub counted_values: bool,
}

/// The state columns of value rows (`GlobalPlan::counted_values`) besides the group keys.
fn is_value_col(c: &str) -> bool {
    c == "__i" || c == "__n" || c.strip_prefix("__v").is_some_and(|i| !i.is_empty() && i.bytes().all(|b| b.is_ascii_digit()))
}

const SUPPORTED_AGGS: [&str; 6] = ["count", "count_star", "sum", "min", "max", "avg"];

impl Plan {
    pub fn is_windowed(&self) -> bool {
        self.window.is_some()
    }

    pub fn is_global(&self) -> bool {
        self.global.is_some()
    }

    /// The sources the view reads, each once, in the order the view names them.
    pub fn sources(&self) -> Vec<String> {
        let mut out = vec![self.source.clone()];
        for b in &self.union {
            if !out.contains(&b.source) {
                out.push(b.source.clone());
            }
        }
        out
    }

    /// Whether a source of the view can retract rows.
    pub fn retracts(&self) -> bool {
        !self.retracting.is_empty()
    }

    /// Whether the plan keeps a state table in `__duckwake`.
    pub fn has_state(&self) -> bool {
        self.is_windowed() || self.is_global() || self.distinct
    }

    /// Whether the view's state depends on every event of its topic, so retention's deletes break it.
    pub fn needs_whole_topic(&self) -> bool {
        self.is_global() || self.distinct
    }

    /// Over a source that can retract, every view's table but a windowed one's has rows replaced or removed.
    pub fn sink_mode(&self) -> SinkMode {
        if self.is_global() || (self.retracts() && !self.is_windowed()) { SinkMode::Upsert } else { SinkMode::Append }
    }

    pub fn kind_name(&self) -> &'static str {
        if self.is_windowed() {
            "windowed"
        } else if self.is_global() {
            "aggregate"
        } else if self.distinct {
            "distinct"
        } else if !self.union.is_empty() {
            "union"
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

    /// Weighted partials per group for one epoch's pre-aggregate rows.
    pub fn delta_sql(&self, pre: &str) -> String {
        let x = |k: usize| format!("__x{k}");
        let mut cols = self.key_cols();
        cols.push("CAST(sum(__w) AS BIGINT) AS __cnt".into());
        for (i, a) in self.aggs.iter().enumerate() {
            let k = a.arg.unwrap_or(0);
            match a.kind {
                AggKind::CountStar => cols.push(format!("CAST(sum(__w) AS BIGINT) AS __a{i}")),
                AggKind::Count => cols.push(format!("{} AS __a{i}", weighted_count(&x(k)))),
                AggKind::Sum | AggKind::Avg => {
                    cols.push(format!("sum({}) AS __a{i}_s", signed(&x(k))));
                    cols.push(format!("{} AS __a{i}_n", weighted_count(&x(k))));
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
    /// New state of every touched group: stored state plus this epoch's partials. With counted values, MIN
    /// and MAX come from `extrema` instead: the touched groups' smallest and largest values still present.
    pub fn merge_sql(&self, delta: &str, old: &str, extrema: Option<&str>, state_columns: &[(String, String)]) -> String {
        let ty = |c: &str| state_columns.iter().find(|(n, _)| n == c).map(|(_, t)| t.clone()).unwrap();
        let add = |c: &str| format!("CAST(coalesce(o.{c}, 0) + coalesce(d.{c}, 0) AS {}) AS {c}", ty(c));
        let mut cols: Vec<String> = self.key_cols().iter().map(|k| format!("d.{k}")).collect();
        cols.push(add("__cnt"));
        for (i, a) in self.aggs.iter().enumerate() {
            match (a.kind, extrema) {
                (AggKind::CountStar | AggKind::Count, _) => cols.push(add(&format!("__a{i}"))),
                (AggKind::Sum | AggKind::Avg, _) => {
                    cols.push(add(&format!("__a{i}_s")));
                    cols.push(add(&format!("__a{i}_n")));
                }
                (AggKind::Min | AggKind::Max, Some(_)) => cols.push(format!("CAST(e.__a{i} AS {}) AS __a{i}", ty(&format!("__a{i}")))),
                (AggKind::Min, None) => cols.push(format!("least(o.__a{i}, d.__a{i}) AS __a{i}")),
                (AggKind::Max, None) => cols.push(format!("greatest(o.__a{i}, d.__a{i}) AS __a{i}")),
            }
        }
        let extrema = extrema.map(|e| format!(" LEFT JOIN {e} e ON {}", self.keys_match("d", "e"))).unwrap_or_default();
        format!("SELECT {} FROM {delta} d LEFT JOIN {old} o ON {}{extrema}", cols.join(", "), self.keys_match("d", "o"))
    }

    /// The incremental aggregate: the epoch's partials per group, the touched groups' stored state (read
    /// from `t.state`) and their new state. With counted values also the epoch's value counts, the touched
    /// groups' stored ones (from `t.state_values`), the changed values' new counts, and the extremes.
    pub fn aggregate_stmts(&self, t: &GlobalTables, state_columns: &[(String, String)]) -> Vec<String> {
        let table = |name: &str, sql: String| format!("CREATE OR REPLACE TABLE {name} AS {sql}");
        let mut stmts = vec![table(&t.delta, self.delta_sql(&t.pre)), table(&t.old, self.old_sql(&t.state, &t.delta))];
        if !self.counted_values {
            stmts.push(table(&t.new_state, self.merge_sql(&t.delta, &t.old, None, state_columns)));
            return stmts;
        }
        let vcols = self.value_columns(state_columns);
        let cols = |a: &str, count: bool| {
            vcols.iter().filter(|(c, _)| count || c != "__n").map(|(c, _)| format!("{a}.{c}")).collect::<Vec<_>>().join(", ")
        };
        let list = |a: &str| cols(a, true);
        let vmatch = |a: &str, b: &str| self.values_match(a, b, state_columns);
        stmts.push(table(&t.vdelta, self.value_delta_sql(&t.pre, state_columns)));
        stmts.push(table(&t.vold, self.old_sql(&t.state_values, &t.delta)));
        stmts.push(table(
            &t.vnew,
            format!(
                "SELECT {}, CAST(coalesce(o.__n, 0) + d.__n AS BIGINT) AS __n FROM {} d LEFT JOIN {} o ON {}",
                cols("d", false),
                t.vdelta,
                t.vold,
                vmatch("d", "o")
            ),
        ));
        let extremes: Vec<String> = self
            .value_aggs()
            .map(|(i, kind)| format!("{}(__v{i}) FILTER (WHERE __i = {i}) AS __a{i}", if kind == AggKind::Min { "min" } else { "max" }))
            .collect();
        let group = if self.keys == 0 { String::new() } else { format!(" GROUP BY {}", self.key_cols().join(", ")) };
        stmts.push(table(
            &t.extrema,
            format!(
                "SELECT {} FROM (SELECT {} FROM {vnew} v WHERE v.__n > 0
                   UNION ALL SELECT {} FROM {vold} o WHERE o.__n > 0 AND NOT EXISTS (SELECT 1 FROM {vnew} v WHERE {}))
                 {group}",
                self.key_cols().into_iter().chain(extremes).collect::<Vec<_>>().join(", "),
                list("v"),
                list("o"),
                vmatch("o", "v"),
                vnew = t.vnew,
                vold = t.vold,
            ),
        ));
        stmts.push(table(&t.new_state, self.merge_sql(&t.delta, &t.old, Some(&t.extrema), state_columns)));
        stmts
    }

    /// The state columns of group rows: all but the value rows' own.
    pub fn group_columns(&self, state_columns: &[(String, String)]) -> Vec<(String, String)> {
        state_columns.iter().filter(|(c, _)| !is_value_col(c)).cloned().collect()
    }

    /// The state columns of value rows: the group keys, `__i`, `__v<i>` and `__n`.
    pub fn value_columns(&self, state_columns: &[(String, String)]) -> Vec<(String, String)> {
        let keys = self.key_cols();
        state_columns.iter().filter(|(c, _)| keys.contains(c) || is_value_col(c)).cloned().collect()
    }

    /// The MIN and MAX aggregates, by index, whose values the state counts.
    fn value_aggs(&self) -> impl Iterator<Item = (usize, AggKind)> + '_ {
        let counted = self.counted_values;
        self.aggs.iter().enumerate().filter(move |(_, a)| counted && matches!(a.kind, AggKind::Min | AggKind::Max)).map(|(i, a)| (i, a.kind))
    }

    /// One epoch's value counts: per group, MIN/MAX aggregate and non-NULL value, the sum of its weights.
    fn value_delta_sql(&self, pre: &str, state_columns: &[(String, String)]) -> String {
        let ty = |c: &str| state_columns.iter().find(|(n, _)| n == c).map(|(_, t)| t.clone()).unwrap();
        let aggs: Vec<(usize, AggKind)> = self.value_aggs().collect();
        let parts: Vec<String> = aggs
            .iter()
            .map(|&(i, _)| {
                let k = self.aggs[i].arg.unwrap();
                let mut cols = self.key_cols();
                cols.push(format!("{i} AS __i"));
                for &(j, _) in &aggs {
                    cols.push(if j == i { format!("__x{k} AS __v{j}") } else { format!("CAST(NULL AS {}) AS __v{j}", ty(&format!("__v{j}"))) });
                }
                cols.push("CAST(sum(__w) AS BIGINT) AS __n".into());
                format!("SELECT {} FROM {pre} WHERE __x{k} IS NOT NULL GROUP BY ALL HAVING sum(__w) <> 0", cols.join(", "))
            })
            .collect();
        parts.join(" UNION ALL ")
    }

    /// Value rows `a` and `b` count the same group, aggregate and value.
    fn values_match(&self, a: &str, b: &str, state_columns: &[(String, String)]) -> String {
        let mut eq = vec![self.keys_match(a, b), format!("{a}.__i = {b}.__i")];
        for (c, _) in state_columns.iter().filter(|(c, _)| c.starts_with("__v") && is_value_col(c)) {
            eq.push(format!("{a}.{c} IS NOT DISTINCT FROM {b}.{c}"));
        }
        eq.join(" AND ")
    }

    /// The aggregate's output change: the touched groups' old finalized rows retracted (weight −1) and their
    /// new ones inserted (+1). Groups at a count of 0 have no row.
    pub fn change_sql(&self, t: &GlobalTables) -> String {
        let cols = self.finalize_cols().join(", ");
        format!(
            "(SELECT {cols}, (-1)::INTEGER AS __w FROM {} WHERE __cnt > 0 UNION ALL SELECT {cols}, 1::INTEGER AS __w FROM {} WHERE __cnt > 0)",
            t.old, t.new_state
        )
    }

    /// The aggregate's writes at commit: replace the touched groups' state.
    pub fn commit_state_sql(&self, t: &GlobalTables, state_columns: &[(String, String)]) -> Vec<String> {
        let names = |cols: Vec<(String, String)>| cols.iter().map(|(c, _)| ident(c)).collect::<Vec<_>>().join(", ");
        let groups = if self.counted_values { "s.__i IS NULL AND " } else { "" };
        let mut stmts = vec![
            format!("DELETE FROM {} s WHERE {groups}EXISTS (SELECT 1 FROM {} d WHERE {})", t.state, t.delta, self.keys_match("s", "d")),
            format!("INSERT INTO {} ({c}) SELECT {c} FROM {} WHERE __cnt <> 0", t.state, t.new_state, c = names(self.group_columns(state_columns))),
        ];
        if self.counted_values {
            // The changed values' counts replaced; a value whose count reached 0 is gone.
            stmts.push(format!(
                "DELETE FROM {} s WHERE s.__i IS NOT NULL AND EXISTS (SELECT 1 FROM {} v WHERE {})",
                t.state,
                t.vnew,
                self.values_match("s", "v", state_columns)
            ));
            stmts.push(format!(
                "INSERT INTO {} ({c}) SELECT {c} FROM {} WHERE __n <> 0",
                t.state,
                t.vnew,
                c = names(self.value_columns(state_columns))
            ));
        }
        stmts
    }

    /// The value rows as of the previous epoch while it may still be committing, like `overlay_sql`: its new
    /// counts for the values it changed (those it emptied left out), the stored ones for every other value.
    pub fn values_overlay_sql(&self, values: &str, prev_vnew: &str, state_columns: &[(String, String)]) -> String {
        let vcols = self.value_columns(state_columns);
        let cast: Vec<String> = vcols.iter().map(|(c, t)| format!("CAST({c} AS {t}) AS {c}")).collect();
        let cols: Vec<String> = vcols.iter().map(|(c, _)| c.clone()).collect();
        format!(
            "(SELECT {} FROM {prev_vnew} WHERE __n <> 0
              UNION ALL SELECT {} FROM {values} s WHERE NOT EXISTS (SELECT 1 FROM {prev_vnew} p WHERE {}))",
            cast.join(", "),
            cols.join(", "),
            self.values_match("s", "p", state_columns)
        )
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

    /// Finalized values (`__g<j>`, `__a<i>`) of a state-shaped table's columns.
    fn finalize_cols(&self) -> Vec<String> {
        let mut cols = self.key_cols();
        for (i, a) in self.aggs.iter().enumerate() {
            cols.push(match a.kind {
                AggKind::CountStar | AggKind::Count | AggKind::Min | AggKind::Max => format!("__a{i}"),
                AggKind::Sum => format!("CASE WHEN __a{i}_n = 0 THEN NULL ELSE __a{i}_s END AS __a{i}"),
                AggKind::Avg => format!("CASE WHEN __a{i}_n = 0 THEN NULL ELSE CAST(__a{i}_s AS DOUBLE) / __a{i}_n END AS __a{i}"),
            });
        }
        cols
    }
}

/// Table names one epoch of a global aggregate uses. Staging tables live in `memory.main`; the ones the commit
/// reads (`delta`, `new_state`, `changes`, `changed`) come in two sets, by epoch parity, so the next epoch can
/// be staged while this one commits.
#[derive(Debug, Clone)]
pub struct GlobalTables {
    pub pre: String,
    pub delta: String,
    pub old: String,
    pub new_state: String,
    /// The sink's input change, consolidated, and the keys of the groups it changes.
    pub changes: String,
    pub changed: String,
    /// The DuckLake state table, written at commit. While staging it is what `old` reads: the table itself, or
    /// `GlobalPlan::overlay_sql` over it when the previous epoch may not have committed yet.
    pub state: String,
    /// With counted values: the epoch's value counts, the touched groups' stored ones, the changed values' new
    /// counts and the touched groups' extremes; and while staging, what `vold` reads (the state's value rows,
    /// or `GlobalPlan::values_overlay_sql` over them).
    pub vdelta: String,
    pub vold: String,
    pub vnew: String,
    pub extrema: String,
    pub state_values: String,
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
            changes: set("changes"),
            changed: set("changed"),
            vdelta: set("gvdelta"),
            vold: mem("gvold"),
            vnew: set("gvnew"),
            extrema: mem("gextrema"),
            state_values: state.clone(),
            state,
            sink,
            sink_columns: plan.sink_columns.iter().map(|(c, _)| c.clone()).collect(),
        }
    }
}

/// Compiles `sql` for job `job`. Creates the (empty) staging table so DuckDB can bind and DESCRIBE the
/// rewritten queries, which also validates lookup-join tables and column names.
pub fn compile<S: SourceSchema>(conn: &Connection, job: &str, sql: &str, sources: &HashMap<String, S>) -> Result<Plan> {
    let ast = serialize(conn, sql)?;
    let stmts = ast["statements"].as_array().ok_or_else(|| anyhow!("no statements"))?;
    if stmts.len() != 1 {
        bail!("a continuous view must be exactly one SELECT statement");
    }
    let mut stmt = stmts[0].clone();
    if stmt["node"]["type"] == "SET_OPERATION_NODE" {
        return compile_union(conn, job, &stmt, sources);
    }
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
            if time_col != &src.event_time() {
                bail!("windows must use the source's event_time column {:?}, not {time_col:?}", src.event_time());
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
    *stream_slot(&mut node["from_table"]).ok_or_else(|| anyhow!("lost stream reference"))? = table_ref.clone();

    // The view in pieces: the FROM with a placeholder where the stream goes, the WHERE, the stream's columns.
    let mut columns: Vec<String> = src.columns().iter().map(|(c, _)| ident(c)).collect();
    if window.is_some() {
        columns.extend(["window_start".into(), "window_end".into()]);
    }
    let query = Query {
        alias: stream.alias.clone(),
        columns,
        from: from_template(conn, &node["from_table"], &table_ref)?,
        lookups: lookup_tables(&node["from_table"], &input_table),
        filter: (!node["where_clause"].is_null()).then(|| expr_sql(conn, &node["where_clause"])).transpose()?,
        ..Query::default()
    };

    // Staging table with the source columns (+ window columns) so the rewritten SQL binds.
    let mut cols: Vec<String> = src.columns().iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
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
        batch_sql,
        sink_columns,
        state_columns: vec![],
        key_columns: vec![],
        merge_ops: vec![],
        final_select: String::new(),
        partials: vec![],
        distinct: false,
        query,
        union: vec![],
        global: None,
        retracting: if src.append_only() { vec![] } else { vec![stream.source.clone()] },
    };

    let node = &mut stmt["node"];
    let has_group = node["aggregate_handling"] == "FORCE_AGGREGATES"
        || node["group_expressions"].as_array().is_some_and(|g| !g.is_empty());
    let has_agg = node["select_list"].as_array().unwrap().iter().any(|e| contains_fn(e, &aggs));
    plan.distinct = node["modifiers"].as_array().is_some_and(|m| m.iter().any(|m| m["type"] == "DISTINCT_MODIFIER"));
    if plan.distinct && (plan.window.is_some() || has_group || has_agg || !node["having"].is_null()) {
        bail!("SELECT DISTINCT is supported over plain views; over windows and aggregates it is not supported yet");
    }
    if plan.window.is_none() {
        if has_group || has_agg || !node["having"].is_null() {
            compile_global(conn, job, &mut stmt, &stream.alias, &aggs, &mut plan)?;
            return Ok(plan);
        }
        // The view's rows, each with its weight; a `*` over the stream leaves the weight out.
        let mut list = node["select_list"].as_array().unwrap().clone();
        for e in list.iter_mut().filter(|e| e["class"] == "STAR") {
            let rel = e["relation_name"].as_str().unwrap_or("");
            if rel.is_empty() || rel.eq_ignore_ascii_case(&stream.alias) {
                e["exclude_list"].as_array_mut().ok_or_else(|| anyhow!("unexpected star expression"))?.push(json!("__w"));
            }
        }
        list.push(weight_ref(conn, &stream.alias)?);
        conn.execute_batch(&format!("ALTER TABLE {} ADD COLUMN __w INTEGER", plan.input_table))?;
        plan.query.columns.push("__w".into());
        plan.query.project = vec![list_sql(conn, &Value::Array(list))?];
        if plan.distinct {
            plan.state_columns = plan.sink_columns.iter().cloned().chain([("__cnt".to_string(), "BIGINT".to_string())]).collect();
        }
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

/// `SELECT ... UNION ALL SELECT ...`: each branch compiled as a view of its own (staging table
/// `in_<job>_<k>`), and the view their concatenation. Branches must be plain views for now.
fn compile_union<S: SourceSchema>(conn: &Connection, job: &str, stmt: &Value, sources: &HashMap<String, S>) -> Result<Plan> {
    fn branches(node: &Value, out: &mut Vec<Value>) -> Result<()> {
        if node["type"] != "SET_OPERATION_NODE" {
            out.push(node.clone());
            return Ok(());
        }
        if node["setop_type"] != "UNION" || node["setop_all"] != true {
            bail!("of the set operations only UNION ALL is supported (UNION, EXCEPT and INTERSECT are not yet)");
        }
        if node["modifiers"].as_array().is_some_and(|m| !m.is_empty()) {
            bail!("ORDER BY and LIMIT are not supported in continuous views");
        }
        node["children"].as_array().into_iter().flatten().try_for_each(|c| branches(c, out))
    }
    let mut nodes = Vec::new();
    branches(&stmt["node"], &mut nodes)?;
    let (mut union, mut retracting) = (Vec::new(), Vec::new());
    for (k, node) in nodes.into_iter().enumerate() {
        let mut branch = stmt.clone();
        branch["node"] = node;
        let sql = deserialize(conn, &branch)?;
        let p = compile(conn, &format!("{job}_{k}"), &sql, sources).with_context(|| format!("branch {} of the UNION ALL", k + 1))?;
        if p.window.is_some() || p.global.is_some() || p.distinct || !p.union.is_empty() {
            bail!("each branch of a UNION ALL must be a plain view (no windows, aggregates or DISTINCT) for now");
        }
        for s in p.retracting {
            if !retracting.contains(&s) {
                retracting.push(s);
            }
        }
        union.push((p.batch_sql, Branch { source: p.source, input_table: p.input_table, query: p.query }));
    }
    let batch_sql = union.iter().map(|(b, _)| format!("({b})")).collect::<Vec<_>>().join(" UNION ALL ");
    let sink_columns = describe(conn, &batch_sql).context("binding the UNION ALL (do the branches' columns line up?)")?;
    let union: Vec<Branch> = union.into_iter().map(|(_, b)| b).collect();
    Ok(Plan {
        source: union[0].source.clone(),
        window: None,
        input_table: union[0].input_table.clone(),
        batch_sql,
        sink_columns,
        state_columns: vec![],
        key_columns: vec![],
        merge_ops: vec![],
        final_select: String::new(),
        partials: vec![],
        distinct: false,
        query: union[0].query.clone(),
        union,
        global: None,
        retracting,
    })
}

fn compile_windowed(
    conn: &Connection,
    stmt: &mut Value,
    alias: &str,
    aggs: &HashSet<String>,
    plan: &mut Plan,
) -> Result<()> {
    let node = &stmt["node"];
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
    // The aggregate's input: the keys `__k<j>`, the aggregates' arguments `__x<k>`, the window, the weight.
    let weighted = plan.retracts();
    let mut project = Vec::new();
    let mut args: Vec<Value> = Vec::new();
    let mut finals = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let (sink_name, sink_type) = &plan.sink_columns[i];
        let is_agg = item["class"] == "FUNCTION" && aggs.contains(item["function_name"].as_str().unwrap_or(""));
        if !is_agg {
            if contains_fn(item, aggs) {
                bail!("aggregates must be top-level select items (column {sink_name:?}); nested expressions over aggregates are not supported");
            }
            let k = format!("__k{}", plan.key_columns.len());
            let mut e = item.clone();
            e["alias"] = json!(k);
            project.push(e);
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
        let fn_args: Vec<Value> = item["arguments"].as_array().cloned().unwrap_or_default().iter()
            .map(|a| a.get("expression").cloned().unwrap_or_else(|| a.clone())).collect();
        if fn_args.iter().any(|a| contains_fn(a, aggs)) {
            bail!("nested aggregates are not supported ({fname})");
        }
        let x = match fname.as_str() {
            "count_star" => String::new(),
            _ => {
                let [a] = fn_args.as_slice() else { bail!("{fname}() takes exactly one argument here") };
                let key = norm(a);
                let k = args.iter().position(|x| norm(x) == key).unwrap_or_else(|| {
                    args.push(a.clone());
                    args.len() - 1
                });
                format!("__x{k}")
            }
        };
        let p = format!("__p{i}");
        let mut partial = |name: &str, sql: String, op: MergeOp| {
            plan.partials.push(format!("{sql} AS {name}"));
            plan.merge_ops.push((name.to_string(), op));
        };
        // Over a source that can retract, the partials sum weights (as Feldera's ExpandAggregates does when the
        // input is not append-only); over Kafka they count rows, which is the same there and cheaper. A sum
        // keeps its weighted non-NULL count too, so that it is NULL again once every value is retracted.
        let finalize = match (fname.as_str(), weighted) {
            ("count_star", false) => {
                partial(&p, "count(*)".into(), MergeOp::Add);
                ident(&p)
            }
            ("count_star", true) => {
                partial(&p, "CAST(sum(__w) AS BIGINT)".into(), MergeOp::Add);
                ident(&p)
            }
            ("count", false) => {
                partial(&p, format!("count({x})"), MergeOp::Add);
                ident(&p)
            }
            ("count", true) => {
                partial(&p, weighted_count(&x), MergeOp::Add);
                ident(&p)
            }
            ("sum", false) => {
                partial(&p, format!("sum({x})"), MergeOp::AddNullable);
                ident(&p)
            }
            ("sum", true) => {
                let (s, c) = (format!("{p}s"), format!("{p}c"));
                partial(&s, format!("sum({})", signed(&x)), MergeOp::AddNullable);
                partial(&c, weighted_count(&x), MergeOp::Add);
                format!("CASE WHEN {} = 0 THEN NULL ELSE {} END", ident(&c), ident(&s))
            }
            ("min" | "max", true) => {
                bail!("{fname}() in a window over a source that can retract needs a count per value: not supported yet")
            }
            ("min", false) => {
                partial(&p, format!("min({x})"), MergeOp::Min);
                ident(&p)
            }
            ("max", false) => {
                partial(&p, format!("max({x})"), MergeOp::Max);
                ident(&p)
            }
            ("avg", _) => {
                let (s, c) = (format!("{p}s"), format!("{p}c"));
                if weighted {
                    partial(&s, format!("sum({})", signed(&x)), MergeOp::AddNullable);
                    partial(&c, weighted_count(&x), MergeOp::Add);
                } else {
                    partial(&s, format!("sum({x})"), MergeOp::AddNullable);
                    partial(&c, format!("count({x})"), MergeOp::Add);
                }
                format!("CAST({} AS DOUBLE) / NULLIF({}, 0)", ident(&s), ident(&c))
            }
            _ => unreachable!(),
        };
        finals.push(format!("CAST({finalize} AS {sink_type}) AS {}", ident(sink_name)));
    }
    // Each window and group's total weight: one whose rows are all retracted is not emitted.
    if weighted {
        plan.partials.push(format!("CAST(sum(__w) AS BIGINT) AS {WINDOW_WEIGHT}"));
        plan.merge_ops.push((WINDOW_WEIGHT.to_string(), MergeOp::Add));
    }
    // A GROUP BY key left out of the select list would merge its groups into one state row.
    if !group_all {
        for g in node["group_expressions"].as_array().into_iter().flatten().filter(|g| !is_window_col(g)) {
            let g = resolve_group(g, &items);
            if !items.iter().any(|e| norm(&stripped(e)) == norm(&g)) {
                bail!("select every GROUP BY key so each group keeps its own window state");
            }
        }
    }
    for (k, a) in args.iter().enumerate() {
        let mut a = a.clone();
        a["alias"] = json!(format!("__x{k}"));
        project.push(a);
    }
    let (ws_ref, we_ref) = window_refs(conn, alias)?;
    project.extend([ws_ref, we_ref, weight_ref(conn, alias)?]);
    conn.execute_batch(&format!("ALTER TABLE {} ADD COLUMN __w INTEGER", plan.input_table))?;
    plan.query.columns.push("__w".into());
    plan.query.project = vec![list_sql(conn, &Value::Array(project))?];

    // State column order: window, keys, partials.
    let partial_cols = describe(conn, &format!("SELECT * FROM ({})", windowed_partials_sql(plan, &format!("({})", plan.query.project_over(&plan.input_table)))))
        .context("binding the partial aggregate")?;
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

/// The state column holding a window and group's total weight, when the view's input can retract.
pub const WINDOW_WEIGHT: &str = "__pw";

/// `x` times the row's weight; written out for ±1 so an integer `x` keeps its type and cannot overflow.
fn signed(x: &str) -> String {
    format!("CASE WHEN __w = 1 THEN {x} WHEN __w = -1 THEN -{x} ELSE {x} * __w END")
}

/// The weighted count of `x`'s non-NULL values.
fn weighted_count(x: &str) -> String {
    format!("CAST(sum(CASE WHEN {x} IS NOT NULL THEN __w ELSE 0 END) AS BIGINT)")
}

/// A windowed aggregate's partials per window and key over `input`, a relation of its input rows.
pub fn windowed_partials_sql(plan: &Plan, input: &str) -> String {
    let keys: Vec<String> = ["__ws".to_string(), "__we".to_string()].into_iter().chain(plan.key_columns.iter().map(|k| ident(k))).collect();
    let keys = keys.join(", ");
    format!("SELECT {keys}, {} FROM {input} GROUP BY {keys}", plan.partials.join(", "))
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
    // Group keys: GROUP BY ALL = the select items without aggregates.
    let group_all = node["aggregate_handling"] == "FORCE_AGGREGATES";
    let mut groups: Vec<Value> = Vec::new();
    if group_all {
        groups.extend(items.iter().filter(|e| !contains_fn(e, aggs)).map(stripped));
    } else {
        groups.extend(node["group_expressions"].as_array().into_iter().flatten().map(|g| resolve_group(g, &items)));
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

    // Operator 1, the aggregate's input: the view's FROM/WHERE producing keys, arguments and the weight.
    conn.execute_batch(&format!("ALTER TABLE {} ADD COLUMN __w INTEGER", plan.input_table))?;
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
    list.push(weight_ref(conn, alias)?);
    plan.query.columns.push("__w".into());
    plan.query.project = vec![list_sql(conn, &Value::Array(list))?];
    let pre_sql = plan.query.project_over(&plan.input_table);
    let pre_cols = describe(conn, &pre_sql).context("binding the pre-aggregate")?;
    let col_type = |name: &str| pre_cols.iter().find(|(c, _)| c == name).map(|(_, t)| t.clone()).unwrap();

    // Operator 2, aggregate: state columns, and the finalized values its output carries.
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

    // Operator 3, post: HAVING and the select list over the finalized values, cast to the sink's types.
    if !having.is_null() {
        plan.query.having = Some(expr_sql(conn, &having)?);
    }
    for (e, (name, ty)) in post_items.iter().zip(&plan.sink_columns) {
        plan.query.finish.push(format!("CAST(({}) AS {ty}) AS {}", expr_sql(conn, e)?, ident(name)));
    }
    // Bind it against a table of the aggregate's output: a column that is not a key fails here.
    let agg_table = format!("memory.main.{}", ident(&format!("agg_{job}")));
    conn.execute_batch(&format!("CREATE OR REPLACE TABLE {agg_table} ({})", agg_cols.join(", ")))?;
    let having_sql = plan.query.having.as_ref().map(|h| format!(" WHERE {h}")).unwrap_or_default();
    describe(conn, &format!("SELECT {} FROM {agg_table}{having_sql}", plan.query.finish.join(", ")))
        .context("binding the select list over the aggregates (every non-aggregate column must be a GROUP BY key)")?;

    // MIN and MAX over a source that can retract: value rows in the same state table.
    let counted_values = plan.retracts() && specs.iter().any(|(_, a)| matches!(a.kind, AggKind::Min | AggKind::Max));
    if counted_values {
        state.push(("__i".into(), "INTEGER".into()));
        for (i, (_, a)) in specs.iter().enumerate().filter(|(_, (_, a))| matches!(a.kind, AggKind::Min | AggKind::Max)) {
            state.push((format!("__v{i}"), col_type(&format!("__x{}", a.arg.unwrap()))));
        }
        state.push(("__n".into(), "BIGINT".into()));
    }
    plan.state_columns = state;
    let aggs = specs.into_iter().map(|(_, a)| a).collect();
    plan.global = Some(GlobalPlan { keys: groups.len(), aggs, sink_keys, counted_values });
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
    // Plain DISTINCT is supported (see `compile`); DISTINCT ON is not.
    let plain_distinct = |m: &Value| m["type"] == "DISTINCT_MODIFIER" && m["distinct_on_targets"].as_array().is_none_or(|t| t.is_empty());
    if let Some(m) = node["modifiers"].as_array().and_then(|m| m.iter().find(|m| !plain_distinct(m))) {
        let what = match m["type"].as_str().unwrap_or("") {
            "ORDER_MODIFIER" => "ORDER BY",
            "DISTINCT_MODIFIER" => "DISTINCT ON",
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

fn rewrite_from<S>(t: &mut Value, sources: &HashMap<String, S>, found: &mut Vec<StreamRef>) -> Result<()> {
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

/// A select list of the view's parse tree as SQL text, aliases included.
fn list_sql(conn: &Connection, list: &Value) -> Result<String> {
    let mut stmt = serialize(conn, "SELECT 1")?["statements"][0].clone();
    stmt["node"]["select_list"] = list.clone();
    let sql = deserialize(conn, &stmt)?;
    Ok(sql.strip_prefix("SELECT ").ok_or_else(|| anyhow!("unexpected SQL {sql:?}"))?.to_string())
}

/// An expression of the view's parse tree as SQL text.
fn expr_sql(conn: &Connection, e: &Value) -> Result<String> {
    list_sql(conn, &Value::Array(vec![stripped(e)]))
}

/// The view's FROM clause as SQL text, with `{input}` where `stream`, the stream's table reference in it, is.
fn from_template(conn: &Connection, from: &Value, stream: &Value) -> Result<String> {
    let text = |t: &Value| -> Result<String> {
        let mut stmt = serialize(conn, "SELECT 1 FROM t")?["statements"][0].clone();
        stmt["node"]["from_table"] = t.clone();
        let sql = deserialize(conn, &stmt)?;
        Ok(sql.strip_prefix("SELECT 1 FROM ").ok_or_else(|| anyhow!("unexpected SQL {sql:?}"))?.to_string())
    };
    let (whole, stream) = (text(from)?, text(stream)?);
    let name = stream.rsplit_once(" AS ").map_or(stream.as_str(), |(n, _)| n);
    if !whole.contains(name) {
        bail!("could not find the stream {name} in the FROM clause {whole}");
    }
    Ok(whole.replacen(name, "{input}", 1))
}

/// The lake tables a FROM clause reads besides the stream (`input_table`).
fn lookup_tables(from: &Value, input_table: &str) -> Vec<String> {
    let mut out = Vec::new();
    walk(from, &mut |v| {
        if v["type"] == "BASE_TABLE" {
            let parts: Vec<&str> = ["catalog_name", "schema_name", "table_name"].iter().filter_map(|k| v[*k].as_str()).filter(|p| !p.is_empty()).collect();
            let name = parts.join(".");
            if !input_table.replace('"', "").ends_with(&name) {
                out.push(name);
            }
        }
    });
    out
}

/// `<alias>.__w AS __w`: the stream's weight, as a select item.
fn weight_ref(conn: &Connection, alias: &str) -> Result<Value> {
    Ok(serialize(conn, &format!("SELECT {}.__w AS __w", ident(alias)))?["statements"][0]["node"]["select_list"][0].clone())
}

fn stripped(e: &Value) -> Value {
    let mut e = e.clone();
    e["alias"] = json!("");
    e
}

/// A GROUP BY key as the expression it groups by: DuckDB lets it name a select item by alias or position.
fn resolve_group(g: &Value, items: &[Value]) -> Value {
    let by_alias = (g["class"] == "COLUMN_REF" && g["column_names"].as_array().is_some_and(|n| n.len() == 1))
        .then(|| g["column_names"][0].as_str().unwrap_or(""))
        .and_then(|name| items.iter().find(|e| e["alias"] == name));
    let by_position = (g["class"] == "CONSTANT")
        .then(|| g["literal"]["text"].as_str().and_then(|t| t.parse::<usize>().ok()))
        .flatten()
        .and_then(|i| i.checked_sub(1))
        .and_then(|i| items.get(i));
    stripped(by_alias.or(by_position).unwrap_or(g))
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
    use crate::tests::setup;

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
        assert!(p.query.from.contains("{input}"), "{}", p.query.from);
        // The partial query must run.
        let input = format!("({})", p.query.project_over(&p.input_table));
        c.execute_batch(&format!("CREATE TABLE part AS {}", windowed_partials_sql(&p, &input))).unwrap();
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
        assert!(err("SELECT DISTINCT ON (game_id) * FROM clicks").contains("DISTINCT ON"));
        assert!(err("SELECT DISTINCT game_id, count(*) FROM clicks GROUP BY ALL").contains("DISTINCT"));
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

