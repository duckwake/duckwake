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
    /// A list of values (quantiles): the lists concatenated.
    Concat,
    /// A list of distinct values (`count(DISTINCT)`, `approx_count_distinct`): their union.
    Union,
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
    /// The inputs the view reads through a DuckLake change feed (lake tables and views), not from Kafka.
    #[serde(default)]
    pub change_feeds: Vec<String>,
    /// Windowed views with HAVING: which closed windows are emitted.
    #[serde(default)]
    pub window_having: Option<WindowHaving>,
    /// A view that reads its Kafka source as upserts: one row per key, replaced by its newer messages and
    /// removed by a tombstone (`UpsertSpec`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upsert: Option<UpsertSpec>,
}

/// How a view reads its Kafka source as upserts (`upsert_key := [...]` on `create_view`).
///
/// Each key has a current row. Read in order, a message with a value replaces it, and a message without one (a
/// tombstone) deletes it; with `version`, a message replaces the row only when its version is at least the row's
/// (NULL lowest), so a message older than the row is ignored. A key's messages must stay in one partition, as
/// Kafka's default partitioner keeps them: across partitions their order is not defined. A deleted key keeps
/// nothing, so a message older than the deleted row brings the key back.
///
/// As a set: a key's current row is the best (`order`) of its messages after its last tombstone.
///
/// A key column comes from the message key: a JSON object's field of that name, or for a single key column the
/// whole key (a JSON scalar or plain text); when the message has no key, or it does not decode, from the value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpsertSpec {
    pub key: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl UpsertSpec {
    /// The view's key columns, quoted.
    pub fn keys(&self) -> Vec<String> {
        self.key.iter().map(|k| ident(k)).collect()
    }

    /// Null-safe equality of the keys of `a` and `b`.
    pub fn keys_match(&self, a: &str, b: &str) -> String {
        self.keys().iter().map(|k| format!("{a}.{k} IS NOT DISTINCT FROM {b}.{k}")).collect::<Vec<_>>().join(" AND ")
    }

    /// One key column's value: from the message key `_key`, else from the value's column.
    fn key_expr(&self, column: &str, ty: &str) -> String {
        let c = ident(column);
        let field = format!("TRY_CAST(json_extract_string(_key, {}) AS {ty})", sql_lit(&format!("$.\"{column}\"")));
        let whole = if self.key.len() == 1 {
            format!("TRY_CAST(json_extract_string(_key, '$') AS {ty})")
        } else {
            "NULL".to_string()
        };
        let raw = if self.key.len() == 1 { format!("TRY_CAST(_key AS {ty})") } else { "NULL".to_string() };
        // json_type raises on text that is not JSON, and CASE is the only short circuit DuckDB keeps.
        format!(
            "coalesce(CASE WHEN json_valid(_key) THEN CASE WHEN json_type(_key) = 'OBJECT' THEN {field} ELSE {whole} END ELSE {raw} END, {c}) AS {c}"
        )
    }

    /// The messages of `input` (with `_partition`, `_offset`, `_key`, `__del` and the source's columns) as rows
    /// with their key columns, position `__p`, `__o` and `__del` (a tombstone).
    pub fn messages_sql(&self, columns: &[(String, String)], input: &str) -> String {
        let cols: Vec<String> = columns
            .iter()
            .map(|(c, t)| if self.key.contains(c) { self.key_expr(c, t) } else { ident(c) })
            .collect();
        format!("SELECT {}, _partition AS __p, _offset AS __o, __del FROM {input}", cols.join(", "))
    }

    /// The order that ranks a key's messages, best first: the highest version (NULL lowest), then the later
    /// message; without a version the later message.
    pub fn order(&self) -> String {
        match &self.version {
            Some(v) => format!("{} DESC NULLS LAST, __p DESC, __o DESC", ident(v)),
            None => "__p DESC, __o DESC".into(),
        }
    }

    /// Whether `e`, a later message of the key, replaces its current row `s`: always without a version, else
    /// when its version is at least the row's (NULL lowest).
    pub fn newer(&self, e: &str, s: &str) -> String {
        match &self.version {
            Some(v) => {
                let v = ident(v);
                format!("coalesce({e}.{v} >= {s}.{v}, {s}.{v} IS NULL)")
            }
            None => "TRUE".into(),
        }
    }

    /// Of `messages` (`messages_sql`), one row per key: the best of its messages after its last tombstone, with
    /// `__del` true when there is none (the key ends deleted), and `__cut` true when the messages hold a
    /// tombstone (whatever row the key had before them is gone).
    pub fn best_sql(&self, messages: &str) -> String {
        let keys = self.keys().join(", ");
        format!(
            "SELECT * EXCLUDE (__n, __tn), __tn IS NOT NULL AS __cut FROM (
               SELECT *, max(CASE WHEN __del THEN __n END) OVER (PARTITION BY {keys}) AS __tn
               FROM (SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY __p, __o) AS __n FROM {messages}))
             WHERE __n >= coalesce(__tn, 0)
             QUALIFY row_number() OVER (PARTITION BY {keys} ORDER BY __del, {order}) = 1",
            order = self.order(),
        )
    }

    /// The view's input as one batch: each key's current row after every message in `input`. What `verify` and
    /// the tests compare the view's table against.
    pub fn batch_sql(&self, columns: &[(String, String)], input: &str) -> String {
        let cols: Vec<String> = columns.iter().map(|(c, _)| ident(c)).collect();
        format!(
            "SELECT {} FROM ({}) WHERE NOT __del",
            cols.join(", "),
            self.best_sql(&format!("({})", self.messages_sql(columns, input)))
        )
    }
}

/// `s` as a SQL string literal.
fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// HAVING in a windowed view, applied as windows close: `condition` over a state row with `columns` added
/// (`<finalized aggregate> AS __h<j>`, the aggregates HAVING reads) and keys as `__k<j>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowHaving {
    pub columns: Vec<String>,
    pub condition: String,
}

impl WindowHaving {
    /// The rows of `state` (a state table or query) with the columns HAVING reads.
    pub fn over(&self, state: &str) -> String {
        format!("(SELECT *, {} FROM {state})", self.columns.join(", "))
    }
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
    /// Variance and standard deviation: from the weighted count, sum and sum of squares, so they are
    /// invertible like `avg`.
    VarSamp,
    VarPop,
    StddevSamp,
    StddevPop,
    /// `count(DISTINCT x)`: the state counts each group's values (`GlobalPlan::counted_values`).
    CountDistinct,
    /// `approx_count_distinct(x)`: kept like `count(DISTINCT x)`, and DuckDB's estimate is taken over the values
    /// still present, which equals the batch query's (the HyperLogLog sketch depends only on the set of values).
    ApproxCountDistinct,
    /// `quantile_cont(x, q)` (and `median(x)`, which is `quantile_cont(x, 0.5)`), `quantile_disc(x, q)` (and
    /// `quantile(x, q)`): exact, from every value with its count. `AggSpec::param` holds `q`.
    QuantileCont,
    QuantileDisc,
    /// The two-argument statistics, `f(y, x)` over the rows where both are non-NULL: from the weighted count and
    /// the sums of y, x, y², x² and xy, so they are invertible like the variances (`paired_finalize`).
    CovarPop,
    CovarSamp,
    Corr,
    RegrCount,
    RegrAvgx,
    RegrAvgy,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrSlope,
    RegrIntercept,
    RegrR2,
}

impl AggKind {
    /// Finalized from the counted values rather than from the group row's own partials.
    fn counts_values(self) -> bool {
        self.always_counts_values() || matches!(self, AggKind::Min | AggKind::Max)
    }

    /// Kept as counted values in every GROUP BY view, not only over a source that can retract.
    fn always_counts_values(self) -> bool {
        matches!(self, AggKind::CountDistinct | AggKind::ApproxCountDistinct | AggKind::QuantileCont | AggKind::QuantileDisc)
    }

    /// Takes two arguments, `f(y, x)`, and keeps the paired sums.
    fn is_paired(self) -> bool {
        matches!(
            self,
            AggKind::CovarPop
                | AggKind::CovarSamp
                | AggKind::Corr
                | AggKind::RegrCount
                | AggKind::RegrAvgx
                | AggKind::RegrAvgy
                | AggKind::RegrSxx
                | AggKind::RegrSyy
                | AggKind::RegrSxy
                | AggKind::RegrSlope
                | AggKind::RegrIntercept
                | AggKind::RegrR2
        )
    }

    fn is_moments(self) -> bool {
        matches!(self, AggKind::VarSamp | AggKind::VarPop | AggKind::StddevSamp | AggKind::StddevPop)
    }

    /// The kind of a canonical name from `canonical_agg`; `count` is `Count` (DISTINCT is decided by the caller).
    fn of(canonical: &str) -> AggKind {
        match canonical {
            "count_star" => AggKind::CountStar,
            "count" => AggKind::Count,
            "sum" => AggKind::Sum,
            "avg" => AggKind::Avg,
            "min" => AggKind::Min,
            "max" => AggKind::Max,
            "var_samp" => AggKind::VarSamp,
            "var_pop" => AggKind::VarPop,
            "stddev_samp" => AggKind::StddevSamp,
            "stddev_pop" => AggKind::StddevPop,
            "approx_count_distinct" => AggKind::ApproxCountDistinct,
            "quantile_cont" => AggKind::QuantileCont,
            "quantile_disc" => AggKind::QuantileDisc,
            "covar_pop" => AggKind::CovarPop,
            "covar_samp" => AggKind::CovarSamp,
            "corr" => AggKind::Corr,
            "regr_count" => AggKind::RegrCount,
            "regr_avgx" => AggKind::RegrAvgx,
            "regr_avgy" => AggKind::RegrAvgy,
            "regr_sxx" => AggKind::RegrSxx,
            "regr_syy" => AggKind::RegrSyy,
            "regr_sxy" => AggKind::RegrSxy,
            "regr_slope" => AggKind::RegrSlope,
            "regr_intercept" => AggKind::RegrIntercept,
            "regr_r2" => AggKind::RegrR2,
            other => unreachable!("not a canonical aggregate: {other}"),
        }
    }

    /// The name of the DuckDB aggregate that finalizes a list of the counted values.
    fn list_aggregate(self) -> &'static str {
        match self {
            AggKind::ApproxCountDistinct => "approx_count_distinct",
            AggKind::QuantileCont => "quantile_cont",
            AggKind::QuantileDisc => "quantile_disc",
            other => unreachable!("{other:?} is not finalized over a list"),
        }
    }
}

/// The aggregate DuckWake runs for `fname`: synonyms map to one name, and `bool_and`/`bool_or` are MIN and MAX
/// of a BOOLEAN (false < true), which gives them retractions and windows for free. `median` is
/// `quantile_cont(x, 0.5)` and `quantile` is `quantile_disc`, as in DuckDB. None when unsupported.
fn canonical_agg(fname: &str) -> Option<&'static str> {
    Some(match fname {
        "count_star" => "count_star",
        "count" => "count",
        "sum" => "sum",
        "avg" | "mean" => "avg",
        "min" | "bool_and" => "min",
        "max" | "bool_or" => "max",
        "var_samp" | "variance" => "var_samp",
        "var_pop" => "var_pop",
        "stddev_samp" | "stddev" => "stddev_samp",
        "stddev_pop" => "stddev_pop",
        "approx_count_distinct" => "approx_count_distinct",
        "quantile_cont" | "median" => "quantile_cont",
        "quantile_disc" | "quantile" => "quantile_disc",
        "covar_pop" => "covar_pop",
        "covar_samp" => "covar_samp",
        "corr" => "corr",
        "regr_count" => "regr_count",
        "regr_avgx" => "regr_avgx",
        "regr_avgy" => "regr_avgy",
        "regr_sxx" => "regr_sxx",
        "regr_syy" => "regr_syy",
        "regr_sxy" => "regr_sxy",
        "regr_slope" => "regr_slope",
        "regr_intercept" => "regr_intercept",
        "regr_r2" => "regr_r2",
        _ => return None,
    })
}

const SUPPORTED_LIST: &str = "count, count(DISTINCT), approx_count_distinct, sum, avg, min, max, bool_and, bool_or, \
     var_samp, var_pop, variance, stddev_samp, stddev_pop, stddev, median, quantile_cont, quantile_disc, quantile, \
     covar_pop, covar_samp, corr, regr_count, regr_avgx, regr_avgy, regr_sxx, regr_syy, regr_sxy, regr_slope, \
     regr_intercept, regr_r2";

/// Why `arg_min`/`arg_max` and their synonyms are refused, for their error message.
const ARG_MIN_MAX_WHY: &str = "when several rows tie on the value, DuckDB's batch result depends on the order it reads \
     rows in, so duckwake_verify could not check the view";

/// The second moment `q - s²/n` of values with weighted count `n`, sum `s` and sum of squares `q` (DOUBLE), as 0
/// when it is within the rounding of computing it that way (below `n·q·10⁻¹⁵`, or negative): DuckDB computes it
/// with Welford's method, which gives exactly 0 for a constant group, and a constant group must not come out as
/// a tiny variance here (its standard deviation, or a slope over it, would differ from the batch query's).
fn second_moment(n: &str, s: &str, q: &str) -> String {
    let raw = format!("({q} - CAST({s} AS DOUBLE) * CAST({s} AS DOUBLE) / {n})");
    format!("(CASE WHEN {raw} <= {q} * {n} * 1e-15 THEN 0 ELSE {raw} END)")
}

/// The variance or standard deviation of a group from its weighted count `n`, sum `s` and sum of squares `q`
/// (DOUBLE). NULL below two values for the sample variants and below one for the population ones, as DuckDB.
fn moments_finalize(kind: AggKind, n: &str, s: &str, q: &str) -> String {
    let m2 = second_moment(n, s, q);
    let var = match kind {
        AggKind::VarSamp | AggKind::StddevSamp => format!("CASE WHEN {n} > 1 THEN {m2} / ({n} - 1) END"),
        _ => format!("CASE WHEN {n} > 0 THEN {m2} / {n} END"),
    };
    if matches!(kind, AggKind::StddevSamp | AggKind::StddevPop) { format!("sqrt({var})") } else { var }
}

/// `x` squared as a DOUBLE.
fn squared(x: &str) -> String {
    format!("(CAST({x} AS DOUBLE) * CAST({x} AS DOUBLE))")
}

/// The paired sums of a two-argument statistic `f(y, x)`, by suffix: the weighted count of the rows where both
/// are non-NULL, and over those rows the sums of y, x, y², x² and xy.
const PAIRED: [&str; 6] = ["n", "y", "x", "yy", "xx", "xy"];

/// One paired sum over the rows of an epoch, with their weights `__w`, for arguments `y` and `x`.
fn paired_partial(part: &str, y: &str, x: &str) -> String {
    let both = format!("{y} IS NOT NULL AND {x} IS NOT NULL");
    let d = |e: &str| format!("CAST({e} AS DOUBLE)");
    let term = match part {
        "n" => return format!("CAST(sum(CASE WHEN {both} THEN __w ELSE 0 END) AS BIGINT)"),
        "y" => d(y),
        "x" => d(x),
        "yy" => format!("{} * {}", d(y), d(y)),
        "xx" => format!("{} * {}", d(x), d(x)),
        _ => format!("{} * {}", d(y), d(x)),
    };
    format!("sum(CASE WHEN {both} THEN ({term}) * __w END)")
}

/// A two-argument statistic from its paired sums, `col` naming each by its suffix in `PAIRED`, with DuckDB's
/// results at the edges: NULL over no pairs (`regr_count` 0); NaN for `corr` when either side is constant and for
/// `regr_slope` when x is, and then NULL for `regr_intercept` and `regr_r2`; `regr_r2` 1 when only y is constant.
/// The co-moment is exactly 0 when either side is constant, as DuckDB's online algorithm gives.
fn paired_finalize(kind: AggKind, col: &dyn Fn(&str) -> String) -> String {
    let (n, sy, sx) = (col("n"), col("y"), col("x"));
    let m2y = second_moment(&n, &sy, &col("yy"));
    let m2x = second_moment(&n, &sx, &col("xx"));
    let c = format!("(CASE WHEN {m2x} = 0 OR {m2y} = 0 THEN 0 ELSE {} - {sy} * {sx} / {n} END)", col("xy"));
    match kind {
        AggKind::CovarPop => format!("CASE WHEN {n} > 0 THEN {c} / {n} END"),
        AggKind::CovarSamp => format!("CASE WHEN {n} > 1 THEN {c} / ({n} - 1) END"),
        AggKind::Corr => format!("CASE WHEN {n} = 0 THEN NULL WHEN {m2x} = 0 OR {m2y} = 0 THEN 'NaN'::DOUBLE ELSE {c} / sqrt({m2x} * {m2y}) END"),
        AggKind::RegrCount => format!("coalesce({n}, 0)"),
        AggKind::RegrAvgx => format!("CASE WHEN {n} > 0 THEN {sx} / {n} END"),
        AggKind::RegrAvgy => format!("CASE WHEN {n} > 0 THEN {sy} / {n} END"),
        AggKind::RegrSxx => format!("CASE WHEN {n} > 0 THEN {m2x} END"),
        AggKind::RegrSyy => format!("CASE WHEN {n} > 0 THEN {m2y} END"),
        AggKind::RegrSxy => format!("CASE WHEN {n} > 0 THEN {c} END"),
        AggKind::RegrSlope => format!("CASE WHEN {n} = 0 THEN NULL WHEN {m2x} = 0 THEN 'NaN'::DOUBLE ELSE {c} / {m2x} END"),
        AggKind::RegrIntercept => format!("CASE WHEN {n} = 0 OR {m2x} = 0 THEN NULL ELSE {sy} / {n} - {c} / {m2x} * ({sx} / {n}) END"),
        AggKind::RegrR2 => format!("CASE WHEN {n} = 0 OR {m2x} = 0 THEN NULL WHEN {m2y} = 0 THEN 1.0 ELSE {c} * {c} / ({m2x} * {m2y}) END"),
        other => unreachable!("{other:?} is not a paired statistic"),
    }
}

/// The finalized value of a list of values: `count(DISTINCT)` of a list kept without duplicates, or a DuckDB
/// aggregate over it (`approx_count_distinct`, the quantiles with their fraction).
fn list_finalize(kind: AggKind, list: &str, param: Option<&str>) -> String {
    match kind {
        AggKind::CountDistinct => format!("coalesce(len({list}), 0)"),
        AggKind::ApproxCountDistinct => format!("coalesce(list_aggregate({list}, 'approx_count_distinct'), 0)"),
        k => format!("list_aggregate({list}, '{}', {})", k.list_aggregate(), param.expect("a quantile's fraction")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggSpec {
    pub kind: AggKind,
    /// Index of the argument column `__x<arg>` in the pre-aggregate output; None for count(*). For a paired
    /// statistic `f(y, x)`, y's.
    pub arg: Option<usize>,
    /// A paired statistic's second argument, x.
    #[serde(default)]
    pub arg2: Option<usize>,
    /// A quantile's fraction (or list of fractions), as SQL: a constant.
    #[serde(default)]
    pub param: Option<String>,
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
/// count, sum, avg and the variances are invertible (sums of weights). min and max are only mergeable, which
/// is correct while sources are append-only; over a retracting input they keep value counts (`counted_values`),
/// as count(DISTINCT) always does.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPlan {
    pub keys: usize,
    pub aggs: Vec<AggSpec>,
    /// Sink column holding group key j.
    pub sink_keys: Vec<String>,
    /// MIN and MAX over a source that can retract, and count(DISTINCT) over any: the state also counts each
    /// group's values, one row per (group, aggregate `__i`, value `__v<i>`) with its count `__n`, so a
    /// retracted extreme gives way to the next value still present (spike S4) and a value is counted while its
    /// count is above 0. Group rows have `__i` NULL.
    #[serde(default)]
    pub counted_values: bool,
}

/// The state columns of value rows (`GlobalPlan::counted_values`) besides the group keys.
fn is_value_col(c: &str) -> bool {
    c == "__i" || c == "__n" || c.strip_prefix("__v").is_some_and(|i| !i.is_empty() && i.bytes().all(|b| b.is_ascii_digit()))
}

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
        self.is_windowed() || self.is_global() || self.distinct || self.upsert.is_some()
    }

    /// Whether the view's state depends on every event of its topic, so retention's deletes break it.
    pub fn needs_whole_topic(&self) -> bool {
        self.is_global() || self.distinct || self.upsert.is_some()
    }

    /// Over a source that can retract, every view's table but a windowed one's has rows replaced or removed.
    pub fn sink_mode(&self) -> SinkMode {
        if self.is_global() || self.upsert.is_some() || (self.retracts() && !self.is_windowed()) { SinkMode::Upsert } else { SinkMode::Append }
    }

    pub fn kind_name(&self) -> &'static str {
        if self.is_windowed() {
            "windowed"
        } else if self.is_global() {
            "aggregate"
        } else if self.distinct {
            "distinct"
        } else if self.upsert.is_some() {
            "upsert"
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
            // The window's columns replace any the input has (a windowed view's table, read by another).
            WindowKind::Tumble => format!(
                "SELECT {KEEP}, time_bucket({size}, {tc}) AS window_start, time_bucket({size}, {tc}) + {size} AS window_end FROM {input}"
            ),
            WindowKind::Hop => {
                let slide = format!("to_microseconds({})", w.slide_us);
                format!(
                    "SELECT *, window_start + {size} AS window_end FROM (
                       SELECT {KEEP}, unnest(generate_series(time_bucket({slide}, {tc}) - {size} + {slide},
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
                    MergeOp::Concat => format!("list_concat(s.{c}, p.{c})"),
                    MergeOp::Union => format!("list_distinct(list_concat(s.{c}, p.{c}))"),
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
                // Counted from the value rows (`aggregate_stmts`).
                kind if kind.always_counts_values() => {}
                kind if kind.is_moments() => {
                    cols.push(format!("sum({}) AS __a{i}_s", signed(&x(k))));
                    cols.push(format!("sum({}) AS __a{i}_q", signed(&squared(&x(k)))));
                    cols.push(format!("{} AS __a{i}_n", weighted_count(&x(k))));
                }
                _ => {
                    let x2 = x(a.arg2.expect("a paired statistic's second argument"));
                    for part in PAIRED {
                        cols.push(format!("{} AS __a{i}_{part}", paired_partial(part, &x(k), &x2)));
                    }
                }
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
                // A touched group with no value left has no extrema row: it counts 0, and its quantiles are NULL.
                (AggKind::CountDistinct | AggKind::ApproxCountDistinct, _) => {
                    cols.push(format!("CAST(coalesce(e.__a{i}, 0) AS BIGINT) AS __a{i}"))
                }
                (AggKind::QuantileCont | AggKind::QuantileDisc, _) => {
                    cols.push(format!("CAST(e.__a{i} AS {}) AS __a{i}", ty(&format!("__a{i}"))))
                }
                (kind, _) if kind.is_moments() => {
                    cols.push(add(&format!("__a{i}_s")));
                    cols.push(add(&format!("__a{i}_q")));
                    cols.push(add(&format!("__a{i}_n")));
                }
                _ => {
                    for part in PAIRED {
                        cols.push(add(&format!("__a{i}_{part}")));
                    }
                }
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
        // Each value row carries its count `__n`: the quantiles take each value that many times.
        let extremes: Vec<String> = self
            .value_aggs()
            .map(|(i, kind)| {
                let only = format!("FILTER (WHERE __i = {i})");
                match kind {
                    AggKind::Min => format!("min(__v{i}) {only} AS __a{i}"),
                    AggKind::Max => format!("max(__v{i}) {only} AS __a{i}"),
                    AggKind::CountDistinct => format!("count(__v{i}) {only} AS __a{i}"),
                    AggKind::ApproxCountDistinct => format!("approx_count_distinct(__v{i}) {only} AS __a{i}"),
                    _ => {
                        let values = format!("flatten(list(list_resize([__v{i}], __n, __v{i})) {only})");
                        format!("{} AS __a{i}", list_finalize(kind, &values, self.aggs[i].param.as_deref()))
                    }
                }
            })
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

    /// The aggregates, by index, whose values the state counts: MIN, MAX and count(DISTINCT).
    fn value_aggs(&self) -> impl Iterator<Item = (usize, AggKind)> + '_ {
        let counted = self.counted_values;
        self.aggs.iter().enumerate().filter(move |(_, a)| counted && a.kind.counts_values()).map(|(i, a)| (i, a.kind))
    }

    /// One epoch's value counts: per group, counted aggregate and non-NULL value, the sum of its weights.
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
                k if k.always_counts_values() => format!("__a{i}"),
                AggKind::Sum => format!("CASE WHEN __a{i}_n = 0 THEN NULL ELSE __a{i}_s END AS __a{i}"),
                AggKind::Avg => format!("CASE WHEN __a{i}_n = 0 THEN NULL ELSE CAST(__a{i}_s AS DOUBLE) / __a{i}_n END AS __a{i}"),
                k if k.is_moments() => {
                    format!("{} AS __a{i}", moments_finalize(k, &format!("__a{i}_n"), &format!("__a{i}_s"), &format!("__a{i}_q")))
                }
                k => format!("{} AS __a{i}", paired_finalize(k, &|part: &str| format!("__a{i}_{part}"))),
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
    compile_with(conn, job, sql, sources, None)
}

/// `compile`, for a view that reads its Kafka source as upserts when `upsert` is given.
pub fn compile_with<S: SourceSchema>(
    conn: &Connection,
    job: &str,
    sql: &str,
    sources: &HashMap<String, S>,
    upsert: Option<&UpsertSpec>,
) -> Result<Plan> {
    let ast = serialize(conn, sql)?;
    let stmts = ast["statements"].as_array().ok_or_else(|| anyhow!("no statements"))?;
    if stmts.len() != 1 {
        bail!("a continuous view must be exactly one SELECT statement");
    }
    let mut stmt = stmts[0].clone();
    if stmt["node"]["type"] == "SET_OPERATION_NODE" {
        if upsert.is_some() {
            bail!("upsert_key reads one Kafka source as upserts; a UNION ALL view cannot use it");
        }
        return compile_union(conn, job, &stmt, sources);
    }
    let node = &mut stmt["node"];
    if node["type"] != "SELECT_NODE" {
        bail!("only plain SELECT is supported (got {}): UNION, VALUES and set operations are rejected", node["type"]);
    }
    reject_unsupported(node)?;
    let aggs = aggregate_names(conn)?;

    // Find the single stream reference in FROM and point it at the staging table. A Kafka source is the
    // stream when there is one, and lake tables joined to it are lookups; otherwise the one lake table or
    // view the SELECT reads is, through its change feed.
    let input_table = format!("memory.main.{}", ident(&format!("in_{job}")));
    let mut found = Vec::new();
    rewrite_from(&mut node["from_table"], sources, false, &mut found)?;
    if found.is_empty() {
        rewrite_from(&mut node["from_table"], sources, true, &mut found)?;
        if found.len() > 1 {
            bail!("a view reads one lake table or view through its change feed; joins of two of them are not supported yet");
        }
    }
    let stream = match found.as_slice() {
        [one] => one.clone(),
        [] => bail!(
            "the view must read from a registered source, a lake table or another view (FROM <name> or FROM TUMBLE/HOP(<name>, ...))"
        ),
        _ => bail!("only one stream reference per view is supported (stream-stream joins are phase 3)"),
    };
    let src = &sources[&stream.source];
    let window = match &stream.window {
        None => None,
        Some((kind, time_col, size, slide)) => {
            let allowed = src.time_columns();
            if !allowed.iter().any(|c| c == time_col) {
                bail!("windows over {} must use {}, not {time_col:?}", stream.source, allowed.iter().map(|c| format!("{c:?}")).collect::<Vec<_>>().join(" or "));
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
    if let Some(u) = upsert {
        check_upsert(u, &stream.source, src)?;
        if window.is_some() {
            bail!("upsert_key keeps one row per key, replaced as it changes; a window over it is not supported (read the upsert view from a windowed view instead, once windows take updates: #38)");
        }
    }
    // The stream an upsert view reads in its batch form: each key's current row, over the staged messages.
    let stream_table = match upsert {
        Some(_) => format!("memory.main.{}", ident(&format!("up_{job}"))),
        None => input_table.clone(),
    };
    let table_ref = base_table_template(conn, &stream_table, &stream.alias)?;
    *stream_slot(&mut node["from_table"]).ok_or_else(|| anyhow!("lost stream reference"))? = table_ref.clone();

    // The view in pieces: the FROM with a placeholder where the stream goes, the WHERE, the stream's columns.
    // A window's columns replace any the source has.
    let src_cols: Vec<&(String, String)> =
        src.columns().iter().filter(|(c, _)| !(window.is_some() && crate::codegen::is_window_column(c))).collect();
    let mut columns: Vec<String> = src_cols.iter().map(|(c, _)| ident(c)).collect();
    if window.is_some() {
        columns.extend(["window_start".into(), "window_end".into()]);
    }
    let query = Query {
        alias: stream.alias.clone(),
        columns,
        from: from_template(conn, &node["from_table"], &table_ref)?,
        lookups: lookup_tables(&node["from_table"], &stream_table),
        filter: (!node["where_clause"].is_null()).then(|| expr_sql(conn, &node["where_clause"])).transpose()?,
        ..Query::default()
    };

    // Staging table with the source columns (+ window columns) so the rewritten SQL binds.
    let mut cols: Vec<String> = src_cols.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
    if window.is_some() {
        cols.push("window_start TIMESTAMP".into());
        cols.push("window_end TIMESTAMP".into());
    }
    if let Some(u) = upsert {
        // The messages as staged: their position, key and whether they are tombstones.
        cols.extend(["_partition INTEGER".into(), "_offset BIGINT".into(), "_key VARCHAR".into(), "__del BOOLEAN".into()]);
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE {input_table} ({})", cols.join(", ")))?;
        let owned: Vec<(String, String)> = src_cols.iter().map(|c| (*c).clone()).collect();
        conn.execute_batch(&format!("CREATE OR REPLACE VIEW {stream_table} AS {}", u.batch_sql(&owned, &input_table)))?;
    } else {
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE {input_table} ({})", cols.join(", ")))?;
    }

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
        change_feeds: if src.is_lake() { vec![stream.source.clone()] } else { vec![] },
        window_having: None,
        upsert: None,
    };

    let node = &mut stmt["node"];
    let has_group = node["aggregate_handling"] == "FORCE_AGGREGATES"
        || node["group_expressions"].as_array().is_some_and(|g| !g.is_empty());
    let has_agg = node["select_list"].as_array().unwrap().iter().any(|e| contains_fn(e, &aggs));
    plan.distinct = node["modifiers"].as_array().is_some_and(|m| m.iter().any(|m| m["type"] == "DISTINCT_MODIFIER"));
    if let Some(u) = upsert {
        if has_group || has_agg || plan.distinct || !node["having"].is_null() {
            bail!(
                "upsert_key makes a view keep one row per key of its source; it filters and selects columns, but \
                 aggregates and DISTINCT go in another view that reads it"
            );
        }
        // Each key's row is replaced by key in the view's table, so the table must carry the keys.
        if let Some(k) = u.key.iter().find(|k| !plan.sink_columns.iter().any(|(c, _)| c == *k)) {
            bail!("select the upsert_key column {k:?} under its name, so the view's table keeps one row per key");
        }
        // The state holds each key's current row: the source's columns.
        plan.state_columns = src_cols.iter().map(|c| (*c).clone()).collect();
        plan.upsert = Some(u.clone());
    }
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
    if !has_group {
        bail!("a windowed view must GROUP BY window_start (or GROUP BY ALL with window_start selected)");
    }
    compile_windowed(conn, &mut stmt, &stream.alias, &aggs, &mut plan)?;
    Ok(plan)
}

/// Refuses an upsert spec the source cannot take: it must be a Kafka source, and name its columns.
fn check_upsert<S: SourceSchema>(u: &UpsertSpec, name: &str, src: &S) -> Result<()> {
    if src.is_lake() {
        bail!("upsert_key reads a Kafka source's messages as upserts; {name} is a lake table or view, whose updates and deletes a view already follows");
    }
    if u.key.is_empty() {
        bail!("upsert_key needs at least one column");
    }
    let has = |c: &str| src.columns().iter().any(|(n, _)| n == c);
    for k in &u.key {
        if !has(k) {
            bail!("upsert_key column {k:?} is not a column of source {name}");
        }
    }
    if u.key.iter().collect::<HashSet<_>>().len() != u.key.len() {
        bail!("upsert_key names a column twice");
    }
    match &u.version {
        Some(v) if !has(v) => bail!("upsert_version {v:?} is not a column of source {name}"),
        Some(v) if u.key.contains(v) => bail!("upsert_version {v:?} is a key column; it must be a column that grows with each change"),
        _ => {}
    }
    Ok(())
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
    let (mut union, mut retracting, mut change_feeds) = (Vec::new(), Vec::new(), Vec::new());
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
        for s in p.change_feeds {
            if !change_feeds.contains(&s) {
                change_feeds.push(s);
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
        change_feeds,
        window_having: None,
        upsert: None,
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
    // Select items that are aggregates, with their finalized SQL over a state row.
    let mut agg_items: Vec<(usize, String)> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let (sink_name, sink_type) = plan.sink_columns[i].clone();
        let is_agg = item["class"] == "FUNCTION" && aggs.contains(item["function_name"].as_str().unwrap_or(""));
        if !is_agg {
            if contains_fn(item, aggs) {
                bail!("aggregates must be top-level select items (column {sink_name:?}) in windowed views; nested expressions over aggregates are not supported");
            }
            let k = format!("__k{}", plan.key_columns.len());
            let mut e = item.clone();
            e["alias"] = json!(k);
            project.push(e);
            plan.key_columns.push(k.clone());
            finals.push(format!("CAST({} AS {sink_type}) AS {}", ident(&k), ident(&sink_name)));
            continue;
        }
        let finalize = windowed_agg(conn, plan, item, aggs, &mut args, &format!("__p{i}"), weighted)?;
        agg_items.push((i, finalize.clone()));
        finals.push(format!("CAST({finalize} AS {sink_type}) AS {}", ident(&sink_name)));
    }
    // HAVING: its aggregates get partials of their own and are read as `__h<j>`; references to a selected key,
    // or to a select item by its alias, read that key's `__k<j>` or that aggregate's finalized value.
    if !node["having"].is_null() {
        let mut having = node["having"].clone();
        let mut columns = Vec::new();
        let keys: Vec<(String, String)> = items
            .iter()
            .enumerate()
            .filter(|(i, _)| !agg_items.iter().any(|(j, _)| j == i))
            .zip(&plan.key_columns)
            .map(|((_, e), k)| (norm(&stripped(e)), k.clone()))
            .collect();
        let aliases: Vec<(String, String)> = items
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                let alias = e["alias"].as_str().filter(|a| !a.is_empty())?.to_string();
                match agg_items.iter().find(|(j, _)| *j == i) {
                    Some((_, fin)) => Some((alias, fin.clone())),
                    None => keys.iter().find(|(n, _)| *n == norm(&stripped(e))).map(|(_, k)| (alias, ident(k))),
                }
            })
            .collect();
        having_refs(&mut having, &mut |e: &Value| -> Result<Option<Value>> {
            if e["class"] == "FUNCTION" && aggs.contains(e["function_name"].as_str().unwrap_or("")) {
                // The partials are named apart from `__h<j>`: a single partial named like the finalized value
                // would be read instead of it (a list, for count(DISTINCT)).
                let h = format!("__h{}", columns.len());
                let fin = windowed_agg(conn, plan, e, aggs, &mut args, &format!("{h}p"), weighted)?;
                columns.push(format!("{fin} AS {h}"));
                return Ok(Some(column_ref(&h, "")));
            }
            if let Some((_, k)) = keys.iter().find(|(n, _)| *n == norm(e)) {
                return Ok(Some(column_ref(k, "")));
            }
            if e["class"] == "COLUMN_REF" {
                if let [name] = e["column_names"].as_array().map(Vec::as_slice).unwrap_or_default() {
                    if let Some((_, fin)) = aliases.iter().find(|(a, _)| Some(a.as_str()) == name.as_str()) {
                        let h = format!("__h{}", columns.len());
                        columns.push(format!("{fin} AS {h}"));
                        return Ok(Some(column_ref(&h, "")));
                    }
                }
            }
            Ok(None)
        })?;
        plan.window_having = Some(WindowHaving { columns, condition: expr_sql(conn, &having)? });
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
    if let Some(h) = &plan.window_having {
        let typed: Vec<String> = plan.state_columns.iter().map(|(c, t)| format!("{} {t}", ident(c))).collect();
        let st = "memory.main.__having_state";
        conn.execute_batch(&format!("CREATE OR REPLACE TABLE {st} ({})", typed.join(", ")))?;
        describe(conn, &format!("SELECT {} FROM {} WHERE {}", plan.final_select, h.over(st), h.condition))
            .context("binding HAVING over the windows (it may read the selected keys and aggregates)")?;
        conn.execute_batch(&format!("DROP TABLE {st}"))?;
    }
    Ok(())
}

/// Replaces, from the top down, each node of `e` that `f` maps to another; children of a replaced node are not
/// visited.
fn having_refs(e: &mut Value, f: &mut dyn FnMut(&Value) -> Result<Option<Value>>) -> Result<()> {
    if let Some(v) = f(e)? {
        *e = v;
        return Ok(());
    }
    match e {
        Value::Object(o) => {
            for (k, v) in o.iter_mut() {
                if !k.starts_with("query_location") {
                    having_refs(v, f)?;
                }
            }
        }
        Value::Array(a) => {
            for v in a.iter_mut() {
                having_refs(v, f)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// One aggregate call `item` of a windowed view: pushes its partials, named from `p`, and returns its finalized
/// value over a state row. Over a source that can retract, the partials sum weights (as Feldera's
/// ExpandAggregates does when the input is not append-only); over Kafka they count rows, which is the same there
/// and cheaper. A sum keeps its weighted non-NULL count too, so that it is NULL again once every value is
/// retracted.
/// The canonical name of aggregate call `item` (see `canonical_agg`), its name as written, and whether it is
/// `count(DISTINCT ...)` (or `approx_count_distinct`, where DISTINCT changes nothing); refuses what DuckWake does
/// not maintain.
fn agg_call(item: &Value, aggs: &HashSet<String>) -> Result<(&'static str, String, bool)> {
    let raw = item["function_name"].as_str().unwrap().to_ascii_lowercase();
    if matches!(raw.as_str(), "arg_min" | "arg_max" | "argmin" | "argmax" | "min_by" | "max_by" | "arg_min_null" | "arg_max_null") {
        bail!("{raw}() is not supported: {ARG_MIN_MAX_WHY}");
    }
    let Some(fname) = canonical_agg(&raw) else {
        bail!("aggregate {raw}() is not supported yet (supported: {SUPPORTED_LIST})");
    };
    let distinct = item["distinct"] == true;
    if distinct && !matches!(fname, "count" | "approx_count_distinct") {
        bail!("{raw}(DISTINCT ...) is not supported yet; count(DISTINCT ...) is");
    }
    if !item["filter"].is_null() || item["order_bys"]["orders"].as_array().is_some_and(|o| !o.is_empty()) {
        bail!("FILTER and ORDER BY inside aggregates are not supported yet ({raw})");
    }
    if agg_fn_args(item).iter().any(|a| contains_fn(a, aggs)) {
        bail!("nested aggregates are not supported ({raw})");
    }
    Ok((fname, raw, distinct))
}

fn agg_fn_args(item: &Value) -> Vec<Value> {
    item["arguments"].as_array().cloned().unwrap_or_default().iter().map(|a| a.get("expression").cloned().unwrap_or_else(|| a.clone())).collect()
}

/// The arguments of aggregate call `item` (canonical name `fname`, written `raw`): the indexes of its input
/// columns `__x<k>` among `args` (shared by calls with the same argument; for `f(y, x)` y's, then x's), and a
/// quantile's fraction as SQL, which must be a constant.
fn agg_arguments(
    conn: &Connection,
    item: &Value,
    fname: &str,
    raw: &str,
    args: &mut Vec<Value>,
) -> Result<(Option<usize>, Option<usize>, Option<String>)> {
    let fn_args = agg_fn_args(item);
    let mut index = |a: &Value| {
        let key = norm(a);
        Some(args.iter().position(|x| norm(x) == key).unwrap_or_else(|| {
            args.push(a.clone());
            args.len() - 1
        }))
    };
    match (fname, fn_args.as_slice()) {
        ("count_star", _) => Ok((None, None, None)),
        ("quantile_cont", [x]) if raw == "median" => Ok((index(x), None, Some("0.5".into()))),
        ("quantile_cont" | "quantile_disc", [x, q]) if raw != "median" => {
            let mut column = false;
            walk(q, &mut |v| column |= v["class"] == "COLUMN_REF");
            if column {
                bail!("{raw}(): the fraction must be a constant, such as 0.9 or [0.5, 0.9]");
            }
            Ok((index(x), None, Some(expr_sql(conn, q)?)))
        }
        ("quantile_cont" | "quantile_disc", _) if raw == "median" => bail!("median() takes one argument"),
        ("quantile_cont" | "quantile_disc", _) => bail!("{raw}() takes a value and a constant fraction, such as {raw}(x, 0.9)"),
        (f, [y, x]) if AggKind::of(f).is_paired() => {
            let y = index(y);
            Ok((y, index(x), None))
        }
        (f, _) if AggKind::of(f).is_paired() => bail!("{raw}() takes two arguments, (y, x)"),
        (_, [a]) => Ok((index(a), None, None)),
        _ => bail!("{raw}() takes exactly one argument here"),
    }
}

fn windowed_agg(
    conn: &Connection,
    plan: &mut Plan,
    item: &Value,
    aggs: &HashSet<String>,
    args: &mut Vec<Value>,
    p: &str,
    weighted: bool,
) -> Result<String> {
    let (fname, raw, distinct) = agg_call(item, aggs)?;
    let (arg, arg2, param) = agg_arguments(conn, item, fname, &raw, args)?;
    let x = arg.map(|k| format!("__x{k}")).unwrap_or_default();
    let mut partial = |name: &str, sql: String, op: MergeOp| {
        plan.partials.push(format!("{sql} AS {name}"));
        plan.merge_ops.push((name.to_string(), op));
    };
    let kind = if fname == "count" && distinct { AggKind::CountDistinct } else { AggKind::of(fname) };
    // Values kept as a list per window: each distinct value once, or every value for the quantiles. A window's
    // list is bounded by its rows; over a source that can retract it would need a count per value.
    if kind.always_counts_values() {
        if weighted {
            bail!("{raw}() in a window over a source that can retract needs a count per value: not supported yet");
        }
        let (list, op) = match kind {
            AggKind::CountDistinct | AggKind::ApproxCountDistinct => (format!("list(DISTINCT {x}) FILTER (WHERE {x} IS NOT NULL)"), MergeOp::Union),
            _ => (format!("list({x}) FILTER (WHERE {x} IS NOT NULL)"), MergeOp::Concat),
        };
        partial(p, list, op);
        return Ok(list_finalize(kind, &ident(p), param.as_deref()));
    }
    if kind.is_paired() {
        let x2 = format!("__x{}", arg2.unwrap());
        for part in PAIRED {
            let op = if part == "n" { MergeOp::Add } else { MergeOp::AddNullable };
            partial(&format!("{p}{part}"), paired_partial(part, &x, &x2), op);
        }
        return Ok(paired_finalize(kind, &|part: &str| ident(&format!("{p}{part}"))));
    }
    Ok(match (fname, weighted) {
        ("count_star", false) => {
            partial(p, "count(*)".into(), MergeOp::Add);
            ident(p)
        }
        ("count_star", true) => {
            partial(p, "CAST(sum(__w) AS BIGINT)".into(), MergeOp::Add);
            ident(p)
        }
        ("count", false) => {
            partial(p, format!("count({x})"), MergeOp::Add);
            ident(p)
        }
        ("count", true) => {
            partial(p, weighted_count(&x), MergeOp::Add);
            ident(p)
        }
        ("sum", false) => {
            partial(p, format!("sum({x})"), MergeOp::AddNullable);
            ident(p)
        }
        ("sum", true) => {
            let (s, c) = (format!("{p}s"), format!("{p}c"));
            partial(&s, format!("sum({})", signed(&x)), MergeOp::AddNullable);
            partial(&c, weighted_count(&x), MergeOp::Add);
            format!("CASE WHEN {} = 0 THEN NULL ELSE {} END", ident(&c), ident(&s))
        }
        ("min" | "max", true) => {
            bail!("{raw}() in a window over a source that can retract needs a count per value: not supported yet")
        }
        ("min", false) => {
            partial(p, format!("min({x})"), MergeOp::Min);
            ident(p)
        }
        ("max", false) => {
            partial(p, format!("max({x})"), MergeOp::Max);
            ident(p)
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
        (_, _) => {
            let kind = match fname {
                "var_samp" => AggKind::VarSamp,
                "var_pop" => AggKind::VarPop,
                "stddev_samp" => AggKind::StddevSamp,
                _ => AggKind::StddevPop,
            };
            let (s, q, c) = (format!("{p}s"), format!("{p}q"), format!("{p}c"));
            if weighted {
                partial(&s, format!("sum({})", signed(&x)), MergeOp::AddNullable);
                partial(&q, format!("sum({})", signed(&squared(&x))), MergeOp::AddNullable);
                partial(&c, weighted_count(&x), MergeOp::Add);
            } else {
                partial(&s, format!("sum({x})"), MergeOp::AddNullable);
                partial(&q, format!("sum({})", squared(&x)), MergeOp::AddNullable);
                partial(&c, format!("count({x})"), MergeOp::Add);
            }
            moments_finalize(kind, &ident(&c), &ident(&s), &ident(&q))
        }
    })
}

/// The state column holding a window and group's total weight, when the view's input can retract.
pub const WINDOW_WEIGHT: &str = "__pw";

/// Every column of a row but `window_start` and `window_end`, which a window then adds.
const KEEP: &str = "COLUMNS(lambda c: c NOT IN ('window_start', 'window_end'))";

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
    let mut extract = |e: &mut Value| -> Result<()> { replace_aggs(conn, e, aggs, &mut specs, &mut args) };
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
            AggKind::CountDistinct | AggKind::ApproxCountDistinct => {
                state.push((format!("__a{i}"), "BIGINT".into()));
                "BIGINT".to_string()
            }
            AggKind::QuantileCont | AggKind::QuantileDisc => {
                let f = if a.kind == AggKind::QuantileCont { "quantile_cont" } else { "quantile_disc" };
                let sql = format!("SELECT {f}(__x{}, {}) FROM ({pre_sql})", a.arg.unwrap(), a.param.as_deref().unwrap());
                let ty = describe(conn, &sql).context("binding the quantile")?.remove(0).1;
                state.push((format!("__a{i}"), ty.clone()));
                ty
            }
            kind if kind.is_moments() => {
                state.push((format!("__a{i}_s"), sum_type(a.arg.unwrap())?));
                state.push((format!("__a{i}_q"), "DOUBLE".into()));
                state.push((format!("__a{i}_n"), "BIGINT".into()));
                "DOUBLE".to_string()
            }
            kind => {
                for part in PAIRED {
                    state.push((format!("__a{i}_{part}"), if part == "n" { "BIGINT" } else { "DOUBLE" }.into()));
                }
                if kind == AggKind::RegrCount { "BIGINT" } else { "DOUBLE" }.to_string()
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

    // MIN and MAX over a source that can retract, and count(DISTINCT), approx_count_distinct and the quantiles
    // over any: value rows in the same state table.
    let counted_values = (plan.retracts() && specs.iter().any(|(_, a)| matches!(a.kind, AggKind::Min | AggKind::Max)))
        || specs.iter().any(|(_, a)| a.kind.always_counts_values());
    if counted_values {
        state.push(("__i".into(), "INTEGER".into()));
        for (i, (_, a)) in specs.iter().enumerate().filter(|(_, (_, a))| a.kind.counts_values()) {
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

/// Replaces each aggregate call in `e` by `__a<i>`, recording its spec and arguments.
fn replace_aggs(conn: &Connection, e: &mut Value, aggs: &HashSet<String>, specs: &mut Vec<(Value, AggSpec)>, args: &mut Vec<Value>) -> Result<()> {
    if e["class"] == "FUNCTION" && aggs.contains(e["function_name"].as_str().unwrap_or("")) {
        let (fname, raw, distinct) = agg_call(e, aggs)?;
        let kind = if fname == "count" && distinct { AggKind::CountDistinct } else { AggKind::of(fname) };
        let (arg, arg2, param) = agg_arguments(conn, e, fname, &raw, args)?;
        let key = norm(e);
        let i = specs.iter().position(|(n, _)| norm(n) == key).unwrap_or_else(|| {
            specs.push((e.clone(), AggSpec { kind, arg, arg2, param }));
            specs.len() - 1
        });
        let alias = e["alias"].as_str().unwrap_or("").to_string();
        *e = column_ref(&format!("__a{i}"), &alias);
        return Ok(());
    }
    match e {
        Value::Array(a) => a.iter_mut().try_for_each(|x| replace_aggs(conn, x, aggs, specs, args)),
        Value::Object(o) => o.values_mut().try_for_each(|x| replace_aggs(conn, x, aggs, specs, args)),
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

/// The name a FROM item uses for a table, as written: `t`, `schema.t` or `catalog.schema.t`.
fn table_key(t: &Value) -> String {
    ["catalog_name", "schema_name", "table_name"]
        .iter()
        .filter_map(|k| t[*k].as_str().filter(|s| !s.is_empty()))
        .collect::<Vec<_>>()
        .join(".")
}

/// Marks the stream references in FROM: names of Kafka sources, or with `lake` of lake tables and views.
fn rewrite_from<S: SourceSchema>(t: &mut Value, sources: &HashMap<String, S>, lake: bool, found: &mut Vec<StreamRef>) -> Result<()> {
    let wanted = |name: &str| sources.get(name).is_some_and(|s| s.is_lake() == lake);
    match t["type"].as_str().unwrap_or("") {
        "BASE_TABLE" => {
            let key = table_key(t);
            if wanted(&key) {
                let name = t["table_name"].as_str().unwrap_or("");
                let alias = t["alias"].as_str().filter(|a| !a.is_empty()).unwrap_or(name).to_string();
                found.push(StreamRef { source: key, alias, window: None });
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
            // The first argument names the stream as written: `clicks`, `shop.orders` or `lake.shop.orders`.
            let source = (args[0]["class"] == "COLUMN_REF")
                .then(|| args[0]["column_names"].as_array())
                .flatten()
                .map(|n| n.iter().filter_map(|p| p.as_str()).collect::<Vec<_>>().join("."))
                .ok_or_else(|| anyhow!("{fname}(): first two arguments must be a source name and a column"))?;
            if !sources.contains_key(&source) {
                bail!("{fname}(): unknown source, lake table or view {source:?}");
            }
            if !wanted(&source) {
                return Ok(());
            }
            let time_col = col(&args[1])?;
            let alias = t["alias"].as_str().filter(|a| !a.is_empty()).unwrap_or(&fname).to_string();
            let slide = (kind == WindowKind::Hop).then(|| args[3].clone());
            found.push(StreamRef { source, alias, window: Some((kind, time_col, args[2].clone(), slide)) });
            t["__stream"] = json!(true);
            Ok(())
        }
        "JOIN" => {
            rewrite_from(&mut t["left"], sources, lake, found)?;
            rewrite_from(&mut t["right"], sources, lake, found)
        }
        "SUBQUERY" => bail!("subqueries in FROM are not supported in continuous views"),
        "EMPTY" => Ok(()),
        other => bail!("unsupported FROM item {other}"),
    }
}

/// Every table name the view's FROM clauses use, as written (`t`, `schema.t`, `catalog.schema.t`), including
/// the stream argument of TUMBLE and HOP, in every branch of a UNION ALL. The caller resolves each to a Kafka
/// source, a view or a lake table before compiling.
pub fn table_names(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    fn from(t: &Value, out: &mut Vec<String>) {
        match t["type"].as_str().unwrap_or("") {
            "BASE_TABLE" => out.push(table_key(t)),
            "TABLE_FUNCTION" => {
                let f = &t["function"];
                let fname = f["function_name"].as_str().unwrap_or("").to_ascii_lowercase();
                if fname == "tumble" || fname == "hop" {
                    let arg = f["arguments"].as_array().and_then(|a| a.first()).map(|a| a.get("expression").unwrap_or(a));
                    if let Some(names) = arg.filter(|a| a["class"] == "COLUMN_REF").and_then(|a| a["column_names"].as_array()) {
                        out.push(names.iter().filter_map(|p| p.as_str()).collect::<Vec<_>>().join("."));
                    }
                }
            }
            "JOIN" => {
                from(&t["left"], out);
                from(&t["right"], out);
            }
            _ => {}
        }
    }
    fn node(n: &Value, out: &mut Vec<String>) {
        if n["type"] == "SET_OPERATION_NODE" {
            n["children"].as_array().into_iter().flatten().for_each(|c| node(c, out));
        } else {
            from(&n["from_table"], out);
        }
    }
    let ast = serialize(conn, sql)?;
    let mut out = Vec::new();
    for s in ast["statements"].as_array().into_iter().flatten() {
        node(&s["node"], &mut out);
    }
    out.sort();
    out.dedup();
    Ok(out)
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
        assert!(err("SELECT game_id, sum(DISTINCT amount) FROM clicks GROUP BY ALL").contains("count(DISTINCT ...) is"));
        assert!(err("SELECT game_id, string_agg(user_id) FROM clicks GROUP BY ALL").contains("not supported yet"));
        // count(DISTINCT) compiles: the state counts the values (`__i`, `__v<i>`, `__n` next to the group rows).
        let (c, s) = setup();
        let p = compile(&c, "j", "SELECT game_id, count(DISTINCT user_id) AS u FROM clicks GROUP BY ALL", &s).unwrap();
        assert!(p.global.as_ref().unwrap().counted_values);
        let state: Vec<_> = p.state_columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(state, ["__g0", "__cnt", "__a0", "__i", "__v0", "__n"]);
        assert!(err("SELECT game_id, count(*) FROM clicks GROUP BY ROLLUP (game_id)").contains("ROLLUP"));
        // The quantiles and approx_count_distinct share the value rows; a paired statistic keeps six sums.
        let p = compile(&c, "j", "SELECT game_id, median(amount) AS m, approx_count_distinct(user_id) AS u, corr(amount, amount) AS r FROM clicks GROUP BY ALL", &s).unwrap();
        let state: Vec<_> = p.state_columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            state,
            ["__g0", "__cnt", "__a0", "__a1", "__a2_n", "__a2_y", "__a2_x", "__a2_yy", "__a2_xx", "__a2_xy", "__i", "__v0", "__v1", "__n"]
        );
        assert!(err("SELECT game_id, min_by(user_id, amount) FROM clicks GROUP BY ALL").contains("tie"));
    }

    #[test]
    fn rejections() {
        assert!(err("SELECT * FROM clicks ORDER BY ts").contains("ORDER BY"));
        assert!(err("SELECT * FROM clicks LIMIT 5").contains("LIMIT"));
        assert!(err("SELECT DISTINCT ON (game_id) * FROM clicks").contains("DISTINCT ON"));
        assert!(err("SELECT DISTINCT game_id, count(*) FROM clicks GROUP BY ALL").contains("DISTINCT"));
        assert!(err("SELECT ts, row_number() OVER () FROM clicks").contains("window functions"));
        assert!(err("SELECT window_start, mode(user_id) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("not supported yet"));
        assert!(err("SELECT window_start, arg_max(user_id, amount) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("tie"));
        assert!(err("SELECT window_start, sum(DISTINCT amount) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("DISTINCT"));
        assert!(err("SELECT window_start, quantile_cont(amount, amount) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("constant"));
        assert!(err("SELECT window_start, corr(amount) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("corr"));
        assert!(err("SELECT game_id, count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("group by window"));
        assert!(err("SELECT window_start, sum(amount) / count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL").contains("top-level"));
        // HAVING in a window compiles, filtering windows as they close.
        let (c, s) = setup();
        let p = compile(&c, "j", "SELECT window_start, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL HAVING sum(amount) > 1", &s).unwrap();
        assert!(p.window_having.as_ref().unwrap().condition.contains("__h0"));
        assert!(err("SELECT * FROM nope").contains("registered source"));
        assert!(err("SELECT window_start, count(*) FROM TUMBLE(clicks, ts, INTERVAL 1 MONTH) GROUP BY ALL").contains("month"));
    }
}

