# SQL coverage

Date: Nov 23, 2026. Status: proposed, not built.

## The goal

Accept as much SQL in a view as DuckWake can keep exactly, and refuse the rest when the view is created, saying
why and what to write instead. Two rules from the start stay:

- **Never recompute silently.** A view whose result cannot be kept from each epoch's changes is refused, not run
  as a full query every epoch.
- **Every accepted view is checked.** `verify` runs the view's SQL as one batch query and compares, so anything
  accepted must give one answer for one set of input rows, whatever order DuckDB reads them in.

The bar is what Materialize and RisingWave accept: nested queries, CTEs, joins of every kind, set operations and
`OVER` functions, all incremental.

## Where it stands

The compiler (`crates/circuit/src/frontend/plan.rs`) matches one `SELECT` block against the shapes it can make
incremental, rewrites it and hands it back to DuckDB, so every expression runs as DuckDB runs it.

| Accepted | Refused today |
| --- | --- |
| Any scalar expression DuckDB has, in `SELECT` and `WHERE` (`split`, `regexp_extract`, `CASE`, casts, JSON functions, list indexing) | Subqueries anywhere: in `FROM`, in expressions (`IN (SELECT ...)`, `EXISTS`, scalar subqueries) |
| `GROUP BY` with 31 aggregates and synonyms, `HAVING`, expressions over aggregates | `WITH` (CTEs) |
| `TUMBLE` and `HOP` windows over Kafka and lake inputs | Expressions over aggregates in windowed views; `SELECT *` in aggregate views |
| `SELECT DISTINCT` over plain views | `DISTINCT` over aggregates and windows |
| `UNION ALL` of plain branches | `UNION`, `EXCEPT`, `INTERSECT`; `UNION ALL` of aggregates or windows |
| A Kafka source joined to lake tables (lookups) | Joins of two Kafka sources (#31, #32) or of two lake tables or views; outer joins |
| Views over lake tables and other views, with updates and deletes | `OVER` functions (#35, #36), `QUALIFY`, session windows (#37) |
| Upsert and Debezium views | `ORDER BY`, `LIMIT`; `GROUPING SETS`, `ROLLUP`, `CUBE` (#30); `list`, `string_agg`, `mode`, `arg_min`, `arg_max` (#30); `FILTER` and `ORDER BY` inside aggregates |

Two gaps are not in that table because they are holes, not refusals:

- **Volatile functions are accepted.** Nothing refuses `now()`, `random()`, `uuid()` or `current_timestamp` in a
  view: `SELECT n, now() AS seen_at, random() AS r FROM raw.lines` was created on 2026-11-23. Their values change
  between an epoch and its replay after a crash, and `verify` cannot reproduce them.
- **An error in a view's SQL at run time ends the worker.** A cast that fails on one row
  (`split(line, '|')[2]::INTEGER` on `''`) raises in staging, and staging errors other than the known ones fail
  the whole worker (`engine.rs`, "Any other failure fails the worker"), with every view on it. Tested on
  2026-11-23: a healthy view on the same worker committed nothing, and the restarted worker meets the same row.

## Three kinds of gap

1. **Rewrites.** SQL that means the same as SQL already accepted. No new state, no new operator: the compiler
   turns it into the accepted form. Cheap, and most of what users trip over.
2. **New incremental operators.** SQL whose result needs state the engine does not keep yet, and a rule for
   updating it from the changes (DBSP's: joins, distinct, top-k, nested aggregates). Each is a piece of work like
   the aggregates were: a state table in the lake, the rule, and the harness test over inserts, updates and
   deletes.
3. **Never.** SQL with no single answer for a set of rows, which neither replay nor `verify` can reproduce.

## The plan

In order of what it gives per cost. Each step is its own PR, and the existing roadmap issues are named where one
covers it.

### S0. Close the two holes

- **Refuse volatile functions** in views, from DuckDB's own list: every function whose `stability` in
  `duckdb_functions()` is not `CONSISTENT` (`random` and `uuid` are `VOLATILE`; `now` is
  `CONSISTENT_WITHIN_QUERY`, the same within one epoch but not across a replay). The way out goes in the message: `_commit_ts` or the event time instead of `now()`, a hash of a column instead of
  `random()`.
- **An error in a view's SQL fails that view, not the worker.** DuckDB's data errors (conversion, out of range,
  invalid input, division by zero where it raises) from staging a view mark it `failed` with the error and the
  row's position, as `max_late_rows` already does; the other views keep running. Errors that are not about the
  data (the catalog, I/O, memory) still fail the worker, which retries. Gate: a failing view beside a healthy
  one on one worker, in a suite, with the healthy one verifying.

### S1. Rewrites

- **Subqueries in `FROM` and CTEs that project and filter**, nested to any depth, under any outer query the
  compiler accepts: the inner expressions are substituted into the outer query and the `WHERE` clauses joined
  with `AND`. `SELECT rc[1] AS name FROM (SELECT split(line, '|') AS rc FROM people) WHERE rc[3] = 'M'` plans
  as the inline form, which is accepted and verified today. A CTE read once is the same; read twice it is a
  self-join (S3).
- **`FILTER (WHERE c)` inside aggregates**: `count(*) FILTER (WHERE c)` is `count(CASE WHEN c THEN 1 END)`,
  `sum(x) FILTER (WHERE c)` is `sum(CASE WHEN c THEN x END)`, and so for every aggregate that skips NULLs.
- **`UNION`** is `UNION ALL` under `DISTINCT`, both accepted over plain branches.
- **`ORDER BY` without `LIMIT`** is accepted and dropped: a view's table has no order, which `create_view`'s
  result says. dbt models often end with one.
- **Expressions over aggregates in windowed views** (`sum(amount) / count(*)`): the partials are kept as now,
  and the expression applied when the window closes, as `GROUP BY` views already do.
- **`GROUPING SETS`, `ROLLUP`, `CUBE`** (#30): a `UNION ALL` of one `GROUP BY` per set, with NULL for the keys
  left out and `GROUPING()` as a constant per branch. Needs `UNION ALL` of aggregates, so after S4.

### S2. A coverage corpus

A file of queries, each either accepted with its result over fixed data or refused with its message, run by
`cargo test` through the harness (QΔ = D ∘ Q ∘ I over inserts, updates and deletes, in order and pipelined). It
is the test of every step from here, and `docs/sql.md` gains a "What a view can contain" page generated from it,
so the docs and the compiler cannot disagree. Today that list is nowhere in the docs: a user finds out from the
error.

### S3. Joins

- **Two lake inputs** (tables or views) joined, inner first. The state is free: both inputs are DuckLake tables
  with snapshots, so an epoch's change is ΔA ⋈ B(before) + A(after) ⋈ ΔB, read with `AT (VERSION => ...)`. No
  state table, and backfill, reset and retention work as they do for one lake input.
- **Two Kafka sources** (#31) and the interval join (#32): each side's rows kept in a state table, bounded by
  the watermark for interval joins, as planned in [stream–stream joins](stream-stream-joins.md).
- **Outer joins**, then **semi and anti joins**, which `IN (SELECT ...)`, `EXISTS` and `NOT EXISTS` become once
  decorrelated (DuckDB decorrelates them the same way). A CTE read twice is a join of the CTE with itself.

### S4. Nested stateful queries in one view

An aggregate over an aggregate, a `UNION ALL` of aggregates or windows, `DISTINCT` over an aggregate: each inner
block becomes a stage of the view's circuit with its result in a state table, and the outer block reads that
table's changes, the way a view reads another view's table today but inside one job, one epoch and one DuckLake
transaction. DBSP's chain rule (the change of Q₂ ∘ Q₁ is ΔQ₂ applied to ΔQ₁) is what makes it exact. The
alternative, creating hidden views for the inner blocks, gives the user jobs they did not ask for and an extra
epoch of latency per level.

### S5. Set operations and collecting aggregates

- **`EXCEPT` and `INTERSECT`**, `ALL` and distinct: arithmetic on each row's count, kept in a state table keyed
  by the whole row.
- **`list`, `array_agg`, `string_agg`** (#30) with an `ORDER BY` inside that orders every row (a total order),
  so the result is one answer; without one they are refused, as `arg_min` is now. **`arg_min` / `arg_max`** take
  the same way out: accepted when ties are broken by an `ORDER BY` (`arg_min(x, (v, id))`). **`mode`** keeps each
  value's count, as `count(DISTINCT)` does, and breaks ties by the smallest value.

### S6. `OVER` functions and top-k

- **`ROW_NUMBER`, `RANK`, `DENSE_RANK`** under `QUALIFY` or a filter on them (top-k per partition, #35), and
  `ORDER BY ... LIMIT n` as top-k of one partition: a state table of each partition's rows, with the rank
  recomputed for the partitions an epoch touches.
- **`LAG`, `LEAD`, rolling aggregates** (`ROWS` or `RANGE BETWEEN ...`, #36), over partitions ordered by event
  time; session windows (#37).

### Never

- Volatile functions (S0 refuses them).
- `LIMIT` without an `ORDER BY` that orders every row: no single answer.
- `USING SAMPLE`, `TABLESAMPLE`.
- Recursive CTEs, unless asked for: DBSP can do them, at a cost no use case has shown yet.

## How a step is checked

- The harness over inserts, updates and deletes, in order and pipelined, for every shape the step accepts.
- The S2 corpus, for what is accepted, what is refused, and the messages.
- `verify` in the integration suites, with workers killed, for each new operator (S3 to S6).
- `docs/sql.md` from the corpus.

## Issues

#30, #31, #32, #35, #36 and #37 cover parts of S1, S3, S5 and S6. S0, the S1 rewrites, S2, the lake–lake join,
S4 and the set operations need issues of their own.
