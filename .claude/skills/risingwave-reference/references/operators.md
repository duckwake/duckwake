# Streaming model and operators

Contents: change streams and stream keys · append-only · aggregates · joins · top-N and window functions ·
watermarks · emit on window close · TTL and temporal filters · what to borrow

## Change streams and stream keys

RisingWave rejected re-running a query per snapshot (time-varying relations) as too costly. Every executor
consumes and emits changes (`+`, `-`, `U-`/`U+`) in the same representation. The optimizer derives a **stream
key** for each operator's output bottom-up: the columns that identify a row in the stream. It comes from a
table's primary key, the group keys after an aggregate, or the union of both sides' keys after a join (adding
hidden columns when the user did not select them). The stream key carries an invariant: no two inserts of a
key without a delete between them, and a delete must match the previous value. An MV's primary key is its
stream key.

The **storage primary key** (`pk_columns`) is a different thing: it orders storage. With `ORDER BY i, id` the
stream key is `[id]` but storage is keyed by `[i, id]`.

An append-only source has no key, so RisingWave generates one (a Snowflake-style row id), and the stream holds
only inserts.

**DuckWake:** a Z-set with weights `__w` instead of ops, and no stream key yet. The analysis marks each edge
append-only or retracting (`Props::append_only`). Sinks follow from the view's shape (`SinkKind`):
- **Append:** windowed aggregates, and stateless views over append-only input.
- **Upsert by group keys:** global aggregates.
- **Upsert by the whole row:** `DISTINCT`.
- **Upsert by the key columns:** upsert views (C2).
- **Counted:** stateless views over retracting input. One row per distinct row, with its multiplicity in
  `_duckwake_count`, because there is no key to replace by.

`docs/design/cdc-sources.md` plans a `key` property derived like RisingWave's stream key:
- declared by a source;
- kept by `Filter`;
- kept by `Map` when it selects every key column;
- replaced by the group keys at an aggregate.

With it, a stateless view over a keyed input could use an upsert sink instead of a counted one.

## Append-only as a property

RisingWave tracks append-only through the plan and picks cheaper operators where it holds. Examples:
- `min`/`max` keep one value per group instead of all inputs;
- temporal joins require an append-only stream side;
- emit-on-window-close produces append-only output;
- a sink is decoupled (buffered) only when it is not append-only and its key differs from the stream key.

**DuckWake** does the same with `Props::append_only`. Kafka sources are append-only, while change feeds, upsert
views and aggregates' outputs retract. Windowed `MIN`/`MAX`, windowed `count(DISTINCT)` and quantiles, and late-
data tables are refused over retracting input (#38 tracks windows over inputs that update or delete).

## Aggregates

RisingWave's `HashAgg` has these parts:
- calls, and a state per call and group;
- groups, created on demand and cached (`AggGroupCache`);
- persisted state, written at the barrier.

Value aggregates keep one row per group. `min`/`max`/`string_agg`/`array_agg` over retracting input keep the
input rows per group ("materialized input"), sorted so the extreme is first. A separate `SimpleAgg` handles
aggregates without `GROUP BY`, and distinct aggregates keep a dedup table per distinct column (both from
RisingWave's source code, not re-read on 2026-11-20). EOWC aggregates (below) emit a group once its window
closes.

**DuckWake:** the aggregate operator works as follows:
- It splits aggregates into mergeable partials (`avg` becomes sum and count; variances become count, sum and
  sum of squares; `covar`/`corr`/`regr_*` become five sums) and merges them into a state table per view.
- Global (`GROUP BY`) views output a Z-set of changed groups (old row −1, new row +1) into an upsert sink.
- Windowed views keep each open window's partials and emit closed windows.
- `MIN`/`MAX`, `count(DISTINCT)`, `approx_count_distinct` and quantiles over retracting input keep value rows
  with counts, which is RisingWave's materialized input in SQL.

Tests: `crates/circuit/src/tests/` compares every view kind epoch by epoch with its batch query. Still open:
`list`/`string_agg`/`array_agg`/`mode` and grouping sets (#30).

## Joins

RisingWave's join kinds:
- **Symmetric hash join** (equi-joins, all outer kinds). Each side keeps a state table keyed `join key | pk` and
  probes the other side. State is unbounded unless something cleans it up:
  - **Interval join:** `a.ts BETWEEN b.ts AND b.ts + INTERVAL ...`. Watermarks expire rows, but per key only
    when that key sees a message.
  - **Window join:** both sides in the same `TUMBLE`/`HOP` window. A closed window clears both sides.
- **Temporal (lookup) join:** `JOIN dim FOR SYSTEM_TIME AS OF PROCTIME() ON ...`:
  - The stream side keeps no state.
  - The dimension (a table, MV or index with a primary key or index on the join columns) is read as it is now.
  - Later changes to the dimension never revise past outputs.
  - The plain form needs an append-only stream side. A non-append-only form keeps state so it can retract.
  - Allowed once per query block; chain MVs for more.
- **As-of join** (streaming since v2.1): one equality and one inequality condition, the nearest row on the
  other side.
- **Dynamic filter:** a column compared to a scalar subquery that changes over time, such as a value above the
  current `max()` of something. It is the one nested-loop shape allowed in streaming.
- **Delta join:** reuses existing indexes' state across queries with the same join keys.

Other details:
- Join ordering makes the plan bushy when it can (RFC 0023).
- Correlated subqueries (semi and anti joins) are unnested the Neumann–Kemper way.
- Nested-loop joins are refused in streaming.
- `streaming_enable_unaligned_join` inserts a log store after joins with high fan-out, so a burst does not
  hold up barriers.

**DuckWake:**
- **Built:** lookups. A lake table joined to a Kafka source is read as it is when each epoch runs; `explain`
  shows `Lookup(...)`. This is RisingWave's process-time temporal join: no state on the stream side, and no
  revision of past outputs. Lookups also work from upsert views (`upsert_with_a_lookup`).
- **Planned:** stream–stream joins (#31, `docs/design/stream-stream-joins.md`). Both sides are kept as DuckLake
  state, joined in SQL per epoch, and the result is committed with both sources' offsets in the epoch
  transaction. Then the interval join (J2, #32), shared readers across views (J3, #33), and lag and freshness
  per source (J4, #34).
- **Borrow from RisingWave for #31/#32:**
  - an interval join whose state the watermark cleans;
  - a window join cleared at window close;
  - refuse unbounded joins over high-volume inputs unless the user sets a limit (`max_state_rows`);
  - keep the lookup's "no revision" rule explicit in the docs.

## Top-N and window functions

**RisingWave:**
- **Top-N per group:** `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` with `WHERE rn <= N` in an outer
  query. A state table keyed `group | sort key | pk` keeps the rows of each group in order. The executor reads
  from storage merged with in-memory changes, a "merge-on-read" design.
- **`ORDER BY ... LIMIT`:** a global top-N.
- **Over-window functions** (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`/`last_value`,
  aggregates over frames) run in two modes. In emit-on-update they recompute the affected partition range. With
  `EMIT ON WINDOW CLOSE` they emit rows in watermark order, once, using a sorted buffer.
- The top-N cache (low, middle and high parts) and the two over-window executors are from RisingWave's source
  code as known before this research, not re-read on 2026-11-20; the docs say only that `row_number` is meant
  for top-N, and that TopN is a merge-on-read executor.

**DuckWake:** top-k and `ROW_NUMBER`/`RANK`/`DENSE_RANK` (#35), `LAG`/`LEAD` and rolling `OVER` aggregates (#36)
and `SESSION` windows (#37) are open. A design that fits DuckWake:
- **State:** the operator keeps, per group, the current top N rows plus a buffer of the next few (RisingWave
  caches a high/low part for the same reason), as a state table keyed `group, sort key, row`.
- **Per epoch:** recompute only the groups the epoch touched, with DuckDB's `row_number()` over (stored rows of
  those groups ∪ epoch changes).
- **Output:** a Z-set of rows entering and leaving the top N, into an upsert sink keyed `group, rank` or
  `group, row`.
- **Batch definition:** the same SQL with `QUALIFY`.
- **Main risk:** retractions of rows inside the top N need the rows below it. The buffer, refilled from the
  state's full row set when it runs dry, is the trade-off to measure.

## Watermarks

RisingWave declares a watermark on a source or table column: `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND`.
- **Advance:** it only moves forward (the maximum seen minus the delay). One plain column per source, so use a
  generated column for nested fields.
- **Use:** watermarks flow through operators and clean state: windows, interval joins, EOWC.
- **`WITH TTL` on a table:** rows at or below the watermark expire. Late changes to them are ignored, and
  cleanup is best effort at compaction. Downstream MVs keep what they computed.

**DuckWake:** each source has `event_time`, `watermark_delay` (default 30 s) and `idle_timeout` (default 60 s).
- **Rule:** watermark = min over partitions of the maximum event time, minus the delay. Partitions idle longer
  than `idle_timeout` are left out.
- **Views:** a view's watermark is the oldest of its sources'. Over lake inputs it comes exactly from
  `_commit_ts` or the upstream's `window_end`.
- **Late events:** an event for a closed window goes to the `late_data` table, or is counted (`rows_late`) and
  dropped.
- **Restart:** per-partition maxima are not checkpointed (`docs/internals.md`, "Watermark after restart").

## Emit on window close (EOWC)

RisingWave defaults to emit-on-update: a windowed aggregate's row changes at every barrier until the window
closes. `EMIT ON WINDOW CLOSE` emits each window once, final, when the watermark passes its end. This makes the
output append-only and suits append-only sinks and functions that cannot be computed incrementally
(percentiles). It works for windowed aggregates, over-window functions and ordering by the watermark column.

**DuckWake:** windowed views are always EOWC. A window is written once, when it closes, and is final.
Non-windowed `GROUP BY` views are emit-on-update per epoch. An emit-on-update mode for windows (a live
current-window row, upserted) would be the RisingWave default; it has not been asked for. "Emit corrections"
for late rows (re-emit a closed window through an upsert sink) is C4 in `docs/design/cdc-sources.md`.

## TTL and temporal filters

RisingWave has two mechanisms:
- **Temporal filter:** `WHERE ts > NOW() - INTERVAL '1 hour'` in an MV.
  - Rows leave the MV as time passes, and retractions are emitted.
  - The expression must be non-decreasing and contain exactly one `NOW()`. It cannot be `OR`ed with another
    temporal filter.
  - `DATE_TRUNC('hour', NOW())` forms cause load spikes at each boundary.
  - `NOW()` elsewhere in an MV (not in a temporal filter) forces a full recompute every barrier.
- **Watermark `WITH TTL`:** cleans a table's state by event time without retracting downstream rows.

**DuckWake:** neither exists. A temporal filter would be a new operator: per epoch, retract the rows that crossed
`now() - interval` since the last epoch, from a state table ordered by the time column. Its batch definition
depends on the epoch's clock, so `verify` would need the checkpoint's `now()` recorded. Today DuckDB's
`now()` inside a view is not refused: the compiler (`crates/circuit/src/frontend/plan.rs`) has no check for
`now()` or other volatile functions, so such a view would not match its batch query. Refusing them is a small,
separate fix. State bounding today
is through windows and `max_state_rows`.

## What to borrow next

| DuckWake item | RisingWave idea to use |
| --- | --- |
| #31 stream–stream joins, #32 interval join | symmetric hash join with state keyed `join key, row`; interval and window joins cleaned by the watermark |
| #35 top-k | group-sorted state with a cached top part; output keyed by group and rank |
| #36 `LAG`/`LEAD`, rolling `OVER` | EOWC mode first (append-only, by watermark order), emit-on-update later |
| #38 windows over updates and deletes | RisingWave's emit-on-update windows with materialized-input `min`/`max`; or keep EOWC and send retractions of closed windows to `late_data` (C4) |
| the `key` property (cdc-sources C1) | stream-key derivation bottom-up; an upsert sink when a key exists, counted rows otherwise |
| state bounding | temporal filter (`NOW()`) and watermark TTL, once the batch definition for `verify` is settled |
