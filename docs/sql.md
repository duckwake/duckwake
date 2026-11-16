# SQL functions

The same functions exist in the server (`duckwaked`, also over Quack) and in the extension. A view is named by the
DuckLake table it maintains (`analytics.clicks_per_min`); functions take that name or the bare table name, which
is the view's job id. Actions return `(view, result)` rows.

A view's `FROM` names a Kafka source, or reads a lake table or another view's table through its DuckLake
change feed: `SELECT game, sum(n) AS n FROM analytics.clicks_per_min GROUP BY game` needs no source. A lake
table joined to a Kafka source is a lookup instead, read as it is when each event is processed;
`duckwake_explain` shows `ChangeFeed(...)` or `Lookup(...)` for each. Every lake input gets a `_commit_ts`
column, the commit time of the row's version. A view takes the table's updates and deletes: it removes the
rows it no longer holds, so a filter's table counts each row in `_duckwake_count`, and `DISTINCT` and
`GROUP BY` views keep their rows up to date. Windows over a lake input use `_commit_ts`, or a windowed view's
`window_start` or `window_end`, and get an exact watermark; a window is final once it closes, so a windowed
view needs its lake tables insert-only (an update or delete stops it) and cannot read a view whose rows
change. To keep time buckets current instead, `GROUP BY time_bucket(INTERVAL 1 HOUR, _commit_ts)`. Dropping a
view that another view reads is refused.

A new view over lake inputs starts from the rows they already hold (`start := 'earliest'`, the default): it reads
each input as it was when the view was created (the snapshot S0, which it holds from then on, even while stopped), `max_rows` rows per epoch in `rowid` order, then the changes
after that snapshot. Each row keeps the `_commit_ts` of its version. A backfill continues where it stopped after a
crash, and a windowed view closes no window until its backfill is done. `start := 'latest'` holds only the rows
written after the view was created. Maintenance never expires a snapshot a view still needs, from the oldest
position of any view that is not dropped, paused and stopped ones included; `duckwake_snapshots_held_seconds`
says how far back that is ([alerts](alerts.md#duckwakesnapshotsheld)).

| Function | Does |
| --- | --- |
| `duckwake_create_source(name, kind, options_json)` | register a Kafka source |
| `duckwake_create_view(name, sql, late_data :=, epoch_interval :=, max_rows :=, max_state_rows :=, max_late_rows :=, start :=, if_exists :=, replace :=)` | create the table `name` and its job; `start := 'latest'` skips the rows its lake inputs already hold. When the view exists, `if_exists := 'update'` changes only its settings (or nothing) and refuses a changed query, and `replace := true` (`if_exists := 'replace'`) replaces it, which recomputes from the start; the result starts with `created`, `unchanged`, `altered` or `replaced` ([details](cli.md#changing-views-and-sources)) |
| `duckwake_add_source_column(source, column, type)` | add a column to a source; views that read it keep their columns |
| `duckwake_alter_view(view, epoch_interval :=, max_rows :=, max_state_rows :=, max_late_rows :=)` | change how a view runs without recomputing it; a limit of 0 removes it ([limits](cli.md#limits)) |
| `duckwake_start(view, worker :=)` / `duckwake_pause(view)` / `duckwake_stop(view)` | lifecycle; `start` puts the view on the server's worker, `worker := 'w2'` pins it to w2, and `worker := 'any'` leaves it to whichever live worker claims it |
| `duckwake_reset(view, to_epoch)` / `duckwake_drop(view, keep_table :=)` | rewind and replay / remove. The views that read the view's table with retractions follow the reset through its change feed; windowed views over it, which read it insert-only, are emptied and backfill again from its table. A reset to an epoch whose snapshot has expired is refused |
| `duckwake_jobs()` / `duckwake_checkpoints(view, n :=)` / `duckwake_sources()` | status, history, sources |
| `duckwake_workers()` / `duckwake_drain(worker)` | every worker that ran on the lake: state, views, load, last heartbeat / hand all of a worker's views to the others before taking it out |
| `duckwake_verify(view)` | the differential exactly-once check |
| `duckwake_explain(view)` | the view's operator circuit, how it was made incremental, and one epoch's SQL ([internals](internals.md#views-as-dbsp-circuits)) |
| `duckwake_wait(view, timeout_s :=)` | block until the view has committed what its topic holds now |
| `duckwake_metrics()` | the worker's numbers as rows: `(metric, worker, view, value)` |
| `duckwake_worker_status()` | the worker: running, restarts, last error and when |

Only in the extension:

| Extension-only function | Does |
| --- | --- |
| `duckwake_connect(catalog, data_path :=)` | pick the lake |
| `duckwake_worker_start(worker :=, stage_threads :=, pipeline :=, shared_readers :=, lean_commit :=, metrics :=, position_refresh :=)` | run a supervised worker in this process; `position_refresh` is `--position-refresh` (default `'5m'`); `metrics` also serves `GET /metrics` |
| `duckwake_worker_stop()` | stop it after its current epoch |

`TUMBLE(src, col, size)` and `HOP(src, col, size, slide)` are used inside the view SQL.

## Aggregates

| Aggregate | Windowed views | `GROUP BY` views |
| --- | --- | --- |
| `count(*)`, `count(x)`, `sum`, `avg` (`mean`) | yes | yes |
| `min`, `max`, `bool_and`, `bool_or` | yes, over inputs that only insert | yes; over inputs that update or delete, the state counts each value |
| `var_samp` (`variance`), `var_pop`, `stddev_samp` (`stddev`), `stddev_pop` | yes | yes |
| `count(DISTINCT x)` | not yet | yes; the state counts each group's values |

`HAVING` works in both: a windowed view emits only the closed windows that pass it, and may read the selected
keys, aggregates (selected or not) and select items by their alias. Aggregates must be top-level select items
in a windowed view; a `GROUP BY` view also takes expressions over them. Any other aggregate, `DISTINCT` inside
an aggregate other than `count`, and `FILTER` or `ORDER BY` inside one are refused when the view is created,
with the list above.

The variances keep each group's count, sum and sum of squares, so they follow updates and deletes as `avg`
does. They are computed as (Σx² − (Σx)²/n) in DOUBLE, which matches DuckDB's batch result to about 10
significant digits while a group's mean is within about 10⁴ times its standard deviation; beyond that the
difference of two large sums loses digits, and `verify` reports it. Subtract a constant near the mean
(`var_samp(x - 1000000)`) to keep full precision, since the variance does not change.

## The same in the CLI

| SQL function (server, extension) | CLI |
| --- | --- |
| `duckwake_create_source(name, kind, options)` | `create-source NAME --kind kafka --options JSON` |
| `duckwake_create_view(name, sql, options)`, where `name` is the view's DuckLake table | `create-view SCHEMA.TABLE --sql [--late-data] [--checkpoint ducklake] [--epoch-interval] [--max-rows] [--max-state-rows] [--max-late-rows] [--if-exists fail\|update\|replace] [--replace]` |
| `duckwake_add_source_column(source, column, type)` | `add-source-column SOURCE COLUMN TYPE` |
| `duckwake_alter_view(view, ...)` | `alter-view NAME [--epoch-interval] [--max-rows] [--max-state-rows] [--max-late-rows]` |
| `duckwake_start` / `duckwake_stop` / `duckwake_pause` | `start NAME [--worker W]` (default `any`) / `stop` / `pause` |
| `duckwake_workers()` / `duckwake_drain(worker)` | `workers` / `drain WORKER` |
| `duckwake_jobs()` / `duckwake_checkpoints(view)` | `jobs` / `checkpoints NAME` |
| `duckwake_reset(view, to_epoch)` | `reset NAME --to-epoch N` |
| `duckwake_verify(view)` / `duckwake_explain(view)` | `verify NAME` / `explain NAME` |
| `TUMBLE(src, col, size)` / `HOP(src, col, size, slide)` | same, inside the view SQL |
