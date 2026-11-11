# SQL functions

The same functions exist in the server (`duckwaked`, also over Quack) and in the extension. A view is named by the
DuckLake table it maintains (`analytics.clicks_per_min`); functions take that name or the bare table name, which
is the view's job id. Actions return `(view, result)` rows.

A view's `FROM` names a Kafka source, or reads a lake table or another view's table through its DuckLake
change feed: `SELECT game, sum(n) AS n FROM analytics.clicks_per_min GROUP BY game` needs no source. A lake
table joined to a Kafka source is a lookup instead, read as it is when each event is processed;
`duckwake_explain` shows `ChangeFeed(...)` or `Lookup(...)` for each. Every lake input gets a `_commit_ts`
column, the commit time of the change. Windows over a lake input use `_commit_ts`, or a windowed view's
`window_start` or `window_end`, and get an exact watermark. Until retracting inputs come, a view reads only
tables whose rows are inserted, never updated or deleted: an update or delete stops it. A view reads the
changes after it was created, and dropping a view that another view reads is refused.

| Function | Does |
| --- | --- |
| `duckwake_create_source(name, kind, options_json)` | register a Kafka source |
| `duckwake_create_view(name, sql, late_data :=, epoch_interval :=, max_rows :=)` | create the table `name` and its job |
| `duckwake_start(view, worker :=)` / `duckwake_pause(view)` / `duckwake_stop(view)` | lifecycle; `start` defaults to the server's worker |
| `duckwake_reset(view, to_epoch)` / `duckwake_drop(view, keep_table :=)` | rewind and replay / remove |
| `duckwake_jobs()` / `duckwake_checkpoints(view, n :=)` / `duckwake_sources()` | status, history, sources |
| `duckwake_verify(view)` | the differential exactly-once check |
| `duckwake_explain(view)` | the view's operator circuit, how it was made incremental, and one epoch's SQL ([internals](internals.md#views-as-dbsp-circuits)) |
| `duckwake_wait(view, timeout_s :=)` | block until the view has committed what its topic holds now |
| `duckwake_metrics()` | the worker's numbers as rows: `(metric, worker, view, value)` |
| `duckwake_worker_status()` | the worker: running, restarts, last error and when |

Only in the extension:

| Extension-only function | Does |
| --- | --- |
| `duckwake_connect(catalog, data_path :=)` | pick the lake |
| `duckwake_worker_start(worker :=, stage_threads :=, pipeline :=, shared_readers :=, lean_commit :=, metrics :=)` | run a supervised worker in this process; `metrics` also serves `GET /metrics` |
| `duckwake_worker_stop()` | stop it after its current epoch |

`TUMBLE(src, col, size)` and `HOP(src, col, size, slide)` are used inside the view SQL.

## The same in the CLI

| SQL function (server, extension) | CLI |
| --- | --- |
| `duckwake_create_source(name, kind, options)` | `create-source NAME --kind kafka --options JSON` |
| `duckwake_create_view(name, sql, options)`, where `name` is the view's DuckLake table | `create-view SCHEMA.TABLE --sql [--late-data] [--checkpoint ducklake] [--epoch-interval] [--max-rows]` |
| `duckwake_start` / `duckwake_stop` / `duckwake_pause` | `start NAME --worker W` / `stop` / `pause` |
| `duckwake_jobs()` / `duckwake_checkpoints(view)` | `jobs` / `checkpoints NAME` |
| `duckwake_reset(view, to_epoch)` | `reset NAME --to-epoch N` |
| `duckwake_verify(view)` / `duckwake_explain(view)` | `verify NAME` / `explain NAME` |
| `TUMBLE(src, col, size)` / `HOP(src, col, size, slide)` | same, inside the view SQL |
