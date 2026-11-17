# SQL functions

Every `duckwake_*` function works the same in the server (`duckwaked`, also over Quack) and in the extension
(`LOAD duckwake`). The [CLI](cli.md) has a command for each one; see [the same in the CLI](#the-same-in-the-cli).

A view is named by the DuckLake table it keeps up to date, such as `analytics.clicks_per_min`. Functions take
that name or the bare table name (`clicks_per_min`), which is also the view's job id. Functions that change
something return `(view, result)` rows.

## At a glance

| Task | Functions |
| --- | --- |
| [Sources](#sources) | `duckwake_create_source`, `duckwake_add_source_column`, `duckwake_sources` |
| [Views](#views) | `duckwake_create_view`, `duckwake_alter_view` |
| [Running views](#running-views) | `duckwake_start`, `duckwake_pause`, `duckwake_stop`, `duckwake_reset`, `duckwake_drop` |
| [Workers](#workers) | `duckwake_workers`, `duckwake_drain` |
| [Checking views](#checking-views) | `duckwake_jobs`, `duckwake_checkpoints`, `duckwake_verify`, `duckwake_explain`, `duckwake_wait`, `duckwake_metrics`, `duckwake_worker_status` |
| [Extension only](#extension-only) | `duckwake_connect`, `duckwake_worker_start`, `duckwake_worker_stop` |
| [In the view's SQL](#windows) | `TUMBLE`, `HOP`, [aggregates](#aggregates) |

## Sources

A source is a Kafka topic with a schema. It holds no data; it tells views how to read the topic.

```sql
CALL duckwake_create_source('clicks', 'kafka', '{
  "brokers": "localhost:19092", "topic": "clicks",
  "event_time": "ts", "watermark_delay": "5 seconds",
  "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR", "amount": "DECIMAL(18,2)"}}');
```

| Function | Does |
| --- | --- |
| `duckwake_create_source(name, kind, options_json)` | Register a source. `kind` is `'kafka'`. |
| `duckwake_add_source_column(source, column, type)` | Add a column to a source while views read it. Views that already read the source keep their columns. |
| `duckwake_sources()` | Every source with its options. |

The options:

| Option | Default | Meaning |
| --- | --- | --- |
| `brokers`, `topic` | required | Where to read. |
| `columns` | required | The columns and their DuckDB types, decoded from each JSON message. |
| `event_time` | required | The column holding each event's time; windows and the watermark use it. |
| `watermark_delay` | `"30 seconds"` | How late an event may arrive before its window closes without it. |
| `idle_timeout` | `"60 seconds"` | A partition with no events for this long no longer holds the watermark back. |
| `start` | `"earliest"` | Where a new view starts in the topic: `earliest` or `latest`. |
| `decode` | `"auto"` | `auto` decodes with Arrow when every column type allows it, `sql` always in SQL, `arrow` refuses types Arrow can't decode. |
| `security_protocol`, `sasl_*`, `ssl_*` | `plaintext` | Kafka authentication and TLS; see [views and sources](cli.md#views-and-sources) in the CLI guide. A password is `env:NAME` or `file:/path`, never stored in the lake. |

## Views

A view is a `SELECT`. DuckWake creates its table and the job that keeps the table equal to the query.

```sql
CALL duckwake_create_view('analytics.clicks_per_min',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
   FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL',
  epoch_interval := '10s', late_data := 'analytics.clicks_late');
```

`duckwake_create_view(name, sql, ...)` takes these options:

| Option | Default | Meaning |
| --- | --- | --- |
| `epoch_interval` | `'10s'` | How often an epoch runs. |
| `max_rows` | `100000` | Most events (or lake rows) one epoch reads. |
| `late_data` | none | A table for events that arrive after their window closed (windowed views). Without it they are counted in the checkpoint (`rows_late`) and dropped. |
| `max_state_rows` | none | Most rows the view's state may hold; past it the view fails alone ([limits](cli.md#limits)). |
| `max_late_rows` | none | Most late rows one epoch may write; past it the view fails alone. |
| `start` | `'earliest'` | Over lake tables and other views: `earliest` starts from the rows they already hold, `latest` from rows written after the view is created ([backfill](#backfill)). |
| `if_exists` | `'fail'` | When the view exists: `fail`; `update` changes only its settings and refuses a changed query; `replace` replaces it and recomputes from the start. `replace := true` is the same as `'replace'`. |
| `inline_row_limit` | `10000` | Rows of the view's table kept inlined in the catalog before they go to Parquet. |
| `checkpoint` | `'ducklake'` | Where the state lives; `ducklake` is the only backend. |

The result starts with what happened: `created`, `unchanged`, `altered` or `replaced`
([details](cli.md#changing-views-and-sources)).

`duckwake_alter_view(view, epoch_interval :=, max_rows :=, max_state_rows :=, max_late_rows :=)` changes how a
view runs without recomputing it. A limit of 0 removes it.

## Running views

| Function | Does |
| --- | --- |
| `duckwake_start(view, worker :=)` | Start the view. On the server it runs on the server's worker; `worker := 'w2'` pins it to w2, and `worker := 'any'` leaves it to whichever live worker claims it. |
| `duckwake_pause(view)` / `duckwake_stop(view)` | Stop the view. Its worker is fenced at once, so an epoch in flight does not land and `drop` or `reset` can follow straight away. `start` continues from the last checkpoint. |
| `duckwake_reset(view, to_epoch)` | Rewind to an epoch and replay from there. Views that read this view's table with retractions follow the reset through its change feed; windowed views that read it insert-only are emptied and backfill again. A reset to an epoch whose snapshot has expired is refused. |
| `duckwake_drop(view, keep_table :=)` | Remove the view and its table (`keep_table := true` keeps the table). Dropping a view that another view reads is refused. |

## Workers

| Function | Does |
| --- | --- |
| `duckwake_workers()` | Every worker that ran on the lake: state, views, load and last heartbeat. |
| `duckwake_drain(worker)` | Hand all of a worker's views to the others before taking it out. |

## Checking views

| Function | Does |
| --- | --- |
| `duckwake_jobs()` | Every view: status, epoch, watermark, freshness, lag. |
| `duckwake_checkpoints(view, n :=)` | The view's last `n` checkpoints (default 10). |
| `duckwake_verify(view)` | The exactly-once check: the view's SQL run as one batch query over everything it has read, compared with its table. |
| `duckwake_explain(view)` | The view's operator circuit, how it was made incremental, and one epoch's SQL ([internals](internals.md#views-as-dbsp-circuits)). |
| `duckwake_wait(view, timeout_s :=)` | Block until the view has committed what its topic holds now (default timeout 60 s). |
| `duckwake_metrics()` | The worker's numbers as rows: `(metric, worker, view, value)`. |
| `duckwake_worker_status()` | The worker: running, restarts, last error and when. |

`duckwake_verify` and `duckwake_jobs` are table functions: `FROM duckwake_verify('clicks_per_min')`.

## Extension only

| Function | Does |
| --- | --- |
| `duckwake_connect(catalog, data_path :=)` | Pick the lake. |
| `duckwake_worker_start(worker :=, stage_threads :=, pipeline :=, shared_readers :=, lean_commit :=, metrics :=, position_refresh :=)` | Run a supervised worker in this process. `position_refresh` is `--position-refresh` (default `'5m'`); `metrics` also serves `GET /metrics`. The flags are described in [performance](performance.md). |
| `duckwake_worker_stop()` | Stop the worker after its current epoch. |

## Windows

`TUMBLE(src, col, size)` and `HOP(src, col, size, slide)` go in the view's `FROM` and add `window_start` and
`window_end`. A window is written to the view's table once the watermark passes its end, and is final from then
on; an event for a closed window goes to the `late_data` table.

```sql
SELECT window_end, game_id, count(*) AS n
FROM HOP(clicks, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND)
GROUP BY ALL
```

## Aggregates

| Aggregate | Windowed views | `GROUP BY` views |
| --- | --- | --- |
| `count(*)`, `count(x)`, `sum`, `avg` (`mean`) | yes | yes |
| `min`, `max`, `bool_and`, `bool_or` | yes, over inputs that only insert | yes; over inputs that update or delete, the state counts each value |
| `var_samp` (`variance`), `var_pop`, `stddev_samp` (`stddev`), `stddev_pop` | yes | yes |
| `covar_pop`, `covar_samp`, `corr`, `regr_count`, `regr_avgx`, `regr_avgy`, `regr_sxx`, `regr_syy`, `regr_sxy`, `regr_slope`, `regr_intercept`, `regr_r2` | yes | yes |
| `count(DISTINCT x)`, `approx_count_distinct(x)` | yes, over inputs that only insert; the state keeps each window's distinct values | yes; the state counts each group's values |
| `median(x)`, `quantile_cont(x, q)`, `quantile_disc(x, q)` (`quantile`) | yes, over inputs that only insert; the state keeps each window's values | yes; the state counts each group's values |

`HAVING` works in both: a windowed view emits only the closed windows that pass it, and may read the selected
keys, aggregates (selected or not) and select items by their alias. Aggregates must be top-level select items
in a windowed view; a `GROUP BY` view also takes expressions over them. Any other aggregate, `DISTINCT` inside
an aggregate other than `count`, and `FILTER` or `ORDER BY` inside one are refused when the view is created,
with the list above. `arg_min` and `arg_max` (`min_by`, `max_by`) are refused: when several rows tie on the
value, DuckDB's batch result depends on the order it reads rows in, so `verify` could not check the view.

The quantiles are exact, the same as DuckDB's: `q` is a constant fraction, or a list of them, which gives a
list (`quantile_cont(latency_ms, [0.5, 0.9, 0.99])`). `median(x)` is `quantile_cont(x, 0.5)`.
`approx_count_distinct` is DuckDB's estimate over the distinct values the state keeps, so it equals the batch
query's; it costs the same as `count(DISTINCT)`, since DuckLake cannot store DuckDB's sketch. These four keep
values: a window's lists grow with its rows, and a `GROUP BY` view keeps one row per group and value, with a
quantile recomputed over the touched groups' values each epoch.

The variances keep each group's count, sum and sum of squares, so they follow updates and deletes as `avg`
does, and the two-argument statistics `f(y, x)` keep the count and the sums of y, x, y², x² and xy over the
rows where neither is NULL. They are computed from these sums in DOUBLE, which matches DuckDB's batch result to
about 10 significant digits while a group's mean is within about 10⁴ times its standard deviation; beyond that
the difference of two large sums loses digits, and `verify` reports it. Subtract a constant near the mean
(`var_samp(x - 1000000)`) to keep full precision, since the variance does not change. A group whose values are
all equal has a variance of exactly 0, as in DuckDB, so `corr` is NaN and `regr_slope` NaN over a constant x,
with `regr_intercept` and `regr_r2` NULL then.

## Views over lake tables and other views

A view's `FROM` can read a lake table, or another view's table, instead of a Kafka source. It needs no source:

```sql
SELECT game_id, sum(n) AS n FROM analytics.clicks_per_min GROUP BY game_id
```

- **Change feed or lookup.** Read on its own, a lake table is read through its DuckLake change feed: the rows
  inserted, updated and deleted since the last snapshot the view read. Joined to a Kafka source, it is a lookup
  instead, read as it is when each event is processed. `duckwake_explain` shows `ChangeFeed(...)` or
  `Lookup(...)` for each.
- **`_commit_ts`.** Every lake input gets a `_commit_ts` column: the commit time of the row's version.
- **Updates and deletes.** A view removes the rows it no longer holds: a filter's table counts each row in
  `_duckwake_count`, and `DISTINCT` and `GROUP BY` views keep their rows up to date.
- **Windows.** Windows over a lake input use `_commit_ts`, or a windowed view's `window_start` or `window_end`,
  and get an exact watermark. A window is final once it closes, so a windowed view needs its lake tables
  insert-only (an update or delete stops it) and cannot read a view whose rows change. To keep time buckets
  current instead, `GROUP BY time_bucket(INTERVAL 1 HOUR, _commit_ts)`.

### Backfill

A new view over lake inputs starts from the rows they already hold (`start := 'earliest'`, the default):

1. It reads each input as it was when the view was created (the snapshot S0, which it holds from then on, even
   while stopped), `max_rows` rows per epoch in `rowid` order. Each row keeps the `_commit_ts` of its version.
2. Then it reads the changes after S0.

A backfill continues where it stopped after a crash, and a windowed view closes no window until its backfill is
done. `start := 'latest'` holds only the rows written after the view was created.

Maintenance never expires a snapshot a view still needs: it keeps every snapshot from the oldest position of
any view that is not dropped, paused and stopped ones included. `duckwake_snapshots_held_seconds` says how far
back that is ([alerts](alerts.md#duckwakesnapshotsheld)).

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
