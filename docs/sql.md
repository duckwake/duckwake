# SQL functions

Every `duckwake_*` function works the same in the server (`duckwaked`, also over Quack) and in the extension
(`LOAD duckwake`). The [CLI](cli.md) has a command for each one; see [the same in the CLI](#the-same-in-the-cli).

A view is named by the DuckLake table it keeps up to date, such as `analytics.clicks_per_min`. Functions take
that name or the bare table name (`clicks_per_min`), which is also the view's job id. Functions that change
something return `(view, result)` rows.

## At a glance

| Task | Functions |
| --- | --- |
| [Sources](#sources) | `duckwake_create_source`, `duckwake_add_source_column`, `duckwake_drop_source`, `duckwake_sources` |
| [Views](#views) | `duckwake_create_view`, `duckwake_alter_view` |
| [Running views](#running-views) | `duckwake_start`, `duckwake_pause`, `duckwake_stop`, `duckwake_reset`, `duckwake_drop` |
| [Workers](#workers) | `duckwake_workers`, `duckwake_drain` |
| [Checking views](#checking-views) | `duckwake_jobs`, `duckwake_checkpoints`, `duckwake_verify`, `duckwake_explain`, `duckwake_wait`, `duckwake_metrics`, `duckwake_worker_status` |
| [Extension only](#extension-only) | `duckwake_connect`, `duckwake_worker_start`, `duckwake_worker_stop` |
| [In the view's SQL](#windows) | `TUMBLE`, `HOP`, [aggregates](#aggregates) |

## Sources

A source is a Kafka topic with a schema. It holds no data; it tells views how to read the topic. A
[PostgreSQL source](#postgresql-sources) is different: it names tables of a database, and DuckWake keeps a
copy of each in the lake.

```sql
CALL duckwake_create_source('clicks', 'kafka', '{
  "brokers": "localhost:19092", "topic": "clicks",
  "event_time": "ts", "watermark_delay": "5 seconds",
  "columns": {"ts": "TIMESTAMP", "game_id": "VARCHAR", "amount": "DECIMAL(18,2)"}}');
```

| Function | Does |
| --- | --- |
| `duckwake_create_source(name, kind, options_json)` | Register a source. `kind` is `'kafka'`, or `'postgres'` ([PostgreSQL sources](#postgresql-sources)). |
| `duckwake_add_source_column(source, column, type)` | Add a column to a source while views read it. Views that already read the source keep their columns. |
| `duckwake_drop_source(name)` | Drop a source no view reads. A PostgreSQL source goes with its mirror tables and its replication slot. |
| `duckwake_sources()` | Every source with its options. |

The options:

| Option | Default | Meaning |
| --- | --- | --- |
| `brokers`, `topic` | required | Where to read. |
| `columns` | required | The columns and their DuckDB types, decoded from each JSON message. |
| `format` | `"json"` | `json`: each message is one row. `debezium`: Debezium change events, read by key ([Debezium sources](#debezium-sources)). |
| `event_time` | required for `json` | The column holding each event's time; windows and the watermark use it. A `debezium` source may leave it out. |
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
| `late_data` | none | A table for events that arrive after their window closed (windowed views), and for late corrections over lake inputs that update or delete (with a `_duckwake_weight` column). Without it they are counted in the checkpoint (`rows_late`) and dropped. |
| `max_state_rows` | none | Most rows the view's state may hold; past it the view fails alone ([limits](cli.md#limits)). |
| `max_late_rows` | none | Most late rows one epoch may write; past it the view fails alone. |
| `start` | `'earliest'` | Over lake tables and other views: `earliest` starts from the rows they already hold, `latest` from rows written after the view is created ([backfill](#backfill)). |
| `upsert_key` | none | Read the Kafka source as upserts, one row per key: the key column, or several comma-separated ([upsert views](#upsert-views)). |
| `upsert_version` | none | With `upsert_key`: the column whose value decides which message wins. Without it the later message wins. |
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
| `duckwake_reset(view, to_epoch)` | Rewind to an epoch and replay from there. Views that read this view's table with retractions follow the reset through its change feed; windowed views that read it, and views that read it insert-only, are emptied and backfill again (a window already closed would take the reset only as late corrections). A reset to an epoch whose snapshot has expired is refused. |
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
about 10 significant digits while a group's mean is within about 10⁴ times its standard deviation, and
`verify` compares DOUBLE and FLOAT columns to within a relative 10⁻⁹ (and 10⁻⁹ absolutely below 1: a statistic
that is truly 0 can come out as a residue such as 5×10⁻¹⁵ once deletes have been subtracted from its sums);
beyond that the difference of two large sums loses digits, and `verify` reports it. Subtract a constant near the mean
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
  and get an exact watermark. They take updates and deletes, of a table or of a view whose rows change: a
  change to a row whose window is still open revises that window (`min`, `max`, distinct counts and quantiles
  keep each value's count, so a deleted extreme gives way to the next value). A window is final once it closes,
  so a change to a row of a closed window, such as the delete of an old row, is a **late correction**: it goes to
  the `late_data` table with `_duckwake_weight` -1 (or +1 for a row added), or is counted in `rows_late` and
  dropped, and the window's row keeps counting the old version. `verify` adds the corrections back; without a
  late-data table it checks the view as long as no correction was dropped. To keep time buckets current
  instead, `GROUP BY time_bucket(INTERVAL 1 HOUR, _commit_ts)`.

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

## Upsert views

A topic of changes, where each message is the latest version of a record, reads as a table with `upsert_key`:

```sql
CALL duckwake_create_view('analytics.orders',
  'SELECT order_id, status, amount, ts FROM orders',
  upsert_key := 'order_id', upsert_version := 'ver');
```

The view's table is a plain DuckLake table with one row per key. A message replaces its key's row, and a
tombstone (a message with a key and no value) deletes it.

- **The key.** It comes from the message key: a JSON object's field of the same name (`{"order_id": 42}`), or,
  for one key column, the whole key (`42`, as JSON or plain text). A message without a key takes it from its
  value. Several key columns are comma-separated: `upsert_key := 'tenant, order_id'`.
- **Which message wins.** Messages apply in their order in the partition, so a key's messages must stay in
  one partition, as Kafka's default partitioner keeps them. Without `upsert_version`, each message replaces its
  key's row. With `upsert_version`, a message replaces the row only when its version is at least the row's,
  so a message older than the row changes nothing; of equal versions the later message wins, and NULL counts
  as the lowest version.
- **Deletes.** A tombstone deletes its key's row, and the view keeps nothing of a deleted key: a later
  message for that key creates it again, whatever its version.
- **Storage.** The view's state holds each key's current row, the source's columns, and nothing for deleted
  keys.
- **What the view can do.** It filters, selects and computes columns, and can look up lake tables. The key
  columns must be selected under their names. A filter applies to each key's current row: when an order's
  latest version no longer matches, its row leaves the table. Aggregates, `DISTINCT`, windows and `UNION ALL`
  are refused. Put them in another view that reads this one's table through the change feed, which carries its
  updates and deletes:

  ```sql
  SELECT status, count(*) AS n, sum(amount) AS total FROM analytics.orders GROUP BY status
  ```

- **Malformed messages.** A message that is not valid JSON, or is missing its event time, is a dead letter,
  as in any view. A tombstone is a dead letter (`error` 'tombstone') only for the views that do not read the
  source as upserts.

`verify` recomputes the table from the topic with the same rules. `duckwake sql` shows each message's `_key`
next to `_partition` and `_offset` when it queries the source.

## Debezium sources

A topic a Debezium connector writes (one table's changes) is a source with `"format": "debezium"`, and its
views are upsert views keyed by the table's primary key:

```sql
CALL duckwake_create_source('orders', 'kafka', '{
  "brokers": "localhost:19092", "topic": "pg.public.orders", "format": "debezium",
  "columns": {"order_id": "BIGINT", "status": "VARCHAR", "amount": "DECIMAL(18,2)", "updated_at": "TIMESTAMP"}}');
CALL duckwake_create_view('analytics.orders', 'SELECT * FROM orders', upsert_key := 'order_id');
```

- **Events.** The columns are read from the event's `after` row. Inserts (`c`), snapshot reads (`r`) and
  updates (`u`) replace their key's row, and a delete (`d`) removes it, as do the tombstones Debezium sends after
  deletes. The key comes from the message key, or for a delete without one from its `before` row. `before` is
  never used as the row to take back: the view's own state holds the current row. Other events (`t` truncate,
  `m` messages, a value that is not a change event) are dead letters whose error names them.
- **Both JSON forms.** Events and keys with `schemas.enable` (`{"schema": ..., "payload": ...}`) and without it.
- **Values.** Debezium sends `timestamp` columns as epoch numbers (`time.precision.mode`: milliseconds,
  microseconds or nanoseconds) and `date` columns as days since 1970-01-01; a `TIMESTAMP` or `DATE` column reads
  both those and text. A number is read in the unit its size fits, which is right for times from 1973 to 5138.
  Set `decimal.handling.mode` to `string` (or `double`) on the connector: its default sends decimals as
  base64 bytes, which are type conflicts here.
- **No event time needed.** An upsert view has no windows, so the source may leave out `event_time`; such a
  source has no watermark, and a window over it is refused. To aggregate, read the view's table from another
  view through the change feed.
- **Values Debezium leaves out.** For a PostgreSQL table without `REPLICA IDENTITY FULL`, Debezium sends
  `__debezium_unavailable_value` in place of a large (TOASTed) column an update did not change. The view
  cannot rebuild such a row, so it stops with an error naming the event. Set `REPLICA IDENTITY FULL` on the
  table, take a new Debezium snapshot of it, and replace the view.
- **Order.** As for any upsert view, a key's events must stay in one partition, which Debezium's keyed
  messages do. A connector restart may send some events again; they are applied again in order, so a row can
  briefly show an older version and then catch up. `upsert_version` on a column that grows with each change
  (an `updated_at`, a version) keeps older events from applying at all.

To try it locally, `docker compose --profile debezium up -d` adds a PostgreSQL with logical replication
(`localhost:15432`, user and database `shop`) and Kafka Connect with Debezium's PostgreSQL connector (REST API on
`localhost:8083`). `scripts/debezium_e2e_test.sh` registers a connector and reads its topics, and serves as a
worked example.

## PostgreSQL sources

A source of kind `postgres` keeps a copy of tables of a PostgreSQL database in the lake, read from a logical
replication slot, with no Kafka or Debezium in between:

```sql
CALL duckwake_create_source('shop', 'postgres', '{
  "dsn": "host=db.internal dbname=shop user=duckwake", "password": "env:SHOP_PG_PASSWORD",
  "tables": ["public.orders", "public.customers"]}');
CALL duckwake_start('shop');
```

Each table gets a mirror table, `lake.shop.orders` and `lake.shop.customers`, with the same columns and one row
per primary key. The source's ingest job, named like the source, first copies every table, then follows their
changes: inserts, updates, deletes, primary key changes and `TRUNCATE`. Views read the mirror tables like any
lake table, through their change feed ([views over lake tables](#views-over-lake-tables-and-other-views)):

```sql
CALL duckwake_create_view('analytics.orders_by_status',
  'SELECT status, count(*) AS n, sum(amount) AS amount FROM shop.orders GROUP BY ALL');
```

| Option | Default | Meaning |
| --- | --- | --- |
| `dsn` | required | A libpq connection string, without the password and without `options`. |
| `tables` | required | The tables to mirror; a name without a schema means `public`. |
| `password` | none | `env:NAME` or `file:/path`, resolved by each process that connects; without it libpq's `PGPASSWORD` or `.pgpass` apply. |
| `schema` | the source's name | The lake schema of the mirror tables. |
| `slot`, `publication` | `duckwake_<name>` | Created when missing; an existing publication must publish the tables and inserts, updates, deletes and truncates. |
| `epoch_interval` | `"2s"` | How often the job reads the slot. |
| `max_changes` | `100000` | Most changes one epoch reads; a transaction is always read whole, so a larger one makes a larger epoch. |

- **What the database needs.** `wal_level = logical`, PostgreSQL 13 or later, UTF8, and a role with the
  `REPLICATION` attribute that can read the tables (and own them, when DuckWake creates the publication). Each
  table needs a primary key, and `REPLICA IDENTITY` `DEFAULT` or `FULL`. Partitioned tables are refused: mirror
  their partitions. `create_source` checks all this and creates the publication and the slot.
- **Exactly once.** Each epoch applies whole transactions and writes the mirror rows and the slot position in one
  DuckLake transaction; the slot is advanced only after that commit, so it is never ahead of the lake and a
  restart resends nothing the mirrors lack. Transactions the slot sends again after a crash are skipped by their
  position. The first epoch copies every table under one snapshot taken after the slot was created, and the
  changes that snapshot already holds are skipped by transaction id.
- **Large values.** An update leaves an unchanged TOASTed value out of the change; the job takes it from the
  row's earlier version, so `REPLICA IDENTITY FULL` is not needed.
- **Types.** Every value crosses as PostgreSQL's text form and is cast to the mirror column's type:

  | PostgreSQL | Mirror |
  | --- | --- |
  | `smallint`, `integer`, `bigint`, `oid` | `SMALLINT`, `INTEGER`, `BIGINT`, `UINTEGER` |
  | `real`, `double precision` | `FLOAT`, `DOUBLE` |
  | `numeric(p,s)` with p up to 38 | `DECIMAL(p,s)` |
  | `numeric` without precision, or wider | `DOUBLE` |
  | `boolean`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `bytea` | `BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `BLOB` |
  | `json`, `jsonb` | `JSON` |
  | text types, and everything else (arrays, enums, `interval`, `timetz`, ranges, network types) | `VARCHAR`, holding PostgreSQL's text form |

  A value that does not fit its column (a `numeric` `NaN` in a `DECIMAL`, a date before year 1) stops the job
  with the table and the value.
- **When the job stops** (status `failed`, the reason in the worker's log): the slot is gone, was invalidated, or
  was advanced past the lake's position by something else, so changes are lost; or a table's columns changed
  (schema changes are not applied yet). For the slot, `duckwake_reset('shop', 0)` empties the mirror tables, and
  the next start takes a new snapshot; the views that read them take the deletes and the new rows through the
  change feed. For a changed table, drop the source (and the views that read it) and create it again. Errors that
  pass, such as the database restarting, are retried with backoff.
- **Checking it.** `duckwake_verify('shop')` compares every mirror with its table as read now, under one
  snapshot; while changes are in flight they show up as differences, so run it when writes have stopped.
  `duckwake_checkpoints('shop')` shows each epoch's slot position (`source_offsets`, an LSN in bytes) and `lag`,
  the bytes of WAL the server had flushed past it.
- **WAL.** The database keeps every change after the slot's position, so a source that is stopped or failing
  makes it keep WAL until its disk fills. Set `max_slot_wal_keep_size` on the server to bound that (the slot is
  then invalidated, and the source needs a reset), and watch `duckwake_lag_events`, which for a PostgreSQL
  source is bytes of WAL. An idle source moves its position every `--position-refresh` (5 minutes by default),
  or sooner after 64 MB of WAL.
- **Dropping it.** `duckwake_drop_source('shop')` is refused while views read a mirror table; stop the source
  first. It drops the mirror tables, the slot and the publication if DuckWake created it. When the database
  cannot be reached, the lake side is dropped anyway and the result says to drop the slot by hand.
- **Size.** The first copy goes through memory (DuckDB spills past `--memory-limit`) and commits in one DuckLake
  transaction; a table too large for that waits for the chunked backfill of
  [D4](design/database-cdc-sources.md#milestones). Each epoch scans the mirror tables it deletes from, as an
  upsert view scans its table.

## The same in the CLI

| SQL function (server, extension) | CLI |
| --- | --- |
| `duckwake_create_source(name, kind, options)` | `create-source NAME --kind kafka\|postgres --options JSON` |
| `duckwake_drop_source(name)` | `drop-source NAME` |
| `duckwake_create_view(name, sql, options)`, where `name` is the view's DuckLake table | `create-view SCHEMA.TABLE --sql [--late-data] [--checkpoint ducklake] [--epoch-interval] [--max-rows] [--max-state-rows] [--max-late-rows] [--upsert-key] [--upsert-version] [--if-exists fail\|update\|replace] [--replace]` |
| `duckwake_add_source_column(source, column, type)` | `add-source-column SOURCE COLUMN TYPE` |
| `duckwake_alter_view(view, ...)` | `alter-view NAME [--epoch-interval] [--max-rows] [--max-state-rows] [--max-late-rows]` |
| `duckwake_start` / `duckwake_stop` / `duckwake_pause` | `start NAME [--worker W]` (default `any`) / `stop` / `pause` |
| `duckwake_workers()` / `duckwake_drain(worker)` | `workers` / `drain WORKER` |
| `duckwake_jobs()` / `duckwake_checkpoints(view)` | `jobs` / `checkpoints NAME` |
| `duckwake_reset(view, to_epoch)` | `reset NAME --to-epoch N` |
| `duckwake_verify(view)` / `duckwake_explain(view)` | `verify NAME` / `explain NAME` |
| `TUMBLE(src, col, size)` / `HOP(src, col, size, slide)` | same, inside the view SQL |
