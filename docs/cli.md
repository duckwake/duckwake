# CLI and playground

## Walkthrough

```sh
docker compose up -d --wait            # PostgreSQL (catalog) on :55432, Redpanda on :19092
cargo build --release                  # first build compiles DuckDB (~6 min)
B=target/release/duckwake

$B init
$B create-source clicks --options '{"brokers":"localhost:19092","topic":"clicks","event_time":"ts",
  "watermark_delay":"5 seconds","columns":{"ts":"TIMESTAMP","game_id":"VARCHAR","user_id":"VARCHAR","amount":"DECIMAL(18,2)"}}'
$B create-view analytics.clicks_per_min --late-data lake.analytics.clicks_late \
  --epoch-interval 10s \
  --sql "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
         FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL"
$B start clicks_per_min --worker w1
$B run --worker w1 &                   # the worker; one structured log line per job per epoch
$B produce --rate 2000 --late-every 500 --bad-every 700
$B jobs                                # status, epoch, watermark, freshness, lag, totals
$B sql "FROM lake.analytics.clicks_per_min ORDER BY window_start DESC LIMIT 5"
$B verify clicks_per_min               # differential exactly-once check
$B explain clicks_per_min              # the view's circuit and one epoch's SQL
```

## Views and sources

A view is named by the DuckLake table it maintains (`analytics.clicks_per_min`; a bare name means the
`main` schema), and `create-view` fails if that table already exists. Its job id is the table name, so the
other commands take either `clicks_per_min` or `analytics.clicks_per_min`. `drop` removes the view's table
too (`--keep-table` keeps it); a late-data table is kept. The old `--sink` flag still works but is
deprecated. Control data lives in `lake.__duckwake`, and output rows carry `_duckwake_epoch`. A lake created
before that rename (`__stream`, `_stream_epoch`) is migrated the first time a new binary opens it.

Sources decode JSON with Arrow (`"decode": "auto"`, the default) when every column is TIMESTAMP, VARCHAR,
BOOLEAN, an integer, DOUBLE or DECIMAL. A message whose JSON Arrow can't read exactly as DuckDB's JSON
functions would goes through the SQL decode instead, so the rows and dead letters are the same either way.
`"decode": "sql"` decodes everything in SQL; `"decode": "arrow"` refuses a source whose column types Arrow
can't decode.

A source reaches its brokers in plaintext unless its options say otherwise, with the names Kafka clients use:
`security_protocol` (`plaintext`, `ssl`, `sasl_plaintext` or `sasl_ssl`), `sasl_mechanism` (`PLAIN`,
`SCRAM-SHA-256` or `SCRAM-SHA-512`), `sasl_username` and `sasl_password`, and with TLS `ssl_ca_location`
(a PEM file of CAs; the public roots otherwise) and, for brokers that want client certificates,
`ssl_certificate_location` and `ssl_key_location`. The password is never stored in the lake: it must be
`env:NAME` or `file:/path`, read by each worker when it connects, so every worker that may run the view needs
that variable or file, and so do the paths. A source whose brokers cannot be reached within 10 s when a view
starts (a wrong password or certificate, or the wrong address) marks the view `failed`, with the reason;
`start` it again once fixed.

```sh
$B create-source clicks --options '{"brokers":"kafka-1:9094","topic":"clicks","event_time":"ts",
  "security_protocol":"sasl_ssl","sasl_mechanism":"SCRAM-SHA-512","sasl_username":"duckwake",
  "sasl_password":"env:KAFKA_PASSWORD","columns":{"ts":"TIMESTAMP","game_id":"VARCHAR"}}'
```

`run`, `serve` and `playground` take the worker flags described in [performance.md](performance.md).

## Changing views and sources

Views and sources change while workers run them:

- **`create-view NAME --replace`** (`CREATE OR REPLACE`, `duckwake_create_view(..., replace := true)`) swaps a
  view's definition in one transaction. The new definition starts over like a new view: Kafka sources from their
  `start` position (`earliest` by default: everything the topic still holds), lake tables from now. Its table,
  state, checkpoints and dead letters are replaced, and so are the rows of a late-data table no other view writes.
  The view keeps its status, worker and pin (a failed one becomes stopped). A worker still running the old
  definition is fenced and starts the new one at its next refresh. A view that other views read cannot be
  replaced: drop those first.
- **`create-view NAME --if-exists update`** (`if_exists := 'update'`) makes an existing view match the command
  without recomputing it, so a deploy script or dbt can run it again and again. When nothing differs it does
  nothing (`unchanged`); when only the epoch interval, rows per epoch or limits differ it changes them as
  `alter-view` does (`altered`); when what the view computes differs (its query, table, late-data table,
  checkpoint or inlining) it fails and asks for `--replace`. Whitespace outside quotes and a trailing `;` do
  not count as a change. Options left out take their defaults, as for a new view. `--if-exists fail` is the
  default, and `--if-exists replace` is `--replace`. The first word of the output, and of the SQL function's
  result, says what happened: `created`, `unchanged`, `altered` or `replaced`.
- **`add-source-column SOURCE COLUMN TYPE`** (`duckwake_add_source_column`) adds a column to a source. The views
  that already read the source keep the columns they were created with, and keep running unchanged; replace a
  view to use the new column. Events without the field get NULL. Columns cannot be removed or retyped: create
  a new source for that.
- **`alter-view NAME`** (`duckwake_alter_view`) changes how a view runs, not what it computes:
  `--epoch-interval`, `--max-rows`, `--max-state-rows`, `--max-late-rows` (0 removes a limit). A running view is
  restarted by its worker with the new settings and continues from where it was.

```sh
$B add-source-column clicks country VARCHAR
$B create-view analytics.big_spend --replace --sql "SELECT ts, game_id, country, amount FROM clicks WHERE amount >= 90"
$B alter-view clicks_per_min --epoch-interval 30s
$B create-view analytics.clicks_per_min --if-exists update --epoch-interval 30s --sql "..."   # unchanged
```

`stop` and `pause` fence the view's worker at once: an epoch it has in flight does not land, so `drop` or
`reset` can follow straight away.

A view over lake tables or other views first backfills the rows its inputs already hold, `--max-rows` per epoch
(`--start latest` skips them); `reset` empties the windowed views that read it, which backfill again. A view
created stopped holds its start snapshot until it starts. `reset --to-epoch N` refuses an epoch whose positions
have expired; `--to-epoch 0` recomputes a view over lake inputs from the rows they hold at its next start. Maintenance
(`--expire-every`, `--expire-older-than`) keeps every snapshot from the oldest position of a view that is not
dropped, so a view paused for a week holds a week of snapshots; `duckwake_snapshots_held_seconds` shows it. Each
running view moves its position to the newest snapshot every `--position-refresh` (default `5m`; `position_refresh :=` in
`duckwake_worker_start`) even when its
inputs did not change, so that idle views hold nothing back. A view whose position has expired anyway (the lake
was maintained by another tool) fails with the snapshot it needs; `create-view --replace` it.

## Limits

A view that grows without bound should fail alone, not slow down or take down its worker:

- **`--max-state-rows N`**: the view fails once its state holds more than N rows (open windows, groups or distinct
  rows; checked every 10 s, `duckwake_state_rows`).
- **`--max-late-rows N`**: the view fails when one epoch writes more than N late rows: a watermark or a producer
  clock that is wrong, not ordinary late data. The epoch that went over is not committed.

A failed view says why in the worker log and in `duckwake_views_failed_total`. Fix the cause or raise the limit
(`alter-view`), then `start` it: it continues from its last checkpoint. Both are set at `create-view` or later
with `alter-view`; by default views have none.

**Memory.** Each worker is one DuckDB instance; `--memory-limit` (`DUCKWAKE_MEMORY_LIMIT`, e.g. `2GB`) caps it.
The default is DuckDB's, 80% of the machine's RAM, which is too much with several workers on a machine. Past the
limit, DuckDB spills to `--temp-dir`/<worker> (default `.tmp/<worker>`, emptied at start; put it on local disk,
not tmpfs). What an epoch needs grows with DuckDB's threads (`--threads`, `DUCKWAKE_THREADS`, default one per
core): under a small limit, fewer threads let more of it spill instead of running out (DuckDB suggests 1 to 4 GB
per thread). An epoch that still does not fit runs out of memory: the worker then starts again from its
checkpoints with half as many rows per epoch, in a fresh instance (logged, `duckwake_out_of_memory_total` and
`duckwake_epoch_rows_cap`), and keeps that cap until it restarts. Output stays exact. Below 1,000 rows per epoch
it gives up and exits. Kafka buffers and the Arrow decode live outside the DuckDB limit: give the process a few
hundred MB more than `--memory-limit` (`scripts/limits_test.sh`: a 64 MB limit with 12 threads peaked at about 330 MB resident).

## Storage and credentials

`--catalog` (`DUCKWAKE_CATALOG`) is the DuckLake catalog and `--data-path` (`DUCKWAKE_DATA_PATH`) where its
Parquet files go: a local path, or `s3://bucket/prefix/` on S3 or any S3-compatible store. None of the
credentials below are stored in the lake; each process that opens the lake reads them from its environment.

- **Catalog password.** Leave `password=` out of the connection string and set `PGPASSWORD`, or use a
  `.pgpass` file (`passfile=` in the string names another one); PostgreSQL's client library reads them. A
  worker started with the password in the string warns, since process listings and shell history show it.
- **S3.** The standard AWS variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
  `AWS_REGION`. Without keys the AWS credential chain is used (config files, instance and container roles).
  For MinIO, GCS's S3 API and the like, `AWS_ENDPOINT_URL` (`http://` turns TLS off) switches to path-style
  URLs; `DUCKWAKE_S3_URL_STYLE=vhost` keeps virtual-host style.
- **Kafka.** In the source's options as `env:` or `file:` references (above).

```sh
export DUCKWAKE_CATALOG="postgres:dbname=lake host=pg.internal user=duckwake" PGPASSWORD=...
export DUCKWAKE_DATA_PATH=s3://analytics-lake/duckwake/ AWS_REGION=eu-west-1   # keys from the instance role
$B run --worker w1
```

Anything else that reads the lake, such as a DuckDB session that attaches it, needs the same access: its
own S3 secret (`CREATE SECRET (TYPE s3, ...)`) and catalog credentials.

## Workers, failover and rebalancing

Run one worker per machine: a worker uses every core it is given (`--stage-threads`, `--pipeline`), so a second one
on the same machine only adds failover between processes. For failover that survives a machine, run workers on
two or more machines. Give each `--metrics 0.0.0.0:9464`; Prometheus finds them through the lake
([alerts.md](alerts.md)).

Every worker heartbeats into the lake, and the workers move views between themselves:

- **A view's topic does not exist yet**: the view waits for it (logged once) and starts when it appears.
- **A worker dies** (killed, frozen, its machine gone): its views move to the live workers once its heartbeat
  has not moved for `--lease-timeout` (default `30s`), and continue from their last checkpoint. Only time
  in which the watching worker's own heartbeats land counts, so a catalog stall that holds everyone up (a
  PostgreSQL failover, a slow disk) moves nothing.
- **A worker stops cleanly** (Ctrl-C, SIGTERM): it is taken to be restarting, so its views wait
  `--stop-grace` (default `60s`) for it and do not move during a rolling restart. To take a worker out for
  good, `drain` it first: `duckwake drain w3` (or `CALL duckwake_drain('w3')`) hands all its views to the
  others within a refresh or two, and it takes none until it restarts.
- **A worker joins, or loads drift apart**: every `--rebalance-every` (default `30s`) the busiest worker
  that has a view it may move gives up to `--rebalance-max-moves` (2) views to the least loaded one, when it
  holds two views more than that worker, or when for three rounds in a row it carries more than
  `1 + --rebalance-epsilon` (1.2) times the average load and at least a tenth of a worker above it. Load is what each view costs (its staging, source and commit time per second, a moving average),
  and a target that already reads the view's source is preferred. A view that just moved or started stays
  put for `--rebalance-cooldown` (default `5m`). `--rebalance-every off` turns this off.

`start NAME` leaves the view to whichever live worker claims it, and lets rebalancing move it.
`start NAME --worker W` pins it to W: rebalancing never moves it, and after a failover it goes back to W once
W is live again. `workers` lists every worker with its state (`running`, `draining`, `stopped`), views,
load and last heartbeat; `duckwake_view_cost`, `duckwake_worker_load`, `duckwake_failover_claims_total` and
`duckwake_handoffs_total` are in `/metrics`. `--lease-timeout off` keeps a worker from taking over other
workers' views, and turns rebalancing off for it. With a PostgreSQL catalog, a worker also has PostgreSQL
end any of its sessions that stalls inside a transaction for longer than the lease timeout, so a frozen
worker cannot block the others' commits; if your catalog string already passes `options`, add
`-c idle_in_transaction_session_timeout=30000` to it.

## Playground

```sh
docker compose up -d --wait
cargo run --release -- playground      # then open http://localhost:8787 and click "Load demo"
```

A local web UI with an in-process worker (`playground`); `--metrics 127.0.0.1:9464` serves its Prometheus
metrics for the Grafana dashboard ([alerts.md](alerts.md)). It shares the local Redpanda's `clicks` topic with
the test scripts: stop it while they run. It lets you:
- create sources (the Kafka topic is created for you), run a test-data generator with late and malformed events, or send your own JSON;
- create lake tables, e.g. dimension tables for lookup joins;
- create continuous views from templates (unsupported SQL is rejected with the compiler's message);
- start, pause, stop, reset and drop views, and run Verify;
- pick a source's JSON decode (auto, arrow or sql) and see, per epoch, how many events went through the
  SQL decode (with Arrow, only the ones it can't read exactly as DuckDB would);
- watch epochs, watermarks, open windows, late rows and dead letters;
- query anything in the SQL console (`schema.table` means the lake) and follow the Activity log;
- query a source by name (`SELECT * FROM clicks`): each query reads the newest 100,000 events of the
  topic into a temporary table with the source's columns plus `_partition`, `_offset`, `_error` and
  `_payload`. It's a snapshot for exploring and debugging, not a stream. `duckwake sql` does the same.

The demo creates a `clicks` source at 300 events/s, a `dim.games` table, and four views: a tumbling window with a lookup join, a hopping window, a filter, and a global aggregate with `HAVING` (an upsert sink: one row per group, replaced when it changes). The playground uses the same lake as the CLI, so `duckwake jobs` sees its views. The test scripts use their own catalog database (`duckwake_test`) and data path, and never touch the playground's lake.
