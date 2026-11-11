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

`run`, `serve` and `playground` take the worker flags described in [performance.md](performance.md).

## Workers and failover

Every worker heartbeats into the lake. When one dies, its views move to the live workers once its heartbeat
has not moved for `--lease-timeout` (default `30s`), and they continue from their last checkpoint; a worker
stopped with Ctrl-C or SIGTERM hands its views over at once. `start NAME` without `--worker` leaves the view
to whichever live worker claims it, and the workers spread such views by count. `workers` lists every worker
with its state, views and last heartbeat. `--lease-timeout off` keeps a worker from taking over other
workers' views. Nothing rebalances views onto a worker that joins later: start the workers before the views,
or move a view with `start NAME --worker W`.

## Playground

```sh
docker compose up -d --wait
cargo run --release -- playground      # then open http://localhost:8787 and click "Load demo"
```

A local web UI with an in-process worker (`playground`). It lets you:
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
