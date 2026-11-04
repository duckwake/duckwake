# DuckWake

Continuous SQL over Kafka into DuckLake, exactly once.

You write a view in SQL; DuckWake keeps a DuckLake table up to date with it as events arrive. It runs the view in
micro-batches (epochs), and each epoch commits the new rows, the view's state and the Kafka offsets in one DuckLake
transaction, so a crash never loses or duplicates output. Views can filter, join lake tables, aggregate over
tumbling and hopping windows, or keep running totals per group. Late events and undecodable messages go to
their own tables.

DuckWake is a proof of concept of the
[DuckDB Streaming Extension design](https://github.com/duckwake/duckwake/tree/main/docs/design), built on DuckDB 2.0 and
DuckLake 1.1.

## Quick start

You need Docker (for PostgreSQL, the lake's catalog, and Redpanda, the Kafka) and Rust.

```sh
docker compose up -d --wait
cargo build --release                       # the first build compiles DuckDB (~6 min)
target/release/duckwaked --worker w1 --quack-token secret
```

Then, in the DuckDB 2.0 CLI:

```sql
ATTACH 'quack:localhost:9494' AS dw (TOKEN 'secret');
CONNECT dw;
CALL duckwake_create_source('clicks', 'kafka', '{"brokers":"localhost:19092","topic":"clicks","event_time":"ts",
  "columns":{"ts":"TIMESTAMP","game_id":"VARCHAR","amount":"DECIMAL(18,2)"}}');
CALL duckwake_create_view('analytics.clicks_per_min',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
   FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL');
CALL duckwake_start('clicks_per_min');
FROM duckwake_jobs();                       -- epoch, watermark, freshness, lag
FROM lake.analytics.clicks_per_min;         -- an ordinary DuckLake table
```

`target/release/duckwake produce --rate 2000` sends test events to the `clicks` topic.

## Three ways to run it

| | What it is | Docs |
| --- | --- | --- |
| **`duckwaked`** | A server: a worker plus the `duckwake_*` SQL functions, served over [Quack](https://duckdb.org/docs/2.0/quack/overview), with Prometheus metrics. Also a Docker image. | [server.md](docs/server.md) |
| **`LOAD duckwake`** | The extension, in your own DuckDB 2.0: the same functions and a worker in that process. Add `quack_serve` and it is a server too. | [extension.md](docs/extension.md) |
| **`duckwake`** | A CLI with a command per function, and a playground web UI. | [cli.md](docs/cli.md) |

## Docs

- [SQL functions](docs/sql.md): every `duckwake_*` function and its CLI equivalent
- [Performance](docs/performance.md): the worker flags and the benchmarks
- [Testing](docs/testing.md): what has been verified, and how to run the tests
- [Internals](docs/internals.md): how the code maps to the design, findings, gaps and layout
- [Roadmap](docs/roadmap.md): what is done and what stands between it and production

## Status

A proof of concept. Kafka with JSON only; filters, projections, lookup joins, windowed and global
aggregates (`count`, `sum`, `avg`, `min`, `max`). Every test checks the views against the same SQL run as one
batch query. Not yet: `CREATE CONTINUOUS VIEW` DDL, a signed extension, `DISTINCT` and stream–stream joins.
See [gaps](docs/internals.md#simplifications-and-gaps) and the [roadmap](docs/roadmap.md) to production.
