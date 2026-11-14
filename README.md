# DuckWake

Continuous SQL over Kafka into DuckLake, exactly once.

You write a view in SQL; DuckWake keeps a DuckLake table up to date with it as events arrive. It runs the view in
micro-batches (epochs), and each epoch commits the new rows, the view's state and the Kafka offsets in one DuckLake
transaction, so a crash never loses or duplicates output. Views can filter, join lake tables, aggregate over
tumbling and hopping windows, or keep running totals per group. Late events and undecodable messages go to
their own tables.

DuckWake is a proof of concept of streaming SQL for DuckDB, built on DuckDB 2.0 and DuckLake 1.1.

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
- [Monitoring and alerts](docs/alerts.md): the metrics, the Prometheus alerts and Grafana dashboard in `deploy/`, and what to do when each fires
- [Releases](docs/releasing.md): versioning, what a release publishes, and how to make one; [changelog](CHANGELOG.md)
- [Roadmap](docs/roadmap.md): what is done and what stands between it and production
- [Design documents](docs/design/): the reasoning behind the engine, with comparisons to other streaming engines

## Status

A proof of concept. Kafka with JSON only; filters, projections, lookup joins, `SELECT DISTINCT`, `UNION ALL`
across sources, windowed and global aggregates (`count`, `sum`, `avg`, `min`, `max`). A view can also read
another view's table or a lake table through its DuckLake change feed (`FROM analytics.clicks_per_min`), with
nothing to declare; when that table's rows are updated or deleted, the view retracts them (windowed views
need it insert-only). Workers heartbeat into the lake: the views of a worker that dies move to the live ones,
and views spread to workers that join, by measured load. Views can be replaced and sources gain columns while
they run; per-view limits fail one view instead of its worker, and a worker short of memory shrinks its epochs.
Sources connect with SASL (PLAIN, SCRAM) and TLS, data files can live on S3, and no
credential is stored in the lake. Every test checks the views against the same SQL run as one batch query. Not yet:
`CREATE CONTINUOUS VIEW` DDL, a signed extension and stream–stream joins.
See [gaps](docs/internals.md#simplifications-and-gaps) and the [roadmap](docs/roadmap.md) to production.

## Contributing

Issues and pull requests are welcome; [CONTRIBUTING.md](CONTRIBUTING.md) explains how to build and test, and
[SECURITY.md](SECURITY.md) how to report a vulnerability. Everyone taking part follows the
[code of conduct](CODE_OF_CONDUCT.md).

## License

DuckWake is released under the [MIT License](LICENSE). The licenses of the crates it includes are in
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).

DuckDB and DuckLake are trademarks of the DuckDB Foundation. DuckWake is an independent project, not affiliated
with or endorsed by the DuckDB Foundation or DuckDB Labs.
