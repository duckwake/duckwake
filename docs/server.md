# Server: `duckwaked`

```sh
cargo build --release
target/release/duckwaked --worker w1 --quack-token secret      # Quack on quack:localhost:9494, metrics on :9464
# or the published image (each v* release; amd64 and arm64):
docker pull ghcr.io/harrydevforlife/duckwake:latest
# or in Docker, with its own catalog database and data volume:
docker compose --profile server up -d --build                 # token: duckwake-dev-token
```

`duckwaked` (the same as `duckwake serve`) runs a worker and serves SQL over Quack. In the DuckDB 2.0 CLI,
attach it and `CONNECT`; every statement then runs on the server:

```sql
ATTACH 'quack:localhost:9494' AS dw (TOKEN 'secret');
CONNECT dw;
CALL duckwake_create_source('clicks', 'kafka', '{"brokers":"localhost:19092","topic":"clicks","event_time":"ts",
  "columns":{"ts":"TIMESTAMP","game_id":"VARCHAR","user_id":"VARCHAR","amount":"DECIMAL(18,2)"}}');
CALL duckwake_create_view('analytics.clicks_per_min',
  'SELECT window_start, game_id, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL',
  epoch_interval := '10s', late_data := 'analytics.clicks_late');
CALL duckwake_start('clicks_per_min');                         -- on the server's own worker
FROM duckwake_jobs();
FROM lake.analytics.clicks_per_min;
DISCONNECT;                                                    -- back to the local session
```

Clients that prepare every statement before running it (the Rust `duckdb` crate, for one) fail under
`CONNECT` with "PREPARE succeeded but the prepared statement was not registered". From those, send each
statement with `quack_query`, which runs locally and ships one SQL string to the server:
`FROM quack_query('quack:localhost:9494', 'FROM duckwake_jobs()', token = 'secret')`.
`duckwake sql --server quack:localhost:9494 --token secret "CALL ...; FROM duckwake_jobs()"` does that for you.

The functions are listed in [sql.md](sql.md).

## Security

Quack serves plain HTTP on localhost only: bound to another address it switches to HTTPS with a self-signed
certificate, so clients must use that address over TLS. To reach the server from other machines, keep it on
localhost and put a TLS reverse proxy in front, as [Quack's security notes](https://duckdb.org/docs/2.0/quack/security.html)
recommend; the image and the compose profile bind Quack and metrics to localhost.

## Metrics and supervision

`GET :9464/metrics` serves per-view freshness, watermark delay, lag, epoch,
stage time and row, late-row and dead-letter counters, plus commit time and conflicts, in the Prometheus
format. A worker that fails restarts from its last checkpoint after 5 s, and the server stays up
(`duckwake_worker_status()` shows the restarts and the last error). `--quack none`
or `--metrics none` turns either off; the worker flags (`--stage-threads`, `--pipeline`, ...) are those of `run`.
