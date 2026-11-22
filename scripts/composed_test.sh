#!/usr/bin/env bash
# DuckWake as an extension of a DuckDB the user runs: a plain DuckDB 2.0 CLI loads the extension, starts a
# worker in its process (with a metrics endpoint) and serves itself over Quack. A second DuckDB CLI attaches
# with CONNECT and does everything in SQL: create the source and four views (one joins a lookup table), start
# them, and wait for them. Then it drops the lookup table: the worker fails, restarts on its own, the three
# other views carry on and the join view is marked failed. Recreate the table, start the view again, and all
# four must catch up and match the batch recomputation. Needs the DuckDB 2.0 CLI (DUCKDB, default `duckdb`).
source "$(dirname "$0")/lib.sh"
DUCKDB=${DUCKDB:-duckdb}
EXT=$PWD/${EXT:-target/release/duckwake.duckdb_extension}
QUACK=quack:localhost:${QUACK_PORT:-9495}; METRICS=127.0.0.1:${METRICS_PORT:-9465}; TOKEN=composed-test
[[ -f $EXT ]] || bash scripts/build_extension.sh
command -v "$DUCKDB" >/dev/null || { echo "needs the DuckDB 2.0 CLI ($DUCKDB)"; exit 1; }
# A client statement runs on the server after CONNECT; -list without headers keeps results easy to read.
client() { "$DUCKDB" -no-agent -list -noheader -c "ATTACH '$QUACK' AS dw (TOKEN '$TOKEN'); CONNECT dw; $1"; }

log "reset lake and topic; produce 40000 events"; reset_env
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate 20000 --count 40000 --late-every 997 --bad-every 1499 2>&1 | tail -1

log "host: plain DuckDB + LOAD duckwake + worker + quack_serve"
mkfifo "$OUT/host.in"
"$DUCKDB" -no-agent -unsigned <"$OUT/host.in" >"$OUT/host.log" 2>&1 &
HOST=$!
exec 3>"$OUT/host.in"
trap 'exec 3>&- 2>/dev/null; kill $HOST 2>/dev/null || true' EXIT
cat >&3 <<SQL
LOAD '$EXT';
CALL duckwake_connect('ducklake:$DUCKWAKE_CATALOG', data_path := '$DUCKWAKE_DATA_PATH');
CALL duckwake_worker_start(worker := 'host', pipeline := true, shared_readers := true, metrics := '$METRICS');
ATTACH 'ducklake:$DUCKWAKE_CATALOG' AS lake (DATA_PATH '$DUCKWAKE_DATA_PATH');
INSTALL quack; LOAD quack;
SELECT listen_uri FROM quack_serve('$QUACK', token = '$TOKEN');
SQL
for _ in $(seq 1 30); do curl -sf "http://$METRICS/healthz" >/dev/null && grep -q "$QUACK" "$OUT/host.log" && break; sleep 1; done
grep -q "$QUACK" "$OUT/host.log" || { cat "$OUT/host.log"; echo "FAIL: host did not start"; exit 1; }

log "client: create, start and wait over CONNECT"
games="SELECT 'g' || i AS game_id, ['Aurora', 'Basalt', 'Cobalt'][i % 3 + 1] || ' Studio' AS studio FROM range(20) t(i)"
client "CREATE SCHEMA lake.dim; CREATE TABLE lake.dim.games AS $games;
CALL duckwake_create_source('clicks', 'kafka', '{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"idle_timeout\":\"30 seconds\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}');
CALL duckwake_create_view('analytics.clicks_per_window', 'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue
  FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL', late_data := 'lake.analytics.clicks_late', epoch_interval := '1s');
CALL duckwake_create_view('analytics.big_spend', 'SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 90', epoch_interval := '1s');
CALL duckwake_create_view('analytics.game_totals', 'SELECT game_id, count(*) AS n, sum(amount) AS revenue FROM clicks GROUP BY ALL',
  epoch_interval := '1s');
CALL duckwake_create_view('analytics.studio_revenue', 'SELECT g.studio, count(*) AS n, sum(c.amount) AS revenue
  FROM clicks c JOIN dim.games g ON g.game_id = c.game_id GROUP BY ALL', epoch_interval := '1s');
CALL duckwake_start('clicks_per_window'); CALL duckwake_start('big_spend');
CALL duckwake_start('game_totals'); CALL duckwake_start('studio_revenue');
CALL duckwake_wait('big_spend'); CALL duckwake_wait('studio_revenue');
SELECT job_id, status, worker, epoch FROM duckwake_jobs();" | sed 's/^/  /'

log "drop the lookup table under the join view: the worker must fail and restart by itself"
client "DROP TABLE lake.dim.games" >/dev/null
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate 20000 --count 20000 2>&1 | tail -1
for _ in $(seq 1 40); do
  [[ "$(client "SELECT restarts FROM duckwake_worker_status()")" -ge 1 ]] && break; sleep 1
done
client "SELECT worker, running, restarts, left(last_error, 90) FROM duckwake_worker_status()" | sed 's/^/  /'
[[ "$(client "SELECT restarts FROM duckwake_worker_status()")" -ge 1 ]] || { echo "FAIL: no restart recorded"; exit 1; }
client "CALL duckwake_wait('big_spend')" >/dev/null
client "SELECT job_id, status FROM duckwake_jobs()" | sed 's/^/  /'
[[ "$(client "SELECT status FROM duckwake_jobs() WHERE job_id = 'analytics.studio_revenue'")" == failed ]] \
  || { echo "FAIL: the join view should be marked failed"; exit 1; }

log "recreate the table and start the join view again"
client "CREATE TABLE lake.dim.games AS $games; CALL duckwake_start('studio_revenue')" >/dev/null
for v in clicks_per_window big_spend game_totals studio_revenue; do client "CALL duckwake_wait('$v', timeout_s := 120)" | sed 's/^/  /'; done

log "metrics: over SQL and on /metrics"
client "SELECT metric, view, value FROM duckwake_metrics() WHERE metric IN ('duckwake_epoch', 'duckwake_worker_restarts_total') ORDER BY ALL" | sed 's/^/  /'
curl -s "http://$METRICS/metrics" | grep -E '^duckwake_(worker_up|worker_restarts_total)\{' | sed 's/^/  /'

log "verify over SQL"
rc=0
for v in clicks_per_window big_spend game_totals studio_revenue; do
  out=$(client "SELECT result FROM duckwake_verify('$v')")
  echo "  $v: $out"
  [[ $out == "VERIFY OK"* ]] || rc=1
done
client "CALL duckwake_worker_stop()" >/dev/null || true
[[ $rc == 0 ]] || { echo "FAIL: verify"; exit 1; }
echo "composed test passed"
