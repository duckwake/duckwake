#!/usr/bin/env bash
# The loadable extension end to end: one DuckDB 2.0 session (`duckwake sql --bare`, a plain in-memory DuckDB
# that allows unsigned extensions) loads duckwake.duckdb_extension, picks the test lake, creates the source
# and the three test views, runs a worker inside the session until the views have committed the topic, and
# reads their tables by attaching the same lake. The views must then match the batch recomputation.
source "$(dirname "$0")/lib.sh"
# The session creates three of the test views.
VIEWS=(clicks_per_window big_spend game_totals)
EVENTS=${EVENTS:-30000}
EXT=${EXT:-target/release/duckwake.duckdb_extension}
[[ -f $EXT ]] || bash scripts/build_extension.sh

log "reset lake and topic; produce $EVENTS events"; reset_env
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate 20000 --count "$EVENTS" \
  --late-every 997 --bad-every 1499 --eos 2>&1 | tail -1

log "one DuckDB session with the extension"
CATALOG=${DUCKWAKE_CATALOG}
"$BIN" sql --bare "
LOAD '$EXT';
CALL duckwake_connect('ducklake:$CATALOG', data_path := '$DUCKWAKE_DATA_PATH');
CALL duckwake_create_source('clicks', 'kafka', '{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"idle_timeout\":\"30 seconds\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}');
CALL duckwake_create_view('analytics.clicks_per_window',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS max_amount
   FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL',
  late_data := 'lake.analytics.clicks_late', epoch_interval := '1s');
CALL duckwake_create_view('analytics.big_spend', 'SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 90', epoch_interval := '1s');
CALL duckwake_create_view('analytics.game_totals',
  'SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount, min(amount) AS lo, max(amount) AS hi
   FROM clicks GROUP BY ALL HAVING avg(amount) > 49.5', epoch_interval := '1s');
CALL duckwake_worker_start(worker := 'notebook', stage_threads := 2, pipeline := true, shared_readers := true);
CALL duckwake_start('analytics.clicks_per_window');
CALL duckwake_start('analytics.big_spend');
CALL duckwake_start('analytics.game_totals');
CALL duckwake_wait('clicks_per_window', timeout_s := 120);
CALL duckwake_wait('big_spend', timeout_s := 120);
CALL duckwake_wait('game_totals', timeout_s := 120);
SELECT job_id, status, worker, epoch, rows_in, rows_out FROM duckwake_jobs();
ATTACH 'ducklake:$CATALOG' AS lake (DATA_PATH '$DUCKWAKE_DATA_PATH', READ_ONLY);
SELECT game_id, n, revenue FROM lake.analytics.game_totals ORDER BY game_id LIMIT 5;
CALL duckwake_worker_stop();
" 2>&1 | tee "$OUT/session.txt"
grep -q "worker notebook stopped" "$OUT/session.txt" || { echo "FAIL: the session did not finish"; exit 1; }

log "verify"; verify_all
echo "extension test passed"
