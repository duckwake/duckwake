#!/usr/bin/env bash
# The server front-end end to end: duckwaked runs a worker and serves the duckwake_* SQL functions over
# Quack. Everything below goes through SQL on that server (`duckwake sql --server`, which runs each
# statement with quack_query): create the source and the three test views, start them, watch
# duckwake_jobs(), read the views' tables, pause, reset and resume one, and verify all three. The metrics
# endpoint must report every view.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-2000}; DURATION=${DURATION:-30}
SERVER=quack:localhost:${QUACK_PORT:-9494}; METRICS=127.0.0.1:${METRICS_PORT:-9464}
export DUCKWAKE_QUACK_TOKEN=serve-test-token
DAEMON=${DAEMON:-$(dirname "$BIN")/duckwaked}
remote() { "$BIN" sql --server "$SERVER" "$@"; }

log "reset lake and topic"; reset_env
# shellcheck disable=SC2086
"$DAEMON" --worker w1 --quack "$SERVER" --metrics "$METRICS" $WORKER_FLAGS >"$OUT/server.log" 2>&1 &
SRV=$!
trap 'kill -TERM $SRV 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -sf "http://$METRICS/healthz" >/dev/null && break; sleep 1; done

log "create the source and views over SQL"
remote "CALL duckwake_create_source('clicks', 'kafka', '{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"idle_timeout\":\"30 seconds\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}')"
remote "CALL duckwake_create_view('analytics.clicks_per_window',
  'SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS max_amount
   FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL',
  late_data := 'lake.analytics.clicks_late', epoch_interval := '2s')"
remote "CALL duckwake_create_view('analytics.big_spend', 'SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 90', epoch_interval := '2s')"
remote "CALL duckwake_create_view('analytics.game_totals',
  'SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount, min(amount) AS lo, max(amount) AS hi
   FROM clicks GROUP BY ALL HAVING avg(amount) > 49.5', epoch_interval := '2s')"
remote "CALL duckwake_create_source('purchases', 'kafka', '{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC2\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"idle_timeout\":\"30 seconds\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}')"
remote "CALL duckwake_create_view('analytics.all_events', 'SELECT ts, game_id, user_id, amount, ''click'' AS kind FROM clicks WHERE amount >= 80
   UNION ALL SELECT ts, game_id, user_id, amount, ''purchase'' FROM purchases', epoch_interval := '2s')"
remote "CALL duckwake_create_view('analytics.active_users', 'SELECT DISTINCT game_id, user_id FROM clicks WHERE amount >= 50', epoch_interval := '2s')"
log "if_exists := 'update' over SQL, as dbt reruns a model"
GT="'SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount, min(amount) AS lo, max(amount) AS hi
   FROM clicks GROUP BY ALL HAVING avg(amount) > 49.5'"
out=$(remote "CALL duckwake_create_view('analytics.game_totals', $GT, epoch_interval := '2s', if_exists := 'update')" 2>&1)
[[ $out == *"unchanged view game_totals"* ]] && echo "rerun: unchanged" || { echo "FAIL: a rerun said: $out"; exit 1; }
if out=$(remote "CALL duckwake_create_view('analytics.game_totals', 'SELECT game_id, count(*) AS n FROM clicks GROUP BY ALL',
    epoch_interval := '2s', if_exists := 'update')" 2>&1); then
  echo "FAIL: a changed query was accepted: $out"; exit 1
fi
echo "changed query refused: $(echo "$out" | grep -o 'its query changed' | head -1)"
for v in "${VIEWS[@]}"; do remote "CALL duckwake_start('$v')"; done

log "produce $((RATE * DURATION)) events at $RATE/s"
produce --rate "$RATE" --count $((RATE * DURATION)) \
  --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
# big_spend is rewound to epoch 1 below, so it must have committed past it (a slow runner needs more than 10 s).
for _ in $(seq 1 60); do
  e=$(remote --csv "SELECT coalesce(max(epoch), 0) FROM duckwake_checkpoints('big_spend', n := 1)" 2>/dev/null || echo 0)
  (( ${e:-0} >= 3 )) && break
  sleep 1
done
remote "SELECT job_id, status, worker, epoch, freshness_s, lag FROM duckwake_jobs()"

log "pause, rewind and resume big_spend while events stream in"
remote "CALL duckwake_pause('big_spend')"
remote "CALL duckwake_reset('analytics.big_spend', 1)"
remote "CALL duckwake_start('big_spend')"
wait "$PRODUCER"; tail -1 "$OUT/producer.log"

end=$(topic_end); end2=$(topic_end "$TOPIC2")
for _ in $(seq 1 90); do
  done=1
  for v in "${VIEWS[@]}"; do caught_up "$v" "$end" "$end2" || done=0; done
  [[ $done == 1 ]] && break
  sleep 1
done
[[ $done == 1 ]] || { echo "FAIL: views did not reach the end of the topic"; exit 1; }

log "jobs, checkpoints and tables through the server"
remote "FROM duckwake_jobs()"
remote "SELECT epoch, rows_in, rows_out, rows_late, rows_dead FROM duckwake_checkpoints('analytics.game_totals', n := 3)"
remote "SELECT (SELECT count(*) FROM lake.analytics.clicks_per_window) AS windows, (SELECT count(*) FROM lake.analytics.big_spend) AS big_spend,
                (SELECT count(*) FROM lake.analytics.game_totals) AS game_totals"

log "metrics"
curl -s "http://$METRICS/metrics" >"$OUT/metrics.txt"
grep -E '^duckwake_(epoch|rows_in_total|freshness_seconds)\{' "$OUT/metrics.txt"
for v in "${VIEWS[@]}"; do
  grep -q "duckwake_epoch{worker=\"w1\",view=\"$v\"}" "$OUT/metrics.txt" || { echo "FAIL: no metrics for $v"; exit 1; }
done

log "verify over SQL"
rc=0
for v in "${VIEWS[@]}"; do
  out=$(remote --csv "SELECT result FROM duckwake_verify('$v')")
  echo "$v: $out"
  [[ $out == "VERIFY OK"* ]] || rc=1
done
[[ $rc == 0 ]] || { echo "FAIL: verify"; exit 1; }
echo "serve test passed"
