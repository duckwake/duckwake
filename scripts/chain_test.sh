#!/usr/bin/env bash
# F1 gate: a chain of three views over Kafka, each after the first reading the previous view's table through
# its DuckLake change feed: stateless over clicks, then windowed on _commit_ts, then windowed again on the
# upstream's window_end. The worker is killed over and over (kill -9 from outside, and aborts injected at every
# step of the epoch); after every crash all three views verify, each against a batch recomputation at its own
# checkpoint (Kafka for the first, time travel for the others), and again after draining. Then the rest of the
# gate: dropping a view that another view reads is refused, and an update to a lake table that a windowed
# view reads (windows need insert-only input) stops that view while the chain keeps running.
# WORKER_FLAGS="--pipeline ..." runs it pipelined.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}; DURATION=${DURATION:-40}; KILLS=${KILLS:-8}; EPOCH=${EPOCH:-2s}
read -r -a POINTS <<< "${POINTS:-external poll commit_begin commit_mid before_commit after_commit maintenance staged_ahead}"
CHAIN=(chain_big chain_per_10s chain_per_30s)

verify_chain() {
  local rc=0 v
  for v in "${CHAIN[@]}"; do
    if "$BIN" verify "$v" >"$OUT/verify_$v.txt" 2>&1; then
      [[ ${1:-} == quiet ]] || tail -1 "$OUT/verify_$v.txt"
    elif grep -q "job has no checkpoint yet" "$OUT/verify_$v.txt"; then
      : # crashed before the view's first commit: nothing to check yet
    else
      rc=1; cat "$OUT/verify_$v.txt"
    fi
  done
  return $rc
}

log "reset lake and topic"; reset_env
log "create the source and the chain"
"$BIN" create-source clicks --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"idle_timeout\":\"30 seconds\",\"decode\":\"${DECODE:-auto}\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}"
"$BIN" create-view analytics.chain_big --epoch-interval "$EPOCH" \
  --sql "SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 50" | sed -n 1p
"$BIN" create-view analytics.chain_per_10s --epoch-interval "$EPOCH" \
  --sql "SELECT window_start, window_end, game_id, count(*) AS n, sum(amount) AS revenue
         FROM TUMBLE(analytics.chain_big, _commit_ts, INTERVAL 10 SECOND) GROUP BY ALL" | sed -n 1p
"$BIN" create-view analytics.chain_per_30s --epoch-interval "$EPOCH" \
  --sql "SELECT window_start, game_id, sum(n) AS n, sum(revenue) AS revenue
         FROM TUMBLE(analytics.chain_per_10s, window_end, INTERVAL 30 SECOND) GROUP BY ALL" | sed -n 1p
for v in "${CHAIN[@]}"; do "$BIN" start "$v" --worker w1 >/dev/null; done

log "produce $((RATE * DURATION)) events at $RATE/s (every 997th 5 min late, every 1499th malformed)"
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) \
  --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!

failed=0
for i in $(seq 1 "$KILLS"); do
  point=${POINTS[$((RANDOM % ${#POINTS[@]}))]}
  if [[ $point == external ]]; then
    "$BIN" run $WORKER_FLAGS --worker w1 --flush-every 3s --merge-every 7s >>"$OUT/worker.log" 2>&1 &
    pid=$!; sleep "$((3 + RANDOM % 5)).$((RANDOM % 10))"; kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
    how="kill -9 from outside"
  else
    DUCKWAKE_CRASH_AT=$point DUCKWAKE_CRASH_PROB=0.4 "$BIN" run $WORKER_FLAGS --worker w1 --flush-every 3s --merge-every 7s >>"$OUT/worker.log" 2>&1 &
    pid=$!; for _ in $(seq 1 80); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$pid" 2>/dev/null && how="no crash in 8 s, killed" || how="aborted at $point"
    wait "$pid" 2>/dev/null || true
  fi
  # Epoch by epoch: whatever each view has committed so far must match its batch recomputation.
  if verify_chain quiet; then echo "run $i: $how; all three verify"; else echo "run $i: $how; VERIFY FAILED"; failed=1; fi
done
wait "$PRODUCER"; tail -1 "$OUT/producer.log"

log "drain to the end of the topic, then let the chain catch up"
"$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 &
pid=$!; end=$(topic_end)
for _ in $(seq 1 120); do [[ "$(committed chain_big)" == "$end" ]] && break; sleep 1; done
# The windows downstream are on commit times (_commit_ts, then window_end), so when the crashes left most rows
# to this run, they all fall in the 30 s window the drain committed them in, which closes only once the
# chain's watermark, carried by its own commits and the worker's heartbeats, passes its end: up to 30 s plus a
# 10 s window later. Wait for that rather than a fixed time.
for _ in $(seq 1 90); do
  [[ $("$BIN" sql --csv "SELECT count(*) FROM lake.analytics.chain_per_30s" 2>/dev/null | tail -1) -gt 0 ]] && break
  sleep 1
done
sleep 3
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), epochs committed: $(grep -c 'epoch committed' "$OUT/worker.log" || true)"
log "verify"; verify_chain || failed=1
rows=$(awk -F': ' '/expected_rows/ {gsub(/,/,"",$2); print $2}' "$OUT/verify_chain_per_30s.txt")
echo "chain_per_30s: $rows closed 30 s windows verified"
[[ ${rows:-0} -gt 0 ]] || { echo "the last view of the chain closed no window"; failed=1; }

log "dropping a view another view reads is refused"
"$BIN" stop chain_per_10s >/dev/null
if out=$("$BIN" drop chain_per_10s 2>&1); then echo "drop of chain_per_10s was not refused"; failed=1
elif [[ $out == *"is read by chain_per_30s"* ]]; then echo "$out" | sed -n 1p
else echo "drop of chain_per_10s failed for another reason: $out"; failed=1; fi
"$BIN" start chain_per_10s --worker w1 >/dev/null

log "an update to a lake table a windowed view reads stops that view, and only that view"
"$BIN" sql "CREATE SCHEMA lake.shop; CREATE TABLE lake.shop.orders (id INTEGER, amount DOUBLE)" >/dev/null
"$BIN" create-view analytics.orders_big --epoch-interval "$EPOCH" --sql "SELECT window_start, count(*) AS n, sum(amount) AS total
  FROM TUMBLE(shop.orders, _commit_ts, INTERVAL 10 SECOND) WHERE amount > 5 GROUP BY ALL" | sed -n 1p
"$BIN" start orders_big --worker w1 >/dev/null
"$BIN" sql "INSERT INTO lake.shop.orders SELECT i, i FROM range(20) t(i)" >/dev/null; sleep 6
"$BIN" sql "UPDATE lake.shop.orders SET amount = 0 WHERE id = 10" >/dev/null; sleep 6
status() { "$BIN" sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id = '$1'" --csv 2>/dev/null | tail -1; }
grep -o "view orders_big needs.*" "$OUT/worker.log" | sed -n 1p
[[ $(status orders_big) == failed ]] && echo "orders_big: failed, as it must" || { echo "orders_big is $(status orders_big), not failed"; failed=1; }
for v in "${CHAIN[@]}"; do [[ $(status "$v") == running ]] || { echo "$v is $(status "$v"), not running"; failed=1; }; done
kill -0 "$pid" 2>/dev/null && echo "worker still running; the chain is still running" || { echo "the worker died"; failed=1; }
kill -TERM "$pid"; wait "$pid" || true

[[ $failed == 0 ]] && echo "CHAIN TEST PASSED" || { echo "CHAIN TEST FAILED"; exit 1; }
