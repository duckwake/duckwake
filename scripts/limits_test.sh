#!/usr/bin/env bash
# Limits gate (P4): a view that goes over a limit fails alone, and a worker short of memory slows down instead
# of failing.
#   1. Per-view limits, next to the five test views on one worker: late_guard (max_late_rows 1) and
#      state_guard (max_state_rows 50) fail with the limit named in the log and duckwake_views_failed_total;
#      the other views go on. Raised with alter-view and started again, both catch up, and every view
#      matches its batch recomputation.
#   2. Memory: a backlog of large events through a high-cardinality view, under a 64 MB DuckDB limit with 12
#      DuckDB threads and at most 32 MB of spill (--max-spill). DuckDB spills before it runs out, so without the
#      spill cap a runner with room to spill fits every epoch and the limit is never exercised. Epochs that do
#      not fit run out of memory; the worker halves its rows per epoch (duckwake_out_of_memory_total,
#      duckwake_epoch_rows_cap), spills to its own directory within the cap, and the view matches its
#      recomputation.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}; DURATION=${DURATION:-90}
BACKLOG=${BACKLOG:-1500000}; MEMORY_LIMIT=${MEMORY_LIMIT:-64MB}; THREADS=${THREADS:-12}; MAX_SPILL=${MAX_SPILL:-32MB}
METRICS=127.0.0.1:19187
PARTS=${PARTS:-12}   # which parts to run
failed=0

trap '[[ -n ${W:-} ]] && kill -9 "$W" 2>/dev/null || true' EXIT
q() { "$BIN" sql --csv "$1" 2>/dev/null | sed -n 1p; }
status() { q "SELECT status FROM lake.__duckwake.jobs WHERE job_id IN ('$1', 'analytics.$1')"; }
epoch() { q "SELECT coalesce(max(epoch), 0) FROM lake.__duckwake.checkpoints WHERE job_id IN ('$1', 'analytics.$1')"; }
metric() { curl -s "http://$METRICS/metrics" | awk -v m="$1" '$1 ~ "^"m"[{]" {print $2; exit}'; }
check() {
  local what=$1 bound=$2 cond=$3 t0=$SECONDS
  while ((SECONDS - t0 <= bound)); do
    if eval "$cond"; then echo "$what: $((SECONDS - t0))s"; return 0; fi
    sleep 1
  done
  echo "FAIL: $what after $((SECONDS - t0))s"; failed=1
}
run_worker() {
  # shellcheck disable=SC2086
  "$BIN" "$@" run $WORKER_FLAGS --worker w1 --lease-timeout off --metrics "$METRICS" >>"$OUT/w1.log" 2>&1 &
  W=$!
  for _ in $(seq 1 40); do curl -sf "http://$METRICS/healthz" >/dev/null && return; sleep 0.25; done
}
catch_up() {
  local end end2
  end=$(topic_end); end2=$(topic_end "$TOPIC2")
  for _ in $(seq 1 240); do
    done=1
    for v in "$@"; do caught_up "$v" "$end" "$end2" || done=0; done
    [[ $done == 1 ]] && return 0
    kill -0 "$W" 2>/dev/null || { echo "FAIL: the worker died"; tail -5 "$OUT/w1.log"; failed=1; return 1; }
    sleep 1
  done
  echo "FAIL: did not catch up"; "$BIN" jobs; failed=1
}

if [[ $PARTS == *1* ]]; then
log "1. per-view limits next to the test views"
reset_env
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
setup_views 2s "10 SECOND" "5 seconds"
"$BIN" create-view analytics.late_guard --late-data lake.analytics.late_guard_late --epoch-interval 2s --max-late-rows 1 \
  --sql "SELECT window_start, count(*) AS n FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL" | sed -n 1p
"$BIN" create-view analytics.state_guard --epoch-interval 2s --max-state-rows 50 \
  --sql "SELECT user_id, count(*) AS n FROM clicks GROUP BY ALL" | sed -n 1p
ALL=("${VIEWS[@]}" late_guard state_guard)
for v in "${ALL[@]}"; do "$BIN" start "$v" --worker w1 >/dev/null; done
run_worker
# Late events come every 97th: a 2 s epoch has about 20.
produce --rate "$RATE" --count $((RATE * DURATION)) --late-every 97 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
check "late_guard failed by its late-row limit" 60 '[[ $(status late_guard) == failed ]]'
check "state_guard failed by its state limit" 40 '[[ $(status state_guard) == failed ]]'
grep -h 'view stopped' "$OUT/w1.log" | grep -o 'error=.*' | cut -c1-150
grep -q 'max_late_rows of 1' "$OUT/w1.log" && grep -q 'max_state_rows of 50' "$OUT/w1.log" \
  || { echo "FAIL: the log does not name the limits"; failed=1; }
[[ $(metric duckwake_views_failed_total) == 2 ]] && echo "duckwake_views_failed_total 2" \
  || { echo "FAIL: duckwake_views_failed_total is $(metric duckwake_views_failed_total)"; failed=1; }
[[ -n $(metric duckwake_state_rows) ]] && echo "duckwake_state_rows reported" || { echo "FAIL: no duckwake_state_rows"; failed=1; }
for v in "${VIEWS[@]}"; do [[ $(status "$v") == running ]] || { echo "FAIL: $v is $(status "$v")"; failed=1; }; done
e0=$(epoch game_totals); sleep 6
(( $(epoch game_totals) > e0 )) && echo "the other views go on" || { echo "FAIL: game_totals stopped committing"; failed=1; }

log "raise both limits and start them again"
"$BIN" alter-view late_guard --max-late-rows 0
"$BIN" alter-view state_guard --max-state-rows 100000
for v in late_guard state_guard; do "$BIN" start "$v" --worker w1 >/dev/null; done
wait "$PRODUCER"; tail -1 "$OUT/producer.log"
catch_up "${ALL[@]}"
for v in late_guard state_guard; do [[ $(status "$v") == running ]] || { echo "FAIL: $v is $(status "$v")"; failed=1; }; done
VIEWS=("${ALL[@]}"); verify_all || failed=1
kill -TERM "$W"; wait "$W" 2>/dev/null || true
fi

if [[ $PARTS == *2* ]]; then
log "2. a backlog of $BACKLOG large events under a $MEMORY_LIMIT memory limit and $MAX_SPILL of spill"
reset_env
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
setup_views 2s "10 SECOND" "5 seconds" >/dev/null
"$BIN" create-view analytics.every_event --max-rows 1000000 \
  --sql "SELECT DISTINCT user_id, ts, amount FROM clicks" | sed -n 1p
produce --rate 50000 --count "$BACKLOG" --pad-bytes 1000 --eos >/dev/null 2>&1
"$BIN" start every_event --worker w1 >/dev/null
t0=$SECONDS
run_worker --memory-limit "$MEMORY_LIMIT" --threads "$THREADS" --max-spill "$MAX_SPILL"
peak=0; spill=0
end=$(topic_end)
while ! caught_up every_event "$end" 0; do
  kill -0 "$W" 2>/dev/null || { echo "FAIL: the worker died"; tail -5 "$OUT/w1.log"; failed=1; break; }
  r=$(ps -o rss= -p "$W" | tr -d ' '); ((r > peak)) && peak=$r
  s=$(du -sk .tmp/w1 2>/dev/null | cut -f1); ((${s:-0} > spill)) && spill=$s
  ((SECONDS - t0 < 600)) || { echo "FAIL: did not catch up in 600 s"; failed=1; break; }
  sleep 0.5
done
echo "caught up in $((SECONDS - t0))s; peak RSS $((peak / 1024)) MB, peak spill $((spill / 1024)) MB"
grep -h 'ran out of memory' "$OUT/w1.log" | grep -o 'rows_per_epoch=[0-9]*' | tr '\n' ' ' || true; echo
oom=$(metric duckwake_out_of_memory_total)
((${oom:-0} >= 1)) && echo "duckwake_out_of_memory_total $oom, duckwake_epoch_rows_cap $(metric duckwake_epoch_rows_cap)" \
  || { echo "FAIL: no epoch ran out of memory, so the limit was not exercised"; failed=1; }
((spill > 0)) || { echo "FAIL: DuckDB never spilled"; failed=1; }
kill -TERM "$W"; wait "$W" 2>/dev/null || true
"$BIN" verify every_event | tail -1 || failed=1
fi

[[ $failed == 0 ]] && echo "LIMITS TEST PASSED" || { echo "LIMITS TEST FAILED"; exit 1; }
