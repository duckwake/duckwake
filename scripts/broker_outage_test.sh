#!/usr/bin/env bash
# Broker-restart drill (#26): a view whose Kafka brokers cannot be reached waits for them instead of failing.
#  1. Kafka is stopped before the worker starts the views: every view waits (`duckwake jobs` shows waiting_for
#     "the Kafka brokers", duckwake_views_waiting counts them) and none is failed. Kafka back, every view
#     starts on its own within BOUND seconds, with no manual step.
#  2. Kafka restarts (stopped for DOWN seconds) while the views run and events stream in: they keep running.
#  3. Kafka is down when the worker is killed and another takes its views over after the lease timeout: they
#     wait on the new worker and start once Kafka is back.
# After the topics are drained every view verifies, no view was marked failed, and the live worker never exited.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-500}; DOWN=${DOWN:-15}; LEASE_TIMEOUT=${LEASE_TIMEOUT:-10}; BOUND=${BOUND:-90}
PORTS=([1]=9474 [2]=9475)
failed=0

kafka_down() { docker compose stop redpanda >/dev/null 2>&1; }
kafka_up() { docker compose up -d --wait redpanda >/dev/null 2>&1 || { echo "Kafka did not come back"; return 1; }; }
run_worker() {
  "$BIN" run $WORKER_FLAGS --worker "w$1" --lease-timeout "${LEASE_TIMEOUT}s" --metrics "127.0.0.1:${PORTS[$1]}" >>"$OUT/w$1.log" 2>&1 &
  echo $! >"$OUT/w$1.pid"
}
pid() { cat "$OUT/w$1.pid"; }
alive() { kill -0 "$(pid "$1")" 2>/dev/null; }
metric() { curl -s "http://127.0.0.1:${PORTS[$1]}/metrics" | awk -v m="$2" '$1 ~ "^"m"[{]" {print $2}' | head -1; }
# Views `duckwake jobs` shows waiting for the brokers, and views started after waiting for them (worker $1).
waiting() { "$BIN" jobs 2>/dev/null | grep -c "the Kafka brokers" || true; }
started_after_wait() { grep -c "done waiting for the Kafka brokers; job started" "$OUT/w$1.log" || true; }
owners() {
  local q="" v
  for v in "${VIEWS[@]}"; do q+="${q:+ UNION ALL }SELECT coalesce(owner_worker, '-') FROM lake.__duckwake.\"lease_analytics.$v\""; done
  "$BIN" sql --csv "$q" 2>/dev/null | sort | uniq -c | awk '{printf "%s:%s ", $2, $1}'
}
# Waits until $1 is true, at most $2 seconds; prints the seconds it took.
until_true() {
  local t0=$SECONDS
  while (( SECONDS - t0 <= $2 )); do eval "$1" && { echo $((SECONDS - t0)); return 0; }; sleep 1; done
  echo $((SECONDS - t0)); return 1
}
trap 'docker compose up -d --wait redpanda >/dev/null 2>&1 || true; for w in 1 2; do [[ -f $OUT/w$w.pid ]] && { kill -9 "$(pid $w)" 2>/dev/null || true; }; done; true' EXIT
n=${#VIEWS[@]}

log "reset lake and topics"; reset_env
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
setup_views 2s "10 SECOND" "5 seconds"

log "1. Kafka is down before the views start"
kafka_down
for v in "${VIEWS[@]}"; do "$BIN" start "$v" >/dev/null; done
run_worker 1
if secs=$(until_true '[[ $(waiting) == '"$n"' ]]' 30); then echo "all $n views wait for the brokers (${secs}s)"
else echo "FAIL: $(waiting) of $n views wait for the brokers"; "$BIN" jobs; failed=1; fi
echo "w1: duckwake_views_waiting $(metric 1 duckwake_views_waiting), duckwake_views_failed_total $(metric 1 duckwake_views_failed_total)"
[[ $(metric 1 duckwake_views_waiting) == "$n" && $(metric 1 duckwake_views_failed_total) == 0 ]] || { echo "FAIL: metrics"; failed=1; }
sleep 15   # the retries back off meanwhile
kafka_up
if secs=$(until_true '[[ $(started_after_wait 1) == '"$n"' ]]' "$BOUND"); then echo "Kafka back: all $n views started on their own in ${secs}s (bound ${BOUND}s)"
else echo "FAIL: $(started_after_wait 1) of $n views started"; failed=1; fi
[[ $(metric 1 duckwake_views_waiting) == 0 && $(waiting) == 0 ]] || { echo "FAIL: views still shown waiting"; failed=1; }

log "2. Kafka restarts while the views run"
produce --rate "$RATE" --count $((RATE * 20)) --eos >"$OUT/producer1.log" 2>&1
kafka_down; echo "Kafka stopped for ${DOWN}s"; sleep "$DOWN"; kafka_up
alive 1 && echo "w1 still running" || { echo "FAIL: w1 exited"; tail -5 "$OUT/w1.log"; failed=1; }
produce --rate "$RATE" --count $((RATE * 20)) --eos >"$OUT/producer2.log" 2>&1

log "3. Kafka is down when the worker is killed and w2 takes its views over"
kafka_down
kill -9 "$(pid 1)"; wait "$(pid 1)" 2>/dev/null || true
run_worker 2
if secs=$(until_true '[[ $(owners) == "w2:'"$n"' " && $(waiting) == '"$n"' ]]' $((LEASE_TIMEOUT + 40))); then
  echo "w2 took all $n views over and they wait for the brokers (${secs}s)"
else echo "FAIL: owners $(owners), $(waiting) waiting"; failed=1; fi
kafka_up
if secs=$(until_true '[[ $(started_after_wait 2) == '"$n"' ]]' "$BOUND"); then echo "Kafka back: w2 started all $n views in ${secs}s"
else echo "FAIL: w2 started $(started_after_wait 2) of $n views"; failed=1; fi
produce --rate "$RATE" --count $((RATE * 10)) --eos >"$OUT/producer3.log" 2>&1

log "drain and verify"
end=$(topic_end); end2=$(topic_end "$TOPIC2")
for _ in $(seq 1 120); do
  done_all=1; for v in "${VIEWS[@]}"; do caught_up "$v" "$end" "$end2" || done_all=0; done
  [[ $done_all == 1 ]] && break; sleep 1
done
[[ $done_all == 1 ]] && echo "every view read both topics to their ends ($end and $end2 events)" || { echo "FAIL: not caught up"; failed=1; }
verify_all || failed=1
alive 2 && echo "w2 still running" || { echo "FAIL: w2 exited"; failed=1; }
fails=$(cat "$OUT"/w*.log | grep -c "marking it failed" || true)
[[ $fails == 0 ]] && echo "no view was marked failed" || { echo "FAIL: $fails views marked failed"; grep "marking it failed" "$OUT"/w*.log | head -3; failed=1; }

if [[ $failed == 0 ]]; then log "PASS"; else log "FAIL"; exit 1; fi
