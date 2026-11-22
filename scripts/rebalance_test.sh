#!/usr/bin/env bash
# Rebalancing gate: views move between live workers so a worker that joins gets work, without churn, while
# events stream in; every view must match its batch recomputation at the end.
#   1. One worker runs the five test views (big_spend pinned to it), started before their topics exist: the
#      worker waits for the topics instead of failing the views.
#   2. A second worker joins, then a third: views spread to them (view counts within one of each other), the
#      pinned view stays, no view moves twice within its cooldown, load moves that may follow settle, and no
#      view is claimed (nobody died).
#   3. Rolling restart: each worker stopped cleanly and started again within the stop grace keeps its views.
#   4. Drain: a drained worker hands every view away and takes none.
#   5. The pinned view's worker dies: the view fails over, then goes back when its worker returns.
#   6. A worker is killed (-9) in the middle of a drain: what it had not handed off fails over.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}; DURATION=${DURATION:-240}
LEASE_TIMEOUT=${LEASE_TIMEOUT:-10}; STOP_GRACE=${STOP_GRACE:-30}; COOLDOWN=${COOLDOWN:-15}; EVERY=${EVERY:-3}
failed=0

run_worker() {
  # shellcheck disable=SC2086
  "$BIN" run $WORKER_FLAGS --worker "$1" --lease-timeout "${LEASE_TIMEOUT}s" --stop-grace "${STOP_GRACE}s" \
    --rebalance-every "${EVERY}s" --rebalance-cooldown "${COOLDOWN}s" >>"$OUT/$1.log" 2>&1 &
  echo $! >"$OUT/$1.pid"
}
pid() { cat "$OUT/$1.pid"; }
stop_worker() { kill -TERM "$(pid "$1")" 2>/dev/null || true; wait "$(pid "$1")" 2>/dev/null || true; }
trap 'for w in w1 w2 w3; do [[ -f $OUT/$w.pid ]] && kill -9 "$(pid $w)" 2>/dev/null || true; done' EXIT
# "view owner" per view; "-" when unassigned.
owners() {
  local q="" v
  for v in "${VIEWS[@]}"; do q+="${q:+ UNION ALL }SELECT '$v', coalesce(owner_worker, '-') FROM lake.__duckwake.\"lease_analytics.$v\""; done
  "$BIN" sql --csv "$q" 2>/dev/null | tr ',' ' '
}
owner() { owners | awk -v v="$1" '$1 == v {print $2}'; }
count() { owners | awk -v w="$1" '$2 == w' | wc -l | tr -d ' '; }
# Whether the live workers' view counts are within one of each other.
spread() {
  local lo=99 hi=0 w n
  for w in "$@"; do n=$(count "$w"); ((n < lo)) && lo=$n; ((n > hi)) && hi=$n; done
  ((hi - lo <= 1))
}
# Waits up to $2 seconds for the condition $3 (evaluated afresh each second) to hold, and reports it as $1.
check() {
  local what=$1 bound=$2 cond=$3 t0=$SECONDS
  while ((SECONDS - t0 <= bound)); do
    if eval "$cond"; then echo "$what: $((SECONDS - t0))s (bound ${bound}s)"; owners | sed 's/^/  /'; return 0; fi
    sleep 1
  done
  echo "FAIL: $what after $((SECONDS - t0))s"; owners | sed 's/^/  /'; failed=1
}
handoffs() { cat "$OUT"/w*.log 2>/dev/null | grep -c 'handed off view' || true; }
claims() { cat "$OUT"/w*.log 2>/dev/null | grep -c 'claimed orphaned view' || true; }

log "reset lake and topic"; reset_env
log "create source and views"; setup_views 2s "10 SECOND" "5 seconds"

log "1. one worker; big_spend pinned to it, the rest started on any worker, before their topics exist"
run_worker w1
for _ in $(seq 1 40); do [[ $("$BIN" workers | grep -c ' running ') -ge 1 ]] && break; sleep 0.5; done
"$BIN" start big_spend --worker w1
for v in "${VIEWS[@]}"; do [[ $v == big_spend ]] || "$BIN" start "$v"; done
check "w1 owns all five" 30 '[[ $(count w1) == 5 ]]'
sleep 4
grep -q 'waiting for the topic' "$OUT/w1.log" && echo "w1 waits for the topics" || { echo "FAIL: w1 did not wait for the missing topics"; failed=1; }
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
produce --rate "$RATE" --count $((RATE * DURATION)) --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
check "every view started once its topic exists" 20 '[[ $(grep -c "job started" "$OUT/w1.log") -ge 5 ]]'
if "$BIN" jobs | grep -q ' failed '; then echo "FAIL: a view was marked failed while its topic was missing"; failed=1; fi

log "2. w2 joins, then w3: the views spread once their cooldown has passed"
claims_alive=$(claims)
run_worker w2
check "spread over w1 and w2" $((COOLDOWN + 30)) 'spread w1 w2'
run_worker w3
check "spread over w1, w2 and w3" $((COOLDOWN + 30)) 'spread w1 w2 w3'
[[ $(owner big_spend) == w1 ]] || { echo "FAIL: pinned big_spend moved to $(owner big_spend)"; failed=1; }
# Count moves (a worker joined) come first. A load move may follow, once a moved view's cooldown has passed
# and the imbalance has lasted LOAD_ROUNDS (3) rebalance rounds; it leaves the pair strictly more even, so
# moves converge. Settled: no handoff for a cooldown plus four rounds, within three such windows, and no more
# load moves than there are views to move.
spread_handoffs=$(handoffs); quiet=$((COOLDOWN + 4 * EVERY)); settled=0
for _ in 1 2 3; do
  before=$(handoffs); sleep "$quiet"
  [[ $(handoffs) == "$before" ]] && { settled=1; break; }
done
extra=$(( $(handoffs) - spread_handoffs ))
if (( settled )); then echo "settled: no handoffs in ${quiet}s ($extra load moves after the counts evened out)"
else echo "FAIL: still moving views after $((3 * quiet))s"; failed=1; fi
(( extra < ${#VIEWS[@]} )) || { echo "FAIL: $extra handoffs after the counts evened out"; failed=1; }
# No view handed off twice within its cooldown (handoff times per view, in seconds).
cat "$OUT"/w*.log | grep 'handed off view' | sed -E 's/^([^ ]+).*job=([a-z_]+).*/\1 \2/' | sort | python3 -c '
import sys, datetime
last, bad = {}, 0
for line in sys.stdin:
    ts, job = line.split()
    t = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    if job in last and t - last[job] < float(sys.argv[1]):
        print(f"FAIL: {job} handed off twice in {t - last[job]:.0f}s"); bad = 1
    last[job] = t
sys.exit(bad)' "$COOLDOWN" || failed=1
echo "handoffs so far: $(handoffs)"
# Every worker is alive throughout: a claim here is a false failover (a stalled catalog judged as death).
[[ $(claims) == "$claims_alive" ]] || { echo "FAIL: views claimed while every worker was alive"; failed=1; }

log "3. rolling restart: each worker stopped and started again within the stop grace keeps its views"
claims_before=$(claims)
for w in w1 w2 w3; do
  mine=$(owners | awk -v w="$w" '$2 == w {print $1}' | sort | tr '\n' ' ')
  stop_worker "$w"; sleep 3; run_worker "$w"; sleep 5
  now=$(owners | awk -v w="$w" '$2 == w {print $1}' | sort | tr '\n' ' ')
  [[ $mine == "$now" ]] && echo "$w kept: $now" || { echo "FAIL: $w had $mine, now $now"; failed=1; }
done
[[ $(claims) == "$claims_before" ]] || { echo "FAIL: views were claimed during the rolling restart"; failed=1; }

log "4. drain w3: it hands every view away and takes none"
"$BIN" drain w3
check "w3 drained" 20 '[[ $(count w3) == 0 ]]'
sleep $((EVERY * 3))
[[ $(count w3) == 0 ]] || { echo "FAIL: drained w3 took views again"; failed=1; }

log "5. kill -9 w1 (big_spend is pinned to it): it fails over, then goes home when w1 is back"
kill -9 "$(pid w1)"; wait "$(pid w1)" 2>/dev/null || true
check "big_spend failed over" $((LEASE_TIMEOUT + 15)) '[[ $(owner big_spend) == w2 ]]'
run_worker w1
check "big_spend back on w1" $((EVERY * 5 + 10)) '[[ $(owner big_spend) == w1 ]]'

log "6. kill -9 w2 in the middle of a drain: what it did not hand off fails over"
"$BIN" drain w2; sleep 0.$((RANDOM % 9)); kill -9 "$(pid w2)"; wait "$(pid w2)" 2>/dev/null || true
check "w2's views on w1" $((LEASE_TIMEOUT + 15)) '[[ $(count w1) == 5 ]]'

log "workers"; "$BIN" workers
log "drain the topic on w1"
wait "$PRODUCER"; tail -1 "$OUT/producer.log"
end=$(topic_end); end2=$(topic_end "$TOPIC2")
for _ in $(seq 1 180); do
  done=1
  for v in "${VIEWS[@]}"; do caught_up "$v" "$end" "$end2" || done=0; done
  [[ $done == 1 ]] && break
  sleep 1
done
[[ $done == 1 ]] || { echo "FAIL: the views did not catch up"; owners; failed=1; }
echo "handoffs: $(handoffs), claims: $(claims), fenced commits: $(cat "$OUT"/w*.log | grep -c 'lease lost' || true)"

log "verify"; verify_all || failed=1
[[ $failed == 0 ]] && echo "REBALANCE TEST PASSED" || { echo "REBALANCE TEST FAILED"; exit 1; }
