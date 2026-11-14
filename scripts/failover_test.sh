#!/usr/bin/env bash
# P2 failover gate: three workers share the test views, started without a worker so the workers spread them.
# While events stream in, each way a worker can go away must move its views to the live workers within a
# bounded time, with exact output:
#   0. first, a catalog stall longer than the lease timeout (PostgreSQL holds every commit up): nobody is
#      declared dead and no view moves.
#   1. kill -9: its heartbeat stops; the views move once the lease timeout passes.
#   2. a frozen worker (SIGSTOP): the same, even when it froze inside its catalog commit (PostgreSQL ends
#      that session after the lease timeout); thawed (SIGCONT), it is a zombie whose commits are fenced and
#      it lets go of the views, or it exits.
#   3. a drained worker (`duckwake drain`) hands its views to the others at once; a clean stop alone is a
#      restart, whose views wait for it (scripts/rebalance_test.sh covers that).
# A restarted worker takes views again. After draining, every view verifies against the batch recomputation.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}; DURATION=${DURATION:-120}
LEASE_TIMEOUT=${LEASE_TIMEOUT:-10}   # seconds
BOUND=$((LEASE_TIMEOUT + 15))        # kill to every view moved: the timeout, 3 of the observer's own heartbeats, a refresh
CLEAN_BOUND=${CLEAN_BOUND:-8}        # drain to every view moved
failed=0

run_worker() {
  "$BIN" run $WORKER_FLAGS --worker "$1" --lease-timeout "${LEASE_TIMEOUT}s" >>"$OUT/$1.log" 2>&1 &
  echo $! >"$OUT/$1.pid"
}
pid() { cat "$OUT/$1.pid"; }
# However the test ends, no worker outlives it (a frozen one is thawed first, or SIGKILL would wait).
trap 'for w in w1 w2 w3; do [[ -f $OUT/$w.pid ]] && { kill -CONT "$(pid $w)"; kill -9 "$(pid $w)"; } 2>/dev/null || true; done' EXIT
# "view owner" per view; owner is "-" when unassigned.
owners() {
  local q="" v
  for v in "${VIEWS[@]}"; do
    q+="${q:+ UNION ALL }SELECT '$v', coalesce(owner_worker, '-') FROM lake.__duckwake.lease_$v"
  done
  "$BIN" sql --csv "$q" 2>/dev/null | tr ',' ' '
}
owned_by() { owners | awk -v w="$1" '$2 == w' | wc -l | tr -d ' '; }
# Waits until no view is owned by $1, at most $2 seconds; prints the seconds it took.
wait_moved() {
  local t0=$SECONDS
  while (( SECONDS - t0 <= $2 )); do
    [[ $(owned_by "$1") == 0 ]] && { echo $((SECONDS - t0)); return 0; }
    sleep 0.5
  done
  echo $((SECONDS - t0)); return 1
}
check_moved() {
  local what=$1 from=$2 bound=$3 secs
  if secs=$(wait_moved "$from" "$bound"); then
    echo "$what: the views of $from moved in ${secs}s (bound ${bound}s)"
  else
    echo "FAIL: $what: $from still owns views after ${secs}s"; failed=1
  fi
  owners | sed 's/^/  /'
}

log "reset lake and topic"; reset_env
log "create source and views"; setup_views 2s "10 SECOND" "5 seconds"
log "three workers, then the views started on any worker: the workers spread them"
# Nothing rebalances views between live workers yet, so the workers come up before the views start.
for w in w1 w2 w3; do run_worker "$w"; done
for _ in $(seq 1 40); do [[ $("$BIN" workers | grep -c ' running ') == 3 ]] && break; sleep 0.5; done
# The topics exist before the views start: a worker that claims a view first would fail it on a missing topic.
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
for v in "${VIEWS[@]}"; do "$BIN" start "$v"; done
produce --rate "$RATE" --count $((RATE * DURATION)) --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
for _ in $(seq 1 40); do
  [[ $(owned_by -) == 0 && $(owned_by w1) -ge 1 && $(owned_by w2) -ge 1 && $(owned_by w3) -ge 1 ]] && break
  sleep 0.5
done
owners | sed 's/^/  /'
for w in w1 w2 w3; do [[ $(owned_by "$w") -ge 1 ]] || { echo "FAIL: $w got no view"; failed=1; }; done
sleep 8

log "0. the catalog stalls for 1.5x the lease timeout: every worker waits, none is declared dead"
before=$(owners); claims0=$(cat "$OUT"/w*.log | grep -c 'claimed orphaned view' || true)
# An exclusive lock on DuckLake's snapshot table holds up every commit, heartbeats included, while reads go on,
# as a stalled disk or a long lock would: the worker whose commit lands first reads the others' heartbeats
# before theirs land. (Before workers counted only time in which their own heartbeats landed, it claimed
# their views.) The session is busy (pg_sleep), so the idle-in-transaction timeout spares it.
docker compose exec -T postgres psql -q -U lake -d "$TEST_DB" -c \
  "BEGIN; LOCK TABLE ducklake_snapshot IN EXCLUSIVE MODE; SELECT pg_sleep($((LEASE_TIMEOUT * 3 / 2))); COMMIT;" >/dev/null
sleep "$LEASE_TIMEOUT"
claims1=$(cat "$OUT"/w*.log | grep -c 'claimed orphaned view' || true)
if [[ $(owners) == "$before" && $claims1 == "$claims0" ]]; then
  echo "catalog stall of $((LEASE_TIMEOUT * 3 / 2))s: no view moved"
else
  echo "FAIL: views moved after a catalog stall ($((claims1 - claims0)) claims)"; owners | sed 's/^/  /'; failed=1
fi
for w in w1 w2 w3; do kill -0 "$(pid "$w")" 2>/dev/null || { echo "FAIL: $w died in the catalog stall"; failed=1; }; done

log "1. kill -9 w1"
kill -9 "$(pid w1)"; wait "$(pid w1)" 2>/dev/null || true
check_moved "kill -9" w1 "$BOUND"
sleep 6

log "2. freeze w2 (SIGSTOP), then thaw it"
kill -STOP "$(pid w2)"
check_moved "frozen" w2 "$BOUND"
sleep 4
kill -CONT "$(pid w2)"
# Thawed, w2 lets go of its views, or, if it was frozen inside a catalog transaction, finds that PostgreSQL
# ended that session after the lease timeout (so it could not block the other workers) and exits.
for _ in $(seq 1 30); do grep -qE 'lease lost|job released' "$OUT/w2.log" || ! kill -0 "$(pid w2)" 2>/dev/null && break; sleep 0.5; done
if grep -qE 'lease lost|job released' "$OUT/w2.log"; then
  echo "thawed w2 let go: $(grep -cE 'lease lost|job released' "$OUT/w2.log") views released or fenced"
elif ! kill -0 "$(pid w2)" 2>/dev/null; then
  echo "thawed w2 exited: it was frozen inside a catalog transaction, which PostgreSQL ended"
  grep -m1 '^Error' "$OUT/w2.log" | cut -c1-120
else
  echo "FAIL: thawed w2 did not let go of its views"; failed=1
fi
[[ $(owned_by w2) == 0 ]] || { echo "FAIL: w2 owns views again"; failed=1; }

log "3. restart w1, then drain w3 and stop it"
run_worker w1
sleep 6
"$BIN" drain w3
check_moved "drain" w3 "$CLEAN_BOUND"
kill -TERM "$(pid w3)"; wait "$(pid w3)" 2>/dev/null || true

log "workers"; "$BIN" workers

log "drain with w1 and w2"
wait "$PRODUCER"; tail -1 "$OUT/producer.log"
end=$(topic_end); end2=$(topic_end "$TOPIC2")
for _ in $(seq 1 180); do
  done=1
  for v in "${VIEWS[@]}"; do caught_up "$v" "$end" "$end2" || done=0; done
  [[ $done == 1 ]] && break
  sleep 1
done
[[ $done == 1 ]] || { echo "FAIL: the views did not catch up"; owners; failed=1; }
for w in w1 w2; do kill -TERM "$(pid "$w")" 2>/dev/null || true; wait "$(pid "$w")" 2>/dev/null || true; done
echo "claims: $(cat "$OUT"/w*.log | grep -c 'claimed orphaned view'), fenced commits: $(cat "$OUT"/w*.log | grep -c 'lease lost')"

log "verify"; verify_all || failed=1
[[ $failed == 0 ]] && echo "FAILOVER TEST PASSED" || { echo "FAILOVER TEST FAILED"; exit 1; }
