#!/usr/bin/env bash
# Gate 1 crash test: stream events with late and malformed rows while the worker is killed over and over
# (kill -9 from outside, and aborts injected at every step of the epoch: poll, mid-transaction, right
# before and right after COMMIT, during maintenance, and with --pipeline while the next epoch is staged
# ahead of a commit). Then drain and check both sinks against a batch recomputation: no duplicates, no gaps.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-2000}; DURATION=${DURATION:-60}; KILLS=${KILLS:-12}  # EPOCH (default 2s): the views' epoch interval
read -r -a POINTS <<< "${POINTS:-external poll commit_begin commit_mid before_commit after_commit maintenance staged_ahead}"

log "reset lake and topic"; reset_env
log "create source and views"; setup_views "${EPOCH:-2s}" "10 SECOND" "5 seconds"; start_views w1

log "produce $((RATE * DURATION)) events at $RATE/s (every 997th 5 min late, every 1499th malformed)"
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) \
  --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!

for i in $(seq 1 "$KILLS"); do
  point=${POINTS[$((RANDOM % ${#POINTS[@]}))]}
  if [[ $point == external ]]; then
    "$BIN" run $WORKER_FLAGS --worker w1 --flush-every 3s --merge-every 7s >>"$OUT/worker.log" 2>&1 &
    pid=$!; sleep "$((2 + RANDOM % 5)).$((RANDOM % 10))"; kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
    echo "run $i: kill -9 from outside"
  else
    # Abort at that point in one of the next few epochs, or get killed after 8 s.
    DUCKWAKE_CRASH_AT=$point DUCKWAKE_CRASH_PROB=0.4 "$BIN" run $WORKER_FLAGS --worker w1 --flush-every 3s --merge-every 7s >>"$OUT/worker.log" 2>&1 &
    pid=$!; for _ in $(seq 1 80); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$pid" 2>/dev/null && how="no crash in 8 s, killed" || how="aborted at $point"
    wait "$pid" 2>/dev/null || true
    echo "run $i: $how"
  fi
done
wait "$PRODUCER"; tail -1 "$OUT/producer.log"

log "drain to the end of the topic with a clean worker"; drain w1 "$OUT/worker.log"
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), epochs committed: $(grep -c 'epoch committed' "$OUT/worker.log" || true)"
log "verify"; verify_all
