#!/usr/bin/env bash
# Shared readers: views of one source at the same position share one reader; a view at another position gets
# a reader of its own, catches up and joins. While events stream in, game_totals is created late (it starts
# from the earliest offset, far behind the other two) and big_spend is reset to an early epoch. Both must
# catch up and join the shared reader, and every view must match the batch recomputation.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-3000}; DURATION=${DURATION:-40}
WORKER_FLAGS=${WORKER_FLAGS:---pipeline --stage-threads 4}
[[ " $WORKER_FLAGS " == *" --shared-readers "* ]] || WORKER_FLAGS="$WORKER_FLAGS --shared-readers"

log "reset lake and topic"; reset_env
log "create source and views"; setup_views 2s "10 SECOND" "5 seconds" >/dev/null
"$BIN" stop game_totals >/dev/null
for v in clicks_per_window big_spend active_users; do "$BIN" start "$v" --worker w1 >/dev/null; done
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) \
  --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
# shellcheck disable=SC2086
"$BIN" run $WORKER_FLAGS --worker w1 >"$OUT/worker.log" 2>&1 &
W=$!

sleep 12
log "start game_totals from the beginning of the topic"
"$BIN" start game_totals --worker w1
sleep 8
log "rewind big_spend to epoch 2"
"$BIN" pause big_spend; "$BIN" reset big_spend --to-epoch 2; "$BIN" start big_spend --worker w1
wait "$PRODUCER"; tail -1 "$OUT/producer.log"
end=$(topic_end)
for _ in $(seq 1 120); do
  done=1
  for v in "${VIEWS[@]}"; do [[ "$(committed "$v")" == "$end" ]] || done=0; done
  [[ $done == 1 ]] && break
  sleep 1
done
kill -TERM "$W"; wait "$W" || true

log "readers"
grep -E "job started|share one reader" "$OUT/worker.log" | sed -E 's/\x1b\[[0-9;]*m//g; s/.*(job started|views caught up)/\1/' | cut -c1-160
joins=$(grep -c "share one reader" "$OUT/worker.log" || true)
echo "joins: $joins"
[[ $joins -ge 2 ]] || { echo "FAIL: expected game_totals and the rewound big_spend to join the shared reader"; exit 1; }
log "verify"; verify_all
echo "shared reader test passed"
