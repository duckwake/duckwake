#!/usr/bin/env bash
# Zombie worker test: after 3 epochs, w1 stalls with its epoch transaction open (an injected 15 s pause
# right before COMMIT). Meanwhile the views are reassigned to w2, which commits several epochs. When w1 wakes up its
# commit must be rejected, so nothing it staged lands in the lake.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}

log "reset lake and topic"; reset_env
log "create source and views"; setup_views 2s "10 SECOND" "5 seconds"; start_views w1
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * 40)) --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!

log "w1 runs a few epochs, then stalls inside its commit transaction"
DUCKWAKE_PAUSE_AT=before_commit DUCKWAKE_PAUSE_SKIP=3 DUCKWAKE_PAUSE_SECS=15 "$BIN" run $WORKER_FLAGS --worker w1 >"$OUT/w1.log" 2>&1 &
W1=$!
sleep 12
log "reassign both views to w2 while w1 is stalled"
for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker w2; done
"$BIN" run $WORKER_FLAGS --worker w2 >"$OUT/w2.log" 2>&1 &
W2=$!
sleep 14
log "w1 woke up; its view of the result"
grep -E "pause injected|lease lost|conflict|commit failed|Error" "$OUT/w1.log" | cut -c1-220 || true
kill -TERM "$W1" 2>/dev/null || true; wait "$W1" 2>/dev/null || true
wait "$PRODUCER"; kill -TERM "$W2"; wait "$W2" || true

log "w1's last commit vs. the takeover"
"$BIN" sql "SELECT worker, min(epoch) AS first_epoch, max(epoch) AS last_epoch, count(*) AS commits
            FROM lake.__duckwake.checkpoints WHERE job_id = 'clicks_per_window' GROUP BY ALL ORDER BY first_epoch"
log "drain on w2"; drain w2 "$OUT/w2.log"
log "verify"; verify_all
