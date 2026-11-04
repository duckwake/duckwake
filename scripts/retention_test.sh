#!/usr/bin/env bash
# verify under retention: trim the oldest events off every partition, as size or time retention would, and
# check verify still proves the windowed and append views over the retained range, refuses the global
# aggregate (it depends on deleted events), and still catches a sink row going missing in that range.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-2000}
DURATION=${DURATION:-60}

log "fresh lake and topic"; reset_env; setup_views 2s
start_views w1
"$BIN" run $WORKER_FLAGS --worker w1 >"$OUT/worker.log" 2>&1 &
WORKER=$!
log "produce $((RATE * DURATION)) events at $RATE/s while the worker runs"
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) \
  --late-every 997 --bad-every 1499 >"$OUT/producer.log" 2>&1
kill -TERM "$WORKER"; wait "$WORKER" || true
drain w1 "$OUT/worker.log"

log "trim the first quarter of every partition"
docker compose exec -T redpanda rpk topic describe "$TOPIC" -p | awk 'NR>1 {print $1, int($NF / 4)}' |
  while read -r p o; do docker compose exec -T redpanda rpk topic trim-prefix "$TOPIC" -p "$p" -o "$o" --no-confirm </dev/null >/dev/null; done
docker compose exec -T redpanda rpk topic describe "$TOPIC" -p

log "verify the retained range"
VIEWS=(clicks_per_window big_spend)
verify_all
# A check over an empty sink proves nothing: both views must have compared output in the retained range.
for v in "${VIEWS[@]}"; do
  rows=$(awk '/sink_rows:/ {gsub(/,/, ""); print $2}' "$OUT/verify_$v.txt")
  echo "$v: $rows sink rows compared"
  [[ $rows -gt 0 ]] || { echo "FAIL: nothing to compare for $v"; exit 1; }
done

log "global aggregate needs the whole topic"
if "$BIN" verify game_totals >"$OUT/verify_game_totals.txt" 2>&1; then
  echo "FAIL: verify accepted game_totals on a trimmed topic"; exit 1
fi
tail -1 "$OUT/verify_game_totals.txt"

log "a row lost inside the retained range is still caught"
"$BIN" sql "DELETE FROM lake.analytics.big_spend WHERE rowid = (SELECT max(rowid) FROM lake.analytics.big_spend)" >/dev/null
if "$BIN" verify big_spend >"$OUT/verify_tampered.txt" 2>&1; then
  echo "FAIL: verify missed a deleted sink row"; exit 1
fi
grep -E 'missing_from_sink' "$OUT/verify_tampered.txt"
echo "retention test passed"
