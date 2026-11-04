#!/usr/bin/env bash
# Arrow decode in the engine: one topic of hostile payloads (every form in the Arrow plan's semantics table,
# framing hazards, random byte mutations, tombstones) read by two sources, one decoding in SQL and one in
# Arrow, each feeding the same pass-through view. Sinks and dead letters must match row for row, and both
# views must verify against the batch recomputation.
source "$(dirname "$0")/lib.sh"
MESSAGES=${MESSAGES:-60000}
SEED=${SEED:-7}
VIEWS=(all_sql all_arrow)

log "fresh lake and topic"; reset_env
cargo run -q --release -p duckwake_core --example phase_a_arrow_decode -- produce localhost:19092 "$TOPIC" "$MESSAGES" "$SEED"

for d in sql arrow; do
  "$BIN" create-source "src_$d" --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
    \"decode\":\"$d\",\"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}"
  # A small max_rows makes many epochs, so chunk boundaries and partial drains are exercised too.
  "$BIN" create-view "analytics.all_$d" --epoch-interval 1s --max-rows 5000 \
    --sql "SELECT ts, game_id, user_id, amount FROM src_$d" | head -1
  "$BIN" start "all_$d" --worker w1
done

log "run until both views have committed the whole topic"
"$BIN" run $WORKER_FLAGS --worker w1 >"$OUT/worker.log" 2>&1 &
WORKER=$!
end=$(topic_end)
for _ in $(seq 1 120); do
  [[ "$(committed all_sql src_sql)" == "$end" && "$(committed all_arrow src_arrow)" == "$end" ]] && break
  sleep 1
done
kill -TERM "$WORKER"; wait "$WORKER" || true
for d in sql arrow; do
  [[ "$(committed all_$d src_$d)" == "$end" ]] || { echo "FAIL: all_$d committed $(committed all_$d src_$d) of $end"; exit 1; }
done
grep 'epoch committed' "$OUT/worker.log" | sed 's/\x1b\[[0-9;]*m//g' |
  awk '{ for (i = 1; i <= NF; i++) { split($i, kv, "="); v[kv[1]] = kv[2] }
         j = v["job"]; n[j] += v["rows_in"]; q[j] += v["rows_sql"]; b[j] += v["bisections"] }
       END { for (j in n) printf "%s: %d messages, %d decoded in SQL, %d bisections\n", j, n[j], q[j], b[j] }' | sort

log "sinks and dead letters must be equal"
diff_count() {
  "$BIN" sql "SELECT count(*) AS n FROM (($1) EXCEPT ALL ($2))" 2>/dev/null | awk -F'|' 'NR==4 {gsub(/ /,"",$2); print $2}'
}
sink() { echo "SELECT ts, game_id, user_id, amount FROM lake.analytics.all_$1"; }
dead() { echo "SELECT source_partition, source_offset, payload, error FROM lake.__duckwake.dead_letter WHERE job_id = 'all_$1'"; }
rc=0
for what in sink dead; do
  a=$(diff_count "$($what sql)" "$($what arrow)"); b=$(diff_count "$($what arrow)" "$($what sql)")
  n=$("$BIN" sql "SELECT count(*) AS n FROM ($($what sql))" 2>/dev/null | awk -F'|' 'NR==4 {gsub(/ /,"",$2); print $2}')
  echo "$what: $n rows; only in SQL $a, only in Arrow $b"
  [[ $n -gt 0 && $a == 0 && $b == 0 ]] || rc=1
done
[[ $rc == 0 ]] || { echo "FAIL: the decode paths differ"; exit 1; }

log "both views match the batch recomputation"
verify_all
echo "decode test passed"
