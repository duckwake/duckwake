#!/usr/bin/env bash
# C2 gate: upsert views over a Kafka topic of order updates. The producer keys each message by its order (a
# JSON object key, or plain text for every third message), keeps an order in one partition, and sends
# tombstones, repeated messages and messages with stale versions. Three upsert views read the topic: last
# message wins, highest version wins, and highest version wins behind a filter. Two more views read their tables
# through the change feed: a filter, and an aggregate with min/max and count(DISTINCT).
#
# The worker is killed over and over (kill -9 from outside, and aborts injected at every step of the epoch);
# after every crash every view verifies against a batch recomputation at its own checkpoint. After draining,
# the upsert views are also checked against the topic read directly, with SQL written here rather than the
# engine's: one row per order, its last message, or its highest version unless a later tombstone deleted it.
# Then a reset rewinds the versioned view a few epochs; it and its reader replay to the same tables.
# WORKER_FLAGS="--pipeline ..." runs it pipelined.
TOPIC=${TOPIC:-orders}
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}; DURATION=${DURATION:-40}; KILLS=${KILLS:-8}; EPOCH=${EPOCH:-2s}; KEYS=${KEYS:-3000}
read -r -a POINTS <<< "${POINTS:-external poll commit_begin commit_mid before_commit after_commit maintenance staged_ahead}"
UPSERTS=(orders_latest orders_versioned orders_paid)
VIEWS=("${UPSERTS[@]}" orders_open orders_by_status)

sql() { "$BIN" sql --csv "$1" 2>>"$OUT/sql.log" | tail -1; }

verify_views() {
  local rc=0 v
  for v in "${VIEWS[@]}"; do
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

# Rows in table $1 (columns $2) and in the expected rows of query $3 that are not in the other: 0 when equal.
differ() {
  sql "WITH expected AS ($3)
       SELECT count(*) FROM ((SELECT $2 FROM expected EXCEPT ALL SELECT $2 FROM lake.analytics.$1)
                             UNION ALL (SELECT $2 FROM lake.analytics.$1 EXCEPT ALL SELECT $2 FROM expected))"
}

# The topic's messages, each with its order: from the key (a JSON object or plain text), else the value.
MESSAGES="SELECT coalesce(try_cast(json_extract_string(_key, '\$.order_id') AS BIGINT), try_cast(_key AS BIGINT), order_id) AS k,
                 _partition AS p, _offset AS o, _error IS NOT NULL AS dead, order_id, status, amount, ver, ts
          FROM orders WHERE _error IS NULL OR _error = 'tombstone'"
# Last message wins: an order's last message, unless it is a tombstone.
LATEST="SELECT * FROM ($MESSAGES) QUALIFY row_number() OVER (PARTITION BY k ORDER BY p DESC, o DESC) = 1 AND NOT dead"
# Highest version wins (the first of equal ones), unless a tombstone comes after it.
versioned() {
  echo "WITH m AS ($MESSAGES),
        best AS (SELECT * FROM m WHERE NOT dead QUALIFY row_number() OVER (PARTITION BY k ORDER BY ver DESC NULLS LAST, p, o) = 1),
        gone AS (SELECT k, max(o) AS t FROM m WHERE dead GROUP BY k)
        SELECT b.* FROM best b LEFT JOIN gone g USING (k) WHERE g.t IS NULL OR g.t < b.o"
}

log "reset lake and topic"; reset_env
log "create the source and the views"
"$BIN" create-source orders --options "{$KAFKA,\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
  \"watermark_delay\":\"5 seconds\",\"idle_timeout\":\"30 seconds\",\"decode\":\"${DECODE:-auto}\",
  \"columns\":{\"ts\":\"TIMESTAMP\",\"order_id\":\"BIGINT\",\"status\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\",\"ver\":\"BIGINT\"}}"
create() { "$BIN" create-view "analytics.$1" --epoch-interval "$EPOCH" "${@:2}" | sed -n 1p; }
create orders_latest --upsert-key order_id --sql "SELECT order_id, status, amount, ts FROM orders"
create orders_versioned --upsert-key order_id --upsert-version ver \
  --sql "SELECT order_id, status, amount, ver, ts FROM orders"
create orders_paid --upsert-key order_id --upsert-version ver \
  --sql "SELECT order_id, amount * 100 AS cents, ver FROM orders WHERE status = 'paid'"
create orders_open --sql "SELECT order_id, status, amount FROM analytics.orders_versioned WHERE status <> 'cancelled'"
create orders_by_status --sql "SELECT status, count(*) AS n, sum(amount) AS total, min(amount) AS lo, max(amount) AS hi,
  count(DISTINCT amount) AS amounts FROM analytics.orders_latest GROUP BY status"
for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker w1 >/dev/null; done

log "refused: an upsert view with an aggregate, a key not in its columns, a retention without a version"
failed=0
refused() {
  local want=$1; shift
  if out=$("$BIN" create-view analytics.bad "$@" 2>&1); then echo "not refused: $*"; failed=1
  elif [[ $out == *"$want"* ]]; then echo "$out" | grep -m1 -o "$want.*"
  else echo "refused for another reason: $out"; failed=1; fi
}
refused "aggregates and DISTINCT go in another view" --upsert-key order_id --sql "SELECT status, count(*) AS n FROM orders GROUP BY status"
refused "select the upsert_key column" --upsert-key order_id --sql "SELECT status, amount FROM orders"
refused "tombstone_retention applies with upsert_version only" --upsert-key order_id --tombstone-retention "1 day" --sql "SELECT order_id, status FROM orders"

log "produce $((RATE * DURATION)) order updates over $KEYS orders at $RATE/s (every 13th a tombstone, every 17th twice, every 7th stale)"
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) \
  --keys "$KEYS" --delete-every 13 --repeat-every 17 --stale-every 7 >"$OUT/producer.log" 2>&1 &
PRODUCER=$!

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
  if verify_views quiet; then echo "run $i: $how; all five verify"; else echo "run $i: $how; VERIFY FAILED"; failed=1; fi
done
wait "$PRODUCER"; tail -1 "$OUT/producer.log"

# Until each upsert view has committed to the end of the topic, and its readers have taken its last change.
settle() {
  local end; end=$(topic_end)
  for _ in $(seq 1 120); do
    local done=1 v
    for v in "${UPSERTS[@]}"; do [[ "$(committed "$v" orders)" == "$end" ]] || done=0; done
    [[ $done == 1 ]] && break
    sleep 1
  done
  for _ in $(seq 1 60); do
    [[ $(sql "SELECT (SELECT coalesce(sum(n), 0) FROM lake.analytics.orders_by_status) = (SELECT count(*) FROM lake.analytics.orders_latest)
              AND (SELECT count(*) FROM lake.analytics.orders_open) = (SELECT count(*) FROM lake.analytics.orders_versioned WHERE status <> 'cancelled')") == true ]] && break
    sleep 1
  done
}

log "drain to the end of the topic"
"$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 &
pid=$!; settle
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), epochs committed: $(grep -c 'epoch committed' "$OUT/worker.log" || true)"
log "verify"; verify_views || failed=1

# Each upsert view against the topic read directly.
check_topic() {
  local d
  d=$(differ orders_latest "order_id, status, amount, ts" "$LATEST")
  echo "orders_latest: $(sql "SELECT count(*) FROM lake.analytics.orders_latest") orders, $d rows differ from the topic's last messages"
  [[ $d == 0 ]] || failed=1
  d=$(differ orders_versioned "order_id, status, amount, ver, ts" "$(versioned)")
  echo "orders_versioned: $(sql "SELECT count(*) FROM lake.analytics.orders_versioned") orders, $d rows differ from the topic's highest versions"
  [[ $d == 0 ]] || failed=1
  d=$(differ orders_paid "order_id, cents, ver" "SELECT order_id, amount * 100 AS cents, ver FROM ($(versioned)) WHERE status = 'paid'")
  echo "orders_paid: $(sql "SELECT count(*) FROM lake.analytics.orders_paid") orders, $d rows differ"
  [[ $d == 0 ]] || failed=1
}
log "the upsert views against the topic"; check_topic
echo "tombstones read: $(sql "SELECT count(*) FROM orders WHERE _error = 'tombstone'"), deleted orders kept as tombstones: $(sql "SELECT count(*) FROM lake.__duckwake.state_orders_versioned WHERE __del")"
[[ $(sql "SELECT count(*) FROM lake.analytics.orders_latest") -gt 0 ]] || { echo "orders_latest is empty"; failed=1; }
dead=$(sql "SELECT count(*) FROM lake.__duckwake.dead_letter WHERE job_id LIKE 'orders_%'" || echo "?")
echo "dead letters of the upsert views (tombstones are not dead letters there): $dead"
[[ $dead == 0 ]] || failed=1
kill -TERM "$pid"; wait "$pid" || true

log "reset orders_versioned a few epochs back; it and orders_open replay"
to=$(( $(sql "SELECT max(epoch) FROM lake.__duckwake.checkpoints WHERE job_id = 'orders_versioned'") - 3 ))
"$BIN" pause orders_versioned >/dev/null
"$BIN" reset orders_versioned --to-epoch "$to" | sed -n 1p
"$BIN" start orders_versioned --worker w1 >/dev/null
"$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 &
pid=$!; settle
kill -TERM "$pid"; wait "$pid" || true
verify_views || failed=1
check_topic

[[ $failed == 0 ]] && echo "UPSERT TEST PASSED" || { echo "UPSERT TEST FAILED"; exit 1; }
