#!/usr/bin/env bash
# C3 end to end: a real PostgreSQL, Debezium's PostgreSQL connector (Kafka Connect) and DuckWake.
#
# Debezium snapshots shop.orders (2,000 rows) and then streams a workload of inserts, updates, deletes and
# primary-key changes, some in multi-statement transactions, into three-partition topics, with schemas.enable
# on (Connect's default), decimals as strings and timestamps as microseconds. Three views read it: the table's
# mirror (an upsert view), a filtered upsert view, and an aggregate over the mirror's change feed. While the
# workload runs, the DuckWake worker is killed again and again and every view verifies after each crash; once,
# Kafka Connect is restarted, so Debezium resends what it had not committed. At the end each view must equal the
# PostgreSQL table, compared with SQL over the table exported from PostgreSQL, and verify again.
#
# Then the TOAST case: shop.docs keeps large text values out of line (TOAST) with the default REPLICA IDENTITY,
# so an update of another column carries Debezium's placeholder for them. docs_mirror must stop with an error
# that says what to do, and the orders views keep running.
TOPIC=${TOPIC:-pg.public.orders}
source "$(dirname "$0")/lib.sh"
ROUNDS=${ROUNDS:-40}; KILLS=${KILLS:-6}; EPOCH=${EPOCH:-2s}
read -r -a POINTS <<< "${POINTS:-external poll commit_begin commit_mid before_commit after_commit staged_ahead}"
CONNECT=http://localhost:8083
RUN=$(date +%s)
DOCS_TOPIC=pg.public.docs
VIEWS=(orders_mirror orders_paid orders_by_status docs_mirror)

# The profile's services by name: compose leaves services of an inactive profile out otherwise.
dc() { docker compose --profile debezium "$@"; }
pg() { dc exec -T pg-source psql -v ON_ERROR_STOP=1 -qtA -U shop -d shop "$@"; }
sql() { "$BIN" sql --csv "$1" 2>>"$OUT/sql.log" | tail -1; }
status() { sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id IN ('$1', 'analytics.$1')"; }

verify_views() {
  local rc=0 v
  for v in "${@:-${VIEWS[@]}}"; do
    if "$BIN" verify "$v" >"$OUT/verify_$v.txt" 2>&1; then
      [[ ${QUIET:-} == 1 ]] || tail -1 "$OUT/verify_$v.txt"
    elif grep -q "job has no checkpoint yet" "$OUT/verify_$v.txt"; then
      : # crashed before the view's first commit: nothing to check yet
    else
      rc=1; cat "$OUT/verify_$v.txt"
    fi
  done
  return $rc
}

log "reset the lake, the source database, Connect and the topics"
dc rm -sf pg-source connect >/dev/null 2>&1 || true
reset_env
for t in "$TOPIC" "$DOCS_TOPIC" connect_configs connect_offsets connect_statuses; do
  docker compose exec -T redpanda rpk topic delete "$t" >/dev/null 2>&1 || true
done
dc up -d --wait pg-source connect
for _ in $(seq 1 90); do curl -fs "$CONNECT/connectors" >/dev/null 2>&1 && break; sleep 2; done
curl -fs "$CONNECT/connectors" >/dev/null || { echo "Kafka Connect did not come up"; dc logs --tail 80 connect; exit 1; }
echo "Kafka Connect $(curl -fs "$CONNECT/" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p') with $(curl -fs "$CONNECT/connector-plugins" | grep -o 'PostgresConnector' | head -1)"

log "tables in PostgreSQL, 2,000 orders before Debezium starts (its snapshot)"
pg <<'SQL'
CREATE TABLE orders (order_id BIGINT PRIMARY KEY, status TEXT NOT NULL, amount NUMERIC(18,2),
                     updated_at TIMESTAMP(6) NOT NULL, placed_on DATE);
INSERT INTO orders SELECT i, (ARRAY['new','paid','shipped','cancelled'])[1 + i % 4], round((random() * 500)::numeric, 2),
                          clock_timestamp(), DATE '2026-01-01' + i % 300
                   FROM generate_series(1, 2000) i;
-- Bodies of about 13 kB of hex: stored out of line (TOAST), and left out of an update that does not change them.
CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, body TEXT);
INSERT INTO docs SELECT i, 'doc ' || i, (SELECT string_agg(md5(random()::text || i || j), '') FROM generate_series(1, 400) j)
                 FROM generate_series(1, 5) i;
SQL
echo "orders: $(pg -c "SELECT count(*) FROM orders"), docs: $(pg -c "SELECT count(*) FROM docs") (REPLICA IDENTITY $(pg -c "SELECT relreplident FROM pg_class WHERE relname = 'docs'"))"

log "register Debezium's connector"
cat >"$OUT/connector.json" <<JSON
{"name": "shop_$RUN", "config": {
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector", "plugin.name": "pgoutput",
  "database.hostname": "localhost", "database.port": "15432", "database.user": "shop", "database.password": "shop",
  "database.dbname": "shop", "topic.prefix": "pg", "table.include.list": "public.orders,public.docs",
  "slot.name": "duckwake_$RUN", "publication.name": "duckwake_$RUN", "publication.autocreate.mode": "filtered",
  "snapshot.mode": "initial", "decimal.handling.mode": "string",
  "topic.creation.default.partitions": "3", "topic.creation.default.replication.factor": "1"}}
JSON
curl -fsS -X POST -H 'Content-Type: application/json' --data @"$OUT/connector.json" "$CONNECT/connectors" >/dev/null
connector_running() { curl -fs "$CONNECT/connectors/shop_$RUN/status" 2>/dev/null | grep -o '"state":"[A-Z]*"' | sort -u | tr '\n' ' '; }
for _ in $(seq 1 60); do [[ $(connector_running) == '"state":"RUNNING" ' ]] && break; sleep 2; done
echo "connector shop_$RUN: $(connector_running)"
[[ $(connector_running) == '"state":"RUNNING" ' ]] || { curl -s "$CONNECT/connectors/shop_$RUN/status"; dc logs --tail 80 connect; exit 1; }
for _ in $(seq 1 60); do (( $(topic_end "$TOPIC") >= 2000 )) && break; sleep 1; done
echo "snapshot: $(topic_end "$TOPIC") events in $TOPIC"

log "DuckWake: the sources and four views"
cols='"order_id":"BIGINT","status":"VARCHAR","amount":"DECIMAL(18,2)","updated_at":"TIMESTAMP","placed_on":"DATE"'
"$BIN" create-source orders --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"format\":\"debezium\",\"columns\":{$cols}}"
"$BIN" create-source docs --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$DOCS_TOPIC\",\"format\":\"debezium\",
  \"columns\":{\"id\":\"INTEGER\",\"title\":\"VARCHAR\",\"body\":\"VARCHAR\"}}"
create() { "$BIN" create-view "analytics.$1" --epoch-interval "$EPOCH" "${@:2}" | sed -n 1p; }
create orders_mirror --upsert-key order_id --sql "SELECT * FROM orders"
create orders_paid --upsert-key order_id --sql "SELECT order_id, amount, updated_at FROM orders WHERE status = 'paid'"
create orders_by_status --sql "SELECT status, count(*) AS n, sum(amount) AS total FROM analytics.orders_mirror GROUP BY status"
create docs_mirror --upsert-key id --sql "SELECT id, title, body FROM docs"
for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker w1 >/dev/null; done

# One round of the workload: new orders, updates, deletes, and a transaction that changes a primary key (Debezium
# sends a delete of the old key and an insert of the new one, possibly in different partitions).
round() {
  pg <<SQL
INSERT INTO orders SELECT 100000 + $1 * 100 + i, 'new', round((random() * 500)::numeric, 2), clock_timestamp(), current_date
                   FROM generate_series(1, 30) i;
UPDATE orders SET status = (ARRAY['new','paid','shipped','cancelled'])[1 + floor(random() * 4)::int],
                  amount = round((random() * 500)::numeric, 2), updated_at = clock_timestamp()
 WHERE order_id % 37 = $1 % 37;
DELETE FROM orders WHERE order_id % 211 = $1 % 211 AND order_id < 1000000000;
BEGIN;
UPDATE orders SET status = 'paid', updated_at = clock_timestamp() WHERE order_id % 53 = $1 % 53;
UPDATE orders SET order_id = order_id + 1000000000, updated_at = clock_timestamp()
 WHERE order_id = (SELECT min(order_id) FROM orders WHERE order_id % 19 = $1 % 19 AND order_id < 1000000000);
COMMIT;
SQL
}

log "the workload ($ROUNDS rounds) while the worker is killed $KILLS times and Connect restarts once"
( for i in $(seq 1 "$ROUNDS"); do round "$i" || exit 1; sleep 0.5; done ) >"$OUT/workload.log" 2>&1 &
WORKLOAD=$!
failed=0
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
  if (( i == KILLS / 2 )); then
    dc restart connect >/dev/null
    how="$how; Connect restarted"
  fi
  if QUIET=1 verify_views; then echo "run $i: $how; all verify"; else echo "run $i: $how; VERIFY FAILED"; failed=1; fi
done
wait "$WORKLOAD" || { echo "the workload failed:"; tail -20 "$OUT/workload.log"; exit 1; }
for _ in $(seq 1 60); do [[ $(connector_running) == '"state":"RUNNING" ' ]] && break; sleep 2; done
echo "connector after its restart: $(connector_running)"

log "drain: Debezium to the end of the WAL, DuckWake to the end of the topic"
# The connector has caught up when the topic stops growing.
last=-1; for _ in $(seq 1 60); do e=$(topic_end "$TOPIC"); [[ $e == "$last" ]] && break; last=$e; sleep 3; done
"$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 &
pid=$!; end=$(topic_end "$TOPIC")
for _ in $(seq 1 120); do [[ "$(committed orders_mirror orders)" == "$end" && "$(committed orders_paid orders)" == "$end" ]] && break; sleep 1; done
for _ in $(seq 1 60); do
  [[ $(sql "SELECT (SELECT coalesce(sum(n), 0) FROM lake.analytics.orders_by_status) = (SELECT count(*) FROM lake.analytics.orders_mirror)") == true ]] && break
  sleep 1
done
echo "$end events in $TOPIC; injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), epochs committed: $(grep -c 'epoch committed' "$OUT/worker.log" || true)"
verify_views || failed=1

log "each view against PostgreSQL"
pg -c "\\copy (SELECT order_id, status, amount, updated_at, placed_on FROM orders) TO STDOUT WITH CSV" >"$OUT/orders.csv"
pg -c "\\copy (SELECT id, title, md5(body) FROM docs) TO STDOUT WITH CSV" >"$OUT/docs.csv"
PG_ORDERS="SELECT * FROM read_csv('$OUT/orders.csv', header = false,
  columns = {'order_id': 'BIGINT', 'status': 'VARCHAR', 'amount': 'DECIMAL(18,2)', 'updated_at': 'TIMESTAMP', 'placed_on': 'DATE'})"
# Rows of query $2 and of query $3 that are not in the other: 0 when they hold the same rows.
differ() { sql "SELECT count(*) FROM (($2 EXCEPT ALL $3) UNION ALL ($3 EXCEPT ALL $2))"; }
check() {
  local d; d=$(differ "$@")
  echo "$1: $(sql "SELECT count(*) FROM ($3)") rows, $d differ from PostgreSQL"
  [[ $d == 0 ]] || failed=1
}
check orders_mirror "SELECT order_id, status, amount, updated_at, placed_on FROM ($PG_ORDERS)" \
  "SELECT order_id, status, amount, updated_at, placed_on FROM lake.analytics.orders_mirror"
check orders_paid "SELECT order_id, amount, updated_at FROM ($PG_ORDERS) WHERE status = 'paid'" \
  "SELECT order_id, amount, updated_at FROM lake.analytics.orders_paid"
check orders_by_status "SELECT status, count(*) AS n, sum(amount) AS total FROM ($PG_ORDERS) GROUP BY status" \
  "SELECT status, n, total FROM lake.analytics.orders_by_status"
check docs_mirror "SELECT * FROM read_csv('$OUT/docs.csv', header = false, columns = {'id': 'INTEGER', 'title': 'VARCHAR', 'h': 'VARCHAR'})" \
  "SELECT id, title, md5(body) FROM lake.analytics.docs_mirror"
echo "PostgreSQL: $(pg -c "SELECT count(*) FROM orders") orders, $(pg -c "SELECT count(*) FROM orders WHERE order_id >= 1000000000") with a changed key"
echo "deletes read: $(sql "SELECT count(*) FROM orders WHERE _error = 'tombstone'") (a d event and a tombstone each)"
dead=$(sql "SELECT count(*) FROM lake.__duckwake.dead_letter")
echo "dead letters: $dead"
[[ $dead == 0 ]] || { sql "SELECT job_id, error, count(*) FROM lake.__duckwake.dead_letter GROUP BY ALL"; failed=1; }

log "an update that leaves a TOASTed value out stops docs_mirror, and only it"
pg -c "UPDATE docs SET title = 'renamed' WHERE id = 1"
for _ in $(seq 1 30); do [[ $(status docs_mirror) == failed ]] && break; sleep 1; done
[[ $(status docs_mirror) == failed ]] && echo "docs_mirror: failed, as it must" || { echo "docs_mirror is $(status docs_mirror), not failed"; failed=1; }
grep -m1 -o "source docs: the Debezium event.*left out" "$OUT/worker.log" && grep -q "Set REPLICA IDENTITY FULL on the table" "$OUT/worker.log" ||
  { echo "the error does not say what to fix"; failed=1; }
for v in orders_mirror orders_paid orders_by_status; do
  [[ $(status "$v") == running ]] || { echo "$v is $(status "$v"), not running"; failed=1; }
done
echo "the orders views are still running"
kill -TERM "$pid"; wait "$pid" || true
curl -fs -X DELETE "$CONNECT/connectors/shop_$RUN" >/dev/null || true

# CI prints the default services' logs on failure; Connect is in a profile, so its log is printed here.
[[ $failed == 0 ]] && echo "DEBEZIUM E2E TEST PASSED" || { dc logs --tail 100 connect; echo "DEBEZIUM E2E TEST FAILED"; exit 1; }
