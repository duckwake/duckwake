#!/usr/bin/env bash
# D1 gate: a PostgreSQL source ingested into mirror tables, exactly once, while the worker is killed again and
# again.
#
# A PL/pgSQL workload changes three tables for DURATION seconds in transactions of one to three statements:
# inserts, updates that leave a large out-of-line (TOAST) value unchanged, updates of that value, primary key
# changes (one column and composite), deletes, and a TRUNCATE now and then. Meanwhile the worker is killed from
# outside or aborted inside the ingest epoch (before its commit, or after it but before the slot is advanced),
# and after every crash the slot's confirmed position must not be past the lake's. Once the workload stops and
# the job catches up, every mirror table must equal its source table (`duckwake verify`, which reads the source
# through the same text forms and casts) and a view over a mirror must verify. Then: a slot advanced past the
# lake stops the job; a reset takes a new snapshot and catches up; a column added in PostgreSQL stops the job;
# drop-source is refused while a view reads a mirror, then drops the slot, the publication and the mirrors.
#
# The source database: with PG_LOCAL=1 a server already on localhost:55432 (role lake, wal_level = logical);
# otherwise the compose service pg-source (profile debezium, localhost:15432). WORKER_FLAGS="--pipeline ..." runs
# the worker with those flags (the ingest job has its own thread; the view over the mirror uses them).
source "$(dirname "$0")/lib.sh"
DURATION=${DURATION:-40}; KILLS=${KILLS:-8}; EPOCH=${EPOCH:-1s}
read -r -a POINTS <<< "${POINTS:-external ingest_before_commit ingest_after_commit}"
SRC_DB=duckwake_src
if [[ $PG_LOCAL == 1 ]]; then
  SRC_PORT=55432; SRC_USER=lake
  psql_src_admin() { PGOPTIONS='-c client_min_messages=warning' psql -h localhost -p 55432 -U lake -v ON_ERROR_STOP=1 -qtA "$@"; }
  PASSWORD_OPT=""
else
  docker compose --profile debezium up -d --wait pg-source >/dev/null
  SRC_PORT=15432; SRC_USER=shop
  psql_src_admin() { docker compose --profile debezium exec -T -e PGOPTIONS='-c client_min_messages=warning' pg-source psql -U shop -v ON_ERROR_STOP=1 -qtA "$@"; }
  # The password reaches DuckWake as a reference, never stored in the lake.
  export DUCKWAKE_SRC_PASSWORD=shop
  PASSWORD_OPT=', "password": "env:DUCKWAKE_SRC_PASSWORD"'
fi
psql_src() { psql_src_admin -d "$SRC_DB" "$@"; }
DSN="host=localhost port=$SRC_PORT dbname=$SRC_DB user=$SRC_USER"
sql() { "$BIN" sql --csv "$1" 2>>"$OUT/sql.log" | tail -1; }
status() { sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id IN ('$1', 'analytics.$1')"; }
lake_lsn() { sql "SELECT coalesce(max_by((source_offsets->'shop'->>'0')::UBIGINT, epoch), 0) FROM lake.__duckwake.checkpoints WHERE job_id = 'shop'"; }
slot_lsn() { psql_src -c "SELECT coalesce((SELECT (confirmed_flush_lsn - '0/0')::bigint FROM pg_replication_slots WHERE slot_name = 'duckwake_shop'), 0)"; }
WORKER=""
start_worker() { "$BIN" run $WORKER_FLAGS --worker w1 --flush-every 3s --merge-every 7s >>"$OUT/worker.log" 2>&1 & WORKER=$!; }
stop_worker() { [[ -z $WORKER ]] || { kill -TERM "$WORKER" 2>/dev/null; wait "$WORKER" 2>/dev/null || true; WORKER=""; }; }
trap 'stop_worker || true' EXIT
# Waits for `verify shop` to pass: the job has caught up once the mirrors equal the source.
wait_verified() {
  for _ in $(seq 1 "${1:-120}"); do
    "$BIN" verify shop >"$OUT/verify_shop.txt" 2>&1 && return 0
    sleep 1
  done
  cat "$OUT/verify_shop.txt"; return 1
}
wait_status() {
  for _ in $(seq 1 60); do [[ $(status "$1") == "$2" ]] && return 0; sleep 1; done
  return 1
}

rm -rf "$OUT"; mkdir -p "$OUT"
log "reset lake and the source database"
reset_lake
psql_src_admin -d postgres -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '$SRC_DB' OR slot_name = 'duckwake_shop'" >/dev/null
psql_src_admin -d postgres -c "DROP DATABASE IF EXISTS $SRC_DB" -c "CREATE DATABASE $SRC_DB" >/dev/null
psql_src <<'SQL'
CREATE TABLE orders (id bigint PRIMARY KEY, customer int, status text, amount numeric(18,2), note text, payload jsonb,
  tags int[], code char(4), blob bytea, wait interval, ratio float8, updated_at timestamptz, ship date);
CREATE TABLE lines (order_id bigint, line int, sku text, qty int, PRIMARY KEY (order_id, line));
CREATE TABLE flags (k text PRIMARY KEY, v int);
-- Hex of random md5s does not compress, so 4,800 characters are stored out of line (TOAST).
CREATE FUNCTION big() RETURNS text LANGUAGE sql AS $$ SELECT string_agg(md5(random()::text || g), '') FROM generate_series(1, 150) g $$;
INSERT INTO orders SELECT i, i % 97, 'new', i * 1.25, CASE WHEN i % 7 = 0 THEN big() END, jsonb_build_object('i', i, 'tag', 'x' || i),
  ARRAY[i, i + 1], 'ab', decode(md5(i::text), 'hex'), make_interval(days => i % 5 - 2, mins => i % 60), i / 7.0,
  now() - make_interval(secs => i), date '2026-01-01' + i FROM generate_series(1, 2000) i;
INSERT INTO lines SELECT i, l, 'sku' || l, l FROM generate_series(1, 2000) i, generate_series(1, 3) l;
INSERT INTO flags SELECT 'k' || i, i FROM generate_series(0, 9) i;
CREATE FUNCTION pick() RETURNS bigint LANGUAGE sql AS $$
  SELECT id FROM orders WHERE id >= (SELECT (random() * max(id))::bigint FROM orders) ORDER BY id LIMIT 1 $$;
-- Transactions of one to three statements, chosen at random, for `seconds`.
CREATE PROCEDURE workload(seconds int) LANGUAGE plpgsql AS $$
DECLARE t0 timestamptz := clock_timestamp(); n bigint := 0; r int; kid bigint; nxt bigint;
BEGIN
  WHILE clock_timestamp() < t0 + make_interval(secs => seconds) LOOP
    n := n + 1; r := (random() * 100)::int; kid := pick();
    SELECT max(id) + 1 INTO nxt FROM orders;
    IF r < 25 OR kid IS NULL THEN
      INSERT INTO orders VALUES (nxt, nxt % 97, 'new', nxt * 1.25, CASE WHEN nxt % 5 = 0 THEN big() END,
        jsonb_build_object('i', nxt), ARRAY[nxt], 'cd', '\x00ff', '1 day', 0.5, now(), current_date);
      INSERT INTO lines SELECT nxt, l, 'sku' || l, l FROM generate_series(1, 2) l;
    ELSIF r < 50 THEN UPDATE orders SET status = 'paid', amount = amount + 1, updated_at = now() WHERE id = kid;
    ELSIF r < 57 THEN UPDATE orders SET note = big() WHERE id = kid;
    ELSIF r < 64 THEN UPDATE orders SET id = nxt WHERE id = kid; UPDATE lines SET order_id = nxt WHERE order_id = kid;
    ELSIF r < 74 THEN DELETE FROM orders WHERE id = kid; DELETE FROM lines WHERE order_id = kid;
    ELSIF r < 84 THEN UPDATE lines SET qty = qty + 1 WHERE order_id = kid;
    ELSIF r < 90 THEN UPDATE lines SET line = line + 100 WHERE order_id = kid AND line < 100;
    ELSIF r < 99 THEN UPDATE flags SET v = v + 1 WHERE k = 'k' || (random() * 9)::int;
    ELSE TRUNCATE flags; INSERT INTO flags SELECT 'k' || i, i * 10 FROM generate_series(0, 9) i;
    END IF;
    IF n % (1 + (random() * 2)::int) = 0 THEN COMMIT; END IF;
  END LOOP;
  COMMIT;
  RAISE WARNING 'workload: % statements', n;
END $$;
SQL

log "create the source and a view over its orders"
"$BIN" create-source shop --kind postgres --options "{\"dsn\": \"$DSN\", \"tables\": [\"orders\", \"lines\", \"public.flags\"],
  \"epoch_interval\": \"$EPOCH\"$PASSWORD_OPT}"
"$BIN" start shop >/dev/null
start_worker
wait_status shop running
for _ in $(seq 1 60); do grep -q "snapshot committed" "$OUT/worker.log" && break; sleep 1; done
grep "snapshot committed" "$OUT/worker.log" | tail -1 | sed 's/.*INFO //'
"$BIN" create-view analytics.orders_by_status --epoch-interval 2s \
  --sql "SELECT status, count(*) AS n, sum(amount) AS amount, count(note) AS notes FROM shop.orders GROUP BY ALL" | sed -n 1p
"$BIN" start orders_by_status >/dev/null
stop_worker

log "workload for ${DURATION}s while the worker is killed $KILLS times"
psql_src -c "CALL workload($DURATION)" >"$OUT/workload.log" 2>&1 &
WORKLOAD=$!
failed=0
for i in $(seq 1 "$KILLS"); do
  point=${POINTS[$((RANDOM % ${#POINTS[@]}))]}
  if [[ $point == external ]]; then
    start_worker; sleep "$((2 + RANDOM % 4)).$((RANDOM % 10))"; kill -9 "$WORKER" 2>/dev/null || true; wait "$WORKER" 2>/dev/null || true
    how="kill -9 from outside"
  else
    DUCKWAKE_CRASH_AT=$point DUCKWAKE_CRASH_PROB=0.3 "$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 & WORKER=$!
    for _ in $(seq 1 80); do kill -0 "$WORKER" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$WORKER" 2>/dev/null && how="no crash in 8 s, killed" || how="aborted at $point"
    wait "$WORKER" 2>/dev/null || true
  fi
  WORKER=""
  lake=$(lake_lsn); slot=$(slot_lsn)
  if (( slot <= lake )); then echo "run $i: $how; slot at $slot, lake at $lake"
  else echo "run $i: $how; THE SLOT IS AHEAD OF THE LAKE: slot $slot, lake $lake"; failed=1; fi
done
wait "$WORKLOAD" || { cat "$OUT/workload.log"; failed=1; }
grep -o "workload: [0-9]* statements" "$OUT/workload.log" || true

log "catch up, then every mirror equals its table"
start_worker
wait_verified 180 && cat "$OUT/verify_shop.txt" || failed=1
(( $(slot_lsn) <= $(lake_lsn) )) && echo "slot $(slot_lsn) <= lake $(lake_lsn)" || { echo "slot ahead of the lake at the end"; failed=1; }
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), ingest epochs: $(grep -c 'ingest epoch committed' "$OUT/worker.log" || true), filled from the mirror: $(grep -o 'filled_from_mirror=[0-9]*' "$OUT/worker.log" | awk -F= '{s+=$2} END {print s+0}')"
snapshots=$(grep -c "snapshot committed" "$OUT/worker.log" || true)
[[ $snapshots == 1 ]] && echo "one snapshot: every restart continued from the lake's position" || { echo "$snapshots snapshots committed, not 1"; failed=1; }
for _ in $(seq 1 30); do "$BIN" verify orders_by_status >"$OUT/verify_view.txt" 2>&1 && break; sleep 1; done
tail -1 "$OUT/verify_view.txt" | grep '^VERIFY OK' || { cat "$OUT/verify_view.txt"; failed=1; }

log "a slot advanced past the lake stops the job"
# The worker stops first, so the lake cannot hold the insert: advancing the slot past it is then always ahead
# of the lake. (Inserting first raced: an idle source moves the lake to the flush position, and when nothing
# else writes WAL, as on CI's own pg-source, the slot advanced to the same LSN was not ahead.)
stop_worker
psql_src -c "INSERT INTO flags VALUES ('lost', 1)" >/dev/null
psql_src -c "SELECT pg_replication_slot_advance('duckwake_shop', pg_current_wal_lsn())" >/dev/null
(( $(slot_lsn) > $(lake_lsn) )) || { echo "the slot did not get ahead of the lake: slot $(slot_lsn), lake $(lake_lsn)"; failed=1; }
start_worker
if wait_status shop failed; then grep -o "has confirmed [^,]*, past the lake's position[^:]*" "$OUT/worker.log" | tail -1
else echo "shop is $(status shop), not failed"; failed=1; fi

log "reset takes a new snapshot, while the workload runs again"
"$BIN" reset shop --to-epoch 0
psql_src -c "CALL workload(12)" >"$OUT/workload2.log" 2>&1 &
WORKLOAD=$!
sleep 2
"$BIN" start shop >/dev/null
wait "$WORKLOAD" || { cat "$OUT/workload2.log"; failed=1; }
wait_verified 120 && cat "$OUT/verify_shop.txt" || failed=1
echo "transactions skipped as already in the lake or a snapshot: $(grep -o 'skipped=[0-9]*' "$OUT/worker.log" | awk -F= '{s+=$2} END {print s+0}')"
[[ $(grep -c "snapshot committed" "$OUT/worker.log") == 2 ]] && echo "a second snapshot after the reset" || { echo "no second snapshot"; failed=1; }
for _ in $(seq 1 30); do "$BIN" verify orders_by_status >"$OUT/verify_view.txt" 2>&1 && break; sleep 1; done
tail -1 "$OUT/verify_view.txt" | grep '^VERIFY OK' || { cat "$OUT/verify_view.txt"; failed=1; }

log "a column added in PostgreSQL stops the job"
psql_src -c "ALTER TABLE flags ADD COLUMN extra int" -c "UPDATE flags SET v = v + 1 WHERE k = 'k1'" >/dev/null
if wait_status shop failed; then grep -o "the columns of public.flags changed[^:]*" "$OUT/worker.log" | tail -1
else echo "shop is $(status shop), not failed"; failed=1; fi
[[ $(status orders_by_status) == running ]] && echo "orders_by_status: still running" || { echo "orders_by_status is $(status orders_by_status)"; failed=1; }

log "drop-source"
stop_worker
"$BIN" stop orders_by_status >/dev/null
if out=$("$BIN" drop-source shop 2>&1); then echo "drop-source was not refused"; failed=1
else echo "$out" | grep -o "views analytics.orders_by_status read source shop's tables[^;]*" || { echo "$out"; failed=1; }; fi
"$BIN" drop orders_by_status >/dev/null
"$BIN" drop-source shop
left=$(psql_src -c "SELECT (SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'duckwake_shop') + (SELECT count(*) FROM pg_publication WHERE pubname = 'duckwake_shop')")
mirrors=$(sql "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'shop'")
[[ $left == 0 && $mirrors == 0 ]] && echo "slot, publication and mirror tables are gone" || { echo "left behind: $left slot/publication, $mirrors mirror tables"; failed=1; }

[[ $failed == 0 ]] && echo "PG INGEST TEST PASSED" || { echo "PG INGEST TEST FAILED"; grep -E "WARN|ERROR" "$OUT/worker.log" | tail -20; exit 1; }
