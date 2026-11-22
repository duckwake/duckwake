#!/usr/bin/env bash
# D3 gate: a MySQL source ingested into mirror tables from the binlog by GTID, exactly once, while the worker is
# killed again and again.
#
# A stored procedure changes three tables for DURATION seconds in transactions of one to three statements:
# inserts, updates (of a large text value too), primary key changes (one column and composite), deletes, and a
# TRUNCATE now and then. Meanwhile the worker is killed from outside or aborted inside the ingest epoch (before
# its commit, or after it). Once the workload stops and the job catches up, every mirror table must equal its
# source table (`duckwake verify`, which reads the server through the same text forms and casts), one snapshot
# must have served every restart, and a view over a mirror must verify. Then: binlog purged past the lake's
# position stops the job; a reset takes a new snapshot and catches up; a column added in MySQL stops the job;
# drop-source is refused while a view reads a mirror, then drops the mirrors.
#
# The server: with MYSQL_LOCAL=1 a MySQL 8 on 127.0.0.1:13306 whose root needs no password (GTIDs on, row binlog);
# otherwise the compose service mysql-source (profile mysql). The catalog's PostgreSQL as for every suite
# (PG_LOCAL=1 for a local one).
source "$(dirname "$0")/lib.sh"
DURATION=${DURATION:-40}; KILLS=${KILLS:-8}; EPOCH=${EPOCH:-1s}
read -r -a POINTS <<< "${POINTS:-external ingest_before_commit ingest_after_commit}"
if [[ ${MYSQL_LOCAL:-} == 1 ]]; then
  my() { mysql -uroot -h127.0.0.1 -P13306 -N -B "$@"; }
else
  docker compose --profile mysql up -d --wait mysql-source >/dev/null
  my() { docker compose --profile mysql exec -T -e MYSQL_PWD=root mysql-source mysql -uroot -N -B "$@"; }
fi
# The password reaches DuckWake as a reference, never stored in the lake.
export DUCKWAKE_MY_PASSWORD=dw-secret
sql() { "$BIN" sql --csv "$1" 2>>"$OUT/sql.log" | tail -1; }
status() { sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id IN ('$1', 'analytics.$1')"; }
WORKER=""
start_worker() { "$BIN" run $WORKER_FLAGS --worker w1 --flush-every 3s --merge-every 7s >>"$OUT/worker.log" 2>&1 & WORKER=$!; }
stop_worker() { [[ -z $WORKER ]] || { kill -TERM "$WORKER" 2>/dev/null; wait "$WORKER" 2>/dev/null || true; WORKER=""; }; }
trap 'stop_worker || true' EXIT
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
my <<'SQL'
CREATE USER IF NOT EXISTS 'duckwake'@'%' IDENTIFIED BY 'dw-secret';
GRANT SELECT, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'duckwake'@'%';
SET SESSION cte_max_recursion_depth = 100000;
DROP DATABASE IF EXISTS duckwake_src; CREATE DATABASE duckwake_src; USE duckwake_src;
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer INT, status VARCHAR(20), amount DECIMAL(18,2), note MEDIUMTEXT,
  payload JSON, kind ENUM('retail','wholesale','return'), tags SET('gift','rush','fragile'), code CHAR(4),
  blob_v VARBINARY(16), ratio DOUBLE, score FLOAT, flags BIT(8), updated_at TIMESTAMP(6) NULL, ship DATE,
  placed DATETIME(3), wait TIME, yr YEAR, big BIGINT UNSIGNED);
CREATE TABLE order_lines (order_id BIGINT, line INT, sku VARCHAR(20), qty INT, PRIMARY KEY (order_id, line));
CREATE TABLE flags (k VARCHAR(10) PRIMARY KEY, v INT);
INSERT INTO orders WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 2000)
  SELECT i, i % 97, 'new', i * 1.25, IF(i % 7 = 0, REPEAT(MD5(i), 200), NULL),
    JSON_OBJECT('i', i, 'tag', CONCAT('x', i), 'nums', JSON_ARRAY(i, 2.5)), ELT(1 + i % 3, 'retail', 'wholesale', 'return'),
    IF(i % 2 = 0, 'gift,rush', 'fragile'), 'ab', UNHEX(MD5(i)), i / 7.0, i / 3.0, i % 256,
    TIMESTAMP('2026-01-01 00:00:00') + INTERVAL i SECOND, DATE('2026-01-01') + INTERVAL i DAY,
    TIMESTAMP('2026-01-01 00:00:00.123') + INTERVAL i MINUTE, SEC_TO_TIME(i * 37 - 30000), 2000 + i % 50, i * 1000003 FROM r;
INSERT INTO order_lines WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 2000)
  SELECT i, l, CONCAT('sku', l), l FROM r, (SELECT 1 AS l UNION ALL SELECT 2 UNION ALL SELECT 3) ls;
INSERT INTO flags WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM r WHERE i < 9) SELECT CONCAT('k', i), i FROM r;
DELIMITER //
-- Transactions of one to three statements, chosen at random, for `seconds`.
CREATE PROCEDURE workload(seconds INT)
BEGIN
  DECLARE t0 DATETIME(6) DEFAULT NOW(6); DECLARE n BIGINT DEFAULT 0; DECLARE r INT; DECLARE kid BIGINT; DECLARE nxt BIGINT;
  SET autocommit = 0;
  WHILE NOW(6) < t0 + INTERVAL seconds SECOND DO
    SET n = n + 1, r = FLOOR(RAND() * 100);
    SELECT id INTO kid FROM orders WHERE id >= (SELECT FLOOR(RAND() * MAX(id)) FROM orders) ORDER BY id LIMIT 1;
    SELECT MAX(id) + 1 INTO nxt FROM orders;
    IF r < 25 OR kid IS NULL THEN
      INSERT INTO orders (id, customer, status, amount, note, payload, kind, tags, code, ratio, updated_at, placed, wait)
        VALUES (nxt, nxt % 97, 'new', nxt * 1.25, IF(nxt % 5 = 0, REPEAT(MD5(nxt), 300), NULL), JSON_OBJECT('i', nxt),
        'retail', 'rush', 'cd', 0.5, NOW(6), NOW(3), '-01:02:03');
      INSERT INTO order_lines VALUES (nxt, 1, 'sku1', 1), (nxt, 2, 'sku2', 2);
    ELSEIF r < 50 THEN UPDATE orders SET status = 'paid', amount = amount + 1, updated_at = NOW(6), tags = 'gift', kind = 'return' WHERE id = kid;
    ELSEIF r < 57 THEN UPDATE orders SET note = REPEAT(MD5(RAND()), 300), payload = JSON_SET(payload, '$.n', n) WHERE id = kid;
    ELSEIF r < 64 THEN UPDATE orders SET id = nxt WHERE id = kid; UPDATE order_lines SET order_id = nxt WHERE order_id = kid;
    ELSEIF r < 74 THEN DELETE FROM orders WHERE id = kid; DELETE FROM order_lines WHERE order_id = kid;
    ELSEIF r < 84 THEN UPDATE order_lines SET qty = qty + 1 WHERE order_id = kid;
    ELSEIF r < 90 THEN UPDATE order_lines SET line = line + 100 WHERE order_id = kid AND line < 100;
    ELSEIF r < 99 THEN UPDATE flags SET v = v + 1 WHERE k = CONCAT('k', FLOOR(RAND() * 10));
    ELSE COMMIT; TRUNCATE flags; INSERT INTO flags WITH RECURSIVE x(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM x WHERE i < 9) SELECT CONCAT('k', i), i * 10 FROM x;
    END IF;
    IF n % (1 + FLOOR(RAND() * 3)) = 0 THEN COMMIT; END IF;
  END WHILE;
  COMMIT;
  SELECT CONCAT('workload: ', n, ' statements');
END//
DELIMITER ;
SQL

log "create the source and a view over its orders"
"$BIN" create-source shop --kind mysql --options "{\"url\": \"mysql://duckwake@127.0.0.1:13306\", \"password\": \"env:DUCKWAKE_MY_PASSWORD\",
  \"tables\": [\"duckwake_src.orders\", \"duckwake_src.order_lines\", \"duckwake_src.flags\"], \"epoch_interval\": \"$EPOCH\"}"
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
my duckwake_src -e "CALL workload($DURATION)" >"$OUT/workload.log" 2>&1 &
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
  echo "run $i: $how; position $(sql "SELECT coalesce(max_by(source_offsets->'shop'->>'0', epoch), 'none') FROM lake.__duckwake.checkpoints WHERE job_id = 'shop'") transactions"
done
wait "$WORKLOAD" || { cat "$OUT/workload.log"; failed=1; }
grep -o "workload: [0-9]* statements" "$OUT/workload.log" || true

log "catch up, then every mirror equals its table"
start_worker
wait_verified 180 && cat "$OUT/verify_shop.txt" || failed=1
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), ingest epochs: $(grep -c 'ingest epoch committed' "$OUT/worker.log" || true)"
snapshots=$(grep -c "snapshot committed" "$OUT/worker.log" || true)
[[ $snapshots == 1 ]] && echo "one snapshot: every restart continued from the lake's position" || { echo "$snapshots snapshots committed, not 1"; failed=1; }
for _ in $(seq 1 30); do "$BIN" verify orders_by_status >"$OUT/verify_view.txt" 2>&1 && break; sleep 1; done
tail -1 "$OUT/verify_view.txt" | grep '^VERIFY OK' || { cat "$OUT/verify_view.txt"; failed=1; }

log "binlog purged past the lake's position stops the job"
stop_worker
my duckwake_src -e "INSERT INTO flags VALUES ('lost', 1); FLUSH BINARY LOGS;"
# SHOW BINARY LOG STATUS from MySQL 8.2, SHOW MASTER STATUS before.
current=$(my -e "SHOW BINARY LOG STATUS" 2>/dev/null | cut -f1 || true)
[[ -n $current ]] || current=$(my -e "SHOW MASTER STATUS" | cut -f1)
my -e "PURGE BINARY LOGS TO '$current'"
start_worker
if wait_status shop failed; then grep -o "the server purged binlog the mirrors had not read" "$OUT/worker.log" | tail -1
else echo "shop is $(status shop), not failed"; failed=1; fi

log "reset takes a new snapshot, while the workload runs again"
"$BIN" reset shop --to-epoch 0
my duckwake_src -e "CALL workload(12)" >"$OUT/workload2.log" 2>&1 &
WORKLOAD=$!
sleep 2
"$BIN" start shop >/dev/null
wait "$WORKLOAD" || { cat "$OUT/workload2.log"; failed=1; }
wait_verified 120 && cat "$OUT/verify_shop.txt" || failed=1
echo "the snapshot under load: $(grep "snapshot committed" "$OUT/worker.log" | tail -1 | grep -o "rows=[0-9]* changes=[0-9]*" || true) (changes: the binlog between the GTID sets read before and after the copy, applied over it)"
[[ $(grep -c "snapshot committed" "$OUT/worker.log") == 2 ]] && echo "a second snapshot after the reset" || { echo "no second snapshot"; failed=1; }
for _ in $(seq 1 30); do "$BIN" verify orders_by_status >"$OUT/verify_view.txt" 2>&1 && break; sleep 1; done
tail -1 "$OUT/verify_view.txt" | grep '^VERIFY OK' || { cat "$OUT/verify_view.txt"; failed=1; }

log "a column added in MySQL stops the job"
my duckwake_src -e "ALTER TABLE flags ADD COLUMN extra INT; UPDATE flags SET v = v + 1 WHERE k = 'k1'"
if wait_status shop failed; then grep -o "duckwake_src.flags changed in MySQL" "$OUT/worker.log" | tail -1
else echo "shop is $(status shop), not failed"; failed=1; fi
[[ $(status orders_by_status) == running ]] && echo "orders_by_status: still running" || { echo "orders_by_status is $(status orders_by_status)"; failed=1; }

log "drop-source"
stop_worker
"$BIN" stop orders_by_status >/dev/null
if out=$("$BIN" drop-source shop 2>&1); then echo "drop-source was not refused"; failed=1
else echo "$out" | grep -o "views analytics.orders_by_status read source shop's tables[^;]*" || { echo "$out"; failed=1; }; fi
"$BIN" drop orders_by_status >/dev/null
"$BIN" drop-source shop
mirrors=$(sql "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'shop'")
[[ $mirrors == 0 ]] && echo "mirror tables are gone" || { echo "left behind: $mirrors mirror tables"; failed=1; }

[[ $failed == 0 ]] && echo "MYSQL INGEST TEST PASSED" || { echo "MYSQL INGEST TEST FAILED"; grep -E "WARN|ERROR" "$OUT/worker.log" | tail -20; exit 1; }
