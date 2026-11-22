#!/usr/bin/env bash
# View names: a view is named schema.table, so two schemas can each have a view of the same table name.
#  1. a.daily and b.daily (different queries over one table) run side by side with b.top over a.daily, a view
#     in main named by a bare name, and c.over_solo reading it as `FROM solo` (main, as DuckDB reads it). A
#     command naming `daily` alone is refused and lists both; `solo` alone names main.solo.
#  2. While a generator inserts, updates and deletes rows, the worker is killed KILLS times (kill -9 and aborts
#     injected at epoch steps); after every crash, and once caught up, every view verifies.
#  3. The metrics label each view by schema.table.
#  4. A reset of a.daily (two epochs back) leaves b.daily's checkpoints and table as they were; dropping b.daily
#     leaves a.daily and b.top running, with b.daily's control tables gone and a.daily's kept.
#  5. A lake with a view named by DuckWake 0.2 (its table name alone) is refused by a worker and by
#     create-view, with a message naming the view; dropping that view by its old name clears it.
# Needs only PostgreSQL: PG_LOCAL=1 runs it against one on localhost:55432 without Docker.
source "$(dirname "$0")/lib.sh"
ROWS=${ROWS:-20000}; PAGE=${PAGE:-2000}; KILLS=${KILLS:-4}; EPOCH=${EPOCH:-1s}
METRICS=${METRICS:-127.0.0.1:9466}
read -r -a POINTS <<< "${POINTS:-external poll commit_begin before_commit after_commit staged_ahead}"
VIEWS=(a.daily b.daily b.top main.solo c.over_solo)

sql() { "$BIN" sql "$1" --csv 2>/dev/null | tail -1; }
snapshot() { sql "SELECT id FROM ducklake_current_snapshot('lake')"; }
# The snapshot view $1 (schema.table) has read its inputs up to, and its backfill cursor (empty once done).
offsets() { echo "SELECT source_offsets, unnest(json_keys(source_offsets)) AS k FROM lake.__duckwake.checkpoints
                  WHERE job_id = '$1' ORDER BY epoch DESC LIMIT 1"; }
position() { sql "SELECT coalesce(min(json_extract(source_offsets, '\$.\"' || k || '\".\"0\"')::BIGINT), 0) FROM ($(offsets "$1"))"; }
cursor() { sql "SELECT max(json_extract(source_offsets, '\$.\"' || k || '\".\"1\"')::BIGINT) FROM ($(offsets "$1"))"; }
last_epoch() { sql "SELECT coalesce(max(epoch), 0) FROM lake.__duckwake.checkpoints WHERE job_id = '$1'"; }
status() { sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id = '$1'"; }
# The table each view reads.
declare -A INPUT=([a.daily]="shop accounts" [b.daily]="shop accounts" [main.solo]="shop accounts" [b.top]="a daily"
                  [c.over_solo]="main solo")
# Whether every running view has backfilled and read its input up to snapshot $1: its position is there, or its
# input has not changed since (an idle view moves its position only every --position-refresh).
caught_up() {
  local v p
  for v in "${VIEWS[@]}"; do
    [[ $(status "$v") == running ]] || continue
    [[ -z $(cursor "$v") && $(last_epoch "$v") != 0 ]] || return 1
    p=$(position "$v"); set -- "$1" ${INPUT[$v]}
    (( p >= $1 )) || [[ $(sql "SELECT count(*) FROM ducklake_table_changes('lake', '$2', '$3', $((p + 1)), $1)") == 0 ]] || return 1
  done
}
wait_caught_up() {
  local at; at=$(snapshot)
  for _ in $(seq 1 180); do
    caught_up "$at" && return 0
    kill -0 "$pid" 2>/dev/null || { echo "the worker exited"; grep -m1 -A4 '^Error' "$OUT/worker.log" || true; return 1; }
    sleep 1
  done
  echo "the views did not catch up to snapshot $at"; return 1
}
verify_views() {
  local rc=0 v
  for v in "$@"; do
    if "$BIN" verify "$v" >"$OUT/verify_$v.txt" 2>&1; then tail -1 "$OUT/verify_$v.txt" | sed "s/^/  $v: /"
    elif grep -q "job has no checkpoint yet" "$OUT/verify_$v.txt"; then :
    else rc=1; cat "$OUT/verify_$v.txt"; fi
  done
  return $rc
}
GAMES="'g' || (random() * 6)::INTEGER"
AMOUNT="round(random() * 100, 2)::DECIMAL(18,2)"
generate() {
  touch "$OUT/generating"
  ( next=$1
    while [[ -f $OUT/generating ]]; do
      "$BIN" sql "INSERT INTO lake.shop.accounts SELECT $next + i, $GAMES, $AMOUNT FROM range(20) t(i);
                  UPDATE lake.shop.accounts SET amount = $AMOUNT WHERE id % 1301 = $((RANDOM % 1301));
                  DELETE FROM lake.shop.accounts WHERE id % 2903 = $((RANDOM % 2903));" >/dev/null 2>>"$OUT/generator.log" || true
      next=$((next + 20)); sleep 0.3
    done ) &
  GENERATOR=$!
}
stop_generating() { rm -f "$OUT/generating"; wait "$GENERATOR" || true; }
start_worker() { "$BIN" run $WORKER_FLAGS --worker w1 --lease-timeout off "$@" >>"$OUT/worker.log" 2>&1 & pid=$!; }
stop_worker() { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; }
failed=0
trap 'rm -f "$OUT/generating"; [[ -z ${pid:-} ]] || kill "$pid" 2>/dev/null || true' EXIT

log "reset lake"; rm -rf "$OUT"; mkdir -p "$OUT"; reset_lake
sql "CREATE SCHEMA lake.shop; CREATE SCHEMA lake.a; CREATE SCHEMA lake.b; CREATE SCHEMA lake.c;
     CREATE TABLE lake.shop.accounts (id INTEGER, game_id VARCHAR, amount DECIMAL(18,2));
     INSERT INTO lake.shop.accounts SELECT i, $GAMES, $AMOUNT FROM range(1, $ROWS + 1) t(i);" >/dev/null

log "1. two views named daily, and views over views across schemas"
create() { local v=$1; shift; "$BIN" create-view "$v" --epoch-interval "$EPOCH" --max-rows "$PAGE" "$@" | sed -n 1p; }
create a.daily --sql "SELECT game_id, count(*) AS n, sum(amount) AS total FROM shop.accounts GROUP BY ALL"
create b.daily --sql "SELECT game_id, max(amount) AS hi, count(DISTINCT floor(amount)) AS levels FROM shop.accounts WHERE amount >= 50 GROUP BY ALL"
create b.top --sql "SELECT game_id, n FROM a.daily WHERE n >= 10"
create solo --sql "SELECT id, game_id, amount FROM shop.accounts WHERE amount >= 95"
create c.over_solo --sql "SELECT game_id, count(*) AS n FROM solo GROUP BY ALL"
deps=$(sql "SELECT string_agg(upstream_job || '>' || job_id, ' ' ORDER BY job_id) FROM lake.__duckwake.dependencies WHERE upstream_job IS NOT NULL")
[[ $deps == "main.solo>c.over_solo a.daily>b.top" || $deps == "a.daily>b.top main.solo>c.over_solo" ]] \
  && echo "reads: $deps" || { echo "FAIL: dependencies are $deps"; failed=1; }
out=$("$BIN" start daily --worker w1 2>&1 || true)
[[ $out == *"2 views are named \"daily\""*"a.daily, b.daily"* ]] && echo "start daily: refused, $(grep -o 'name one.*' <<<"$out")" \
  || { echo "FAIL: start daily said: $out"; failed=1; }
[[ $(status a.daily) == created && $(status b.daily) == created ]] || { echo "FAIL: start daily started a view"; failed=1; }
"$BIN" start solo --worker w1 | sed 's/^/  /'
[[ $(status main.solo) == running ]] || { echo "FAIL: start solo did not start main.solo"; failed=1; }
for v in a.daily b.daily b.top c.over_solo; do "$BIN" start "$v" --worker w1 >/dev/null; done

log "2. the table changes while the worker is killed $KILLS times"
generate $((ROWS + 1000))
for i in $(seq 1 "$KILLS"); do
  point=${POINTS[$((RANDOM % ${#POINTS[@]}))]}
  if [[ $point == external ]]; then
    start_worker; sleep "$((3 + RANDOM % 4)).$((RANDOM % 10))"; kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
    how="kill -9 from outside"
  else
    DUCKWAKE_CRASH_AT=$point DUCKWAKE_CRASH_PROB=0.5 start_worker
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
    how="abort at $point"
  fi
  echo "crash $i: $how; epochs $(for v in "${VIEWS[@]}"; do printf '%s=%s ' "$v" "$(last_epoch "$v")"; done)"
  verify_views "${VIEWS[@]}" >"$OUT/verify_round.txt" || { cat "$OUT/verify_round.txt"; failed=1; }
done
stop_generating
start_worker --metrics "$METRICS"
wait_caught_up || failed=1
verify_views "${VIEWS[@]}" || failed=1
[[ $(sql "SELECT count(*) FROM lake.a.daily") -gt 0 && $(sql "SELECT count(*) FROM lake.b.daily") -gt 0 ]] \
  || { echo "FAIL: a.daily or b.daily is empty"; failed=1; }
[[ $(sql "SELECT count(*) FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'daily' AND column_name = 'hi'") == 1 ]] \
  || { echo "FAIL: the two daily tables do not have their own columns"; failed=1; }

log "3. metrics"
curl -s "http://$METRICS/metrics" >"$OUT/metrics.txt" || true
for v in "${VIEWS[@]}"; do
  grep -q "duckwake_epoch{worker=\"w1\",view=\"$v\"}" "$OUT/metrics.txt" || { echo "FAIL: no duckwake_epoch for view=\"$v\""; failed=1; }
done
grep -E '^duckwake_epoch\{' "$OUT/metrics.txt" | sed 's/^/  /' || true

log "4. a reset of a.daily leaves b.daily alone; dropping b.daily leaves a.daily"
"$BIN" pause a.daily >/dev/null; "$BIN" pause b.daily >/dev/null; "$BIN" pause b.top >/dev/null
for _ in $(seq 1 20); do grep -q 'job released.*job=b.top' "$OUT/worker.log" && break; sleep 0.5; done
b_epoch=$(last_epoch b.daily); b_rows=$(sql "SELECT count(*) || '/' || sum(hi) FROM lake.b.daily")
to=$(( $(last_epoch a.daily) - 2 ))
"$BIN" reset a.daily --to-epoch "$to" | sed 's/^/  /' || failed=1
[[ $(last_epoch a.daily) == "$to" ]] || { echo "FAIL: a.daily is at epoch $(last_epoch a.daily) after the reset to $to"; failed=1; }
[[ $(last_epoch b.daily) == "$b_epoch" && $(sql "SELECT count(*) || '/' || sum(hi) FROM lake.b.daily") == "$b_rows" ]] \
  && echo "b.daily: still at epoch $b_epoch, table unchanged" || { echo "FAIL: resetting a.daily changed b.daily"; failed=1; }
for v in a.daily b.daily b.top; do "$BIN" start "$v" --worker w1 >/dev/null; done
"$BIN" sql "INSERT INTO lake.shop.accounts SELECT 900000 + i, $GAMES, $AMOUNT FROM range(50) t(i)" >/dev/null
wait_caught_up || failed=1
verify_views a.daily b.daily b.top || failed=1
"$BIN" stop b.daily >/dev/null
for _ in $(seq 1 20); do [[ $(status b.daily) == stopped ]] && break; sleep 0.5; done
"$BIN" drop b.daily | sed 's/^/  /'
left=$(sql "SELECT string_agg(table_name, ' ' ORDER BY table_name) FROM duckdb_tables()
            WHERE database_name = 'lake' AND schema_name = '__duckwake' AND table_name LIKE '%daily'")
[[ $left == "lease_a.daily state_a.daily" ]] && echo "control tables left: $left" || { echo "FAIL: control tables left: $left"; failed=1; }
[[ $(sql "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'b' AND table_name = 'daily'") == 0 ]] \
  || { echo "FAIL: lake.b.daily is still there"; failed=1; }
"$BIN" sql "INSERT INTO lake.shop.accounts SELECT 910000 + i, $GAMES, $AMOUNT FROM range(50) t(i)" >/dev/null
VIEWS=(a.daily b.top main.solo c.over_solo)
wait_caught_up || failed=1
verify_views "${VIEWS[@]}" || failed=1
stop_worker

log "5. a lake with a view named by DuckWake 0.2"
create main.legacy_v --sql "SELECT id FROM shop.accounts WHERE amount >= 99" >/dev/null
sql "BEGIN;
     UPDATE lake.__duckwake.jobs SET job_id = 'legacy_v' WHERE job_id = 'main.legacy_v';
     UPDATE lake.__duckwake.dependencies SET job_id = 'legacy_v' WHERE job_id = 'main.legacy_v';
     ALTER TABLE lake.__duckwake.\"lease_main.legacy_v\" RENAME TO lease_legacy_v;
     COMMIT;" >/dev/null
"$BIN" run --worker w1 >"$OUT/legacy.log" 2>&1 && { echo "FAIL: a worker ran on a lake with a 0.2 view"; failed=1; }
grep -q 'views legacy_v were created by DuckWake 0.2' "$OUT/legacy.log" \
  && echo "worker: $(grep -o 'views legacy_v were created.\{0,60\}' "$OUT/legacy.log" | head -1)..." \
  || { echo "FAIL: the worker said: $(head -3 "$OUT/legacy.log")"; failed=1; }
out=$("$BIN" create-view c.another --sql "SELECT id FROM shop.accounts" 2>&1 || true)
[[ $out == *"views legacy_v were created by DuckWake 0.2"* ]] && echo "create-view: refused" || { echo "FAIL: create-view said: $out"; failed=1; }
"$BIN" drop legacy_v | sed 's/^/  /' || failed=1
start_worker
sleep 3
kill -0 "$pid" 2>/dev/null && echo "a worker runs once legacy_v is dropped" || { echo "FAIL: the worker exited"; tail -3 "$OUT/worker.log"; failed=1; }
stop_worker

if [[ $failed == 0 ]]; then log "PASS"; else log "FAIL"; exit 1; fi
