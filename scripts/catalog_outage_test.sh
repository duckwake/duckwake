#!/usr/bin/env bash
# PostgreSQL-down drill (#27): workers wait out a catalog outage instead of exiting, and resume exactly.
#  1. Two workers (w1, w2) run three views over a lake table (a grouped aggregate, a filter, and a view over the
#     aggregate) while a generator inserts, updates and deletes rows. The catalog goes down for longer than the
#     lease timeout and comes back: both workers are still running, report the outage in their metrics
#     (duckwake_catalog_reachable 0, then 1; one outage counted), and every view verifies.
#  2. A third worker started while the catalog is down waits for it and registers once it is back.
#  3. Three short outages in a row, a few seconds apart, while epochs commit: every view verifies after.
#  4. A worker asked to stop while the catalog is down exits cleanly (status 0) within seconds.
#  At the end no view is failed and every worker but the stopped one is still running.
# Needs only PostgreSQL. PG_LOCAL=1 stops and starts a local server with pg_ctl (as the postgres user, data in
# PG_DATA); otherwise the compose service `postgres` is stopped and started.
source "$(dirname "$0")/lib.sh"
ROWS=${ROWS:-20000}; LEASE=${LEASE:-10s}; DOWN=${DOWN:-20}
PG_DATA=${PG_DATA:-/var/lib/postgresql/duckwake-test}
PG_CTL=${PG_CTL:-/usr/lib/postgresql/16/bin/pg_ctl}
VIEWS=(a.by_game a.big b.top)
PORTS=([1]=9471 [2]=9472 [3]=9473)

sql() { "$BIN" sql "$1" --csv 2>/dev/null | tail -1; }
catalog_down() {
  if [[ $PG_LOCAL == 1 ]]; then su postgres -c "$PG_CTL -D $PG_DATA -m fast stop" >/dev/null
  else docker compose stop postgres >/dev/null 2>&1; fi
}
catalog_ready() {
  if [[ $PG_LOCAL == 1 ]]; then pg_isready -h localhost -p 55432 -q
  else docker compose exec -T postgres pg_isready -U lake -q >/dev/null 2>&1; fi
}
catalog_up() {
  if [[ $PG_LOCAL == 1 ]]; then
    su postgres -c "$PG_CTL -D $PG_DATA -o '-p 55432 -k /tmp -c wal_level=logical' -l $PG_DATA/server.log start" >/dev/null
  else docker compose start postgres >/dev/null 2>&1; fi
  for _ in $(seq 1 60); do catalog_ready && return 0; sleep 0.5; done
  echo "the catalog did not come back"; return 1
}
metric() { curl -s "http://127.0.0.1:${PORTS[$1]}/metrics" | awk -v m="$2" '$1 ~ "^"m"[{]" {print $2}' | head -1; }
start_worker() {
  "$BIN" run $WORKER_FLAGS --worker "w$1" --lease-timeout "$LEASE" --metrics "127.0.0.1:${PORTS[$1]}" >>"$OUT/w$1.log" 2>&1 &
  echo $! >"$OUT/w$1.pid"
}
pid() { cat "$OUT/w$1.pid"; }
alive() { kill -0 "$(pid "$1")" 2>/dev/null; }
status() { sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id = '$1'"; }
offsets() { echo "SELECT source_offsets, unnest(json_keys(source_offsets)) AS k FROM lake.__duckwake.checkpoints
                  WHERE job_id = '$1' ORDER BY epoch DESC LIMIT 1"; }
position() { sql "SELECT coalesce(min(json_extract(source_offsets, '\$.\"' || k || '\".\"0\"')::BIGINT), 0) FROM ($(offsets "$1"))"; }
declare -A INPUT=([a.by_game]="shop accounts" [a.big]="shop accounts" [b.top]="a by_game")
# Whether every view has read its input up to snapshot $1 (or its input has not changed since).
caught_up() {
  local v p
  for v in "${VIEWS[@]}"; do
    p=$(position "$v"); set -- "$1" ${INPUT[$v]}
    [[ -n $p ]] || return 1
    (( p >= $1 )) || [[ $(sql "SELECT count(*) FROM ducklake_table_changes('lake', '$2', '$3', $((p + 1)), $1)") == 0 ]] || return 1
  done
}
wait_caught_up() {
  local at; at=$(sql "SELECT id FROM ducklake_current_snapshot('lake')")
  for _ in $(seq 1 120); do caught_up "$at" && return 0; sleep 1; done
  echo "the views did not catch up to snapshot $at"; return 1
}
verify_all() {
  local rc=0 v
  for v in "${VIEWS[@]}"; do
    "$BIN" verify "$v" >"$OUT/verify.txt" 2>&1 && tail -1 "$OUT/verify.txt" | sed "s/^/  /" || { rc=1; cat "$OUT/verify.txt"; }
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
                  DELETE FROM lake.shop.accounts WHERE id % 2903 = $((RANDOM % 2903));" >/dev/null 2>&1 || true
      next=$((next + 20)); sleep 0.3
    done ) &
  GENERATOR=$!
}
stop_generating() { rm -f "$OUT/generating"; wait "$GENERATOR" 2>/dev/null || true; }
failed=0
cleanup() {
  rm -f "$OUT/generating"
  catalog_ready || catalog_up || true
  for w in 1 2 3; do [[ -f $OUT/w$w.pid ]] && { kill "$(pid $w)" 2>/dev/null || true; }; done; true
}
trap cleanup EXIT

log "reset lake"; rm -rf "$OUT"; mkdir -p "$OUT"; reset_lake
sql "CREATE SCHEMA lake.shop; CREATE SCHEMA lake.a; CREATE SCHEMA lake.b;
     CREATE TABLE lake.shop.accounts (id INTEGER, game_id VARCHAR, amount DECIMAL(18,2));
     INSERT INTO lake.shop.accounts SELECT i, $GAMES, $AMOUNT FROM range(1, $ROWS + 1) t(i);" >/dev/null
create() { "$BIN" create-view "$1" --epoch-interval 1s --sql "$2" | sed -n 1p; }
create a.by_game "SELECT game_id, count(*) AS n, sum(amount) AS total FROM shop.accounts GROUP BY ALL"
create a.big "SELECT id, game_id, amount FROM shop.accounts WHERE amount >= 90"
create b.top "SELECT game_id, n FROM a.by_game WHERE n >= 10"
for v in "${VIEWS[@]}"; do "$BIN" start "$v" >/dev/null; done
start_worker 1; start_worker 2
generate $((ROWS + 1000))
wait_caught_up || failed=1

log "1. the catalog goes down for ${DOWN}s, longer than the lease timeout ($LEASE)"
catalog_down
sleep 5
for w in 1 2; do
  alive "$w" || { echo "FAIL: w$w exited when the catalog went down"; tail -5 "$OUT/w$w.log"; failed=1; }
  [[ $(metric "$w" duckwake_catalog_reachable) == 0 ]] && echo "w$w: duckwake_catalog_reachable 0" \
    || { echo "FAIL: w$w reports the catalog reachable during the outage"; failed=1; }
done
sleep "$((DOWN - 5))"
catalog_up && echo "catalog back after ${DOWN}s"
wait_caught_up || failed=1
for w in 1 2; do
  alive "$w" || { echo "FAIL: w$w exited"; failed=1; }
  echo "w$w: reachable $(metric "$w" duckwake_catalog_reachable), outages $(metric "$w" duckwake_catalog_outages_total), up $(metric "$w" duckwake_worker_up)"
  [[ $(metric "$w" duckwake_catalog_reachable) == 1 && $(metric "$w" duckwake_catalog_outages_total) == 1 ]] \
    || { echo "FAIL: w$w's metrics do not show one outage, over"; failed=1; }
  grep -q "the catalog answers again; resumed from the last checkpoints" "$OUT/w$w.log" \
    || { echo "FAIL: w$w did not log the end of the outage"; failed=1; }
done
verify_all || failed=1

log "2. a worker started while the catalog is down waits for it"
catalog_down
start_worker 3
sleep 4
alive 3 && echo "w3 is waiting: $(grep -c 'catalog' "$OUT/w3.log") lines about the catalog" || { echo "FAIL: w3 exited"; tail -5 "$OUT/w3.log"; failed=1; }
catalog_up
for _ in $(seq 1 60); do grep -q "worker started worker=w3" "$OUT/w3.log" && break; sleep 1; done
grep -q "worker started worker=w3" "$OUT/w3.log" && echo "w3 registered once the catalog was back" \
  || { echo "FAIL: w3 never started"; tail -5 "$OUT/w3.log"; failed=1; }

log "3. three short outages in a row"
for i in 1 2 3; do
  sleep "$((2 + RANDOM % 3))"; catalog_down; sleep "$((2 + RANDOM % 3))"; catalog_up
  echo "outage $i over"
done
wait_caught_up || failed=1
verify_all || failed=1

log "4. a worker asked to stop while the catalog is down exits cleanly"
catalog_down
sleep 3
kill -TERM "$(pid 3)"
t0=$SECONDS; rc=0; wait "$(pid 3)" || rc=$?
(( rc == 0 && SECONDS - t0 <= 15 )) && echo "w3 stopped with status 0 in $((SECONDS - t0))s" \
  || { echo "FAIL: w3 stopped with status $rc after $((SECONDS - t0))s"; tail -5 "$OUT/w3.log"; failed=1; }
catalog_up

log "at the end"
stop_generating
wait_caught_up || failed=1
verify_all || failed=1
for w in 1 2; do alive "$w" && echo "w$w still running; outages seen: $(metric "$w" duckwake_catalog_outages_total)" || { echo "FAIL: w$w exited"; failed=1; }; done
for v in "${VIEWS[@]}"; do [[ $(status "$v") == running ]] || { echo "FAIL: $v is $(status "$v")"; failed=1; }; done
fails=$(cat "$OUT"/w*.log | grep -c "marking it failed" || true)
[[ $fails == 0 ]] && echo "no view was marked failed" || { echo "FAIL: $fails views marked failed"; failed=1; }

if [[ $failed == 0 ]]; then log "PASS"; else log "FAIL"; exit 1; fi
