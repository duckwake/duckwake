#!/usr/bin/env bash
# F3 gate: backfill and retention of views over lake tables and other views.
#  1. Views created over tables that already hold ROWS rows start from those rows (start := 'earliest', the
#     default), a page of PAGE rows per epoch, while a generator inserts, updates and deletes rows and the
#     worker is killed over and over (kill -9 from outside, and aborts injected at every step of the epoch).
#     After every crash each view verifies against its batch query at its checkpoint's snapshot, up to its
#     backfill cursor while it backfills; at the end it holds every row. A `start := 'latest'` view holds only
#     the rows written after it was created. Among the views: count(DISTINCT), the variances and bool_or over
#     the changing table, and a window with HAVING over the insert-only one.
#  2. Under aggressive maintenance (expiry every 2 s), a paused view keeps the snapshots after its position;
#     started again, it catches up exactly. The commit times of expired snapshots stay known.
#     A view stopped and started again ten times in a row keeps running and verifies.
#  3. A reset to an epoch whose snapshot has expired fails with a clear error. A reset of a windowed view that
#     another windowed view reads insert-only recomputes that reader, and both verify.
# Needs only PostgreSQL: PG_LOCAL=1 runs it against one on localhost:55432 without Docker.
source "$(dirname "$0")/lib.sh"
ROWS=${ROWS:-200000}; PAGE=${PAGE:-5000}; KILLS=${KILLS:-6}; EPOCH=${EPOCH:-1s}
read -r -a POINTS <<< "${POINTS:-external poll commit_begin commit_mid before_commit after_commit maintenance staged_ahead}"
VIEWS=(bf_rich bf_by_game bf_bands bf_busy bf_windows bf_windows_10s bf_latest bf_spread bf_quiet)

sql() { "$BIN" sql "$1" --csv 2>/dev/null | tail -1; }
snapshot() { sql "SELECT id FROM ducklake_current_snapshot('lake')"; }
# The snapshot a view has read its inputs up to, and its backfill cursor (empty once its backfill is done).
position() {
  sql "SELECT coalesce(min(json_extract(source_offsets, '\$.\"' || k || '\".\"0\"')::BIGINT), 0) FROM (
         SELECT source_offsets, unnest(json_keys(source_offsets)) AS k FROM lake.__duckwake.checkpoints
         WHERE job_id = '$1' ORDER BY epoch DESC LIMIT 1)"
}
cursor() {
  sql "SELECT max(json_extract(source_offsets, '\$.\"' || k || '\".\"1\"')::BIGINT) FROM (
         SELECT source_offsets, unnest(json_keys(source_offsets)) AS k FROM lake.__duckwake.checkpoints
         WHERE job_id = '$1' ORDER BY epoch DESC LIMIT 1)"
}
# Whether view $1 has backfilled and read its input table $2.$3 up to snapshot $4.
caught_up() {
  local p; p=$(position "$1")
  [[ -z $(cursor "$1") && $(sql "SELECT count(*) FROM lake.__duckwake.checkpoints WHERE job_id = '$1'") != 0 ]] || return 1
  (( p >= $4 )) || [[ $(sql "SELECT count(*) FROM ducklake_table_changes('lake', '$2', '$3', $((p + 1)), $4)") == 0 ]]
}
wait_for() {
  local at; at=$(snapshot)
  for _ in $(seq 1 180); do
    caught_up "$@" "$at" && return 0
    if [[ -n ${pid:-} ]] && ! kill -0 "$pid" 2>/dev/null; then
      echo "$1 did not catch up: the worker exited"; grep -m1 -A4 '^Error' "$OUT/worker.log" || true; return 1
    fi
    sleep 1
  done
  echo "$1 did not catch up to snapshot $at"; return 1
}
verify_views() {
  local rc=0 v
  for v in "${@:-${VIEWS[@]}}"; do
    [[ $v == quiet ]] && continue
    if "$BIN" verify "$v" >"$OUT/verify_$v.txt" 2>&1; then
      [[ ${1:-} == quiet ]] || tail -1 "$OUT/verify_$v.txt"
    elif grep -q "job has no checkpoint yet" "$OUT/verify_$v.txt"; then
      :
    else
      rc=1; cat "$OUT/verify_$v.txt"
    fi
  done
  return $rc
}
GAMES="'g' || (random() * 6)::INTEGER"
AMOUNT="round(random() * 100, 2)::DECIMAL(18,2)"
change_round() {
  local next=$1 a=$((RANDOM % 13)) b=$((RANDOM % 29)) c=$((RANDOM % 31))
  "$BIN" sql "
    INSERT INTO lake.shop.accounts SELECT $next + i, $GAMES, $AMOUNT FROM range(20) t(i);
    UPDATE lake.shop.accounts SET amount = $AMOUNT WHERE id % 1301 = $a;
    DELETE FROM lake.shop.accounts WHERE id % 2903 = $b;
    BEGIN;
    UPDATE lake.shop.accounts SET game_id = $GAMES WHERE id % 3101 = $c;
    INSERT INTO lake.shop.accounts SELECT $next + 20 + i, $GAMES, $AMOUNT FROM range(10) t(i);
    COMMIT;
    INSERT INTO lake.shop.events SELECT $next + i, $GAMES, $AMOUNT FROM range(30) t(i);" >/dev/null 2>>"$OUT/generator.log"
}
generate() {
  touch "$OUT/generating"
  ( next=$1; while [[ -f $OUT/generating ]]; do change_round $next || true; next=$((next + 30)); sleep 0.3; done ) &
  GENERATOR=$!
}
stop_generating() { rm -f "$OUT/generating"; wait "$GENERATOR" || true; }
start_worker() { "$BIN" run $WORKER_FLAGS --worker w1 --lease-timeout off "$@" >>"$OUT/worker.log" 2>&1 & pid=$!; }
stop_worker() { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; }
failed=0
# The worker and the generator go with the script, however it ends.
trap 'rm -f "$OUT/generating"; [[ -z ${pid:-} ]] || kill "$pid" 2>/dev/null || true' EXIT

log "reset lake"; rm -rf "$OUT"; mkdir -p "$OUT"; reset_lake
log "two tables of $ROWS rows each, written before any view"
sql "CREATE SCHEMA lake.shop;
     CREATE TABLE lake.shop.accounts (id INTEGER, game_id VARCHAR, amount DECIMAL(18,2));
     INSERT INTO lake.shop.accounts SELECT i, $GAMES, $AMOUNT FROM range(1, $ROWS + 1) t(i);
     CREATE TABLE lake.shop.events (id INTEGER, kind VARCHAR, amount DECIMAL(18,2));
     INSERT INTO lake.shop.events SELECT i, $GAMES, $AMOUNT FROM range(1, $ROWS + 1) t(i);" >/dev/null
create() { local v=$1; shift; "$BIN" create-view "analytics.$v" --epoch-interval "$EPOCH" --max-rows "$PAGE" "$@" | sed -n 1p; }
create bf_rich --sql "SELECT id, game_id, amount, _commit_ts FROM shop.accounts WHERE amount >= 90"
create bf_by_game --sql "SELECT game_id, count(*) AS n, sum(amount) AS total, min(amount) AS lo, max(amount) AS hi FROM shop.accounts GROUP BY ALL"
create bf_bands --sql "SELECT DISTINCT game_id, floor(amount / 25) AS band FROM shop.accounts"
# Over a view's table; and windows over the insert-only events, then windows over those windows. A windowed
# view's watermark waits for its backfill, so no window closes before the old rows are in.
create bf_busy --sql "SELECT game_id, n, lo FROM analytics.bf_by_game WHERE n >= 10"
create bf_windows --sql "SELECT window_start, window_end, kind, count(*) AS n, sum(amount) AS total
                         FROM TUMBLE(shop.events, _commit_ts, INTERVAL 2 SECOND) WHERE amount >= 50 GROUP BY ALL"
create bf_windows_10s --sql "SELECT window_start, kind, sum(n) AS n, sum(total) AS total
                             FROM TUMBLE(analytics.bf_windows, window_end, INTERVAL 10 SECOND) GROUP BY ALL"
create bf_latest --start latest --sql "SELECT id, game_id, amount FROM shop.accounts"
# The statistics aggregates over a table whose rows change: count(DISTINCT) keeps value rows, the variances
# running sums, bool_or is MAX of a BOOLEAN; and a window with HAVING, which emits only the windows that pass it.
create bf_spread --sql "SELECT game_id, count(DISTINCT floor(amount)) AS levels, var_samp(amount) AS v,
                               stddev_pop(amount) AS sd, bool_or(amount > 99) AS any_top
                        FROM shop.accounts GROUP BY ALL"
create bf_quiet --sql "SELECT window_start, kind, count(*) AS n, stddev_samp(amount) AS sd, bool_and(amount >= 1) AS all_paid
                       FROM TUMBLE(shop.events, _commit_ts, INTERVAL 2 SECOND) GROUP BY ALL HAVING count(*) > 3"
for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker w1 >/dev/null; done

log "backfill while the tables change and the worker is killed $KILLS times"
generate $((ROWS + 1000))
for i in $(seq 1 "$KILLS"); do
  point=${POINTS[$((RANDOM % ${#POINTS[@]}))]}
  if [[ $point == external ]]; then
    start_worker --flush-every 3s --merge-every 7s
    sleep "$((4 + RANDOM % 5)).$((RANDOM % 10))"; kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
    how="kill -9 from outside"
  else
    DUCKWAKE_CRASH_AT=$point DUCKWAKE_CRASH_PROB=0.5 start_worker --flush-every 3s --merge-every 7s
    for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$pid" 2>/dev/null && how="no crash in 10 s, killed" || how="aborted at $point"
    wait "$pid" 2>/dev/null || true
  fi
  if verify_views quiet; then c=$(cursor bf_rich)
    if [[ $(sql "SELECT count(*) FROM lake.__duckwake.checkpoints WHERE job_id = 'bf_rich'") == 0 ]]; then c="not started"
    elif [[ -n $c ]]; then c="at row $c"; else c="done"; fi
    echo "run $i: $how; bf_rich backfill $c; all ${#VIEWS[@]} verify"
  else echo "run $i: $how; VERIFY FAILED"; failed=1; fi
done
stop_generating

log "let the views finish their backfills and catch up"
start_worker
for v in bf_rich bf_by_game bf_bands bf_latest; do wait_for "$v" shop accounts || failed=1; done
wait_for bf_busy analytics bf_by_game || failed=1
wait_for bf_windows shop events || failed=1
wait_for bf_windows_10s analytics bf_windows || failed=1
wait_for bf_spread shop accounts || failed=1
wait_for bf_quiet shop events || failed=1
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), backfill starts: $(grep -c 'backfilling' "$OUT/worker.log" || true)"
verify_views || failed=1
rich=$(sql "SELECT count(*) FROM lake.analytics.bf_rich"); want=$(sql "SELECT count(*) FROM lake.shop.accounts WHERE amount >= 90")
[[ $rich == "$want" ]] && echo "bf_rich holds all $rich rows of the table with amount >= 90" || { echo "bf_rich holds $rich rows, the table $want"; failed=1; }
latest=$(sql "SELECT count(*) FROM lake.analytics.bf_latest")
(( latest < ROWS )) && echo "bf_latest holds $latest rows: only those written after it was created" || { echo "bf_latest holds $latest rows"; failed=1; }

log "a view stopped and started again ten times in a row while its table changes"
# Each start is a new run of the view on the same worker; the tables an old run staged into are dropped after
# it left, on the commit thread when epochs are pipelined, and must not take the new run's with them.
generate $((ROWS + 300000))
for i in $(seq 1 10); do
  released=$(grep -c 'job released.*job=bf_bands' "$OUT/worker.log" || true)
  "$BIN" stop bf_bands >/dev/null
  for _ in $(seq 1 60); do (( $(grep -c 'job released.*job=bf_bands' "$OUT/worker.log" || true) > released )) && break; sleep 0.5; done
  "$BIN" start bf_bands --worker w1 >/dev/null
  sleep "0.$((RANDOM % 10))"
done
stop_generating
wait_for bf_bands shop accounts || failed=1
kill -0 "$pid" 2>/dev/null && echo "the worker ran through ten restarts of bf_bands" || { echo "the worker exited"; failed=1; }
verify_views bf_bands bf_rich bf_by_game || failed=1
stop_worker

log "aggressive maintenance with a paused view"
start_worker --expire-every 2s --expire-older-than 1s --flush-every 2s --merge-every 3s --cleanup-every 3s --position-refresh 3s
sleep 3
"$BIN" pause bf_rich >/dev/null
held=$(position bf_rich)
generate $((ROWS + 500000))
sleep 20
stop_generating
oldest=$(sql "SELECT min(snapshot_id) FROM ducklake_snapshots('lake')")
echo "bf_rich paused at snapshot $held; the lake's oldest snapshot is $oldest of $(snapshot); commit times recorded for $(sql "SELECT count(*) FROM lake.__duckwake.snapshot_times") of the $((oldest - 1)) expired (those that wrote rows)"
(( oldest <= held )) || { echo "maintenance expired snapshots bf_rich still needs"; failed=1; }
(( oldest > 1 )) || { echo "maintenance expired nothing"; failed=1; }
"$BIN" start bf_rich --worker w1 >/dev/null
for v in bf_rich bf_by_game bf_bands bf_latest; do wait_for "$v" shop accounts || failed=1; done
wait_for bf_windows_10s analytics bf_windows || failed=1
verify_views || failed=1

log "reset past retention, and a reset of a view another view reads insert-only"
# Maintenance back to its defaults, so the epochs committed from here on keep their snapshots.
stop_worker; start_worker
"$BIN" stop bf_by_game >/dev/null
for _ in $(seq 1 30); do grep -q 'job released.*job=bf_by_game' "$OUT/worker.log" && break; sleep 1; done
if out=$("$BIN" reset bf_by_game --to-epoch 1 2>&1); then echo "reset to an expired epoch was accepted: $out"; failed=1
else echo "refused: $(grep -o 'no snapshot.*expired' <<<"$out" | head -1)"; fi
grep -q "has been expired" <<<"$out" || failed=1
"$BIN" start bf_by_game --worker w1 >/dev/null
generate $((ROWS + 800000)); sleep 12; stop_generating
"$BIN" stop bf_windows >/dev/null
for _ in $(seq 1 30); do grep -q 'job released.*job=bf_windows' "$OUT/worker.log" && break; sleep 1; done
before=$(sql "SELECT count(*) FROM lake.analytics.bf_windows_10s")
to=$(( $(sql "SELECT max(epoch) FROM lake.__duckwake.checkpoints WHERE job_id = 'bf_windows'") - 2 ))
"$BIN" reset bf_windows --to-epoch "$to" 2>&1 | tee "$OUT/reset.txt" | sed -n 1p || failed=1
grep -q "bf_windows_10s" "$OUT/reset.txt" || { echo "the reset did not recompute bf_windows_10s"; failed=1; }
"$BIN" start bf_windows --worker w1 >/dev/null
generate $((ROWS + 900000)); sleep 15; stop_generating
wait_for bf_windows shop events || failed=1
wait_for bf_windows_10s analytics bf_windows || failed=1
after=$(sql "SELECT count(*) FROM lake.analytics.bf_windows_10s")
echo "bf_windows_10s: $before windows before the reset, $after after its recomputation"
verify_views bf_windows bf_windows_10s bf_by_game bf_busy || failed=1

log "reset to epoch 0 after the start snapshot expired, and a rerun of a latest view's create"
stop_view() {
  "$BIN" stop "$1" >/dev/null
  for _ in $(seq 1 30); do grep -q "job released.*job=$1" "$OUT/worker.log" && break; sleep 1; done
}
s0=$(sql "SELECT json_extract(definition, '\$.start_snapshot') FROM lake.__duckwake.jobs WHERE job_id = 'bf_rich'")
oldest=$(sql "SELECT min(snapshot_id) FROM ducklake_snapshots('lake')")
(( s0 < oldest )) && echo "bf_rich's start snapshot $s0 has expired (oldest $oldest)" || { echo "bf_rich's start snapshot $s0 is still there: nothing to test"; failed=1; }
for v in bf_rich bf_latest; do
  stop_view "$v"
  "$BIN" reset "$v" --to-epoch 0 | sed -n 1p || failed=1
done
# A deploy reruns the create of a `start latest` view after its reset: nothing changed.
out=$(create bf_latest --if-exists update --start latest --sql "SELECT id, game_id, amount FROM shop.accounts" 2>&1) || true
echo "$out" | sed -n 1p
[[ $out == unchanged* ]] || { echo "rerunning bf_latest's create did not report unchanged"; failed=1; }
for v in bf_rich bf_latest; do "$BIN" start "$v" --worker w1 >/dev/null; done
for v in bf_rich bf_latest; do wait_for "$v" shop accounts || failed=1; done
verify_views bf_rich bf_latest || failed=1
rich=$(sql "SELECT count(*) FROM lake.analytics.bf_rich"); want=$(sql "SELECT count(*) FROM lake.shop.accounts WHERE amount >= 90")
[[ $rich == "$want" ]] && echo "bf_rich recomputed: all $rich matching rows" || { echo "bf_rich holds $rich rows, the table $want"; failed=1; }
latest=$(sql "SELECT count(*) FROM lake.analytics.bf_latest")
(( latest < ROWS )) && echo "bf_latest recomputed: $latest rows, still only those written after it was created" || { echo "bf_latest holds $latest rows"; failed=1; }
stop_worker

if (( failed )); then echo "FAILED"; exit 1; fi
echo "PASSED"
