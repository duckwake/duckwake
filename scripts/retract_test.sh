#!/usr/bin/env bash
# F2 gate: views over a lake table whose rows are inserted, updated and deleted, and over views whose tables
# change in turn. A generator rewrites shop.accounts with mixed inserts, updates and deletes (some in one
# transaction, some updating and then deleting the same rows, many touching rows written before the views
# existed, which the views backfill) while the worker is killed over and over: kill -9 from outside, and aborts injected at every step
# of the epoch. After every crash each view verifies against its batch query over its input as of the
# snapshot in its checkpoint (time travel), and again once the views have caught up. Then: a reset of a view
# reaches the views that read it through the change feed, and a window over a view whose rows change is
# refused. WORKER_FLAGS="--pipeline ..." runs it pipelined.
source "$(dirname "$0")/lib.sh"
KILLS=${KILLS:-8}; EPOCH=${EPOCH:-1s}
read -r -a POINTS <<< "${POINTS:-external poll commit_begin commit_mid before_commit after_commit maintenance staged_ahead}"
# Over the table: counted rows, a grouped aggregate (MIN/MAX keep a count per value), DISTINCT, and counted rows
# that repeat. Over those views: an aggregate over the repeating counted rows, rows of the grouped aggregate,
# and a UNION ALL of the table and the grouped aggregate (two change feeds read to one snapshot).
LEVEL1=(acc_rich acc_by_game acc_bands acc_amounts)
VIEWS=("${LEVEL1[@]}" acc_amount_totals acc_busy_games acc_mixed)

sql() { "$BIN" sql "$1" --csv 2>/dev/null | tail -1; }
snapshot() { sql "SELECT id FROM ducklake_current_snapshot('lake')"; }
# The snapshot a view has read its input up to, from its latest checkpoint (key "0"; "1" is a backfill's cursor).
position() {
  sql "SELECT coalesce(max(o::BIGINT), 0) FROM (SELECT unnest(json_extract(source_offsets, '\$.*.\"0\"')) AS o FROM (
         SELECT source_offsets FROM lake.__duckwake.checkpoints WHERE job_id = '$1' ORDER BY epoch DESC LIMIT 1))"
}
# Whether view $1 has read its input table $2.$3 up to snapshot $4: its position is there, or no change to the
# table lies after it (a view skips epochs with nothing to do, so its position can stay behind the lake's).
caught_up() {
  local p; p=$(position "$1")
  (( p >= $4 )) || [[ $(sql "SELECT count(*) FROM ducklake_table_changes('lake', '$2', '$3', $((p + 1)), $4)") == 0 ]]
}
wait_for() {
  local at; at=$(snapshot)
  for _ in $(seq 1 120); do
    caught_up "$@" "$at" && return 0
    # A worker that died will not catch up: say why instead of waiting it out.
    if [[ -n ${pid:-} ]] && ! kill -0 "$pid" 2>/dev/null; then
      echo "$1 did not catch up: the worker exited"; grep -m1 -A4 '^Error' "$OUT/worker.log" || true; return 1
    fi
    sleep 1
  done
  echo "$1 did not catch up to snapshot $at"; return 1
}
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

# One round of changes, each statement its own snapshot except the transaction: new rows, new amounts for a
# slice of rows, deletes, and a transaction that moves rows between games, deletes, inserts, and updates rows
# it then deletes. Ids are taken from $1 on; rows 1-400 were written before the views.
GAMES="'g' || (random() * 6)::INTEGER"
AMOUNT="round(random() * 100, 2)::DECIMAL(18,2)"
change_round() {
  local next=$1 a=$((RANDOM % 13)) b=$((RANDOM % 29)) c=$((RANDOM % 31)) d=$((RANDOM % 37)) e=$((RANDOM % 41))
  "$BIN" sql "
    INSERT INTO lake.shop.accounts SELECT $next + i, $GAMES, $AMOUNT FROM range(15) t(i);
    UPDATE lake.shop.accounts SET amount = $AMOUNT WHERE id % 13 = $a AND random() < 0.4;
    DELETE FROM lake.shop.accounts WHERE id % 29 = $b AND random() < 0.5;
    BEGIN;
    UPDATE lake.shop.accounts SET game_id = $GAMES WHERE id % 31 = $c;
    DELETE FROM lake.shop.accounts WHERE id % 37 = $d AND random() < 0.5;
    INSERT INTO lake.shop.accounts SELECT $next + 15 + i, $GAMES, $AMOUNT FROM range(10) t(i);
    UPDATE lake.shop.accounts SET amount = amount + 1 WHERE id % 41 = $e;
    DELETE FROM lake.shop.accounts WHERE id % 41 = $e AND amount > 60;
    COMMIT;" >/dev/null 2>>"$OUT/generator.log"
}

log "reset lake"; reset_env
log "create the table, its first rows, and the views"
sql "CREATE SCHEMA lake.shop; CREATE TABLE lake.shop.accounts (id INTEGER, game_id VARCHAR, amount DECIMAL(18,2));
     INSERT INTO lake.shop.accounts SELECT i, $GAMES, $AMOUNT FROM range(1, 401) t(i)" >/dev/null
create() { "$BIN" create-view "analytics.$1" --epoch-interval "$EPOCH" --sql "$2" | sed -n 1p; }
create acc_rich "SELECT id, game_id, amount, _commit_ts FROM shop.accounts WHERE amount >= 50"
create acc_by_game "SELECT game_id, count(*) AS n, sum(amount) AS total, avg(amount) AS mean, min(amount) AS lo, max(amount) AS hi
                    FROM shop.accounts GROUP BY game_id"
create acc_bands "SELECT DISTINCT game_id, floor(amount / 25) AS band FROM shop.accounts"
create acc_amounts "SELECT game_id, floor(amount / 10) * 10 AS bucket FROM shop.accounts"
create acc_amount_totals "SELECT game_id, count(*) AS n, sum(bucket) AS total, max(bucket) AS hi FROM analytics.acc_amounts GROUP BY game_id"
create acc_busy_games "SELECT game_id, n, lo, hi FROM analytics.acc_by_game WHERE n >= 60"
create acc_mixed "SELECT game_id, amount FROM shop.accounts WHERE amount >= 95 UNION ALL SELECT game_id, lo FROM analytics.acc_by_game"
for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker w1 >/dev/null; done

log "rewrite the table while the worker is killed $KILLS times"
touch "$OUT/generating"
( next=1000; while [[ -f $OUT/generating ]]; do change_round $next || true; next=$((next + 25)); sleep 0.2; done ) &
GENERATOR=$!
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
  # Epoch by epoch: whatever each view has committed so far must match its batch query at its snapshot.
  if verify_views quiet; then echo "run $i: $how; all ${#VIEWS[@]} verify"; else echo "run $i: $how; VERIFY FAILED"; failed=1; fi
done
rm -f "$OUT/generating"; wait "$GENERATOR" || true
echo "the table: $(sql "SELECT count(*) FROM lake.shop.accounts") rows after $(sql "SELECT count(*) FROM ducklake_table_changes(
  'lake', 'shop', 'accounts', 0, $(snapshot)) WHERE change_type IN ('delete', 'update_preimage')") deletes and updates"

log "let the views catch up, then verify"
"$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 &
pid=$!
for v in "${LEVEL1[@]}"; do wait_for "$v" shop accounts || failed=1; done
wait_for acc_amount_totals analytics acc_amounts || failed=1
wait_for acc_busy_games analytics acc_by_game || failed=1
wait_for acc_mixed shop accounts && wait_for acc_mixed analytics acc_by_game || failed=1
echo "injected aborts: $(grep -c 'crash injected' "$OUT/worker.log" || true), epochs committed: $(grep -c 'epoch committed' "$OUT/worker.log" || true)"
verify_views || failed=1
for v in "${VIEWS[@]}"; do
  [[ $(sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id = '$v'") == running ]] || { echo "$v is not running"; failed=1; }
done

log "reset a view another view reads: the reader follows it through the change feed"
"$BIN" stop acc_by_game >/dev/null
# Stopping does not fence: wait for the worker to let the view go before rewinding it.
for _ in $(seq 1 30); do grep -q 'job released.*job=acc_by_game' "$OUT/worker.log" && break; sleep 1; done
to=$(( $(sql "SELECT max(epoch) FROM lake.__duckwake.checkpoints WHERE job_id = 'acc_by_game'") / 2 ))
"$BIN" reset acc_by_game --to-epoch "$to" | sed -n 1p
"$BIN" start acc_by_game --worker w1 >/dev/null
wait_for acc_by_game shop accounts || failed=1
wait_for acc_busy_games analytics acc_by_game || failed=1
wait_for acc_mixed analytics acc_by_game || failed=1
for v in acc_by_game acc_busy_games acc_mixed; do
  if "$BIN" verify "$v" >"$OUT/verify_$v.txt" 2>&1; then tail -1 "$OUT/verify_$v.txt"; else failed=1; cat "$OUT/verify_$v.txt"; fi
done

log "a window over a view whose rows change is refused"
if out=$("$BIN" create-view analytics.acc_windows --sql "SELECT window_start, count(*) AS n
      FROM TUMBLE(analytics.acc_by_game, _commit_ts, INTERVAL 10 SECOND) GROUP BY ALL" 2>&1); then
  echo "the windowed view over acc_by_game was created"; failed=1
elif [[ $out == *"windows over it are not supported yet"* ]]; then echo "$out" | grep -o "acc_by_game is the table.*passes it)"
else echo "refused for another reason: $out"; failed=1; fi
kill -TERM "$pid"; wait "$pid" || true

[[ $failed == 0 ]] && echo "RETRACT TEST PASSED" || { echo "RETRACT TEST FAILED"; exit 1; }
