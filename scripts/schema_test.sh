#!/usr/bin/env bash
# Schema-change gate (P4): views and sources change while two workers run them and events stream in, and every
# view still matches its batch recomputation at the end.
#   1. A column is added to a running source: the views that read it keep their columns and keep committing;
#      a duplicate column and a type that is not one are refused.
#   2. CREATE OR REPLACE of two running views, one windowed with a late-data table: each recomputes from the
#      start with its new definition, keeps running, and the workers and the other views go on.
#   3. Replacing a view that another view reads is refused.
#   4. alter-view changes a running view's epoch interval: its worker restarts it with the new setting.
#   5. create-view --if-exists update, as dbt reruns it: the same view (reformatted) changes nothing, a new
#      epoch interval is altered in place without recomputing, and a changed query is refused.
#   6. stop then drop right away, three times, while the worker has the view's epoch in flight: the worker
#      fences that epoch instead of failing.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-1000}; DURATION=${DURATION:-150}
failed=0

run_worker() {
  # shellcheck disable=SC2086
  "$BIN" run $WORKER_FLAGS --worker "$1" --lease-timeout 10s >>"$OUT/$1.log" 2>&1 &
  echo $! >"$OUT/$1.pid"
}
pid() { cat "$OUT/$1.pid"; }
trap 'for w in w1 w2; do [[ -f $OUT/$w.pid ]] && kill -9 "$(pid $w)" 2>/dev/null || true; done' EXIT
q() { "$BIN" sql --csv "$1" 2>/dev/null | sed -n 1p; }
status() { q "SELECT status FROM lake.__duckwake.jobs WHERE job_id IN ('$1', 'analytics.$1')"; }
epoch() { q "SELECT coalesce(max(epoch), 0) FROM lake.__duckwake.checkpoints WHERE job_id IN ('$1', 'analytics.$1')"; }
columns() { q "SELECT string_agg(column_name, ',' ORDER BY column_index) FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = 'analytics' AND table_name = '$1'"; }
# Waits up to $2 seconds for the condition $3 (evaluated afresh each second) to hold, and reports it as $1.
check() {
  local what=$1 bound=$2 cond=$3 t0=$SECONDS
  while ((SECONDS - t0 <= bound)); do
    if eval "$cond"; then echo "$what: $((SECONDS - t0))s"; return 0; fi
    sleep 1
  done
  echo "FAIL: $what after $((SECONDS - t0))s"; failed=1
}
expect_error() {
  local what=$1; shift
  if out=$("$@" 2>&1); then echo "FAIL: $what was accepted: $out"; failed=1; else echo "$what refused: $(echo "$out" | grep -m1 -i error | cut -c1-140)"; fi
}
all_committed() { for v in "${VIEWS[@]}"; do (( $(epoch "$v") >= $1 )) || return 1; done; }
starts() { cat "$OUT"/w*.log | grep -c "job started job=analytics.$1 " || true; }
alive() {
  for w in w1 w2; do kill -0 "$(pid $w)" 2>/dev/null || { echo "FAIL: $w died"; tail -5 "$OUT/$w.log"; failed=1; }; done
  if "$BIN" jobs 2>/dev/null | grep -q ' failed '; then echo "FAIL: a view failed"; "$BIN" jobs; failed=1; fi
}

log "reset lake and topics, create the views, two workers"
reset_env
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
setup_views 2s "10 SECOND" "5 seconds"
run_worker w1; run_worker w2
for v in "${VIEWS[@]}"; do "$BIN" start "$v" >/dev/null; done
produce --rate "$RATE" --count $((RATE * DURATION)) --late-every 997 --bad-every 1499 --eos >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
check "every view committed 3 epochs" 60 'all_committed 3'

log "1. add a column to the running source clicks"
before_big=$(columns big_spend); before_all=$(columns all_events); e0=$(epoch big_spend)
"$BIN" add-source-column clicks seq BIGINT
expect_error "a duplicate column" "$BIN" add-source-column clicks seq BIGINT
expect_error "a type that is not one" "$BIN" add-source-column clicks oops "INT); DROP TABLE x; --"
expect_error "an unknown type" "$BIN" add-source-column clicks oops NOT_A_TYPE
check "big_spend commits after the column was added" 30 '(( $(epoch big_spend) >= e0 + 3 ))'
[[ $(columns big_spend) == "$before_big" && $(columns all_events) == "$before_all" ]] \
  && echo "the views kept their columns: big_spend ($before_big)" \
  || { echo "FAIL: columns changed: big_spend $(columns big_spend), all_events $(columns all_events)"; failed=1; }
alive

log "2. replace two running views: big_spend (now with seq), clicks_per_window (with its late-data table)"
"$BIN" create-view analytics.big_spend --replace --epoch-interval 2s \
  --sql "SELECT ts, game_id, user_id, amount, seq FROM clicks WHERE amount >= 95" | sed -n 1p
"$BIN" create-view analytics.clicks_per_window --replace --late-data lake.analytics.clicks_late --epoch-interval 2s \
  --sql "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, min(amount) AS min_amount
         FROM TUMBLE(clicks, ts, INTERVAL 10 SECOND) GROUP BY ALL" | sed -n 1p
[[ $(status big_spend) == running && $(status clicks_per_window) == running ]] && echo "both still running" \
  || { echo "FAIL: replaced views are $(status big_spend), $(status clicks_per_window)"; failed=1; }
[[ $(columns big_spend) == *,seq,* ]] && echo "big_spend now has seq: $(columns big_spend)" || { echo "FAIL: big_spend is $(columns big_spend)"; failed=1; }
check "both replaced views commit again" 40 '(( $(epoch big_spend) >= 2 && $(epoch clicks_per_window) >= 2 ))'
check "the new big_spend fills seq" 20 '(( $(q "SELECT count(seq) FROM lake.analytics.big_spend") > 0 ))'
alive

log "3. replacing a view that another view reads is refused"
"$BIN" create-view analytics.spend_by_game --sql "SELECT game_id, count(*) AS n FROM analytics.big_spend GROUP BY ALL" | sed -n 1p
expect_error "replacing big_spend while spend_by_game reads it" "$BIN" create-view analytics.big_spend --replace \
  --sql "SELECT ts, game_id, amount FROM clicks WHERE amount >= 99"
"$BIN" drop spend_by_game >/dev/null

log "4. alter-view: game_totals to 3 s epochs, while it runs"
s0=$(starts game_totals); e0=$(epoch game_totals)
"$BIN" alter-view game_totals --epoch-interval 3s
check "game_totals restarted with its new settings" 20 '(( $(starts game_totals) > s0 ))'
check "and commits on from epoch $e0" 20 '(( $(epoch game_totals) > e0 ))'
[[ $(q "SELECT definition->>'epoch_interval' FROM lake.__duckwake.jobs WHERE job_id = 'analytics.game_totals'") == 3s ]] \
  || { echo "FAIL: game_totals' epoch interval was not changed"; failed=1; }
alive

log "5. create-view --if-exists update on the running game_totals"
GT_SQL="SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount, min(amount) AS lo, max(amount) AS hi
  FROM clicks
  GROUP BY ALL HAVING avg(amount) > 49.5;"
s0=$(starts game_totals); e0=$(epoch game_totals)
out=$("$BIN" create-view analytics.game_totals --if-exists update --epoch-interval 3s --sql "$GT_SQL" 2>&1)
[[ $out == "unchanged view analytics.game_totals" ]] && echo "the same view, reformatted: $out" \
  || { echo "FAIL: rerunning the same view said: $out"; failed=1; }
sleep 3
(( $(starts game_totals) == s0 )) || { echo "FAIL: an unchanged rerun restarted game_totals"; failed=1; }
expect_error "an existing view without --if-exists" "$BIN" create-view analytics.game_totals --epoch-interval 3s --sql "$GT_SQL"
out=$("$BIN" create-view analytics.game_totals --if-exists update --epoch-interval 2s --sql "$GT_SQL" 2>&1)
[[ $out == "altered view analytics.game_totals: epoch_interval 2s,"* ]] && echo "$out" || { echo "FAIL: a new interval said: $out"; failed=1; }
check "game_totals restarted with 2 s epochs" 20 '(( $(starts game_totals) > s0 ))'
[[ $(q "SELECT definition->>'epoch_interval' FROM lake.__duckwake.jobs WHERE job_id = 'analytics.game_totals'") == 2s ]] \
  || { echo "FAIL: game_totals' epoch interval is not 2s"; failed=1; }
expect_error "a changed query with --if-exists update" "$BIN" create-view analytics.game_totals --if-exists update \
  --epoch-interval 2s --sql "${GT_SQL/49.5/50}"
check "game_totals commits on from epoch $e0, not recomputed" 20 '(( $(epoch game_totals) > e0 ))'
alive

log "6. stop then drop at once, three times, with the view's epoch in flight"
for i in 1 2 3; do
  "$BIN" create-view analytics.scratch --epoch-interval 1s --sql "SELECT ts, amount FROM clicks WHERE amount >= 10" >/dev/null
  "$BIN" start scratch >/dev/null
  check "scratch $i commits" 30 '(( $(epoch scratch) >= 2 ))'
  "$BIN" stop scratch >/dev/null && "$BIN" drop scratch >/dev/null
done
sleep 3
grep -h 'epoch commit failed' "$OUT"/w*.log && { echo "FAIL: an epoch commit failed"; failed=1; }
echo "fenced or dropped mid-epoch: $(cat "$OUT"/w*.log | grep -c 'lease lost\|dropped during its epoch' || true)"
alive

log "drain the topics"
wait "$PRODUCER"; tail -1 "$OUT/producer.log"
end=$(topic_end); end2=$(topic_end "$TOPIC2")
for _ in $(seq 1 180); do
  done=1
  for v in "${VIEWS[@]}"; do caught_up "$v" "$end" "$end2" || done=0; done
  [[ $done == 1 ]] && break
  sleep 1
done
[[ $done == 1 ]] || { echo "FAIL: the views did not catch up"; "$BIN" jobs; failed=1; }
alive

log "verify"; verify_all || failed=1
[[ $failed == 0 ]] && echo "SCHEMA TEST PASSED" || { echo "SCHEMA TEST FAILED"; exit 1; }
