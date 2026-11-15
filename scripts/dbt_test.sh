#!/usr/bin/env bash
# dbt-duckwake gate: the example project (dbt/example) deployed and changed with dbt while events stream in and
# a worker runs its views.
#   1. dbt run creates the source and five views and starts them; a second run changes nothing.
#   2. A changed setting alters the views in place; a changed query fails until --full-refresh, which needs the
#      view's readers selected too (-s big_spend+), drops them and creates them again after it.
#   3. A new view over a running one (top_games over game_totals) fills from its current rows, and the source
#      gains a column.
#   4. Once the topic ends, every view catches up (duckwake_wait in on-run-end) and dbt test passes:
#      duckwake_verified on every view, and dbt's own tests on their tables.
#   5. duckwake_drop_orphans lists, then drops, a view whose model was removed.
# Needs Python 3 and the extension built (scripts/build_extension.sh); installs dbt-duckdb into .venv-dbt.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-500}; DURATION=${DURATION:-60}
DBT_DUCKDB=${DBT_DUCKDB:-1.11.1}; DUCKDB_PY=${DUCKDB_PY:-2.0.0}
export DBT_SEND_ANONYMOUS_USAGE_STATS=False DUCKWAKE_TEST_BROKERS=localhost:19092
failed=0

if [[ -z ${DBT:-} ]]; then
  [[ -x .venv-dbt/bin/dbt ]] || { python3 -m venv .venv-dbt && .venv-dbt/bin/pip install -q "dbt-duckdb==$DBT_DUCKDB" "duckdb==$DUCKDB_PY"; }
  DBT=.venv-dbt/bin/dbt PY=.venv-dbt/bin/python
fi
PY=${PY:-python3}
# The extension as a repository the profile installs from: dist/extensions/<duckdb version>/<platform>/.
export DUCKWAKE_EXTENSION_REPO=$PWD/.lake/dbt_extensions
platform=$("$PY" -c 'import duckdb; print(duckdb.execute("PRAGMA platform").fetchone()[0])')
mkdir -p "$DUCKWAKE_EXTENSION_REPO/v$DUCKDB_PY/$platform"
cp target/release/duckwake.duckdb_extension "$DUCKWAKE_EXTENSION_REPO/v$DUCKDB_PY/$platform/"
gzip -kf "$DUCKWAKE_EXTENSION_REPO/v$DUCKDB_PY/$platform/duckwake.duckdb_extension"
# The profile's own extension directory, without DuckWake: dbt installs this build afresh.
export DUCKWAKE_DUCKDB_EXTENSIONS=$PWD/.lake/duckdb_extensions
rm -f "$DUCKWAKE_DUCKDB_EXTENSIONS"/v*/*/duckwake.duckdb_extension*

VARS="topic: $TOPIC"
dbt() {
  # From the repository root: the lake's data path is relative to it.
  "$DBT" "$@" --project-dir dbt/example --profiles-dir dbt/example --vars "{$VARS}" >"$OUT/dbt.log" 2>&1
  local rc=$?
  cat "$OUT/dbt.log" >>"$OUT/dbt_all.log"
  return $rc
}
# The lines the package logs for each model: "<view>: <outcome> ...".
outcomes() { { grep -oE '[a-z_.]+: (created|unchanged|altered|replaced|added column|dropped)' "$OUT/dbt.log" || true; } | sed 's/^analytics\.//' | sort | tr '\n' ' '; }
expect() {
  local what=$1 want=$2 got; got=$(outcomes)
  [[ $got == "$want" ]] && echo "$what: $got" || { echo "FAIL: $what: got [$got], want [$want]"; failed=1; }
}
expect_error() {
  local what=$1 text=$2
  if grep -q "$text" "$OUT/dbt.log"; then echo "$what: refused ($(grep -o "$text.\{0,60\}" "$OUT/dbt.log" | head -1 || true))"
  else echo "FAIL: $what: no '$text' in the dbt output"; tail -20 "$OUT/dbt.log"; failed=1; fi
}
status() { "$BIN" sql "SELECT status FROM lake.__duckwake.jobs WHERE job_id = '$1'" --csv 2>/dev/null | tail -1; }
trap '[[ -z ${pid:-} ]] || kill "$pid" 2>/dev/null || true' EXIT

log "reset lake and topic"; reset_env
"$BIN" sql "CREATE SCHEMA lake.shop; CREATE TABLE lake.shop.accounts (id INTEGER, game_id VARCHAR, amount DECIMAL(18,2));
            INSERT INTO lake.shop.accounts SELECT i, 'g' || (i % 5), (i % 100) FROM range(1, 5001) t(i)" >/dev/null
docker compose exec -T redpanda rpk topic create "$TOPIC" -p "$PARTITIONS" >/dev/null
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate "$RATE" --count $((RATE * DURATION)) --eos \
  >"$OUT/producer.log" 2>&1 &
PRODUCER=$!
"$BIN" run $WORKER_FLAGS --worker w1 >>"$OUT/worker.log" 2>&1 &
pid=$!
dbt deps >/dev/null || { echo "dbt deps failed"; cat "$OUT/dbt.log"; exit 1; }

log "1. deploy, then run again"
dbt run || { tail -30 "$OUT/dbt.log"; failed=1; }
expect "first run" "account_totals: created big_spend: created clicks: created clicks_per_window: created game_totals: created spend_by_game: created "
for v in account_totals big_spend clicks_per_window game_totals spend_by_game; do
  [[ $(status "$v") == running ]] || { echo "FAIL: $v is $(status "$v") after the first run"; failed=1; }
done
dbt run || failed=1
expect "second run" "account_totals: unchanged big_spend: unchanged clicks: unchanged clicks_per_window: unchanged game_totals: unchanged spend_by_game: unchanged "

log "2. a setting, then a query"
VARS="$VARS, epoch_interval: 3s"
dbt run || failed=1
expect "epoch_interval 3s" "account_totals: altered big_spend: altered clicks: unchanged clicks_per_window: altered game_totals: altered spend_by_game: altered "
VARS="$VARS, big_spend_min: 95"
if dbt run; then echo "FAIL: a changed query was applied without --full-refresh"; failed=1; fi
expect_error "changed query" "only a replace recomputes it"
if dbt run -s big_spend --full-refresh; then echo "FAIL: big_spend was replaced while spend_by_game reads it"; failed=1; fi
expect_error "full refresh without its reader" "is read by spend_by_game"
dbt run -s big_spend+ --full-refresh || { tail -30 "$OUT/dbt.log"; failed=1; }
expect "full refresh with its reader" "big_spend: replaced spend_by_game: created spend_by_game: dropped "

log "3. a new view over a running one, and a new source column"
"$BIN" sql "UPDATE lake.shop.accounts SET amount = amount + 1 WHERE id % 7 = 0;
            DELETE FROM lake.shop.accounts WHERE id % 11 = 0;
            INSERT INTO lake.shop.accounts SELECT i, 'g' || (i % 5), (i % 100) FROM range(5001, 6001) t(i)" >/dev/null
VARS="$VARS, with_top_games: true, with_country: true"
dbt run || { tail -30 "$OUT/dbt.log"; failed=1; }
expect "top_games and country" "account_totals: unchanged big_spend: unchanged clicks: added column clicks_per_window: unchanged game_totals: unchanged spend_by_game: unchanged top_games: created "

log "4. the topic ends; every view catches up and dbt test passes"
wait "$PRODUCER"; tail -1 "$OUT/producer.log"
VARS="$VARS, duckwake_wait: true, duckwake_wait_s: 300" dbt run || { tail -30 "$OUT/dbt.log"; failed=1; }
grep -oE '[a-z_]+: caught up at epoch [0-9]+' "$OUT/dbt.log" | sed 's/^/  /' || true
dbt test || failed=1
grep -E ' (PASS|FAIL|ERROR) ' "$OUT/dbt.log" | sed -E 's/.*[0-9]+ of [0-9]+ //; s/ \.+.*\[/ [/' | sed 's/^/  /' || true
tests=$(grep -oE 'PASS=[0-9]+ WARN=[0-9]+ ERROR=[0-9]+' "$OUT/dbt.log" | tail -1 || true); echo "dbt test: $tests"
[[ $tests == *"ERROR=0"* ]] || failed=1

log "5. a model removed: duckwake_drop_orphans"
VARS=${VARS/with_top_games: true/with_top_games: false}
dbt run-operation duckwake_drop_orphans || failed=1
grep -q 'orphaned: top_games' "$OUT/dbt.log" && echo "listed: top_games" || { echo "FAIL: top_games not listed"; failed=1; }
[[ $(status top_games) == running ]] || { echo "FAIL: the dry run changed top_games"; failed=1; }
dbt run-operation duckwake_drop_orphans --args '{dry_run: false}' || failed=1
[[ -z $(status top_games) ]] && echo "dropped: top_games" || { echo "FAIL: top_games is still there"; failed=1; }

kill -TERM "$pid"; wait "$pid" || true
if (( failed )); then echo "DBT TEST FAILED"; exit 1; fi
echo "DBT TEST PASSED"
