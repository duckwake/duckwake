# Shared helpers for the POC test scripts. Source from a script in this directory.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
BIN=${BIN:-target/release/duckwake}
TOPIC=${TOPIC:-clicks}
PARTITIONS=${PARTITIONS:-3}
OUT=${OUT:-.lake/run}
# Tests get their own catalog database and data path, so they never touch the playground's lake.
TEST_DB=${TEST_DB:-duckwake_test}
export DUCKWAKE_CATALOG="postgres:dbname=$TEST_DB host=localhost port=55432 user=lake password=lake"
export DUCKWAKE_DATA_PATH=".lake/test/data/"
VIEWS=(clicks_per_window big_spend game_totals)
# How the source decodes JSON: auto (Arrow where the column types allow), arrow or sql.
export DECODE=${DECODE:-auto}
# Extra flags for every worker the tests start, e.g. "--stage-threads 4 --pipeline --shared-readers".
WORKER_FLAGS=${WORKER_FLAGS:-}

log() { printf '\n== %s\n' "$*"; }

# Recreates the test catalog database, its data files and the Kafka topic.
reset_env() {
  docker compose up -d --wait >/dev/null
  docker compose exec -T redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
  rm -rf "$OUT"; mkdir -p "$OUT"
  reset_lake
}

# Recreates the test catalog database and its data files; the topic stays.
reset_lake() {
  docker compose exec -T -e PGOPTIONS='-c client_min_messages=warning' postgres psql -q -U lake -d lake \
    -c "DROP DATABASE IF EXISTS $TEST_DB" -c "CREATE DATABASE $TEST_DB" >/dev/null
  rm -rf .lake/test
  "$BIN" init >/dev/null
}

# Source plus the views used by the tests: a windowed aggregate (with late-data table) and a filter.
setup_views() {
  local epoch=${1:-2s} window=${2:-10 SECOND} delay=${3:-5 seconds}
  "$BIN" create-source clicks --options "{\"brokers\":\"localhost:19092\",\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
    \"watermark_delay\":\"$delay\",\"idle_timeout\":\"30 seconds\",\"decode\":\"${DECODE:-auto}\",
    \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}"
  "$BIN" create-view analytics.clicks_per_window --late-data lake.analytics.clicks_late \
    --epoch-interval "$epoch" \
    --sql "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS max_amount
           FROM TUMBLE(clicks, ts, INTERVAL $window) GROUP BY ALL" | head -1
  "$BIN" create-view analytics.big_spend --epoch-interval "$epoch" \
    --sql "SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 90" | head -1
  # Global aggregate with HAVING over a changing average: groups enter and leave the upsert sink.
  "$BIN" create-view analytics.game_totals --epoch-interval "$epoch" \
    --sql "SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount, min(amount) AS lo, max(amount) AS hi
           FROM clicks GROUP BY ALL HAVING avg(amount) > 49.5" | head -1
}

start_views() {
  for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker "$1"; done
}

# Sum of committed next-offsets of a job's latest checkpoint, over all partitions of source $2 (default clicks).
committed() {
  "$BIN" sql "SELECT coalesce(sum(o::BIGINT), 0) AS n FROM (
      SELECT unnest(json_extract(source_offsets, '\$.${2:-clicks}.*')) AS o FROM (
        SELECT source_offsets FROM lake.__duckwake.checkpoints WHERE job_id = '$1' ORDER BY epoch DESC LIMIT 1))" \
    2>/dev/null | awk -F'|' 'NR==4 {gsub(/ /,"",$2); print $2}'
}

topic_end() {
  docker compose exec -T redpanda rpk topic describe "$TOPIC" -p 2>/dev/null | awk 'NR>1 {s+=$NF} END {print s+0}'
}

# Runs a worker until every view has committed up to the end of the topic.
drain() {
  local worker=$1 logf=$2 end
  "$BIN" run $WORKER_FLAGS --worker "$worker" >>"$logf" 2>&1 &
  local pid=$!
  end=$(topic_end)
  for _ in $(seq 1 120); do
    local done=1
    for v in "${VIEWS[@]}"; do [[ "$(committed "$v")" == "$end" ]] || done=0; done
    [[ $done == 1 ]] && break
    sleep 1
  done
  kill -TERM "$pid"; wait "$pid" || true
}

verify_all() {
  local rc=0
  for v in "${VIEWS[@]}"; do
    if "$BIN" verify "$v" >"$OUT/verify_$v.txt" 2>&1; then tail -1 "$OUT/verify_$v.txt"; else rc=1; cat "$OUT/verify_$v.txt"; fi
  done
  return $rc
}
