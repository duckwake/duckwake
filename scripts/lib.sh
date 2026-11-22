# Shared helpers for the test scripts. Source from a script in this directory.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
BIN=${BIN:-target/release/duckwake}
TOPIC=${TOPIC:-clicks}
# A second topic and source, read with clicks by the UNION ALL view all_events.
TOPIC2=${TOPIC2:-${TOPIC}_purchases}
PARTITIONS=${PARTITIONS:-3}
OUT=${OUT:-.lake/run}
# Tests get their own catalog database and data path, so they never touch the playground's lake.
TEST_DB=${TEST_DB:-duckwake_test}
# The catalog password comes from PGPASSWORD (libpq), not the connection string.
export DUCKWAKE_CATALOG="postgres:dbname=$TEST_DB host=localhost port=55432 user=lake"
export PGPASSWORD=lake
# Where the data files go: unset for local disk, or s3 for the bucket in the local MinIO, with the
# credentials in the standard AWS variables.
LAKE_STORAGE=${LAKE_STORAGE:-}
case $LAKE_STORAGE in
  "") export DUCKWAKE_DATA_PATH=".lake/test/data/" ;;
  s3)
    export DUCKWAKE_DATA_PATH="s3://duckwake/test/" AWS_ACCESS_KEY_ID=duckwake AWS_SECRET_ACCESS_KEY=duckwake-dev \
      AWS_ENDPOINT_URL=http://localhost:19900 AWS_REGION=us-east-1 ;;
  *) echo "LAKE_STORAGE must be s3 or unset"; exit 1 ;;
esac
VIEWS=(clicks_per_window big_spend game_totals active_users all_events)
# How the source decodes JSON: auto (Arrow where the column types allow), arrow or sql.
export DECODE=${DECODE:-auto}
# Extra flags for every worker the tests start, e.g. "--stage-threads 4 --pipeline --shared-readers".
WORKER_FLAGS=${WORKER_FLAGS:-}
# How the test sources reach Redpanda (docker/kafka/redpanda.yaml): unset for the open listener, or sasl_plaintext,
# sasl_ssl or mtls for the authenticated ones. Producers and rpk always use the open listener.
KAFKA_AUTH=${KAFKA_AUTH:-}
export DUCKWAKE_TEST_KAFKA_PASSWORD=duckwake-dev
CERTS=$PWD/docker/kafka
SCRAM='"security_protocol":"%s","sasl_mechanism":"SCRAM-SHA-256","sasl_username":"duckwake","sasl_password":"env:DUCKWAKE_TEST_KAFKA_PASSWORD"'
case $KAFKA_AUTH in
  "") KAFKA='"brokers":"localhost:19092"' ;;
  sasl_plaintext) KAFKA="\"brokers\":\"localhost:19093\",$(printf "$SCRAM" sasl_plaintext)" ;;
  sasl_ssl) KAFKA="\"brokers\":\"localhost:19094\",$(printf "$SCRAM" sasl_ssl),\"ssl_ca_location\":\"$CERTS/ca.crt\"" ;;
  mtls) KAFKA="\"brokers\":\"localhost:19095\",\"security_protocol\":\"ssl\",\"ssl_ca_location\":\"$CERTS/ca.crt\",\"ssl_certificate_location\":\"$CERTS/client.crt\",\"ssl_key_location\":\"$CERTS/client.key\"" ;;
  *) echo "KAFKA_AUTH must be sasl_plaintext, sasl_ssl or mtls"; exit 1 ;;
esac

log() { printf '\n== %s\n' "$*"; }

# Recreates the test catalog database, its data files and the Kafka topic.
reset_env() {
  docker compose up -d --wait >/dev/null
  docker compose exec -T redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
  docker compose exec -T redpanda rpk topic delete "$TOPIC2" >/dev/null 2>&1 || true
  rm -rf "$OUT"; mkdir -p "$OUT"
  reset_lake
}

# psql on the catalog's PostgreSQL: the compose service, or with PG_LOCAL=1 a server already listening on
# localhost:55432 with a `lake` role (suites that read only lake tables then run without Docker).
PG_LOCAL=${PG_LOCAL:-}
psql_lake() {
  if [[ $PG_LOCAL == 1 ]]; then
    PGOPTIONS='-c client_min_messages=warning' psql -h localhost -p 55432 -U lake "$@"
  else
    docker compose exec -T -e PGOPTIONS='-c client_min_messages=warning' postgres psql -U lake "$@"
  fi
}

# Recreates the test catalog database and its data files; the topic stays.
reset_lake() {
  psql_lake -q -d lake -c "DROP DATABASE IF EXISTS $TEST_DB" -c "CREATE DATABASE $TEST_DB" >/dev/null
  rm -rf .lake/test
  [[ $LAKE_STORAGE != s3 ]] || docker compose exec -T minio mc rm -r --force local/duckwake/test >/dev/null 2>&1 || true
  "$BIN" init >/dev/null
}

# Source plus the views used by the tests: a windowed aggregate (with late-data table) and a filter.
setup_views() {
  local epoch=${1:-2s} window=${2:-10 SECOND} delay=${3:-5 seconds}
  "$BIN" create-source clicks --options "{$KAFKA,\"topic\":\"$TOPIC\",\"event_time\":\"ts\",
    \"watermark_delay\":\"$delay\",\"idle_timeout\":\"30 seconds\",\"decode\":\"${DECODE:-auto}\",
    \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}"
  "$BIN" create-view analytics.clicks_per_window --late-data lake.analytics.clicks_late \
    --epoch-interval "$epoch" \
    --sql "SELECT window_start, game_id, count(*) AS n, sum(amount) AS revenue, max(amount) AS max_amount
           FROM TUMBLE(clicks, ts, INTERVAL $window) GROUP BY ALL" | sed -n 1p
  "$BIN" create-view analytics.big_spend --epoch-interval "$epoch" \
    --sql "SELECT ts, game_id, user_id, amount FROM clicks WHERE amount >= 90" | sed -n 1p
  # Global aggregate with HAVING over a changing average: groups enter and leave the upsert sink.
  "$BIN" create-view analytics.game_totals --epoch-interval "$epoch" \
    --sql "SELECT game_id, count(*) AS n, sum(amount) AS revenue, avg(amount) AS avg_amount, min(amount) AS lo, max(amount) AS hi
           FROM clicks GROUP BY ALL HAVING avg(amount) > 49.5" | sed -n 1p
  # UNION ALL of two sources: big clicks and every purchase.
  "$BIN" create-source purchases --options "{$KAFKA,\"topic\":\"$TOPIC2\",\"event_time\":\"ts\",
    \"watermark_delay\":\"$delay\",\"idle_timeout\":\"30 seconds\",\"decode\":\"${DECODE:-auto}\",
    \"columns\":{\"ts\":\"TIMESTAMP\",\"game_id\":\"VARCHAR\",\"user_id\":\"VARCHAR\",\"amount\":\"DECIMAL(18,2)\"}}" >/dev/null
  "$BIN" create-view analytics.all_events --epoch-interval "$epoch" \
    --sql "SELECT ts, game_id, user_id, amount, 'click' AS kind FROM clicks WHERE amount >= 80
           UNION ALL SELECT ts, game_id, user_id, amount, 'purchase' FROM purchases" | sed -n 1p
  # DISTINCT: each pair once, the first time it is seen.
  "$BIN" create-view analytics.active_users --epoch-interval "$epoch" \
    --sql "SELECT DISTINCT game_id, user_id FROM clicks WHERE amount >= 50" | sed -n 1p
}

start_views() {
  for v in "${VIEWS[@]}"; do "$BIN" start "$v" --worker "$1"; done
}

# Sum of committed next-offsets of a job's latest checkpoint, over all partitions of source $2 (default clicks).
committed() {
  "$BIN" sql "SELECT coalesce(sum(o::BIGINT), 0) AS n FROM (
      SELECT unnest(json_extract(source_offsets, '\$.${2:-clicks}.*')) AS o FROM (
        SELECT source_offsets FROM lake.__duckwake.checkpoints WHERE job_id IN ('$1', 'analytics.$1') ORDER BY epoch DESC LIMIT 1))" \
    2>/dev/null | awk -F'|' 'NR==4 {gsub(/ /,"",$2); print $2}'
}

topic_end() {
  docker compose exec -T redpanda rpk topic describe "${1:-$TOPIC}" -p 2>/dev/null | awk 'NR>1 {s+=$NF} END {print s+0}'
}

# The produce command to both topics: the same arguments, a quarter of the rate and count to the second.
produce() {
  local args=("$@") i
  for ((i = 0; i < ${#args[@]} - 1; i++)); do
    [[ ${args[i]} == --rate || ${args[i]} == --count ]] && args[i+1]=$(( (args[i+1] + 3) / 4 ))
  done
  "$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" "$@" &
  local first=$!
  "$BIN" produce --topic "$TOPIC2" --partitions "$PARTITIONS" "${args[@]}" >/dev/null 2>&1
  wait "$first"
}

# Whether view $1 has committed up to the given ends of both topics (all_events reads both).
caught_up() {
  [[ "$(committed "$1")" == "$2" ]] || return 1
  [[ $1 != all_events ]] || [[ "$(committed "$1" purchases)" == "$3" ]]
}

# Runs a worker until every view has committed up to the end of the topic.
drain() {
  local worker=$1 logf=$2 end
  "$BIN" run $WORKER_FLAGS --worker "$worker" >>"$logf" 2>&1 &
  local pid=$!
  end=$(topic_end)
  local end2; end2=$(topic_end "$TOPIC2")
  for _ in $(seq 1 120); do
    local done=1
    for v in "${VIEWS[@]}"; do caught_up "$v" "$end" "$end2" || done=0; done
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
