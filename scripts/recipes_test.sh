#!/usr/bin/env bash
# The recipes in docs/recipes/ run as written: every SQL statement and command in them, in order, against a
# duckwaked like the one their "Before you start" starts, and every table of results they show must be what the
# server returns (scripts/recipes.py). Each recipe ends by dropping what it made, so they run one after another
# on one lake.
#
# The recipes use the compose stack: Redpanda (rpk in its container), and for the PostgreSQL recipe the service
# pg-source (profile debezium). RECIPES picks some (default: all). With PG_LOCAL=1 only the PostgreSQL recipe
# runs, against a local server on localhost:55432 started with wal_level=logical; the script creates the role
# and database `shop` there and rewrites the recipe's psql and port to reach it.
source "$(dirname "$0")/lib.sh"
SERVER=quack:localhost:${QUACK_PORT:-9494}; METRICS=127.0.0.1:${METRICS_PORT:-9464}
export SERVER DUCKWAKE_QUACK_TOKEN=secret SHOP_PG_PASSWORD=shop BIN
DAEMON=${DAEMON:-$(dirname "$BIN")/duckwaked}
TOPICS=(purchases order_updates page_views)

if [[ $PG_LOCAL == 1 ]]; then
  RECIPES=${RECIPES:-docs/recipes/postgres-revenue.md}
  shop() { PGOPTIONS='-c client_min_messages=warning' PGPASSWORD=shop psql -h localhost -p 55432 -U shop -d shop -qtA "$@"; }
  psql_lake -q -d lake -c "DO \$\$BEGIN CREATE ROLE shop LOGIN SUPERUSER PASSWORD 'shop'; EXCEPTION WHEN duplicate_object THEN NULL; END\$\$" >/dev/null
  psql_lake -q -d lake -tA -c "SELECT 1 FROM pg_database WHERE datname = 'shop'" | grep -q 1 \
    || psql_lake -q -d lake -c "CREATE DATABASE shop OWNER shop" >/dev/null
  export RECIPE_SUBST="docker compose --profile debezium exec -T pg-source psql -U shop=>PGPASSWORD=shop psql -h localhost -p 55432 -U shop -d shop
port=15432=>port=55432"
  log "reset lake"; rm -rf "$OUT"; mkdir -p "$OUT"; reset_lake
else
  RECIPES=${RECIPES:-$(ls docs/recipes/*.md)}
  docker compose --profile debezium up -d --wait pg-source >/dev/null
  shop() { docker compose --profile debezium exec -T -e PGOPTIONS='-c client_min_messages=warning' pg-source psql -U shop -d shop -qtA "$@"; }
  log "reset lake and topics"; reset_env
  for t in "${TOPICS[@]}"; do docker compose exec -T redpanda rpk topic delete "$t" >/dev/null 2>&1 || true; done
fi
# What an earlier run may have left in the source database.
shop -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'duckwake_shop'" >/dev/null
shop -c "DROP PUBLICATION IF EXISTS duckwake_shop" -c "DROP TABLE IF EXISTS orders" >/dev/null

# The server the recipes start: one worker, SQL over Quack with the token `secret`.
# shellcheck disable=SC2086
"$DAEMON" --worker w1 --quack "$SERVER" --metrics "$METRICS" $WORKER_FLAGS >"$OUT/server.log" 2>&1 &
SRV=$!
trap 'kill -TERM $SRV 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -sf "http://$METRICS/healthz" >/dev/null && break; sleep 1; done

log "run the recipes"
# shellcheck disable=SC2086
if python3 -u scripts/recipes.py $RECIPES; then
  log "PASS"
else
  echo "-- the server's last log lines:"; tail -20 "$OUT/server.log"
  log "FAIL"; exit 1
fi
