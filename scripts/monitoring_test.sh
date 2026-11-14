#!/usr/bin/env bash
# Monitoring gate (P4): Prometheus finds the workers itself, from the lake, with no list of targets.
#   1. Two workers with --metrics on one machine: Prometheus discovers both through one worker's /sd and scrapes
#      them, each named after its worker.
#   2. A third worker starts: it is scraped without any change to Prometheus' configuration.
#   3. It stops cleanly: it leaves the targets within two discovery refreshes, and no alert fires for it.
#   4. A worker is killed (-9): it stays a target, its scrapes fail, and DuckWakeWorkerDown becomes pending.
# Prometheus runs from the compose monitoring profile (host network on Linux; on macOS with docker-compose.mac.yml).
source "$(dirname "$0")/lib.sh"
failed=0
if [[ $(uname) == Darwin ]]; then COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.mac.yml); else COMPOSE=(docker compose); fi
PROM=http://localhost:19090

run_worker() {   # worker id, metrics port
  # shellcheck disable=SC2086
  "$BIN" run $WORKER_FLAGS --worker "$1" --lease-timeout 10s --metrics "127.0.0.1:$2" >>"$OUT/$1.log" 2>&1 &
  echo $! >"$OUT/$1.pid"
}
pid() { cat "$OUT/$1.pid"; }
trap 'for w in w1 w2 w3; do [[ -f $OUT/$w.pid ]] && kill -9 "$(pid $w)" 2>/dev/null || true; done; "${COMPOSE[@]}" --profile monitoring rm -sf prometheus >/dev/null 2>&1 || true' EXIT
# "worker health" per discovered duckwake target.
targets() {
  curl -s "$PROM/api/v1/targets?state=active" | python3 -c '
import sys, json
for t in json.load(sys.stdin)["data"]["activeTargets"]:
    if t["labels"]["job"] == "duckwake": print(t["labels"]["instance"], t["health"])' | sort
}
alert_state() {   # alert name, instance
  curl -s "$PROM/api/v1/alerts" | python3 -c '
import sys, json
for a in json.load(sys.stdin)["data"]["alerts"]:
    if a["labels"]["alertname"] == sys.argv[1] and a["labels"].get("instance") == sys.argv[2]: print(a["state"])' "$1" "$2"
}
check() {
  local what=$1 bound=$2 cond=$3 t0=$SECONDS
  while ((SECONDS - t0 <= bound)); do
    if eval "$cond"; then echo "$what: $((SECONDS - t0))s"; targets | sed 's/^/  /'; return 0; fi
    sleep 2
  done
  echo "FAIL: $what after $((SECONDS - t0))s"; targets | sed 's/^/  /'; failed=1
}

log "reset lake and topics, views, two workers with metrics"
reset_env
docker compose exec -T redpanda rpk topic create "$TOPIC" "$TOPIC2" -p "$PARTITIONS" >/dev/null
setup_views 2s "10 SECOND" "5 seconds" >/dev/null
run_worker w1 9464; run_worker w2 9465
for v in "${VIEWS[@]}"; do "$BIN" start "$v" >/dev/null; done
produce --rate 500 --count 60000 --eos >"$OUT/producer.log" 2>&1 &
"${COMPOSE[@]}" --profile monitoring up -d --force-recreate prometheus >/dev/null 2>&1
"$BIN" workers

log "1. Prometheus discovers both workers from the lake"
check "w1 and w2 discovered and up" 90 '[[ $(targets | tr "\n" " ") == "w1 up w2 up " ]]'

log "2. a third worker starts: scraped with no configuration change"
run_worker w3 9466
check "w3 discovered and up" 90 '[[ $(targets | grep -c " up$") == 3 ]]'

log "3. w3 stops cleanly: it leaves the targets"
kill -TERM "$(pid w3)"; wait "$(pid w3)" 2>/dev/null || true
check "w3 no longer a target" 90 '! targets | grep -q "^w3 "'
# Until it leaves (at most two discovery refreshes, 30 s) its failed scrapes may make the alert pending, but it
# holds for 2 minutes before firing: it must never fire, and must clear once w3 has left.
[[ $(alert_state DuckWakeWorkerDown w3) != firing ]] || { echo "FAIL: DuckWakeWorkerDown fired for the cleanly stopped w3"; failed=1; }
check "no alert for the cleanly stopped w3" 60 '[[ -z $(alert_state DuckWakeWorkerDown w3) ]]'

log "4. w2 is killed: it stays a target, down, and the alert is pending"
kill -9 "$(pid w2)"; wait "$(pid w2)" 2>/dev/null || true
check "w2 down" 60 'targets | grep -q "^w2 down$"'
check "DuckWakeWorkerDown pending for w2" 60 '[[ -n $(alert_state DuckWakeWorkerDown w2) ]]'
echo "DuckWakeWorkerDown for w2: $(alert_state DuckWakeWorkerDown w2)"

[[ $failed == 0 ]] && echo "MONITORING TEST PASSED" || { echo "MONITORING TEST FAILED"; exit 1; }
