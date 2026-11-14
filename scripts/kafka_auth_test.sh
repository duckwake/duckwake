#!/usr/bin/env bash
# P2 Kafka auth gate, against the authenticated listeners of the local Redpanda (docker/kafka/redpanda.yaml).
# For each of SASL/SCRAM in plaintext, SASL/SCRAM over TLS, and TLS with a client certificate, the test
# views read their sources through that listener while events stream in, drain and verify against the batch
# recomputation (which reads Kafka through the same listener). Then what must be refused: a password stored
# in the definition, a wrong password, an unset password variable, and a plaintext client on a SASL listener.
# Any suite built on lib.sh's views runs authenticated the same way: KAFKA_AUTH=sasl_ssl scripts/crash_test.sh
source "$(dirname "$0")/lib.sh"
MODES=${MODES:-sasl_plaintext sasl_ssl mtls}
failed=0

if [[ ${1:-} == --one ]]; then
  log "views over $KAFKA_AUTH"
  reset_env
  setup_views 2s "10 SECOND" "5 seconds" >/dev/null
  "$BIN" sql "SELECT name, options->>'brokers' AS brokers, options->>'security_protocol' AS protocol,
                     options->>'sasl_password' AS password FROM lake.__duckwake.sources"
  start_views w1 >/dev/null
  produce --rate 5000 --count 20000 --late-every 997 --bad-every 1499 --eos 2>&1 | tail -1
  drain w1 "$OUT/worker.log"
  grep -qE 'cannot start job|worker failed' "$OUT/worker.log" && { grep -E 'cannot start job|worker failed' "$OUT/worker.log" | head -3; exit 1; }
  verify_all
  exit
fi

for mode in $MODES; do
  KAFKA_AUTH=$mode bash "$0" --one || { echo "FAIL: $mode"; failed=1; }
done

log "refused"
reset_env
docker compose exec -T redpanda rpk topic create "$TOPIC" -p "$PARTITIONS" >/dev/null 2>&1 || true
"$BIN" produce --topic "$TOPIC" --partitions "$PARTITIONS" --rate 5000 --count 1000 >/dev/null 2>&1
cols='"columns":{"ts":"TIMESTAMP","game_id":"VARCHAR","user_id":"VARCHAR","amount":"DECIMAL(18,2)"}'
scram='"sasl_mechanism":"SCRAM-SHA-256","sasl_username":"duckwake"'
src() { "$BIN" create-source "$1" --options "{\"topic\":\"$TOPIC\",\"event_time\":\"ts\",$cols,$2}" 2>&1; }

out=$(src literal "\"brokers\":\"localhost:19093\",\"security_protocol\":\"sasl_plaintext\",$scram,\"sasl_password\":\"duckwake-dev\"") && failed=1
echo "password in the definition: ${out##*Error: }"

# Each of these sources is created, then a view over it must fail to start, with the reason in the log.
export WRONG_PASSWORD=not-the-password
src wrong "\"brokers\":\"localhost:19093\",\"security_protocol\":\"sasl_plaintext\",$scram,\"sasl_password\":\"env:WRONG_PASSWORD\"" >/dev/null
src unset "\"brokers\":\"localhost:19093\",\"security_protocol\":\"sasl_plaintext\",$scram,\"sasl_password\":\"env:DUCKWAKE_UNSET_PASSWORD\"" >/dev/null
src plain "\"brokers\":\"localhost:19093\"" >/dev/null
for s in wrong unset plain; do
  "$BIN" create-view "analytics.v_$s" --epoch-interval 1s --sql "SELECT ts, amount FROM $s" >/dev/null
  "$BIN" start "v_$s" --worker w1 >/dev/null
done
"$BIN" run --worker w1 >"$OUT/refused.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do [[ $(grep -c 'cannot start job' "$OUT/refused.log") -ge 3 ]] && break; sleep 0.5; done
kill -TERM "$pid"; wait "$pid" || true
for s in wrong unset plain; do
  line=$(grep "cannot start job" "$OUT/refused.log" | grep "job=v_$s" | sed -n 1p)
  if [[ -n $line ]]; then
    echo "$s: refused: $(echo "$line" | grep -oE 'error=.*' | cut -c1-200)"
  else
    echo "FAIL: the view over $s started"; failed=1
  fi
done
"$BIN" jobs | grep -E 'v_(wrong|unset|plain)' | awk -F'|' '{print " ", $2, $3}'

[[ $failed == 0 ]] && echo "KAFKA AUTH TEST PASSED" || { echo "KAFKA AUTH TEST FAILED"; exit 1; }
