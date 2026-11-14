#!/usr/bin/env bash
# Test-only TLS material for the local Redpanda's TLS listeners (docker-compose.yml): a CA, a broker
# certificate for localhost, and a client certificate for the mTLS listener. Never use these anywhere real;
# the keys are in the repository on purpose, so `docker compose up` works without a setup step.
set -euo pipefail
cd "$(dirname "$0")"
days=36500
openssl req -x509 -newkey rsa:2048 -nodes -days $days -subj "/CN=duckwake test CA" -keyout ca.key -out ca.crt
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout broker.key -out broker.csr
printf 'subjectAltName=DNS:localhost,DNS:redpanda,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >broker.ext
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $days -extfile broker.ext -out broker.crt
openssl req -newkey rsa:2048 -nodes -subj "/CN=duckwake" -keyout client.key -out client.csr
printf 'extendedKeyUsage=clientAuth\n' >client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days $days -extfile client.ext -out client.crt
rm -f ./*.csr ./*.ext ca.srl ca.key
# Redpanda runs as its own user in the container.
chmod 644 ./*.key ./*.crt
