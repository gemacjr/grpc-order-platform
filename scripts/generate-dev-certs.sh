#!/usr/bin/env bash
set -euo pipefail

# Development certificates only. Never commit the output; never use in production.
OUT="${1:-certs}"
mkdir -p "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$OUT/ca.key" -out "$OUT/ca.crt" \
  -subj "/CN=order-dev-ca"

openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/server.key" -out "$OUT/server.csr" \
  -subj "/CN=order-service"

openssl x509 -req -in "$OUT/server.csr" -days 365 \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/server.crt" \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:order-service,IP:127.0.0.1")

rm -f "$OUT/server.csr"
echo "Development certificates written to $OUT/"
