#!/usr/bin/env bash
# Smoke-test SubState against a running server (default: Compose on :8080).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_URL="${SUBSTATE_URL:-http://127.0.0.1:8080}"
WS_URL="${SUBSTATE_WS_URL:-ws://127.0.0.1:8080/v1/sync}"

echo "==> Waiting for ${BASE_URL}/health"
for _ in $(seq 1 60); do
  if curl -sf "${BASE_URL}/health" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "${BASE_URL}/health" | grep -q '"status"' || {
  echo "health check failed" >&2
  exit 1
}
echo "health ok"

echo "==> Building @substate/client"
(
  cd "${ROOT}/clients/typescript"
  npm install --silent
  npm run build --silent
)

echo "==> Running smoke (subscribe + ingest + delta)"
SUBSTATE_URL="${BASE_URL}" SUBSTATE_WS_URL="${WS_URL}" \
  node "${ROOT}/scripts/smoke.mjs"

echo "smoke passed"
