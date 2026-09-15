#!/usr/bin/env bash
# Smoke-test SubState against a running server (default: Compose on :8080).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_URL="${SUBSTATE_URL:-http://127.0.0.1:8080}"
WS_URL="${SUBSTATE_WS_URL:-ws://127.0.0.1:8080/v1/sync}"
COMPOSE="${COMPOSE:-docker compose}"

echo "==> Waiting for ${BASE_URL}/health"
for _ in $(seq 1 90); do
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

SMOKE_PG_CMD="${SMOKE_PG_CMD:-}"
SMOKE_KAFKA_CMD="${SMOKE_KAFKA_CMD:-}"

if [[ -z "${SMOKE_PG_CMD}" ]] && ${COMPOSE} ps postgres >/dev/null 2>&1; then
  SMOKE_PG_CMD="${COMPOSE} exec -T postgres psql -U substate -d substate -c \"UPDATE drivers SET name='__NAME__' WHERE id=1\""
fi
if [[ -z "${SMOKE_KAFKA_CMD}" ]] && ${COMPOSE} ps redpanda >/dev/null 2>&1; then
  SMOKE_KAFKA_CMD="printf '%s\\n' '{\"driver_id\":\"1\",\"location\":{\"lat\":36.17,\"lng\":-86.79},\"sequence\":__SEQ__}' | ${COMPOSE} exec -T redpanda rpk topic produce driver-locations --brokers=redpanda:9092"
fi

echo "==> Running smoke (subscribe + postgres + kafka/http)"
SUBSTATE_URL="${BASE_URL}" SUBSTATE_WS_URL="${WS_URL}" \
  SMOKE_PG_CMD="${SMOKE_PG_CMD}" SMOKE_KAFKA_CMD="${SMOKE_KAFKA_CMD}" \
  node "${ROOT}/scripts/smoke.mjs"

echo "smoke passed"
