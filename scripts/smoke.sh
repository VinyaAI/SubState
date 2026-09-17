#!/usr/bin/env bash
# End-to-end smoke: Postgres-in-Docker + substate serve + CDS + WS subscribe + ingest.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PG_NAME="${SMOKE_PG_NAME:-substate-smoke-pg}"
PG_PORT="${SMOKE_PG_PORT:-54329}"
BIND_HOST="127.0.0.1"
BIND_PORT="${SMOKE_BIND_PORT:-18080}"
BIND_ADDR="${BIND_HOST}:${BIND_PORT}"
API_KEY="${SMOKE_API_KEY:-smoke-secret}"
BASE_URL="http://${BIND_ADDR}"
WS_URL="ws://${BIND_ADDR}/v1/sync"

SERVE_PID=""
WS_PID=""
TMP_DIR=""

if docker info >/dev/null 2>&1; then
  DOCKER=(docker)
elif sudo docker info >/dev/null 2>&1; then
  DOCKER=(sudo docker)
else
  echo "error: docker is required (docker info failed)" >&2
  exit 1
fi

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: missing required command: $1" >&2
    exit 1
  }
}

need curl
need cargo
need node

NODE_MAJOR="$(node -p "process.versions.node.split('.')[0]")"
if [[ "${NODE_MAJOR}" -lt 20 ]]; then
  echo "error: Node 20+ required for smoke_ws.mjs (found $(node -v))" >&2
  exit 1
fi

cleanup() {
  local code=$?
  if [[ -n "${WS_PID}" ]] && kill -0 "${WS_PID}" 2>/dev/null; then
    kill "${WS_PID}" 2>/dev/null || true
    wait "${WS_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SERVE_PID}" ]] && kill -0 "${SERVE_PID}" 2>/dev/null; then
    kill "${SERVE_PID}" 2>/dev/null || true
    wait "${SERVE_PID}" 2>/dev/null || true
  fi
  "${DOCKER[@]}" rm -f "${PG_NAME}" >/dev/null 2>&1 || true
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
  exit "${code}"
}
trap cleanup EXIT INT TERM

echo "==> starting Postgres (${PG_NAME} on :${PG_PORT})"
"${DOCKER[@]}" rm -f "${PG_NAME}" >/dev/null 2>&1 || true
"${DOCKER[@]}" run -d \
  --name "${PG_NAME}" \
  -e POSTGRES_PASSWORD=smoke \
  -e POSTGRES_DB=substate \
  -p "${PG_PORT}:5432" \
  postgres:16 >/dev/null

echo "==> waiting for Postgres"
for _ in $(seq 1 60); do
  if "${DOCKER[@]}" exec "${PG_NAME}" pg_isready -U postgres -d substate >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! "${DOCKER[@]}" exec "${PG_NAME}" pg_isready -U postgres -d substate >/dev/null 2>&1; then
  echo "error: Postgres did not become ready" >&2
  exit 1
fi

echo "==> applying SQL fixture"
"${DOCKER[@]}" exec -i "${PG_NAME}" psql -U postgres -d substate \
  < "${ROOT}/scripts/fixtures/smoke.sql" >/dev/null

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/substate-smoke.XXXXXX")"
READY_FILE="${TMP_DIR}/ws-ready"
export DATABASE_URL="postgresql://postgres:smoke@127.0.0.1:${PG_PORT}/substate"
export SCHEMA_PATH="${ROOT}/scripts/fixtures/smoke-schema.yaml"
export BIND_ADDR="${BIND_ADDR}"
export POSTGRES_FOLLOW=poll
export CDS_POLL_MS=500
export SUBSTATE_API_KEY="${API_KEY}"
export SUBSTATE_SNAPSHOT_PATH="${TMP_DIR}/snapshot.json"
export SUBSTATE_KAFKA_OFFSETS_PATH="${TMP_DIR}/kafka-offsets.json"
export RUST_LOG="${RUST_LOG:-info}"

echo "==> ensuring TypeScript client deps (ws)"
if [[ ! -d "${ROOT}/clients/typescript/node_modules/ws" ]]; then
  (cd "${ROOT}/clients/typescript" && npm ci)
fi

echo "==> building substate-cli"
cargo build -p substate-cli

BIN="${ROOT}/target/debug/substate"
if [[ ! -x "${BIN}" ]]; then
  echo "error: expected binary at ${BIN}" >&2
  exit 1
fi

echo "==> starting substate serve on ${BIND_ADDR}"
"${BIN}" serve >"${TMP_DIR}/serve.log" 2>&1 &
SERVE_PID=$!

echo "==> waiting for /health"
ready=0
for _ in $(seq 1 90); do
  if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "${SERVE_PID}" 2>/dev/null; then
    echo "error: serve exited early; log:" >&2
    cat "${TMP_DIR}/serve.log" >&2 || true
    exit 1
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  echo "error: /health never became ready; serve log:" >&2
  cat "${TMP_DIR}/serve.log" >&2 || true
  exit 1
fi

health="$(curl -fsS "${BASE_URL}/health")"
echo "health: ${health}"
echo "${health}" | grep -q '"status"[[:space:]]*:[[:space:]]*"ok"' || {
  echo "error: unexpected health body: ${health}" >&2
  exit 1
}

echo "==> GET /v1/cds/driver/1"
cds=""
for _ in $(seq 1 30); do
  if cds="$(curl -fsS -H "Authorization: Bearer ${API_KEY}" "${BASE_URL}/v1/cds/driver/1" 2>/dev/null)"; then
    if echo "${cds}" | grep -q 'available'; then
      break
    fi
  fi
  sleep 0.5
done
echo "cds: ${cds}"
echo "${cds}" | grep -q 'available' || {
  echo "error: CDS body missing status available" >&2
  exit 1
}
echo "${cds}" | grep -Eq '"id"[[:space:]]*:[[:space:]]*"?1"?' || {
  echo "error: CDS body missing id 1" >&2
  exit 1
}

echo "==> starting WebSocket waiter"
rm -f "${READY_FILE}"
SMOKE_WS_URL="${WS_URL}" \
SMOKE_API_KEY="${API_KEY}" \
SMOKE_EXPECT=delta \
SMOKE_TIMEOUT_MS=30000 \
SMOKE_READY_FILE="${READY_FILE}" \
node "${ROOT}/scripts/smoke_ws.mjs" >"${TMP_DIR}/ws.log" 2>&1 &
WS_PID=$!

echo "==> waiting for WS subscribed+snapshot"
for _ in $(seq 1 60); do
  if [[ -f "${READY_FILE}" ]]; then
    break
  fi
  if ! kill -0 "${WS_PID}" 2>/dev/null; then
    echo "error: smoke_ws.mjs exited before ready; log:" >&2
    cat "${TMP_DIR}/ws.log" >&2 || true
    exit 1
  fi
  sleep 0.25
done
if [[ ! -f "${READY_FILE}" ]]; then
  echo "error: WS helper never became ready; log:" >&2
  cat "${TMP_DIR}/ws.log" >&2 || true
  exit 1
fi

echo "==> POST /v1/ingest location"
ingest="$(curl -fsS \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "content-type: application/json" \
  -d '{"source":"http","entity_type":"driver","id":"1","fields":{"location":{"lat":36.16,"lng":-86.78}},"versions":{"location":1}}' \
  "${BASE_URL}/v1/ingest")"
echo "ingest: ${ingest}"
echo "${ingest}" | grep -q '"accepted"[[:space:]]*:[[:space:]]*true' || {
  echo "error: ingest not accepted: ${ingest}" >&2
  exit 1
}

echo "==> waiting for WebSocket delta"
if ! wait "${WS_PID}"; then
  echo "error: smoke_ws.mjs failed; ws log:" >&2
  cat "${TMP_DIR}/ws.log" >&2 || true
  echo "serve log:" >&2
  cat "${TMP_DIR}/serve.log" >&2 || true
  exit 1
fi
WS_PID=""
cat "${TMP_DIR}/ws.log" || true

echo "OK: smoke passed"
