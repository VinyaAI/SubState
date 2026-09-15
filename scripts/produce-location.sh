#!/usr/bin/env bash
# Produce one location message onto the Compose Redpanda topic.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"
SEQ="${1:-$(date +%s)}"
printf '%s\n' "{\"driver_id\":\"1\",\"location\":{\"lat\":36.17,\"lng\":-86.79},\"sequence\":${SEQ}}" \
  | docker compose exec -T redpanda rpk topic produce driver-locations --brokers=redpanda:9092
