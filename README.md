# SubState

Open-source real-time state sync engine. Run it as a **sidecar** next to your
databases; apps subscribe over WebSocket and push non-DB fields via HTTP ingest.

## Quick start (company-shaped)

```bash
# Self-contained Postgres + SubState
docker compose up --build

# Health
curl http://127.0.0.1:8080/health

# Smoke: subscribe + ingest + delta (Node 18+)
./scripts/smoke.sh
```

See [deploy/compose/README.md](deploy/compose/README.md) for overrides (external `DATABASE_URL`, custom schema).

## Local binary

```bash
# from repo root — requires DATABASE_URL + SCHEMA_PATH (see components/cli/.env.example)
cargo run -p substate-cli -- serve   # headless sidecar
cargo run -p substate-cli -- shell   # interactive cds> debug REPL
```

## TypeScript client

```bash
cd clients/typescript && npm install && npm run build
```

Package: [`@substate/client`](clients/typescript) — `connect`, `subscribe`, `resume`, `ingest`.

## Architecture (short)

Postgres / HTTP (and later Kafka) → CDS (schema-aware merge) → Subscription Index
→ User State → ordered deltas → `/v1/sync`.

Handwritten sync schema: [`schema.yaml`](schema.yaml) (or Compose demo under
`deploy/compose/schema.yaml`).
