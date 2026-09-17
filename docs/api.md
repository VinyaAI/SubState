# HTTP / WebSocket API

**Status:** `0.1.0-alpha`. The paths below are the intended public surface, but
**`/v1` may break** between commits without a major version bump. Pin a commit
for anything beyond local experiments.

When `SUBSTATE_API_KEY` is set, every `/v1/*` route requires
`Authorization: Bearer <key>` or `x-api-key: <key>`. `/health` stays open.
`/metrics` is open by default (same key optional depending on deploy).

Base URL examples use `http://127.0.0.1:8080` / `ws://127.0.0.1:8080`.

## Stability note

| Endpoint | Intent |
| --- | --- |
| `GET /health` | Liveness; stable for probes |
| `GET /metrics` | Prometheus text; metric names may grow |
| `GET /v1/cds` | Merged CDS snapshot; shape may tighten |
| `POST /v1/ingest` | Source-owned field writes |
| `WS /v1/sync` | Subscribe / resume / ack / unsubscribe |

Message types and filter grammar under `/v1/sync` can change in alpha. See
[CHANGELOG.md](../CHANGELOG.md).

---

### `GET /health`

```bash
curl http://127.0.0.1:8080/health
# {"status":"ok"}
```

### `GET /metrics`

Prometheus exposition of connection, ingest, CDC/poll, and coalesce counters.

### `GET /v1/cds`

Current merged state (Postgres/MySQL/Mongo + Kafka/HTTP fields).

```bash
curl http://127.0.0.1:8080/v1/cds
curl http://127.0.0.1:8080/v1/cds/<entity>
curl http://127.0.0.1:8080/v1/cds/<entity>/<id>
```

Query: `?limit=50` (default 20, max 100) caps rows per entity type on the
root listing.

### `POST /v1/ingest`

Push fields owned by the named source (usually an `http` source id):

```bash
curl -s http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' \
  -H "Authorization: Bearer $SUBSTATE_API_KEY" \
  -d '{
    "source": "<http_source>",
    "entity_type": "<entity>",
    "id": "1",
    "fields": { "<live_field>": { "lat": 36.16, "lng": -86.78 } },
    "versions": { "<live_field>": 1 }
  }'
```

### `WS /v1/sync`

Connect, then send JSON messages.

**Subscribe** — filter document supports equality, comparisons, and nesting:

```json
{
  "type": "subscribe",
  "entity_type": "<entity>",
  "where": {
    "$or": [
      { "status": "available" },
      { "status": { "eq": "busy" } }
    ],
    "updated_at": { "gt": "2026-01-01T00:00:00Z" }
  }
}
```

Comparisons: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`. Combine with `$and` / `$or`.

Server replies: `subscribed`, then `snapshot`, then `delta` (`add` / `update` /
`remove`). Other client messages: `resume` (with `resume_after` seq),
`unsubscribe`, `ack`.

If the WebSocket send buffer backs up, the server stops broadcasting to that
session and sends `reset` plus a fresh snapshot on recovery. `ack` advances the
resume cursor; it is not a credit window yet.

## TypeScript client

See [clients/typescript/README.md](../clients/typescript/README.md) for
`connect` / `cds` / `ingest` with `apiKey`, reconnect, and `resume_after`.
