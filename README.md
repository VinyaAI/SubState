# SubState

**0.1.0-alpha** — Apache-2.0. SubState sits next to your systems, keeps a live
in-memory copy of the entities you care about, and pushes changes to apps over
WebSocket.

Subscribe to a filtered live view of your data and get updates when it changes —
without your app polling the database itself.

> **`/v1` may break** in alpha. Pin a commit for anything beyond local
> experiments. See [CHANGELOG.md](CHANGELOG.md) and [docs/api.md](docs/api.md).

| I want… | Do this |
| --- | --- |
| Get running | [Quickstart](#quickstart) |
| Write / understand schema | [docs/schema.md](docs/schema.md) |
| Call HTTP / WebSocket | [docs/api.md](docs/api.md) |
| Understand the design | [SubState.md](SubState.md) |

## What is this?

Companies often have data in Postgres (and other places). Different users need
different live subsets of that data.

SubState:

1. Snapshots selected tables (Postgres / MySQL / Mongo), then follows changes
   (Postgres logical CDC when available, otherwise poll)
2. Merges extra fields from Kafka topics or `POST /v1/ingest`
3. Lets clients subscribe over WebSocket and receive a snapshot, then small
   change messages (deltas)

It is **not** a database and not a hosted cloud service. Sources speak one inbox
(`SourceUpdate`); the CDS merges. Run it as a local sidecar.

## Core ideas

```mermaid
flowchart LR
  pg[Your_sources] --> cds[Memory_state_CDS]
  kafka[Kafka_or_HTTP] --> cds
  cds --> idx[Who_cares]
  idx --> us[Per_subscriber_view]
  us --> deltas[Deltas]
  deltas --> ws[App_WebSocket]
```

| Term | Plain meaning |
| --- | --- |
| **CDS** | In-memory “what’s true now” for your entities |
| **Schema** | YAML that maps physical tables/columns → logical names |
| **Subscribe** | Ask for a filtered live view over WebSocket |
| **Ingest** | Push source-owned fields with `POST /v1/ingest` |
| **Delta** | A small change: `add`, `update`, or `remove` |

Subscriptions query SubState’s memory (CDS), **not** the database directly.

## Quickstart

You’ll need [Rust](https://rustup.rs/) and a database URL you control.

```bash
cp components/cli/.env.example .env
# Edit .env:
#   DATABASE_URL=postgresql://user:password@localhost:5432/mydb
#   SCHEMA_PATH=./schema.yaml

# Scan sources and write schema.yaml
cargo run -p substate-cli -- init
# Or accept all proposals:
# cargo run -p substate-cli -- init --defaults

cargo run -p substate-cli -- serve
curl http://127.0.0.1:8080/health
# {"status":"ok"}
```

| Env | Meaning |
| --- | --- |
| `DATABASE_URL` | Postgres URL for sources / `init` scan |
| `SCHEMA_PATH` | **Required for serve/shell** |
| `CDS_SCHEMA` | Postgres schema to read (default: `public`) |
| `CDS_POLL_MS` | Poll interval if CDC unavailable (default: `2000`) |
| `POSTGRES_FOLLOW` | `auto` (default), `cdc`, or `poll` |
| `KAFKA_BROKERS` | When the schema has a `kafka` source |
| `MYSQL_URL` / `MONGO_URL` | When the schema has `mysql` / `mongodb` sources |
| `BIND_ADDR` | Listen address (default: `127.0.0.1:8080`) |
| `SUBSTATE_API_KEY` | Optional shared secret for `/v1/*` |

More options: [components/cli/.env.example](components/cli/.env.example).

Schema details: [docs/schema.md](docs/schema.md). API details: [docs/api.md](docs/api.md).

## HTTP / WebSocket (summary)

When `SUBSTATE_API_KEY` is set, `/v1/*` requires `Authorization: Bearer` or
`x-api-key`. `/health` stays open.

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/cds
curl -s http://127.0.0.1:8080/v1/ingest -H 'content-type: application/json' -d '{...}'
# WebSocket: ws://127.0.0.1:8080/v1/sync
```

Full message shapes and filter grammar: [docs/api.md](docs/api.md).

## Optional extras

### TypeScript client

```bash
cd clients/typescript && npm install && npm run build
```

See [clients/typescript/README.md](clients/typescript/README.md).

### Reference dispatcher map

```bash
cd examples/dispatcher-map && npm install && npm run dev
```

See [examples/dispatcher-map/README.md](examples/dispatcher-map/README.md).

### Debug shell

```bash
cargo run -p substate-cli -- shell
```

### Docker image

[Dockerfile](Dockerfile) builds the `substate` sidecar binary. Mount your schema
and pass `DATABASE_URL` / `SCHEMA_PATH` at runtime.

## Known limits

| Today | Not in this OSS tree |
| --- | --- |
| Postgres / MySQL / Mongo poll; Postgres CDC | Oracle; Redis / MQTT |
| Kafka JSON + Debezium unwrap; HTTP ingest | Full Schema Registry / Avro pipeline |
| Disk snapshot of CDS + resume history | Clustering / HA |
| Range + `$or` / `$and` filters | Spatial indexes; multi-hop joins |
| ~500 deltas per subscription, then reset + snapshot | Unlimited / durable resume |
| WS backpressure → `reset` + snapshot | Ack-as-credit window |
| Shared-secret `SUBSTATE_API_KEY` | SSO / SCIM / multi-tenant cloud |
| Single process sidecar | Hosted control plane, SOC 2, private link |

## Contributing & security

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [SECURITY.md](SECURITY.md)
- [LICENSE](LICENSE) (Apache-2.0)
- [CHANGELOG.md](CHANGELOG.md)

## Learn more

- [SubState.md](SubState.md) — architecture and long-term vision
- [docs/schema.md](docs/schema.md) — sync schema reference
- [docs/api.md](docs/api.md) — `/v1` surface and stability notes
