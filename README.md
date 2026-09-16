# SubState

**Early prototype.** SubState sits next to your systems, keeps a live in-memory
copy of the entities you care about, and pushes changes to apps over WebSocket.

Think: subscribe to a filtered live view of your data and get updates when it
changes — without your app polling the database itself.

| I want… | Do this |
| --- | --- |
| Run against my database | [Connect your Postgres](#connect-your-postgres) |
| Understand the schema | [Schema](#schema) |
| Understand the full design | [SubState.md](SubState.md) |

## What is this?

Companies often have data in Postgres (and other places). Different users need
different live subsets of that data.

SubState:

1. Snapshots selected Postgres tables, then follows changes (logical CDC when
   `wal_level=logical`, otherwise a table poll)
2. Merges extra fields from Kafka topics or `POST /v1/ingest`
3. Lets clients subscribe over WebSocket and receive a snapshot, then small
   change messages (deltas)

It is **not** a database and not a hosted cloud service. Sources speak one inbox
(`SourceUpdate`); the CDS merges. This repo is a working local prototype.

## Core ideas (simple)

```mermaid
flowchart LR
  pg[Your_Postgres] --> cds[Memory_state_CDS]
  kafka[Kafka_topic] --> cds
  ingest[HTTP_ingest] --> cds
  cds --> idx[Who_cares]
  idx --> us[Per_subscriber_view]
  us --> deltas[Deltas]
  deltas --> ws[App_WebSocket]
```

| Term | Plain meaning |
| --- | --- |
| **CDS** | SubState’s in-memory “what’s true now” for your entities |
| **Schema** | A YAML file that maps Postgres tables/columns → logical names clients use |
| **Subscribe** | Ask for a filtered live view over WebSocket |
| **Ingest** | Push any source-owned fields with `POST /v1/ingest` (universal fallback) |
| **Delta** | A small change: `add`, `update`, or `remove` |

Important: subscriptions query SubState’s memory (CDS), **not** Postgres
directly. That avoids races and keeps reconnects off your production database.

## Connect your Postgres

You’ll need [Rust](https://rustup.rs/), a Postgres URL you control, and a schema
that matches your columns.

```bash
cp components/cli/.env.example .env
cp schema.template.yaml schema.yaml
# Edit schema.yaml: replace every <placeholder>
# Edit .env:
#   DATABASE_URL=postgresql://user:password@localhost:5432/mydb
#   SCHEMA_PATH=./schema.yaml

cargo run -p substate-cli -- serve
curl http://127.0.0.1:8080/health
```

| Env | Meaning |
| --- | --- |
| `DATABASE_URL` | **Required.** Your Postgres connection string |
| `SCHEMA_PATH` | **Required.** Path to your sync schema YAML |
| `CDS_SCHEMA` | Postgres schema to read (default: `public`) |
| `CDS_POLL_MS` | Poll interval if CDC is unavailable (default: `2000`) |
| `POSTGRES_FOLLOW` | `auto` (default), `cdc`, or `poll` |
| `KAFKA_BROKERS` | Required when the schema has a `kafka` source |
| `BIND_ADDR` | Listen address (default: `127.0.0.1:8080`) |

`POSTGRES_FOLLOW=auto` tries logical decoding (`pgoutput` slot `substate`) and
falls back to a full-table poll if `wal_level` is not `logical`.

More options: [components/cli/.env.example](components/cli/.env.example).

## Schema

Clients subscribe and ingest by **logical name** (the entity key you choose),
never by the physical table name.

Copy [schema.template.yaml](schema.template.yaml) to `schema.yaml` (gitignored).
Bare keys (`entities`, `identity`, `type`, `mode`, …) are required vocabulary.
`<angle brackets>` are names you choose — the same token must match everywhere
it is referenced (`identity.field` ↔ `fields`, `fields.*.source` ↔ `sources`).

```yaml
entities:
  <entity>:                          # name clients use
    identity:
      field: <id_field>              # must also be a key under fields
    sources:
      <pg_source>:                   # nickname; does not have to be "postgres"
        type: postgres               # enum: postgres | kafka | http
        table: <postgres_table>      # physical table name
      <stream_source>:
        type: kafka
        topic: <kafka_topic>
        entity_key: <payload_id>
      <http_source>:
        type: http
    fields:
      <id_field>:
        source: <pg_source>
        mode: transactional          # enum: transactional | latest_value
      <field>:
        source: <pg_source>
        mode: transactional
      <live_field>:
        source: <stream_source>
        mode: latest_value
        ordering: <ordering_field>
```

Rules in short:

- Identity field must be listed under `fields`
- Each field’s `source` must exist under `sources`
- Postgres tables need a primary key, or that entity is skipped
- Only fields owned by a `postgres` source are loaded from the table
- Kafka/HTTP fields appear when a message or ingest arrives and are lost if
  SubState restarts
- Supported source types: `postgres`, `kafka`, `http`
- Automatic schema discovery is not built yet (see
  [Schema_Generation.md](Schema_Generation.md))

## HTTP / WebSocket

Four endpoints. For local work, bind to localhost. `/v1/*` has **no auth**.

### Health

```bash
curl http://127.0.0.1:8080/health
# {"status":"ok"}
```

### Current state (CDS)

```bash
curl http://127.0.0.1:8080/v1/cds
curl http://127.0.0.1:8080/v1/cds/<entity>
curl http://127.0.0.1:8080/v1/cds/<entity>/1
```

`GET /v1/cds` is the merged snapshot (Postgres + Kafka/HTTP fields). Add `?limit=50`
(default 20, max 100) to change how many rows per entity type are included.

### Ingest (HTTP)

Push fields owned by the named source (usually your `http` source id):

```bash
curl -s http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' \
  -d '{
    "source": "<http_source>",
    "entity_type": "<entity>",
    "id": "1",
    "fields": { "<live_field>": { "lat": 36.16, "lng": -86.78 } },
    "versions": { "<live_field>": 1 }
  }'
```

### Sync (WebSocket)

Connect to `ws://127.0.0.1:8080/v1/sync`, then send:

```json
{ "type": "subscribe", "entity_type": "<entity>", "where": { "<field>": "value" } }
```

You’ll get `subscribed`, then a `snapshot`, then `delta` messages as data
changes. Other client messages: `resume`, `unsubscribe`, `ack`.

## Optional extras

### TypeScript client

Local package (build it yourself):

```bash
cd clients/typescript && npm install && npm run build
```

See [clients/typescript/README.md](clients/typescript/README.md).

### Debug shell

Interactive REPL with the same engine boot (needs Rust + `.env`):

```bash
cargo run -p substate-cli -- shell
```

Try `help`, `tables`, `show <entity>`, `subscribe <entity> <field>=value`.

### Docker image

[Dockerfile](Dockerfile) builds the `substate` sidecar binary. Mount your own
schema and pass `DATABASE_URL` / `SCHEMA_PATH` at runtime.

## Known limits

This is a prototype — good for learning and local experiments, not production.

| Today | Not yet |
| --- | --- |
| Postgres snapshot + CDC (`pgoutput`) or poll fallback | MySQL / Mongo / Oracle adapters |
| Kafka JSON consumer + HTTP ingest | Schema Registry / Avro / Debezium envelope |
| Handwritten schema YAML | Schema discovery / generation |
| In-memory CDS, subscriptions, history | Persistence across restart |
| Equality `where` (AND only) | Ranges, spatial filters, OR, joins |
| Last 500 deltas per subscription; then reset | Durable / unlimited resume history |
| Ack does not pause delivery | Backpressure |
| No auth on `/v1/*` | Auth, multi-tenant |
| Single process | Clustering / scale-out |

HTTP-owned fields and resume history do not survive a restart. Postgres fields
are loaded again on boot.

## Learn more

- [SubState.md](SubState.md) — architecture and long-term vision
- [Schema_Generation.md](Schema_Generation.md) — future schema discovery
- [clients/typescript/README.md](clients/typescript/README.md) — local TS client
