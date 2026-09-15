# SubState

**Early prototype.** SubState sits next to your systems, keeps a live in-memory
copy of the entities you care about, and pushes changes to apps over WebSocket.

Think: subscribe to “available drivers” and get updates when data changes —
without your app polling the database itself.

| I want… | Do this |
| --- | --- |
| Just see it work | [First run](#first-run-2-minutes) (Docker Compose) |
| Point it at my database | [Connect your own Postgres](#connect-your-own-postgres) |
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

## First run (2 minutes)

**You need:** [Docker](https://docs.docker.com/get-docker/) and Node.js 18+.

From the repo root:

```bash
docker compose up --build
```

Leave that terminal running. In a second terminal:

```bash
curl http://127.0.0.1:8080/health
./scripts/smoke.sh
```

**Success looks like:**

- Health returns `{"status":"ok"}`
- Smoke prints that it subscribed, applied a Postgres change, received a Kafka
  location delta
- Final line: `smoke passed`

**If it fails:** Is Docker Desktop running? Is port `8080` free? Is Node 18+ on
your PATH?

This path starts a toy Postgres with sample data for you. You do **not** need
Rust, a `.env` file, or your own database yet.

## What just happened?

Compose started Postgres (logical WAL), Redpanda (Kafka API), a location
producer, and the SubState sidecar.

1. Postgres was seeded with a `drivers` table — Alice, Bob, and Carol
2. SubState read those rows into memory (the CDS — “what’s true now”)
3. The smoke script subscribed: *drivers where region = nashville*
4. It got a **snapshot** (Alice and Bob)
5. It `UPDATE`d Alice’s name in Postgres; CDC emitted a **delta**
6. It produced a `location` on the Kafka topic; that merged onto the same driver

That’s the product loop: load state → subscribe → source change → delta.

HTTP ingest is still there for sources you do not have an adapter for. The root
[schema.yaml](schema.yaml) keeps `location` on `http` for runs without a broker.

Demo data lives in [deploy/compose/init.sql](deploy/compose/init.sql). The demo
schema is [deploy/compose/schema.yaml](deploy/compose/schema.yaml) (table
`drivers`). The root [schema.yaml](schema.yaml) is a separate template for when
you connect your own database.

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

## Connect your own Postgres

Do this **after** Compose works. You’ll need [Rust](https://rustup.rs/), a
Postgres URL you control, and a schema that matches your columns.

```bash
cp components/cli/.env.example .env
# Edit .env:
#   DATABASE_URL=postgresql://user:password@localhost:5432/mydb
#   SCHEMA_PATH=./schema.yaml
# Edit schema.yaml so table + field names match your database

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

## Schema (when you’re ready)

Clients subscribe and ingest by **logical name** (e.g. `driver`), never by the
physical table name.

Start from the demo shape (`drivers`), then retarget columns for your DB. Root
[schema.yaml](schema.yaml) is a template (its example uses table `Customers`).

```yaml
entities:
  driver:                          # name clients use
    identity:
      field: id
    sources:
      postgres:
        type: postgres
        table: drivers             # physical table name
      gps:
        type: kafka
        topic: driver-locations
        entity_key: driver_id
      http:
        type: http
    fields:
      id:
        source: postgres
        mode: transactional
      name:
        source: postgres
        mode: transactional
      status:
        source: postgres
        mode: transactional
      location:                    # Kafka in Compose; http in the root template
        source: gps
        mode: latest_value
        ordering: sequence
```

Rules in short:

- Identity field must be listed under `fields`
- Each field’s `source` must exist under `sources`
- Postgres tables need a primary key, or that entity is skipped
- Only fields owned by `postgres` are loaded from the table
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
curl http://127.0.0.1:8080/v1/cds/driver
curl http://127.0.0.1:8080/v1/cds/driver/1
```

`GET /v1/cds` is the merged snapshot (Postgres + Kafka/HTTP fields). Add `?limit=50`
(default 20, max 100) to change how many rows per entity type are included.

### Ingest (HTTP)

Push fields owned by the named source (usually `http`):

```bash
curl -s http://127.0.0.1:8080/v1/ingest \
  -H 'content-type: application/json' \
  -d '{
    "source": "http",
    "entity_type": "driver",
    "id": "1",
    "fields": { "location": { "lat": 36.16, "lng": -86.78 } },
    "versions": { "location": 1 }
  }'
```

### Sync (WebSocket)

Connect to `ws://127.0.0.1:8080/v1/sync`, then send:

```json
{ "type": "subscribe", "entity_type": "driver", "where": { "status": "available" } }
```

You’ll get `subscribed`, then a `snapshot`, then `delta` messages as data
changes. Other client messages: `resume`, `unsubscribe`, `ack`.

Full worked example: [scripts/smoke.mjs](scripts/smoke.mjs).

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

Try `help`, `tables`, `show driver`, `subscribe driver status=available`.

### Compose against your DB

You can still use the Docker image but point it at an external database — see
[deploy/compose/README.md](deploy/compose/README.md).

## Known limits

This is a prototype — good for demos and learning, not production.

| Today | Not yet |
| --- | --- |
| Postgres snapshot + CDC (`pgoutput`) or poll fallback | MySQL / Mongo / Oracle adapters |
| Kafka JSON consumer + HTTP ingest | Schema Registry / Avro / Debezium envelope |
| Handwritten `schema.yaml` | Schema discovery / generation |
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
- [deploy/compose/README.md](deploy/compose/README.md) — Compose demo details
- [clients/typescript/README.md](clients/typescript/README.md) — local TS client
