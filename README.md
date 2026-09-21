# SubState

SubState is an open-source, streaming API and real-time database. It's built
for apps where the data is constantly changing, but each user only needs
updates to what they subscribed to (e.g., Uber, Robinhood, FedEx, DraftKings,
etc). Instead of polling the database and pushing a different set of changes
to every user, SubState keeps a live database and syncs each subscription as
the data changes.

![Sources to SubState to subscribers](images/SubState_Intro.png)

## Contents
- [Architecture of SubState](#architecture-of-substate)
- [Quickstart](#quickstart)
- [HTTP / WebSocket](#http--websocket-summary)
- [Optional extras](#optional-extras)
- [Known limits](#known-limits)
- [Contributing](#contributing--security)
- [Learn more](#learn-more)

## Architecture of SubState

SubState can be divided into 3 separate parts.

### 1. CDS (Current Database State)

The CDS is an in-memory copy of your data. SubState pulls from all of your
data sources and merges them into one central reference copy.

For example, a `driver` record might get `name` and `status` from Postgres,
and `location` from a Kafka topic:

```text
driver {
  id:       "728"
  name:     "Alice"              // Postgres
  status:   "available"          // Postgres
  location: { lat: 36.16, lng: -86.78 }  // Kafka
}
```

When a source changes, that copy is updated. The rest of SubState reads the
CDS when it needs data that is current, instead of re-querying your data
sources.

SubState includes connectors for Postgres, MySQL, and Mongo but you can also
send data into the CDS through Kafka or HTTP.

### 2. Subscription Index

A subscription is a request from a user for a specific set of records, and
for updates whenever that set changes. For example, if you call an Uber,
your request might look like:

```text
{
  type: "driver",
  where: { region: "chicago", model: "XL", pickup: "priority", status: "available" }
}
```

Here, the user wants every driver whose region is Chicago, model size is XL,
pickup is immediate and whose status is available. The user also wants to be
told when that set changes (e.g., new drivers become available, a driver
goes out of range, a driver accepts a different ride, etc). In SubState,
this query runs against the CDS, not against your database.

The Subscription Index keeps track of every open subscription that a user
has. When any record in the CDS changes (e.g., new driver becomes
available), SubState uses this list to find which subscriptions needs to be
updated.

For example, if the CDS updates `driver:728` in Chicago, the index looks at
who has subscribed to drivers in Chicago so they can update the change.
Once the user has finished, they can unsubscribe and the Subscription Index
will be updated.

### 3. User State

User State is the mirror image of what data the user currently has.

When a user subscribes, SubState builds the requested data set from the CDS
and sends it to the user (via WebSocket) as a snapshot. After that, any new
changes (only the updates) are sent to the user via small messages (deltas).

Deltas will be of three states:

- `add` — a record entered the set
- `update` — a record in the set changed
- `remove` — a record left the set

For example, the user looking for Uber drivers in Chicago will be sent this
as their initial User State:

```text
user_state {
  driver:12, model: X;
  driver:31, model: XL;
  driver:57, model: XL;
}
```

If `driver:31` accepts a different ride, then they no longer match the
query. The socket will remove `driver:31`. So the User State becomes:

```text
user_state {
  driver:12, model: X
  driver:57, model: XL
}
```

If a new available driver appears in Chicago, they are added. A user
searching in New York is not sent these messages.

## Quickstart

You’ll need [Rust](https://rustup.rs/) and a Postgres URL you control.
This path uses Postgres. Kafka, HTTP ingest, MySQL, and Mongo are optional;
see [Architecture](#architecture-of-substate).

1. Copy the env file and set `DATABASE_URL` and `SCHEMA_PATH`:

```bash
cp .env.example .env
```

```bash
# .env
DATABASE_URL=postgresql://user:password@localhost:5432/mydb
SCHEMA_PATH=./schema.yaml
```

2. Generate `schema.yaml` from your sources (`--defaults` accepts every
   proposal without prompting):

```bash
cargo run -p substate-cli -- init
# cargo run -p substate-cli -- init --defaults
```

3. Start SubState:

```bash
cargo run -p substate-cli -- serve
```

4. Check that it is up:

```bash
curl http://127.0.0.1:8080/health
# {"status":"ok"}
```

### Verify (smoke)

On a machine with Docker, Rust, and **Node 20+**:

```bash
./scripts/smoke.sh
```

This boots Postgres in Docker, runs `substate serve`, checks `/health` + CDS,
then confirms WebSocket subscribe + HTTP ingest delivers a delta. CI runs the
same script.

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

Env template: [.env.example](.env.example). Schema:
[docs/schema.md](docs/schema.md). API: [docs/api.md](docs/api.md).

## HTTP / WebSocket (summary)

When `SUBSTATE_API_KEY` is set, `/v1/*` requires `Authorization: Bearer` or
`x-api-key`. `/health` stays open.

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/v1/cds
curl -s http://127.0.0.1:8080/v1/ingest -H 'content-type: application/json' -d '{...}'
# WebSocket: ws://127.0.0.1:8080/v1/sync
```

Subscribe on `ws://127.0.0.1:8080/v1/sync`. The client gets a snapshot,
then deltas (`add`, `update`, `remove`).

Full message shapes and filter grammar: [docs/api.md](docs/api.md).

## Optional extras

### TypeScript client

```bash
cd clients/typescript && npm install && npm run build
```

See [clients/typescript/README.md](clients/typescript/README.md).

### Reference dispatcher map

A live map of the Architecture example (drivers, filters, snapshot + deltas).

```bash
cd examples/dispatcher-map && npm install && npm run dev
```

See [examples/dispatcher-map/README.md](examples/dispatcher-map/README.md).

### Debug shell

```bash
cargo run -p substate-cli -- shell
```

### Docker image

[Dockerfile](Dockerfile) builds the `substate` binary. Mount your schema
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

- [SubState.md](SubState.md) — long-term vision and design notes
- [docs/schema.md](docs/schema.md) — sync schema reference
- [docs/api.md](docs/api.md) — `/v1` surface and stability notes
