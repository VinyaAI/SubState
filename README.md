# SubState

SubState is an open-source, streaming API and real-time database. It's built
for apps where the data is constantly changing, but each user only needs
updates to what they subscribed to (e.g., Uber, Robinhood, FedEx, DraftKings,
etc). Instead of polling the database and pushing a different set of changes
to every user, SubState keeps a live database and syncs each subscription as
the data changes.

**[Live rideshare demo](https://vinyaai.github.io/substate-demo/)** — auto-running command center simulation.

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

![Current Database State](images/CDS.png)

The CDS is an in-memory copy of your data. SubState pulls from all of your
data sources and merges them into one central reference copy.

Take Uber as an example. One `driver` record can pull identity and status
from Postgres, vehicle fields from MySQL, profile fields from Mongo, and
live `location` from Kafka. The CDS is the live map of those drivers:

```text
cds {
  driver:12 {
    id:           "12"
    name:         "Mei Chen"           // Postgres
    status:       "available"          // Postgres
    region:       "chicago"            // Postgres
    assigned_job: null                 // Postgres
    model:        "X"                  // MySQL
    pickup:       "priority"           // MySQL
    rating:       4.92                 // Mongo
    vehicle:      { color: "black", plate: "IL-4K91" }  // Mongo
    location:     { lat: 41.882, lng: -87.629 }  // Kafka
    heading:      92                   // Kafka
    speed_mph:    14                   // Kafka
  }
  driver:31 {
    id:           "31"
    name:         "Jordan Hale"        // Postgres
    status:       "available"          // Postgres
    region:       "chicago"            // Postgres
    assigned_job: null                 // Postgres
    model:        "XL"                 // MySQL
    pickup:       "priority"           // MySQL
    rating:       4.81                 // Mongo
    vehicle:      { color: "white", plate: "IL-9M22" }  // Mongo
    location:     { lat: 41.891, lng: -87.620 }  // Kafka
    heading:      18                   // Kafka
    speed_mph:    8                    // Kafka
  }
  driver:57 {
    id:           "57"
    name:         "Sam Okonkwo"        // Postgres
    status:       "available"          // Postgres
    region:       "chicago"            // Postgres
    assigned_job: null                 // Postgres
    model:        "XL"                 // MySQL
    pickup:       "priority"           // MySQL
    rating:       4.88                 // Mongo
    vehicle:      { color: "gray", plate: "IL-2T14" }  // Mongo
    location:     { lat: 41.875, lng: -87.641 }  // Kafka
    heading:      270                  // Kafka
    speed_mph:    21                   // Kafka
  }
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

![Subscription Index](images/SubscriptionIndex.png)

The Subscription Index keeps track of every open subscription that a user
has. When any record in the CDS changes (e.g., new driver becomes
available), SubState uses this list to find which subscriptions needs to be
updated.

Back to Uber: if the CDS updates `driver:31` in Chicago, the index looks
at who is waiting for a ride (or dispatching) in Chicago so they can get
that change. Once the rider is matched, they can unsubscribe and the
Subscription Index will be updated.

Here is what that Uber index might look like — two riders and one
dispatcher, each with a different open query:

```text
subscription_index {
  sub:rider_17 {
    user:      "alex"
    type:      "driver"
    where:     { region: "chicago", model: "XL", pickup: "priority", status: "available" }
    depends_on: [region, model, pickup, status]
  }
  sub:rider_44 {
    user:      "priya"
    type:      "driver"
    where:     { region: "nashville", status: "available" }
    depends_on: [region, status]
  }
  sub:dispatcher_chicago {
    user:      "ops"
    type:      "driver"
    where:     { region: "chicago" }
    depends_on: [region]
  }
}
```

If `driver:31` (Chicago, XL, available) changes `status` to `busy`:

```text
CDS change:  driver:31.status  available → busy

selected:
  sub:rider_17             // query uses status, region, model
  sub:dispatcher_chicago   // still a Chicago driver; their view may update
skipped:
  sub:rider_44             // Nashville only
```

If only `location` changes on `driver:31`, `sub:rider_17` is skipped — that
query does not use `location`. `sub:dispatcher_chicago` is skipped for the
same reason unless its filter also depends on that field.

### 3. User State

![User State](images/User_State.png)

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
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [LICENSE](LICENSE) (Apache-2.0)
- [CHANGELOG.md](CHANGELOG.md)

## Learn more

- [Live rideshare demo](https://vinyaai.github.io/substate-demo/) — auto-running command center simulation
- [SubState.md](SubState.md) — long-term vision and design notes
- [docs/schema.md](docs/schema.md) — sync schema reference
- [docs/api.md](docs/api.md) — `/v1` surface and stability notes
