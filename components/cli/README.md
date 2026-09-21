# CLI — process entrypoint (`substate`)

The binary that starts SubState. This crate wires sources, the engine, and
the network together. It is the sidecar, not the merge logic.

CDS, schema validation, filter matching, and source adapters do **not** live
here. They are called from this process.

```
  .env + schema.yaml
          │
          ▼
     substate serve / shell     ← this crate
          │
    ┌─────┼─────────────────┐
    ▼     ▼                 ▼
  Postgres / Kafka     HTTP/WS
  follow tasks         /v1/cds /v1/sync /v1/ingest
          │                 │
          ▼                 ▼
     SourceEvent        DeliveryHub
          │                 │
          └────────► engine / CDS
```

`substate init` is separate: it only scans sources and writes YAML. It does
not boot CDS or listen on a port.

## When to open this folder

Edit `cli` when the question is about **the running process**:

- How do I start SubState? (`serve`, `shell`, `init`)
- Which env vars are required?
- Where is `/health`, `/v1/cds`, `/v1/sync`, `/v1/ingest` defined?
- How do source tasks get their events into the engine?
- How does the debug REPL talk to the same hub as WebSocket clients?

Do **not** start here for:

| Need | Go to |
| --- | --- |
| YAML load / field authority | `components/schema` |
| Generating a schema from Postgres/Kafka | `components/schema_gen` |
| Merged current state / stale versions | `components/cds` |
| Apply update *and* notify subscribers | `components/engine` |
| Postgres snapshot, CDC, poll | `components/postgres_source` |
| Kafka consume / sample | `components/kafka_source` |
| Inbox `SourceEvent` enum | `components/source` |
| Filter matching / “who cares?” | `components/subscription_index` |
| Per-subscription membership (ADD/UPDATE/REMOVE) | `components/user_state` |
| Sequenced wire deltas / resume history | `components/delta` |

Inside this crate, pick the file first:

| Need | File |
| --- | --- |
| Commands and flags | `src/main.rs` |
| Env → `RuntimeConfig` | `src/config.rs` |
| Dotenv, snapshot, spawn follows + HTTP | `src/boot.rs` |
| `SourceEvent` → hub | `src/dispatch.rs` |
| Subscribe / ingest / CDS inspect / delta broadcast | `src/hub.rs` |
| Axum routes + WebSocket protocol | `src/ws.rs` |
| Interactive `cds>` REPL | `src/shell.rs` |
| `substate init` prompts | `src/init.rs` |
| Env template | repo-root [`.env.example`](../../.env.example) |

## Layout

One binary crate. Crate name `substate-cli`, binary name `substate`.

```
components/cli/
  Cargo.toml          # crate: substate-cli, bin: substate
  src/
    main.rs           # clap: init | serve | shell
    config.rs
    boot.rs
    dispatch.rs
    hub.rs
    ws.rs
    shell.rs
    init.rs
```

## Commands

```bash
cargo run -p substate-cli -- init              # write schema.yaml
cargo run -p substate-cli -- init --defaults   # no prompts
cargo run -p substate-cli -- serve             # headless sidecar
cargo run -p substate-cli -- shell             # same boot, then REPL
```

| Command | What it does |
| --- | --- |
| `init` | Scan Postgres/Kafka, prompt, write YAML. No engine, no HTTP. |
| `serve` | `boot::start()`, then park forever. |
| `shell` | `boot::start()`, then `cds>` on stdin. HTTP/WS still run. |

`init` flags: `--out <path>` (default `SCHEMA_PATH` or `./schema.yaml`),
`--defaults`, `--sample-size` (Kafka messages per topic, default 20).

## Config

Loaded by `boot::load_dotenv()` then `RuntimeConfig::from_env()`.

Dotenv search order: `DOTENV_PATH`, then `./.env`, then
`components/cli/.env`. Already-exported shell vars win.

Template: repo-root [`.env.example`](../../.env.example). Copy to `./.env`.

| Env | Required when | Default |
| --- | --- | --- |
| `SCHEMA_PATH` | `serve` / `shell` | — |
| `DATABASE_URL` | schema has a `postgres` source, or `init` scan | — |
| `MYSQL_URL` | schema has a `mysql` source | — |
| `MONGO_URL` | schema has a `mongodb` source | — |
| `MONGO_DATABASE` | optional with `MONGO_URL` | `substate` |
| `KAFKA_BROKERS` | schema has a `kafka` source, or `init` scan | — |
| `CDS_SCHEMA` | optional | `public` |
| `CDS_POLL_MS` | optional | `2000` |
| `POSTGRES_FOLLOW` | optional | `auto` (`cdc` / `poll`) |
| `BIND_ADDR` | optional | `127.0.0.1:8080` |
| `RUST_LOG` | optional | `info` |

`POSTGRES_FOLLOW=auto` tries logical CDC (`pgoutput` slot `substate`) and
falls back to a full-table poll. `cdc` fails if WAL is not logical.

## Boot (`serve` / `shell`)

`boot::start()` is the whole sidecar:

1. Load YAML via `SyncSchema::load_path`.
2. If any `postgres` source: connect, snapshot into CDS. Else: empty catalog.
3. Wrap CDS + schema in `Engine`, wrap engine in `DeliveryHub`.
4. Spawn `dispatch` on a `SourceEvent` channel.
5. Spawn Postgres follow (CDC or poll) and/or Kafka follow.
6. Spawn Axum on `BIND_ADDR`.

The snapshot writes CDS directly (no subscriber fan-out). Live traffic goes
`Follow → SourceEvent → dispatch → hub → engine`.

## Delivery hub

`DeliveryHub` is the in-process API used by HTTP, WebSocket, the shell, and
dispatch. It holds the engine lock, per-subscription `DeltaHistory`, and a
broadcast of `HubEvent::Delta`.

| Method | Used by |
| --- | --- |
| `apply_and_publish` | ingest + source upserts |
| `remove_and_publish` / `reconcile_and_publish` | CDC delete, poll reconcile |
| `subscribe` / `unsubscribe` / `ack` / `resume_since` | WS + shell |
| `cds_catalog` / `cds_list` / `cds_get` | `GET /v1/cds`, shell inspect |

One WebSocket connection tracks its own subscription ids (`SessionSubs`) and
only forwards deltas for those ids.

## HTTP / WebSocket (`ws.rs`)

`/v1/*` has **no auth**.

| Route | Role |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /v1/cds` | Catalog + rows (`?limit=`, default 20, max 100) |
| `GET /v1/cds/:entity` | One type |
| `GET /v1/cds/:entity/:id` | One entity |
| `POST /v1/ingest` | HTTP source upsert → hub |
| `GET /v1/sync` | WebSocket |

Client messages on `/v1/sync`: `subscribe`, `resume`, `unsubscribe`, `ack`.
Server replies: `subscribed`, `snapshot`, `delta`, `reset`, `error`.
Expired history → `reset` + a fresh snapshot.

## Debug shell (`shell.rs`)

Same hub as the API. Live deltas print as they arrive.

```
help / tables / schema <entity> / show <entity> [n] / get <entity> <id>
subscribe <entity> [key=value…] / subscriptions / state <id> / unsub <id>
```

`subscribe` uses logical entity names and equality filters only.

## Init (`init.rs`)

Prompt loop only. Discovery and YAML assembly live in other crates:

- Postgres catalog → `postgres_source::load_catalog`
- Kafka samples → `kafka_source::discover_topics`
- Drafts / YAML → `schema_gen`

Needs `DATABASE_URL` and/or `KAFKA_BROKERS`. Review the file, then `serve`.

## What this crate does not do

- Merge fields or reject stale versions (`cds`)
- Decide who is subscribed (`subscription_index` + `engine`)
- Talk to Postgres or Kafka except by calling those crates
- Persist CDS or delta history across restart

## Tests

| File | Covers |
| --- | --- |
| `src/ws.rs` | CDS inspect via the hub |
| `src/shell.rs` | Command parse, subscribe, table formatting |
| `src/init.rs` | `--defaults` prompter + generated YAML validates |

```bash
cargo test -p substate-cli
```
