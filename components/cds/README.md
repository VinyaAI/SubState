# CDS — Current Database State

In-memory store of **what SubState currently believes is true** for each logical
entity. This crate is the snapshot, not the subscription engine.

Subscriptions, deltas, WebSockets, and source adapters do **not** live here.
They read or write through this map.

```
Postgres / Kafka / HTTP ingest
            │
            ▼
     SourceUpdate
            │
            ▼
           CDS          ← this crate
            │
         Change
            │
            ▼
   engine → User State → deltas → clients
```

## When to open this folder

Edit `cds` when the question is about **merged current state**:

- How do two sources share one entity without wiping each other?
- How is a stale GPS/HTTP update rejected?
- What does `GET /v1/cds` actually return?
- How is an entity identity string built from a Postgres row?

Do **not** start here for:

| Need | Go to |
| --- | --- |
| Who owns a field / schema YAML | `components/schema` |
| Applying a source update *and* notifying subscribers | `components/engine` |
| Postgres snapshot, CDC, poll | `components/postgres_source` |
| Kafka consume / sample | `components/kafka_source` |
| Inbox `SourceEvent` enum | `components/source` |
| Filter matching / “who cares?” | `components/subscription_index` |
| Per-subscription membership (ADD/UPDATE/REMOVE) | `components/user_state` |
| Sequenced wire deltas / resume history | `components/delta` |
| HTTP, WebSocket, `substate serve` | `components/cli` |

## Layout

One library crate. There is no binary and no submodules.

```
components/cds/
  Cargo.toml          # crate name: cds
  src/lib.rs          # entire implementation + unit tests
```

Dependencies: `serde`, `serde_json`. No Tokio, no SQL, no schema crate.
Authority filtering is passed in as an `allowed_fields` list; the caller
(usually `engine`) gets that list from `SyncSchema`.

## What is stored

The CDS is a `HashMap<EntityId, EntityState>` plus a small inspection catalog.

### `EntityId`

One row in the map.

- `entity_type` — logical name from the sync schema (`"driver"`), **not** the
  Postgres table name (`"drivers"`).
- `id` — identity as a string (`"728"`). Built by `entity_id()` for Postgres
  rows; Kafka/HTTP supply it from the payload / request.

### `EntityState`

Current values for that entity.

- `fields` — `serde_json` object. Values are whatever the source sent
  (string, number, nested object such as `{ "lat", "lng" }`).
- `field_meta` — per-field `FieldMeta { source, version }`. `source` is the
  schema source **id** that last won (e.g. `"postgres"`, `"gps"`). `version`
  is source-local, never compared across sources.

Postgres can update `name` without touching `location`. Kafka can update
`location` without touching `name`. That is the whole point of per-field meta.

### `SourceUpdate`

What a source proposes. Adapters never write `EntityState` directly for live
traffic; they emit this patch.

| Field | Meaning |
| --- | --- |
| `source` | Source id from the schema (`"postgres"`, `"gps"`, `"http"`) |
| `entity_type` | Logical entity |
| `id` | Entity identity string |
| `fields` | Proposed field values |
| `versions` | Optional per-field versions. Missing key → `stored+1` or `1` |

### `Change` / `ChangeKind`

What actually happened inside the map after a write. `None` from a write
method means a no-op (stale, unauthorized, or identical).

- `Insert` — entity did not exist; now it does. `state` is the new row.
- `Update { fields }` — entity existed; listed field **values** changed.
  `state` is the full merged row after the write.
- `Delete` — entity removed. `state` is the last known row (so fan-out can
  still see what disappeared).

The engine fans each `Change` into the subscription index. CDS itself does
not know who is subscribed.

### Catalog (`Catalog`, `TableCatalog`, `SkippedTable`)

Inspection metadata for the debug shell (`tables`) and `GET /v1/cds`. It is
**not** the sync schema.

- `schema` — usually the Postgres schema name used at boot (`public`), or a
  label when there is no Postgres source.
- `tables` — loaded logical types: name, identity field(s), column list,
  `row_count`.
- `skipped` — types we refused to load (missing table, no primary key, …).

`has_entity_type` looks at this catalog, not at whether any rows exist.

## Write path (the important one)

### `apply_source_update(update, allowed_fields)`

This is the merge used in production: Postgres bootstrap, CDC, poll, Kafka,
and `POST /v1/ingest` all end here (via the engine, except bootstrap which
calls CDS directly).

Rules, in order:

1. **Authority filter.** A field is ignored unless its name is in
   `allowed_fields`. The engine fills that list from
   `schema.fields_owned_by(entity, source)`. CDS does not load YAML.
2. **Version.** If the entity already has `field_meta` for that field and
   `new_version <= stored_version`, skip it (stale).
3. **Default version.** If the update omitted a version: use `stored+1` when
   meta exists, otherwise `1`. So a poller that does not send versions still
   advances.
4. **Value vs meta.** If the JSON value is unchanged **and** the version did
   not advance, skip. If the value is unchanged but the version *did* advance,
   store the new meta and emit **no** `Change` (version-only bump).
5. **Partial merge.** Accepted fields are written onto the existing row.
   Fields owned by other sources stay put.
6. **Insert vs update.** No existing row → `Insert` and bump catalog
   `row_count`. Existing row with at least one value change → `Update` with
   the sorted list of changed names.

Wrong-source fields never error here; they are dropped. The engine *does*
error if the caller included a field it does not own, before calling CDS.

### `upsert` / `insert` / `remove`

- `insert` — put a full `EntityState` with no `Change`. Tests and helpers.
- `upsert` — replace the **entire** row. Emits Insert/Update/noop. Does not
  preserve other sources’ fields. Prefer `apply_source_update` for live
  merges. Used as a legacy full-row path.
- `remove` — delete the entity, decrement `row_count`, return `Delete` with
  the last state. Used for CDC deletes and poll reconcile (row vanished
  upstream).

## Read path

| Method | Use |
| --- | --- |
| `get(entity_type, id)` | One entity (`GET /v1/cds/:entity/:id`, shell `show`) |
| `list(entity_type, limit)` | Sorted by id; returns `(total, page)` |
| `iter_type(entity_type)` | Full scan of one type (subscribe snapshot) |
| `ids_for_type(entity_type)` | Identity list (poll reconcile: drop missing ids) |
| `entity_count()` | All entities in memory |
| `catalog()` | Inspection catalog |

Nothing here applies subscription filters. `engine.subscribe` walks
`iter_type` and asks `subscription_index` whether each row matches.

## `entity_id(primary_key, row)`

Helper for Postgres adapters (not used by Kafka/HTTP).

- One PK column → the raw value as a string (`728`).
- Several PK columns → `col=value|col=value`.

JSON strings stay unquoted; numbers/bools/null stringify. The live schema
requires a single-column identity, but this helper still supports composites.

## Who writes and who reads

```
postgres_source::snapshot
    Cds::new → add_table / skip → apply_source_update   (boot, no fan-out)

source adapters (postgres follow, kafka, HTTP ingest)
    SourceEvent::Upsert(SourceUpdate)
        → cli/dispatch → hub → engine.apply_source_update
            → schema authority check
            → cds.apply_source_update
            → fanout Change → user_state → delta

SourceEvent::Delete / Reconcile
        → engine.remove_entity → cds.remove
```

Reads: `cli` hub (`GET /v1/cds`), debug shell, `engine.subscribe` (initial
User State).

## What this crate does not do

- Persist across process restart. Memory only. Postgres fields are
  re-snapshotted on boot; Kafka/HTTP fields are gone until the next message.
- Validate the sync schema or reject unknown sources. That is `schema` +
  `engine`.
- Coalesce latest-value fields on a timer (`flush_ms` is schema metadata;
  CDS stores the latest accepted value immediately).
- Assign subscription sequence numbers.
- Talk to the network or a database.

## Tests

All in `src/lib.rs` under `#[cfg(test)]`:

- `entity_id` single vs composite keys
- `insert` / `get` / `list` sort order
- `upsert` insert, noop, update, remove, `row_count`
- Multi-source merge: Postgres + HTTP, stale version rejected, newer
  accepted, wrong-source field ignored

```bash
cargo test -p cds
```
