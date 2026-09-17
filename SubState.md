SubState Architecture

Open-Source Real-Time State Synchronization Engine

Status: Working architecture
Date: September 2026

1. What SubState Is

SubState is an open-source engine for keeping each user's relevant
application state synchronized with continuously changing backend state.

Companies may have data spread across databases, event streams, caches,
APIs, and telemetry systems. Different users need different live subsets
of that data.

Maintain a current unified view of operational state, determine who
cares when it changes, and keep each user's state synchronized.

SubState does not replace Postgres, Kafka, Redis, or other systems of
record.

2. High-Level Architecture

      Database     Database       Kafka / Events
          │            │               │
          └────────────┴───────┬───────┘
                               ▼
                  ┌────────────────────────┐
                  │ Current Database State │
                  │         (CDS)          │
                  └───────────┬────────────┘
                              │ state changes
                              ▼
                  ┌────────────────────────┐
                  │   Subscription Index   │
                  └───────────┬────────────┘
                              │ affected subscriptions
                   ┌──────────┼──────────┐
                   ▼          ▼          ▼
              User State  User State  User State
                   │          │          │
                   ▼          ▼          ▼
                 User       User       User

The architecture centers on three questions:

CDS: What is true now?
Subscription Index: Who cares about what changed?
User State: What should this user currently have?

3. Sources

SubState connects to existing systems rather than replacing them.

Long-term examples include Postgres, MySQL, MongoDB, Kafka, Redis, MQTT,
HTTP/SDK inputs, APIs, and telemetry streams.

Different sources may contribute fields to the same logical entity:

Postgres
  driver.name
  driver.status
  driver.region
  driver.assigned_job

Kafka / GPS
  driver.location
  driver.heading
  driver.speed

SubState combines these into one current logical entity.

4. Current Database State (CDS)

The Current Database State (CDS) is SubState's current
representation of the live operational state it knows about. It is not
the customer's system of record.

driver:728

name          = Alice
status        = available
region        = nashville
assigned_job  = job:912
location      = 36.16, -86.78

Fields may come from different sources:

name / status / region / job ← Postgres
location                     ← GPS stream

The CDS:

bootstraps state from connected sources;

receives subsequent source updates;

merges fields from heterogeneous sources;

enforces field authority;

rejects stale updates;

handles latest-value fields such as GPS;

emits state changes to the Subscription Index.

The CDS does not invent merge behavior. Its behavior is governed by
SubState's schema.

5. Schema

The schema is the contract that maps physical sources onto logical
entities. A field's `source` is a source id (a key under `sources`),
not a physical backend name.

entities:
  driver:
    identity:
      field: id
    sources:
      postgres:
        type: postgres
        table: drivers
      gps:
        type: kafka
        topic: driver-locations
        entity_key: driver_id
    fields:
      id:
        source: postgres
        mode: transactional

      status:
        source: postgres
        mode: transactional

      region:
        source: postgres
        mode: transactional

      assigned_job:
        source: postgres
        mode: transactional

      location:
        source: gps
        mode: latest_value
        ordering: sequence

It tells SubState which source owns a field, how it is ordered, whether
every transition matters, whether updates can be coalesced, and how
source records map to logical entities.

`substate init` can propose this file from a Postgres catalog and
sampled Kafka topics; you can also write it by hand. Either way the CDS
never guesses merge behavior at runtime.

Discover structure automatically. Infer cautiously. Confirm authority,
identity, and field modes before `serve`.

Field remapping (`column` / `path`), merge-into-existing `init`, `ttl_ms`,
and one-hop relations are implemented. Later work (not in this prototype):
Schema Registry / Avro / Protobuf.

6. Source Authority and Versions

Versions from independent systems are not globally comparable.

status
  source: postgres
  version: 9182

location
  source: gps
  sequence: 552819

The rule is:

Accept an update if it comes from the authoritative source for that
field and its source-local version is newer than the currently stored
version. Otherwise discard it.

For GPS-like data, sequence determines ordering. A timestamp such as
observed_at may describe freshness but should not determine ordering
in the prototype.

7. Transactional vs. Latest-Value State

Transactional state includes status, job assignment, and
availability. Meaningful transitions may need to be preserved.

Latest-value state includes location, heading, and speed.
Intermediate values may be discarded when superseded.

A → B → C → D → E

can become:

E

For the prototype, latest-value fields can use a 100 ms flush interval.

Only deltas actually emitted to a user receive user-stream sequence
numbers. Coalesced internal updates do not consume those numbers.

8. Subscription Index

The Subscription Index stores active subscriptions and determines which
subscriptions may be affected by a CDS change.

sync.subscribe({
  type: "driver",
  where: {
    region: "nashville",
    status: "available"
  }
})

This subscription depends on:

driver.region
driver.status

A location change does not require a membership check for it. A status
change does.

CDS change
driver.status: available → busy
        ↓
Subscription Index
        ↓
subscriptions depending on driver.status
        ↓
candidate subscriptions

The prototype can use coarse field-level dependency indexes. Later
versions may add value-level, range, spatial/H3, and shared-predicate
indexes.

9. Creating a Subscription

Subscriptions evaluate the CDS, not upstream databases directly.

User
  ↓
create subscription
  ↓
Subscription Index
  ↓
query CDS
  ↓
build User State
  ↓
send snapshot

The engine does not issue a fresh Postgres query for each subscription.
This avoids races with incoming changes and prevents reconnects from
repeatedly hitting production databases.

10. User State

User State is the materialized state currently relevant to a
user/subscription.

Subscription:
region = nashville
status = available

User State:
driver:12
driver:31
driver:57

If Driver 31 becomes busy:

CDS change
        ↓
Subscription Index
        ↓
User State

before: 12, 31, 57
after:  12, 57
        ↓
REMOVE driver:31

User State records not only what the user requested, but what the user
currently has.

11. Membership Transitions

Before     After      Result

outside    inside     ADD
inside     inside     UPDATE
inside     outside    REMOVE
deleted    —          REMOVE

Filter exit and entity deletion both appear as REMOVE to the client.
SubState may retain an internal reason, but clients should not require
it for basic synchronization.

12. Deltas

After the initial snapshot, SubState sends changes instead of the full
User State.

The basic message types are:

ADD
UPDATE
REMOVE

ADD contains enough state to construct the entity locally. UPDATE is
normally a patch. REMOVE contains the entity identity.

{
  "subscription": "dispatcher_17",
  "seq": 1044,
  "op": "update",
  "entity": "driver",
  "id": "728",
  "fields": {
    "location": {
      "lat": 36.161,
      "lng": -86.781
    }
  }
}

13. Subscription Sequence Numbers

Source versions and subscription sequence numbers solve different
problems.

SOURCE ORDERING

Postgres version 9182
GPS sequence 552819
        ↓
       CDS
        ↓
SUBSCRIPTION ORDERING

delta 1042
delta 1043
delta 1044

Source versions answer: Should the CDS accept this incoming update?

Subscription sequence numbers answer: What has this user already
received?

A sequence number is assigned only when SubState emits a delta. A
sequence gap therefore means a delivered delta is missing; it never
represents a coalesced internal update.

14. Reconnect and Resume

If a client has processed delta 1042 and disconnects, it reconnects
with:

resume_after = 1042

If retained history contains later deltas, SubState replays them.
Otherwise it sends a fresh snapshot from the CDS.

For the prototype, retain:

last 500 delivered deltas per subscription

If the resume point is older than retained history:

RESET
  ↓
fresh snapshot

For latest-value state, a fresh snapshot contains the newest known value
rather than every intermediate value missed.

15. Acknowledgements and backpressure

Clients acknowledge the highest subscription sequence they have
successfully applied.

Acknowledgements only advance the known resume point. They do not form a
credit window and do not slow or pause delivery by themselves.

When the WebSocket send buffer backs up, SubState stops broadcasting live
deltas to that session. On recovery it sends `reset` plus a fresh snapshot
from the CDS (same shape as an out-of-window resume).

16. Prototype Architecture

                 POSTGRES
            drivers + jobs
                  │
          snapshot + CDC
           (poll fallback)
                  │
                  ▼
            ┌───────────┐
            │    CDS    │
            └─────┬─────┘
              ▲         ▲
              │         │
      HTTP / SDK       Kafka
      any source     location /
       via ingest    data bus
              │         │
              └────┬────┘
                   │
                   ▼
          Subscription Index
                   │
                   ▼
              User State
                   │
            ordered deltas
                   │
                   ▼
               WebSocket
                   │
                   ▼
            Dispatcher Map

Postgres bootstraps mapped tables into the CDS at startup, then follows
the WAL through a `pgoutput` slot when `wal_level=logical`. Otherwise it
re-reads tables on a timer.

Kafka consumers map JSON messages onto schema-owned fields. Anything else
can POST `/v1/ingest`. All three paths become the same inbox message
before the CDS merges.

The core architecture does not change when a new adapter is added.

17. Prototype Technology

Current preferred implementation:

Engine:          Rust
Async runtime:   Tokio
HTTP/WebSocket:  Axum
Serialization:   Serde
Postgres:        SQLx
Logging:         tracing

Browser client:  TypeScript
Reference UI:    TypeScript / React

Keep the first implementation simple:

CDS:
HashMap<EntityId, EntityState>

Subscriptions:
HashMap<SubscriptionId, Subscription>

User State:
Set / Map of matching entities

Delta history:
VecDeque<Delta>

Do not introduce a custom storage engine, distributed consensus, or
complex concurrency architecture into the prototype.

18. Open-Source Deployment Model

SubState is open-source first.

The intended long-term model allows the data plane to run inside the
user's infrastructure:

CUSTOMER INFRASTRUCTURE

Databases ──────┐
Event streams ──┤
                ▼
             SubState
                │
                ▼
         Applications

SubState should be useful without requiring production operational data
to pass through a hosted SubState service.

Production-grade deployment, clustering, upgrades, and observability are
not part of the first prototype.

19. Deferred Problems

Important deferred questions include:

Persistence beyond the local disk snapshot --- How should CDS /
subscription / history durability work across machines?

Ack-as-credit backpressure --- Acknowledgements still only advance the
resume cursor; send-buffer backup already triggers pause + reset +
snapshot, but there is no client credit window yet.

Cross-source ordering --- What guarantees should a user receive when
independent sources change different fields?

Scaling --- How should CDS, Subscription Index, and User State be
partitioned across nodes?

Spatial relevance --- How should moving subscriptions use H3 or
another spatial index?

These should be solved when validated workloads require them.

20. Architecture Invariant

The most important rule is:

Subscriptions evaluate SubState's Current Database State, not
upstream databases directly.

Sources continuously update the CDS. Subscriptions evaluate the CDS. The
Subscription Index determines who may be affected. User State records
the current result. Deltas synchronize that result to the user.

SOURCE
   ↓
CURRENT DATABASE STATE
   ↓
WHO CARES?
   ↓
USER STATE
   ↓
WHAT CHANGED?
   ↓
DELTA
   ↓
USER

That is the core SubState architecture.