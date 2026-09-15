Schema Discovery & Synchronization Semantics

Purpose

This document captures the schema idea for the open-source real-time state synchronization engine.

The goal is to make source onboarding mostly automatic while still requiring explicit confirmation for semantics that cannot be inferred safely.

The engine should be able to connect to sources such as Postgres and Kafka, discover the shape of the data, propose a unified entity model, and generate a configuration file that the developer can review and confirm.

The principle is:

Discover structure automatically. Infer cautiously. Require confirmation for synchronization semantics.

1. Why a Schema Is Needed

The engine must know more than field names and types.

It also needs to know:

which source owns each field;

how to identify the same entity across sources;

whether updates are transactional or latest-value;

how incoming updates are ordered;

whether updates may be coalesced;

whether stale updates should be discarded;

what relationships exist between entities.

Example:

Postgres
  drivers.id
  drivers.name
  drivers.status
  drivers.region

Kafka
  driver_id
  lat
  lng
  sequence
  observed_at

The engine needs to understand that these can form one logical entity:

driver:728
├── name        ← Postgres
├── status      ← Postgres
├── region      ← Postgres
└── location    ← Kafka

The schema is the contract that tells the Current Database State layer how to build and maintain that entity.

2. Three Layers of Schema

Layer 1 — Source Schema

This describes what physically exists in each connected source and should be as automatic as possible.

For Postgres, discovery can include:

tables
columns
types
primary keys
foreign keys
nullable fields
indexes
constraints

For Kafka, discovery may include:

topics
message keys
field names
field types
registered schemas
schema versions

If Kafka uses Avro, Protobuf, JSON Schema, or a Schema Registry, discovery can be highly reliable. If a topic contains arbitrary JSON without a registered schema, the engine may sample messages and generate an inferred source schema, but it should mark that schema as inferred.

Layer 2 — Entity Mapping

This defines how fields from multiple sources combine into one logical entity.

Example:

Kafka.driver_id
      ↓
Postgres.drivers.id

The engine may infer likely mappings based on matching names, compatible types, primary keys, foreign keys, Kafka message keys, naming conventions, and repeated values.

These mappings should be proposed, not silently accepted.

Example generated proposal:

entities:
  driver:
    identity:
      source: postgres
      table: drivers
      field: id

    sources:
      postgres:
        table: drivers

      gps:
        type: kafka
        topic: driver-locations
        entity_key: driver_id

The developer confirms or edits the mapping.

Layer 3 — Synchronization Semantics

This defines how the engine should behave and usually requires developer confirmation.

Examples:

authoritative source
transactional vs latest-value
ordering field
coalescing
flush interval
freshness
TTL
conflict resolver

Example:

fields:
  status:
    source: postgres
    mode: transactional

  location:
    source: gps
    mode: latest_value
    ordering: sequence
    flush_ms: 100

This layer is where human intent matters most.

3. What Can Be Auto-Generated

From Postgres

The engine can usually discover:

table names
column names
column types
primary keys
foreign keys
unique constraints
nullability
indexes

Example generated source definition:

sources:
  postgres:
    type: postgres

    tables:
      drivers:
        primary_key: id

        fields:
          id:
            type: uuid
          name:
            type: string
          region:
            type: string
          status:
            type: string
          assigned_job:
            type: uuid

From Kafka

If a schema exists, the engine can discover topic, key, message schema, field names, field types, and schema version.

Example:

sources:
  driver_locations:
    type: kafka
    topic: driver-locations
    key: driver_id

    fields:
      driver_id:
        type: uuid
      latitude:
        type: float
      longitude:
        type: float
      sequence:
        type: integer
      observed_at:
        type: timestamp

If no formal schema exists, the engine may infer a candidate schema by sampling messages. Any inferred field should be labeled accordingly.

4. What Should Not Be Silently Inferred

Field Authority

If two sources both contain:

driver.status

the engine should not arbitrarily decide which source wins.

Instead:

status:
  sources:
    - postgres
    - kafka
  authority: REQUIRED

The developer chooses:

authority: postgres

Ordering

Versions are source-local. The engine should never compare a Postgres transaction version directly to a Kafka sequence number.

Rule:

Accept an update only if it comes from the authoritative source for that field and its source-local version is newer than the currently stored version.

For GPS-like data:

location:
  source: gps
  ordering: sequence

sequence determines ordering. observed_at is used for freshness, not ordering. This avoids relying on clocks across machines.

Latest-Value vs Transactional

The engine may suggest that fields such as latitude, longitude, heading, and speed are likely latest-value state, but it should ask for confirmation.

location:
  mode: latest_value
  ordering: sequence
  flush_ms: 100

Transactional examples:

status:
  mode: transactional

assigned_job:
  mode: transactional

5. Proposed Configuration Shape

A future sync.yaml might look like:

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
        type: uuid
        source: postgres
        mode: transactional

      name:
        type: string
        source: postgres
        mode: transactional

      region:
        type: string
        source: postgres
        mode: transactional

      status:
        type: string
        source: postgres
        mode: transactional

      assigned_job:
        type: uuid
        source: postgres
        mode: transactional
        relation:
          entity: job
          field: id

      location:
        source: gps
        mode: latest_value
        ordering: sequence
        flush_ms: 100

        fields:
          lat: latitude
          lng: longitude

        freshness:
          observed_at: observed_at

This is only a conceptual starting point. The exact syntax should emerge from implementation needs rather than being frozen too early.

6. Discovery Workflow

The ideal onboarding flow could be:

connect Postgres
      +
connect Kafka
      ↓
sync discover
      ↓
inspect source schemas
      ↓
propose entity mappings
      ↓
ask only unresolved semantic questions
      ↓
generate sync.yaml
      ↓
developer confirms / edits
      ↓
start Current Database State engine

Example CLI interaction:

Scanning Postgres...

Found:
  drivers
  jobs
  customers

Primary keys:
  drivers.id
  jobs.id
  customers.id

Foreign keys:
  drivers.assigned_job → jobs.id

Scanning Kafka...

Found topic:
  driver-locations

Detected fields:
  driver_id
  latitude
  longitude
  sequence
  observed_at

Possible entity mapping:

  driver-locations.driver_id
            ↕
  drivers.id

Accept?
[Y/n]

Then:

`location` appears to be high-frequency state.

Treat as latest-value?
[Y/n]

Ordering field:
> sequence

Coalesce updates?
[Y/n]

Flush interval:
> 100ms

The result is a generated schema file that the developer can inspect and commit to source control.

7. Relationship to Current Database State

The schema governs how connected sources update the Current Database State (CDS).

            sync.yaml
               │
               ▼
Postgres ───────┐
Kafka ──────────┤
Other sources ──┤
                ▼
               CDS

The CDS should not guess how to merge arbitrary data. It applies source updates according to the confirmed schema.

For each incoming field update, the CDS can ask:

Which entity is this?
Which field is changing?
Which source owns that field?
Is this update newer?
Is it transactional or latest-value?
Can it be coalesced?

That keeps merge behavior explicit and deterministic.

8. Prototype Scope

For the first dispatcher prototype, the schema should be handwritten.

entities:
  driver:
    fields:
      id:
        source: postgres
        mode: transactional

      name:
        source: postgres
        mode: transactional

      region:
        source: postgres
        mode: transactional

      status:
        source: postgres
        mode: transactional

      assigned_job:
        source: postgres
        mode: transactional

      location:
        source: gps
        mode: latest_value
        ordering: sequence
        flush_ms: 100

The goal of the first prototype is to validate that these semantics are useful. Automatic schema discovery should come after the engine works.

9. Why This Matters

The schema is not just setup metadata. It is the contract between heterogeneous source systems and the engine's unified current-state model.

A good schema system could make onboarding feel like:

Connect your systems, let the engine discover what it can, and answer only the questions that require domain knowledge.

This may become one of the most important developer-experience features of the project.

The ideal target is not to eliminate configuration entirely. It is to:

Auto-generate most of the schema and ask the developer only for the semantics that cannot be safely inferred.