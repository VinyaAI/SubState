# Sync schema

The sync schema is a YAML file that maps physical sources onto **logical**
entity and field names. Clients subscribe and ingest by those logical names
only. Point `SCHEMA_PATH` at the file (required for `serve` / `shell`).

Prefer generating a draft:

```bash
cargo run -p substate-cli -- init
# or non-interactive:
cargo run -p substate-cli -- init --defaults --out ./schema.yaml
```

`init` merges into an existing file when one is present (adds/updates drafts;
does not wipe unrelated hand-edits). You can also start from
[schema.template.yaml](../schema.template.yaml).

## Shape

```yaml
entities:
  driver:                              # logical name clients use
    identity:
      field: id                        # or: fields: [tenant_id, id]
    sources:
      pg:
        type: postgres                 # postgres | kafka | http | mysql | mongodb
        table: drivers
      stream:
        type: kafka
        topic: driver.location
        entity_key: driver_id
      api:
        type: http
    fields:
      id:
        source: pg
        mode: transactional            # transactional | latest_value
      status:
        source: pg
        mode: transactional
        column: driver_status          # optional physical column
      location:
        source: stream
        mode: latest_value
        path: loc                      # optional JSON key
        ordering: ts
        flush_ms: 100                  # coalesce; default 100 if omitted
        ttl_ms: 5000                   # optional; drop stale latest-value
    relations:                         # optional one-hop only
      assigned_job:
        entity: job
        local: assigned_job
        # remote: id
```

## Rules

| Rule | Detail |
| --- | --- |
| Identity | Every identity field must appear under `fields` |
| Sources | Each field’s `source` must exist under `sources` |
| Authority | A field is owned by exactly one source |
| Remapping | `column` (SQL) / `path` (JSON) map physical → logical; clients use the logical name |
| Modes | `transactional` applies immediately; `latest_value` coalesces for `flush_ms` |
| TTL | `ttl_ms` only applies to `latest_value`; stale values are dropped or marked |
| Relations | One hop (`local` field → remote entity id); not a join planner |
| Composite PK | `identity.fields: [a, b]` → entity id `a=…\|b=…` |

Supported source `type` values: `postgres`, `kafka`, `http`, `mysql`,
`mongodb`. Kafka payloads may be plain JSON or a Debezium envelope (payload
unwrapped automatically).

## Validation

`SyncSchema::validate` rejects unknown source refs, missing identity fields,
and invalid remaps. Fix errors before `serve`; the process will not start on an
invalid schema.
