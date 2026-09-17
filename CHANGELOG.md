# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/) once
stable. Until then, the product is **`0.1.0-alpha`**: **`/v1` may break**
between commits without a major version bump.

## [0.1.0-alpha] — 2026-09-17

First public alpha of the local SubState sidecar.

### Added

- Sync schema YAML with transactional / `latest_value` fields, `flush_ms`
  coalesce, optional `ttl_ms`, physical `column` / `path` remapping, composite
  identity, and one-hop relations
- `substate init` / `serve` / `shell` CLI; Postgres snapshot + CDC or poll;
  Kafka JSON (incl. Debezium envelope) with partition offsets; HTTP ingest;
  MySQL and MongoDB poll adapters; JSON Schema–assisted Kafka discovery
- WebSocket `/v1/sync` with equality, range, and `$or` / `$and` filters;
  resume via delta history; basic backpressure reset
- Disk snapshot of CDS + subscriptions + delta history across restarts
- Optional `SUBSTATE_API_KEY` on `/v1/*`; Prometheus-style `GET /metrics`
- TypeScript client (`apiKey`, reconnect / `resume_after`, subscribe helpers)
- Reference dispatcher-map example UI

### Stability

`/v1/sync`, `/v1/ingest`, and `/v1/cds` are the intended public surface, but
payload shapes and filter grammar may change in alpha. Prefer pinning a commit
for anything beyond local experiments. See [docs/api.md](docs/api.md).
