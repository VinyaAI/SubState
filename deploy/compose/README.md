# Optional Compose demo

Self-contained Postgres (logical WAL) + Redpanda + SubState toy stack. This is
**not** the primary run path — for pointing SubState at your own database with
`cargo run`, see the [root README](../../README.md).

Seeds three `drivers` rows (id `1` = Alice). The smoke script expects that seed
and the demo schema under [`schema.yaml`](schema.yaml) (`location` owned by
Kafka source `gps`).

```bash
# from repo root
docker compose up --build
curl http://127.0.0.1:8080/health

# smoke (needs Node 18+): Postgres UPDATE + Kafka location
./scripts/smoke.sh
```

Postgres is started with `wal_level=logical` so SubState can use a `pgoutput`
slot named `substate`. If CDC cannot start, it falls back to table poll
(`POSTGRES_FOLLOW=auto`).

Redpanda advertises Kafka at `redpanda:9092` inside the network
(`KAFKA_BROKERS`). A `location-producer` writes `{driver_id, location, sequence}`
to `driver-locations` every 5 seconds. To send one message yourself:

```bash
./scripts/produce-location.sh
```

Override `DATABASE_URL` / mount your own schema to point the Compose image at
an external database:

```bash
DATABASE_URL=postgresql://... SCHEMA_PATH=./schema.yaml docker compose run --service-ports substate
```

A schema with a `kafka` source still needs `KAFKA_BROKERS`. Or edit
`docker-compose.yml` environment / volumes.
