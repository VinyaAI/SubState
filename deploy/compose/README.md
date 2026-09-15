# Optional Compose demo

Self-contained Postgres + SubState toy stack for local smoke tests. This is
**not** the primary run path — for pointing SubState at your own database with
`cargo run`, see the [root README](../../README.md).

Seeds three `drivers` rows (id `1` = Alice). The smoke script expects that seed
and the demo schema under [`schema.yaml`](schema.yaml).

```bash
# from repo root
docker compose up --build
curl http://127.0.0.1:8080/health

# smoke (needs Node 18+)
./scripts/smoke.sh
```

Override `DATABASE_URL` / mount your own schema to point the Compose image at
an external database:

```bash
DATABASE_URL=postgresql://... SCHEMA_PATH=./schema.yaml docker compose run --service-ports substate
```

Or edit `docker-compose.yml` environment / volumes.
