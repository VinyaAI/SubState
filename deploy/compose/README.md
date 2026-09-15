# Local / CI Compose stack

Self-contained Postgres + SubState sidecar.

```bash
# from repo root
docker compose up --build
curl http://127.0.0.1:8080/health

# smoke (needs Node 18+)
./scripts/smoke.sh
```

Override `DATABASE_URL` / mount your own schema to point at an external database:

```bash
DATABASE_URL=postgresql://... SCHEMA_PATH=./schema.yaml docker compose run --service-ports substate
```

Or edit `docker-compose.yml` environment / volumes.
