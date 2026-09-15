-- Demo schema for Docker Compose (self-contained; no external DB required).

CREATE TABLE drivers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'available',
    region      TEXT NOT NULL DEFAULT 'nashville',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO drivers (id, name, status, region) VALUES
    (1, 'Alice', 'available', 'nashville'),
    (2, 'Bob', 'busy', 'nashville'),
    (3, 'Carol', 'available', 'memphis');

SELECT setval(pg_get_serial_sequence('drivers', 'id'), (SELECT MAX(id) FROM drivers));
