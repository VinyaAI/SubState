-- Minimal fixture for scripts/smoke.sh
CREATE TABLE IF NOT EXISTS drivers (
    id integer PRIMARY KEY,
    status text NOT NULL
);

TRUNCATE drivers;
INSERT INTO drivers (id, status) VALUES (1, 'available');
