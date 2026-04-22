-- Runs once on first container start via /docker-entrypoint-initdb.d/.
-- Subsequent runs should use scripts/seed-source-demo.sh.
CREATE DATABASE etl_source_demo;

\c etl_source_demo

CREATE TABLE customers (
    id         BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO customers (id, name, email, created_at, updated_at) VALUES
    (1, 'Alice',   'alice@example.com',   '2026-04-20 10:00:00+00', '2026-04-20 10:00:00+00'),
    (2, 'Bob',     NULL,                  '2026-04-20 11:00:00+00', '2026-04-20 11:00:00+00'),
    (3, 'Carol',   'carol@example.com',   '2026-04-20 12:00:00+00', '2026-04-20 12:00:00+00'),
    (4, 'Dave',    'dave@example.com',    '2026-04-21 09:00:00+00', '2026-04-21 09:00:00+00'),
    (5, 'Eve',     'eve@example.com',     '2026-04-21 10:00:00+00', '2026-04-21 10:00:00+00'),
    (6, 'Frank',   NULL,                  '2026-04-21 11:00:00+00', '2026-04-21 11:00:00+00'),
    (7, 'Grace',   'grace@example.com',   '2026-04-21 12:00:00+00', '2026-04-21 12:00:00+00'),
    (8, 'Heidi',   'heidi@example.com',   '2026-04-22 09:00:00+00', '2026-04-22 09:00:00+00'),
    (9, 'Ivan',    'ivan@example.com',    '2026-04-22 10:00:00+00', '2026-04-22 10:00:00+00'),
    (10,'Judy',    'judy@example.com',    '2026-04-22 11:00:00+00', '2026-04-22 11:00:00+00');
