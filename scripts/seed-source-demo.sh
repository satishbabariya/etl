#!/usr/bin/env bash
# Idempotent source-db seed. Safe to run against an existing container.
# Resets customers to a known 10-row baseline.
set -euo pipefail

docker exec -i etl-postgres psql -U etl -d postgres <<'SQL'
SELECT 'CREATE DATABASE etl_source_demo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_source_demo')\gexec
SQL

docker exec -i etl-postgres psql -U etl -d etl_source_demo <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id         BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
TRUNCATE customers;
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
SELECT COUNT(*) AS customer_count FROM customers;
SQL

# Phase I.6: CDC source demo (separate DB to avoid slot contention with
# the cursor-incremental pipelines above).
docker exec -i etl-postgres psql -U etl -d postgres <<'SQL' || true
SELECT 'CREATE DATABASE cdc_source_demo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'cdc_source_demo')\gexec
SQL

docker exec -i etl-postgres psql -U etl -d cdc_source_demo <<'SQL'
CREATE TABLE IF NOT EXISTS orders (
    id         BIGINT PRIMARY KEY,
    customer   TEXT NOT NULL,
    amount     TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE orders REPLICA IDENTITY FULL;
TRUNCATE orders;
INSERT INTO orders (id, customer, amount) VALUES
  (1, 'Alice', '100'),
  (2, 'Bob',   '200'),
  (3, 'Carol', '300');
SELECT COUNT(*) AS order_count FROM orders;
SQL
