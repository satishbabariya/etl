#!/usr/bin/env bash
# Reseeds etl_source_demo.customers with 100 rows across 100 distinct
# updated_at timestamps (1 per minute starting 2026-04-20T00:00:00Z).
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
INSERT INTO customers (id, name, email, created_at, updated_at)
SELECT
    g,
    'user-' || g,
    CASE WHEN g % 3 = 0 THEN NULL ELSE 'u' || g || '@example.com' END,
    TIMESTAMPTZ '2026-04-20 00:00:00+00' + (g * interval '1 minute'),
    TIMESTAMPTZ '2026-04-20 00:00:00+00' + (g * interval '1 minute')
FROM generate_series(1, 100) AS g;
SELECT COUNT(*) AS customer_count FROM customers;
SQL
