# ETL Platform

Rust + Temporal + WebAssembly ETL platform targeting the Fivetran ingestion market. Full architecture across 23 RFCs in `docs/rfc/`, roadmap in `docs/superpowers/specs/2026-04-22-implementation-roadmap-design.md`, current phase plan in `docs/superpowers/plans/2026-04-22-phase-1-2-first-pipeline.md`.

## Prerequisites

- Rust 1.88+ (`rust-toolchain.toml` pins the channel; rustup auto-fetches)
- Docker + Docker Compose (the whole dev stack runs in containers)

No host-side `psql` or `temporal` CLI required — everything runs via `docker exec`.

## Local dev bootstrap

```bash
# 1. Start the full stack: Postgres (catalog + source), Temporal server, Temporal UI
docker compose up -d

# 2. Seed the source-demo database (creates etl_source_demo.customers with 10 rows)
bash scripts/seed-source-demo.sh

# 3. Env
cp .env.example .env
source .env

# 4. Build
cargo build --workspace

# 5. Seed a demo pipeline in the catalog
docker exec -i etl-postgres psql -U etl -d etl_catalog <<'SQL'
INSERT INTO tenants (tenant_id, name)
  VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('22222222-2222-2222-2222-222222222222',
          '11111111-1111-1111-1111-111111111111',
          'source-demo', 'postgres@0.1.0',
          '{"url":"postgres://etl:etl@localhost:5432/etl_source_demo"}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('33333333-3333-3333-3333-333333333333',
          '11111111-1111-1111-1111-111111111111',
          'customers-sync',
          '22222222-2222-2222-2222-222222222222',
          '{"source":{"type":"postgres","schema":"public","table":"customers","cursor_column":"updated_at","cursor_kind":"timestamp_tz","pk_columns":["id"]},"destination":{"type":"local_parquet","base_path":"./data"},"batch_size":4}'::jsonb);
SQL

# 6. Run the worker (separate terminal)
cargo run --bin worker

# 7. Submit the pipeline
cargo run --bin platform -- pipeline run pipe-33333333-3333-3333-3333-333333333333
```

Expected outcome on first run:
- Worker logs show start_run → 3× (read_batch + load_batch + commit_cursor) → complete_run
- Three Parquet files under `./data/33333333-.../<run_id>/batch-0000{0,1,2}.parquet` (4 + 4 + 2 rows)
- Cursor persisted: `docker exec -i etl-postgres psql -U etl -d etl_catalog -c "SELECT stream_name, cursor_value FROM stream_state;"` shows `customers | 2026-04-22T11:00:00.000000Z`

Re-running the command replays zero rows (cursor matches head). To exercise incremental behavior, insert rows with later `updated_at` into `etl_source_demo.customers` and re-run; only the new rows land in a new Parquet file in a new run subdirectory, and the cursor advances.

## Tests

Workspace unit/integration tests (requires Postgres running):

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test --workspace -- --test-threads=1
```

End-to-end integration tests (require full docker stack + source-demo seeded):

```bash
# Fresh + incremental sync (~75s)
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  cargo test -p integration-tests incremental_sync -- --ignored --nocapture

# Kill-restart durability (~10–180s depending on machine speed)
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  cargo test -p integration-tests sync_survives -- --ignored --nocapture

# Phase I.1 workflow durability (~80s)
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  cargo test -p integration-tests workflow_survives -- --ignored --nocapture
```

## Crate map

| Crate | Role | Phase |
|---|---|---|
| `common-types` | ID newtypes, `PipelineSpec`, `CursorValue`, `ConnectionConfig`, `SourceSpec::Wasm` | I.1 / I.2 / I.3 |
| `catalog` | Postgres-backed metadata store (RFC-10) + stream_state | I.1 (minimal) → I.4 (full) |
| `connector-sdk` | `SourceConnector` trait + WIT definition | I.2 (trait) / I.3 (WIT) |
| `loader-sdk` | `DestinationLoader` trait | I.2 (trait) → II.3 (warehouse variants) |
| `worker` | Temporal worker, PipelineRunWorkflow, Postgres connector, Parquet loader, WASM runtime | I.1 → I.6 |
| `control-api` | Public HTTP/gRPC surface (stub) | III.1 |
| `cli` | `platform` command-line tool (RFC-13) incl. `connector build` | I.1 / I.2 / I.3 |
| `examples/csv-source` | Reference WASM source connector (wasm32-wasip2) | I.3 |
| `tests/integration` | End-to-end tests | I.1 / I.2 / I.3 |

## Stack

| Component | Host port | Notes |
|---|---|---|
| `etl-postgres` | 5432 | Catalog DB (`etl_catalog`) + source demo DB (`etl_source_demo`); `wal_level=logical` ready for Phase I.6 CDC |
| `etl-temporal-postgres` | (internal) | Temporal's backing DB |
| `etl-temporal` | 7233 | Temporal server (gRPC) |
| `etl-temporal-ui` | 8080 | Temporal Web UI |

## Phase

Currently: **Phase I.3 — WASM Runtime (complete)**. Next: Phase I.4 — full catalog entities (streams, schemas, evolution policies) + YAML DSL. See the roadmap spec for the four-era trajectory.

## Phase I.3 — WASM connector demo

```bash
# 1. Build the reference WASM connector (re-run after guest code changes)
cargo run --bin platform -- connector build examples/csv-source
# → ./connectors/csv-source@0.1.0/component.cwasm (~3.4 MB precompiled)

# 2. Seed a pipeline pointed at the WASM connector
docker exec -i etl-postgres psql -U etl -d etl_catalog <<'SQL'
TRUNCATE runs, stream_state, pipelines, connections, tenants CASCADE;
INSERT INTO tenants (tenant_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('44444444-4444-4444-4444-444444444444',
          '11111111-1111-1111-1111-111111111111',
          'csv-inline', 'wasm:csv-source@0.1.0', '{"url":""}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('55555555-5555-5555-5555-555555555555',
          '11111111-1111-1111-1111-111111111111',
          'csv-sync',
          '44444444-4444-4444-4444-444444444444',
          '{"source":{"type":"wasm","config":{"csv_text":"id,name\nA,Alice\nB,Bob\nC,Carol\n","has_header":true}},"destination":{"type":"local_parquet","base_path":"./data"},"batch_size":2}'::jsonb);
SQL

# 3. Run worker + submit
cargo run --bin worker &
cargo run --bin platform -- pipeline run pipe-55555555-5555-5555-5555-555555555555
```

Expected: workflow fires `start_run → discover → read → load → commit (×N) → complete_run` via the WASM sandbox. Parquet files land under `./data/`. Worker logs interleave guest-emitted `log()` messages (tagged `guest=true`) with host traces.

Resource limits + capability sandboxing validated by three unit tests (`wasm_runtime::tests::*`): fuel exhaustion traps an infinite loop, memory cap denies oversized allocation, and components importing un-linked host functions fail at instantiation.
