# ETL Platform

Rust + Temporal + WebAssembly ETL platform targeting the Fivetran ingestion market. Full architecture across 23 RFCs in `docs/rfc/`, roadmap in `docs/superpowers/specs/2026-04-22-implementation-roadmap-design.md`, current phase plan in `docs/superpowers/plans/2026-04-22-phase-1-1-skeleton.md`.

## Prerequisites

- Rust 1.88+ (`rust-toolchain.toml` pins the channel; rustup auto-fetches)
- Docker + Docker Compose (the whole dev stack runs in containers)

No host-side `psql` or `temporal` CLI required — both are run via `docker exec`.

## Local dev bootstrap

```bash
# 1. Start the full stack: Postgres (catalog), Postgres (temporal),
#    Temporal server (auto-setup), Temporal UI
docker compose up -d

# 2. Env
cp .env.example .env
source .env

# 3. Build
cargo build --workspace

# 4. Seed one tenant + connection + pipeline for manual testing
docker exec -i etl-postgres psql -U etl -d etl_catalog <<'SQL'
INSERT INTO tenants (tenant_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111',
          'dev-pg', 'postgres@0.1.0', '{}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111',
          'demo', '22222222-2222-2222-2222-222222222222', '{}'::jsonb);
SQL

# 5. Run the worker (leave running in a terminal)
cargo run --bin worker

# 6. In another terminal, submit a pipeline run
cargo run --bin platform -- pipeline run pipe-33333333-3333-3333-3333-333333333333
```

Then watch:
- Worker logs show `run started` → 30s pause → `run completed`
- Temporal UI at <http://localhost:8080> shows the workflow
- Run status: `docker exec -i etl-postgres psql -U etl -d etl_catalog -c "SELECT status, started_at, completed_at FROM runs ORDER BY started_at DESC LIMIT 1;"`

## Tests

Unit + catalog-integration tests (requires Postgres running):

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test --workspace -- --test-threads=1
```

End-to-end durability test (requires full stack running; ~80s runtime):

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  cargo test -p integration-tests -- --ignored --nocapture
```

The durability test spawns a worker, submits a pipeline, kills the worker while the workflow's 30s timer is pending server-side, spawns a fresh worker, and asserts the run reaches `completed`. The elapsed-time invariant (`complete_run` fires exactly 30s after `start_run` despite the restart) proves Temporal's server-side timer survived.

## Crate map

| Crate | Role | Phase |
|---|---|---|
| `common-types` | Non-forgeable ID newtypes (`TenantId`, `PipelineId`, `ConnectionId`, `RunId`) | I.1 |
| `catalog` | Postgres-backed metadata store (RFC-10) | I.1 (minimal) → I.4 (full) |
| `worker` | Temporal worker, activities, `PipelineRunWorkflow` (RFC-4) | I.1 (skeleton) → I.6 (CDC) |
| `control-api` | Public HTTP/gRPC surface (stub) | III.1 |
| `connector-sdk` | Developer-facing connector SDK (stub) | I.3 |
| `loader-sdk` | Rust-native loader trait (stub) | II.3 |
| `cli` | `platform` command-line tool (RFC-13) | I.1 (subset) → I.4 (full) |
| `tests/integration` | End-to-end durability test | I.1 |

## Stack

| Component | Host port | Notes |
|---|---|---|
| `etl-postgres` | 5432 | Catalog DB (`etl_catalog`) with `wal_level=logical` ready for Phase I.6 CDC |
| `etl-temporal-postgres` | (internal) | Temporal's backing DB |
| `etl-temporal` | 7233 | Temporal server (gRPC) |
| `etl-temporal-ui` | 8080 | Temporal Web UI |

## Phase

Currently: **Phase I.1 — Skeleton (complete)**. Next: Phase I.2 — first real pipeline (Postgres cursor-incremental → Parquet). See the roadmap spec for the four-era trajectory.
