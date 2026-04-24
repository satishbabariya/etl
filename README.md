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
| `common-types` | IDs, `PipelineSpec`, `CursorValue`, `SchemaFingerprint`, `EvolutionPolicy`, DSL types | I.1 → I.4 |
| `catalog` | Metadata store — tenants, workspaces, connections, pipelines, streams, schemas, runs | I.1 → I.4 |
| `connector-sdk` | `SourceConnector` trait + WIT definition | I.2 / I.3 |
| `loader-sdk` | `DestinationLoader` trait | I.2 → II.3 |
| `worker` | Temporal worker, PipelineRunWorkflow, Postgres connector, Parquet loader, WASM runtime, schema_evolution | I.1 → I.6 |
| `control-api` | Public HTTP/gRPC surface (stub) | III.1 |
| `cli` | `platform` CLI (RFC-13): pipeline run, connector build, apply/get/diff/validate | I.1 → I.4 |
| `examples/csv-source` | Reference WASM source connector (wasm32-wasip2) | I.3 |
| `examples/dsl` | YAML resource files | I.4 |
| `tests/integration` | End-to-end tests | I.1 → I.4 |

## Stack

| Component | Host port | Notes |
|---|---|---|
| `etl-postgres` | 5432 | Catalog DB (`etl_catalog`) + source demo DB (`etl_source_demo`); `wal_level=logical` ready for Phase I.6 CDC |
| `etl-temporal-postgres` | (internal) | Temporal's backing DB |
| `etl-temporal` | 7233 | Temporal server (gRPC) |
| `etl-temporal-ui` | 8080 | Temporal Web UI |

## Phase

Currently: **Phase I.6 — Postgres CDC (complete)**. Next: Era I exit / Phase II.1 multi-tenancy. See the roadmap spec for the four-era trajectory.

## Phase I.6 — Postgres CDC demo

```bash
# 1. Seed source (creates cdc_source_demo DB with orders table)
bash scripts/seed-source-demo.sh

# 2. Start the worker
cargo run --bin worker &

# 3. Create a CDC pipeline via catalog + CLI (sync_mode: cdc dispatches CdcPipelineWorkflow)
# ...create pipeline with spec.source.sync_mode = "cdc"...
cargo run --bin platform -- pipeline run <pipeline-uuid>

# 4. Drive DML; events land as append-only Parquet under ./data/<pid>/cdc/<rid>/
docker exec -i etl-postgres psql -U etl -d cdc_source_demo -c \
  "INSERT INTO orders VALUES (99,'Zed','999'); UPDATE orders SET amount='1000' WHERE id=99; DELETE FROM orders WHERE id=99;"
```

CDC pipelines emit one Parquet row per event with `_cdc.op` ∈ `s/i/u/d`, plus `_cdc.lsn`, `_cdc.commit_ts`, `_cdc.txid`. The loader appends forever; compaction into a current-state view is Phase II.

## Phase I.5 — Transformation DAG + dead-letter demo

Pipelines gain an optional `transform` field with a linear chain of operators. Schema derivation is pure; the catalog stores the post-transform schema so downstream consumers see what actually lands at the destination.

```yaml
# examples/dsl/customers-filter-mask.yaml
kind: Pipeline
name: customers-filter-mask
spec:
  source:
    type: postgres
    schema: public
    table: customers
    cursor_column: updated_at
    cursor_kind: timestamp_tz
    pk_columns: [id]
  destination: { type: local_parquet, base_path: ./data }
  batch_size: 4
  evolution_policy: propagate_additive
  transform:
    dead_letter_threshold: 0.0
    operators:
      - { type: filter, predicate: "email IS NOT NULL" }
      - { type: mask,   column: email, strategy: { kind: hash } }
```

MVP operators: `select`, `filter` (subset SQL — `IS [NOT] NULL`, `= <literal>`, `IN (...)`), `mask` (hash/null/redact), `add_column`, `validate` (row-level; fails land in dead-letter), `wasm_scalar` (pure per-row UDF via the Phase I.3 runtime under a tighter capability set — only `log`).

**Dead-letter routing.** Rows rejected by `validate` are written to `<base_path>/<pipeline_id>/dead-letter/<run_id>/batch-<seq>.parquet` with the original columns preserved. Cumulative `rows_rejected / rows_total` is compared to `dead_letter_threshold` (default `0.0` — fail on any). Over threshold → `load_batch` returns NonRetryable, the workflow's new `fail_run` shim marks the `runs` row Failed.

**Scalar UDFs.** Build a WASM scalar UDF with `cargo run --bin platform -- connector build <dir> --kind scalar`. See `examples/upper-case-scalar/` for the reference guest. Reference from a pipeline with `{ type: wasm_scalar, udf: "upper-case-scalar@0.1.0", input_column: name, output_column: name_upper }`.

## Phase I.4 — YAML DSL + schema evolution demo

```bash
# 1. Validate then apply the demo YAML (creates 1 connection + 1 pipeline)
cargo run --bin platform -- validate -f examples/dsl/customers-sync.yaml
cargo run --bin platform -- apply    -f examples/dsl/customers-sync.yaml

# 2. Reseed the source and run the pipeline — Schema v1 is auto-captured
bash scripts/seed-source-demo.sh
cargo run --bin worker &
cargo run --bin platform -- pipeline run <pipeline-id-from-apply>

# 3. Alter the source schema and rerun — Schema v2 appears with a typed diff
docker exec -i etl-postgres psql -U etl -d etl_source_demo -c \
  "ALTER TABLE customers ADD COLUMN nickname TEXT;"
docker exec -i etl-postgres psql -U etl -d etl_source_demo -c \
  "UPDATE customers SET updated_at = updated_at + interval '1 day';"
cargo run --bin platform -- pipeline run <pipeline-id>
docker exec -i etl-postgres psql -U etl -d etl_catalog -c \
  "SELECT version, change_summary FROM schemas ORDER BY version;"

# 4. `get` round-trips a catalog row back to YAML; `diff -f` shows pending changes
cargo run --bin platform -- get pipeline customers-sync
cargo run --bin platform -- diff -f examples/dsl/customers-sync.yaml
```

Evolution policies: `propagate_additive` (default — additive changes flow through), `freeze` (retain old), `strict` (fail run on any change). Set via `PipelineDslSpec.evolution_policy` in YAML.

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
