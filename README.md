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

Currently: **Phase II.3.d.3 — Typed Postgres CDC snapshot batches (complete)** on top of II.3.d.2. Snapshot now captures all data columns (not just PK) with native Arrow types matching the streaming schema. Postgres CDC is fully type-aware end-to-end. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (MySQL initial snapshot, multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).

## Auth (Phase II.2.b + II.2.c)

Phase II.2.c moves auth to a separate `etl-auth` issuer that owns RSA keypairs and exposes a JWKS endpoint. Tokens are RS256-signed with `kid`, `iss`, `aud`, `jti` claims; verifiers fetch the public set over HTTP and cache for 10 minutes. Login issues a 15-minute access token + a 30-day refresh token; refresh tokens rotate on use (replay rejected). `etl-auth revoke <jti>` blocks a stolen access token immediately when `ETL_AUTH_REVOCATION_CHECK=1` is set.

```bash
# Generate the issuer keypair.
etl-auth init-issuer

# Start the issuer (HTTP + JWKS on :8400 by default).
etl-auth serve --database-url $DATABASE_URL &

# Provision a principal.
platform tenant create acme
platform auth create-principal --tenant acme alice --password hunter2 --role operator

# Log in (15m access + 30d refresh, cached at ~/.etl/credentials.json).
ETL_AUTH_ISSUER=http://localhost:8400 platform auth login alice --password hunter2

# Manually refresh the access token.
platform auth refresh

# Logout invalidates the refresh server-side and clears the cache.
platform auth logout

# Rotate the issuer key (old keys remain in JWKS for verification).
etl-auth rotate-key

# Revoke a compromised access token by jti.
etl-auth revoke <jti> --tenant acme
```

Set `ETL_AUTH_REVOCATION_CHECK=1` in production to enforce the revocation list at every CLI write path. The HS256 dev seam from II.2.b stays alive behind `ETL_JWT_SECRET` for back-compat. `ETL_AUTH_BYPASS=1` is the integration-test escape hatch — forges a fake admin JWT.

Three roles: `admin`, `operator`, `viewer`. Tenants gain a `status` column (`active` / `suspended`); suspended tenants cannot run pipelines.

```bash
# Bootstrap an admin in tenant 'acme'.
cargo run --bin platform -- tenant create acme
cargo run --bin platform -- auth create-principal --tenant acme alice \
  --password hunter2 --role admin

# Log in (caches at ~/.etl/credentials.json).
cargo run --bin platform -- auth login alice --password hunter2
cargo run --bin platform -- auth whoami

# Admin can override tenant per-call.
cargo run --bin platform -- --tenant other-tenant pipeline run <pid>

# Suspend / resume.
cargo run --bin platform -- tenant suspend acme
cargo run --bin platform -- tenant resume acme
```

`ETL_AUTH_BYPASS=1` is the integration-test escape hatch — forges a fake admin JWT so existing tests run without a login dance. Production builds should disable it.

### RBAC matrix

| Role     | Read | Run | Write | Admin |
|----------|------|-----|-------|-------|
| Admin    | yes  | yes | yes   | yes   |
| Operator | yes  | yes | yes   | no    |
| Viewer   | yes  | no  | no    | no    |

## Connector SDK (Phase II.3.a)

Build a custom source connector in 5 steps:

```bash
platform connector create my-source
cd my-source
# edit src/lib.rs to implement discover() and read_batch()
platform connector test .
platform connector publish . --registry ./connectors
```

`platform connector test` runs `cargo build --release --target wasm32-wasip2` plus `cargo test`. `publish` writes the precompiled `.cwasm` artifact and a `manifest.yaml` (sha256, version, kind) to the registry directory. The worker reads from `ETL_CONNECTORS_DIR` (default `./connectors`).

See `docs/connector-sdk-guide.md` for the full authoring walkthrough. II.3.d+ ship the MySQL CDC / Snowflake / BigQuery / Postgres connectors using this same SDK.

**TypeScript authoring (Phase II.3.b).** `platform connector create my-source --lang typescript` materializes a TS skeleton (package.json + esbuild + jco + apache-arrow + vitest). `platform connector test` runs `npm test` + esbuild bundle + `jco componentize`; `publish` produces the same `.cwasm` artifact shape as Rust. Bundle size is larger (~16 MB vs ~650 KB Rust) because componentize-js embeds StarlingMonkey, but the worker host treats both identically.

**Example connector: Stripe customers (Phase II.3.c / II.3.b).** `examples/stripe-source/` ships a complete `/v1/customers` source connector built on the Rust SDK — bearer-token auth, `starting_after` pagination, 429 backoff, JSON-schema discovery. Build with `platform connector publish examples/stripe-source --registry ./connectors`. `examples/stripe-source-ts/` is the TypeScript port: same WIT contract, same wiremock e2e test, ~25× larger artifact.

## Production hardening (Phase II.2.e)

> Originally labeled "Phase II.4" — corrected because per the roadmap II.4 is Helm/Terraform/`platform install` packaging. What's below is prerequisite hardening that fits between II.2.d (audit) and the real II.3 (connector SDK).

**Sealed issuer keys.** Set `ETL_MASTER_KEY` (32-byte hex) and `etl-auth init-issuer` writes `private.enc` (XChaCha20-Poly1305 envelope) instead of `private.pem`. Upgrade an existing keystore with `etl-auth seal-keys --confirm`.

```bash
ETL_MASTER_KEY=$(openssl rand -hex 32)
export ETL_MASTER_KEY
etl-auth init-issuer        # writes private.enc
etl-auth serve              # decrypts to memory on boot
```

**Audit retention + chain verification.** `etl-auth serve` runs three background tasks: hourly audit retention prune (`--audit-retention-days N`, default 365), 6-hourly chain verification (records a checkpoint on success, emits `AUDIT_CHAIN_BREAK` on mismatch), and hourly `revoked_tokens` cleanup. One-shot equivalents: `etl-auth verify-once`, `etl-auth prune-audit`, `platform audit prune --older-than-days N`. Pruning never breaks `verify-chain` because the checkpoint table records the last verified `(audit_id, hash)` and verify resumes from there.

**Hot-reload tenants.** The worker watches `tenants` every 30s and spawns a Temporal poller for newly-created tenants without restart.

**Health endpoints.** `/healthz` (always 200) and `/readyz` (catalog reachable) on both worker (`:9898`) and etl-auth (`:8400`). Use these as k8s liveness/readiness probes.

## Audit (Phase II.2.d)

Every security-relevant action — auth login/refresh/logout/revoke, secret read/create/delete, tenant create/suspend/resume, connection/pipeline apply, `--tenant` admin override — writes one row to a per-tenant `audit_log` table. Each row's hash chains the prior row (SHA-256 of `prev_hash || canonical_bytes`). Tampering with any row's payload trips `audit verify-chain`.

```bash
# Tail recent events for the current tenant.
platform audit tail --limit 20

# Walk the chain and report the first integrity break.
platform audit verify-chain

# Admin: see the chain for another tenant.
platform --tenant acme audit verify-chain
```

`tenant_id IS NULL` rows are system-scoped (e.g. AUTH_LOGIN_FAILED before the principal is identified). `tenant terminate` cascades the audit history with the tenant — no retention beyond that today; II.4 will add an admin-tenant audit copy.

## Secrets (Phase II.2.a)

Connection credentials live behind `SecretRef` pointers in the catalog — never as plaintext rows. Two backends in II.2.a: env-var (`ETL_SECRET_<KEY>`) and a JSON file (`./.etl-secrets.json`, override with `ETL_SECRETS_FILE`). Vault lands in II.2.b.

```bash
# Stash a plaintext in the file backend AND register a catalog SecretRef row.
cargo run --bin platform -- secret put pg-source-url \
  "postgres://etl:etl@localhost:5432/etl_source_demo" --register

# Reference it from a Connection YAML — `apply` rewrites the name to a full SecretRef:
#
#   spec:
#     connector_ref: postgres@0.1.0
#     config:
#       url_secret: pg-source-url
cargo run --bin platform -- apply -f examples/dsl/customers-sync-secret.yaml

# List what's registered (no plaintexts).
cargo run --bin platform -- secret list
```

Existing pipelines that use `config: { url: "postgres://..." }` keep working unchanged — `ConnectionConfig` accepts either `url` or `url_secret`. As of II.2.b the worker activities (sync + CDC) resolve the SecretRef at activity start; the CLI no longer touches plaintexts. Plaintext lifetime is the activity body only.

**Vault backend (II.2.b):** set `VAULT_ADDR` + `VAULT_TOKEN` (and optionally `VAULT_KV_MOUNT`); register a SecretRef with `--backend vault --key etl/pg-url`. The worker resolves at activity start through `vaultrs` against KV v2.

`PlaintextSecret` zeros on drop and refuses to serialize. `secrets` table is RLS-scoped per tenant.

## Tenant lifecycle (Phase II.1.c)

```bash
# Provision a tenant — catalog row + Temporal namespace etl-<uuid>
cargo run --bin platform -- tenant create acme

# List
cargo run --bin platform -- tenant list

# Wind down (catalog cascade + ./data/<tenant_id>/ deletion)
cargo run --bin platform -- tenant terminate acme
```

Each tenant's pipelines run in a Temporal namespace `etl-<tenant_id_simple>`, write Parquet under `./data/<tenant_id>/<pipeline_id>/...`, and emit metrics with a `tenant_id` label that filters every Grafana panel via the `tenant` template variable.

The worker boots one Temporal worker per known tenant + a `default` backstop. New tenants picked up after restart (Phase II.4 will hot-reconfigure).

## Multi-tenancy (Phase II.1.a + II.1.b)

The catalog uses Postgres row-level security on every tenant-scoped table. The `etl_app` non-superuser role is what the worker connects as for app-layer queries; admin paths (migrations, tenant CRUD) keep using the `etl` superuser. RLS is now **active in production paths**, not just dormant for tests.

- Every public `Catalog::*` method takes a `TenantContext` and opens a transaction with `SET LOCAL app.tenant_id`.
- Worker boots with `Catalog::connect_app` (etl_app role); `DATABASE_URL_APP` overrides the auto-rewrite.
- Every counter and gauge carries a `tenant_id` Prometheus label.
- Admin-only paths (migrations, tenant CRUD, `truncate_all_for_tests`, the slot-lag poller) explicitly run with empty `app.tenant_id` (NULL = admin).

```bash
# SQL-level adversarial test: cross-tenant reads/updates/inserts blocked at the DB layer.
cargo test -p integration-tests --test rls_cross_tenant -- --ignored

# API-level adversarial test: cross-tenant reads via Catalog::get_pipeline / get_connection
# return None even when asking for the other tenant's id by-id.
cargo test -p integration-tests --test tenant_api_isolation -- --ignored
```

What's still pending (Phase II.1.c):
- `platform tenant create | list | suspend | terminate` CLI
- Per-tenant Temporal namespace (`etl-<tenant_simple>`)
- Per-tenant storage prefix (`./data/<tenant_id>/<pipeline_id>/...`)
- Grafana dashboard `tenant` template variable

## Observability (Era I exit)

```bash
# Bring up the full stack (postgres + temporal + prometheus + grafana)
docker-compose up -d

# Worker exposes /metrics on :9898
cargo run --bin worker &

# Grafana is anonymous-admin at http://localhost:3000 → ETL — Overview
open http://localhost:3000/d/etl-overview
```

Available metrics: `etl_runs_{started,completed,failed}_total`,
`etl_rows_{read,loaded,rejected}_total`, `etl_cdc_events_total{op}`,
`etl_cdc_slot_lag_bytes{pipeline_id}`.

## Operator commands

```bash
# Status of a single pipeline as JSON
cargo run --bin platform -- pipeline status <pipeline-id>

# Stop a stuck workflow and mark its run Failed
cargo run --bin platform -- workflow terminate <workflow-id> --reason "cleanup"
```

## Writing a connector (Era I exit)

See `crates/connector-sdk/README.md` for a 30-minute tutorial that
walks you from zero to a registered WASM source connector. Reference
examples:
- `examples/hello-world-source/` — 3 rows, smallest viable
- `examples/csv-source/` — real files with cursor iteration

## Dogfood against your own database

```bash
scripts/dogfood-real-db.sh \
  'postgres://you:pw@host:5432/yourdb' \
  public.events \
  updated_at
```

Replicates `public.events` to `./data/dogfood/<pipeline-id>/` as
Parquet and prints a row-count + size summary.

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
