# Phase II.1 — Multi-Tenancy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the platform's hardcoded "dev" tenant into a real multi-tenant system: Postgres RLS at the catalog layer, Temporal namespace-per-tenant, per-tenant object-storage prefix, tenant-labeled metrics, and a `platform tenant` CLI for lifecycle.

**Architecture:** A new non-superuser `etl_app` Postgres role connects to the catalog with `BYPASSRLS` removed. Every tenant-scoped table gets an `ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` pair keyed off `current_setting('app.tenant_id', true)::uuid`. The `Catalog` struct gains a `with_tenant(tenant_id, |tx| ...)` helper that opens a transaction, issues `SET LOCAL app.tenant_id = '<uuid>'`, and runs the closure — every call site converts. The CLI threads a `TenantContext` from arg → catalog → Temporal client (whose namespace is `etl-<tenant_simple>`) → workflow input → activities → loaders. Loader paths become `<base>/<tenant_id>/<pipeline_id>/...`. Metrics gain a `tenant_id` label. New `platform tenant {create|suspend|terminate}` provisions/teardowns the catalog row + Temporal namespace + storage prefix.

**Tech Stack:** Unchanged — Rust 1.88, sqlx 0.8 (transactions are the RLS vehicle), temporalio-client 0.2 (`register_namespace` is exposed), Arrow/Parquet, wasmtime. New: nothing — RLS is pure Postgres.

---

## File Structure

### Modified
- `db/postgres-init/01-app-role.sql` — create `etl_app` non-superuser role on container init (NEW init script — Postgres processes `/docker-entrypoint-initdb.d/*.sql` once on first boot)
- `crates/catalog/src/lib.rs` — `Catalog::connect_app` (uses `etl_app`), `Catalog::with_tenant<F>` helper; rewire every method to take `&TenantContext`
- `crates/catalog/src/{tenant,workspace,connection,pipeline,run,stream,schema,stream_state,cdc}.rs` — every CRUD function gains a `tx: &mut sqlx::PgTransaction<'_>` parameter and is called inside `with_tenant`
- `crates/catalog/migrations/0005_rls_prep.sql` — add `tenant_id` to `cdc_slots` + `stream_state`, backfill from join, NOT NULL
- `crates/catalog/migrations/0006_rls_policies.sql` — enable RLS + policies on every tenant-scoped table; grant catalog perms to `etl_app`
- `crates/common-types/src/ids.rs` — `TenantContext { tenant_id: TenantId }` newtype (carries through workflow inputs)
- `crates/worker/src/loaders/parquet_local.rs` — path: `<base>/<tenant_id>/<pipeline_id>/...`
- `crates/worker/src/loaders/cdc_parquet.rs` — path: `<base>/<tenant_id>/<pipeline_id>/cdc/<run_id>/...`
- `crates/worker/src/activities/sync/mod.rs` — dead-letter path uses tenant prefix; `LoadBatchInput` carries `tenant_id`
- `crates/worker/src/activities/cdc/mod.rs` — `CdcParquetLoader.write` callers pass tenant_id
- `crates/worker/src/workflows/pipeline_run.rs` — `PipelineRunInput.tenant_id` already there; threaded into LoadBatchInput
- `crates/worker/src/workflows/cdc_pipeline.rs` — `CdcPipelineInput.tenant_id` already there; threaded into snapshot/read activities
- `crates/worker/src/main.rs` — Temporal client per-namespace; for now hard-coded to read `TEMPORAL_NAMESPACE` from env (compat). The CLI overrides at workflow-start time.
- `crates/worker/src/temporal.rs` — `make_client_for_namespace(cfg, namespace)` helper
- `crates/worker/src/metrics.rs` — counters helper accepting `tenant_id` consistently
- `crates/worker/src/activities/run_lifecycle.rs` — `tenant_id` label on counters
- `crates/worker/src/activities/sync/mod.rs` — `tenant_id` label on counters
- `crates/worker/src/activities/cdc/mod.rs` — `tenant_id` label on counters
- `crates/worker/src/cdc_monitor.rs` — `tenant_id` label on slot-lag gauge
- `crates/cli/src/main.rs` — `Tenant { Create | Suspend | Terminate | List }` subcommands; every existing command gains `--tenant <name|id>` (default: `dev`)
- `crates/cli/src/tenant.rs` — new module: provisioning + Temporal namespace registration
- `tests/integration/tests/transforms_filter_mask.rs` — output path moves under `<tenant_id>/<pipeline_id>/`
- `tests/integration/tests/transforms_dead_letter.rs` — same
- `tests/integration/tests/cdc_insert_update_delete.rs` — same
- `tests/integration/tests/cdc_snapshot_streaming_handoff.rs` — same
- `tests/integration/tests/incremental_sync.rs` — same (verify no regression)
- `tests/integration/tests/durability_midbatch.rs` — same
- `tests/integration/tests/schema_evolution.rs` — same
- `ops/grafana/dashboards/etl-overview.json` — add `tenant_id` template variable + per-panel filter
- `README.md` — Phase II.1 demo section
- `docker-compose.yml` — POSTGRES_DB unchanged; init script gets the new role

### New
- `crates/cli/src/tenant.rs` — `create_tenant_full / suspend / terminate / list`
- `crates/catalog/src/tenant_context.rs` — `TenantContext` re-export + helpers
- `tests/integration/tests/tenant_isolation.rs` — adversarial cross-tenant read/write tests
- `tests/integration/tests/tenant_lifecycle.rs` — provision → run a pipeline → suspend → terminate, verify Parquet + catalog cleanup
- `db/postgres-init/01-app-role.sql` — non-superuser role provisioning

---

## Task 1: Non-superuser app role

**Files:**
- Create: `db/postgres-init/01-app-role.sql`

- [ ] **Step 1: Write the role provisioning SQL**

```sql
-- 01-app-role.sql — runs once on first container boot.
-- Creates a non-superuser role the catalog connects as in production-like
-- mode. RLS policies bypass for SUPERUSER and for any role with
-- BYPASSRLS, so this role explicitly has neither.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_app') THEN
    CREATE ROLE etl_app LOGIN PASSWORD 'etl_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

-- Grant on the catalog DB. Object-level grants (per-table) come in
-- migration 0006 alongside the RLS policies themselves.
GRANT CONNECT ON DATABASE etl_catalog TO etl_app;
GRANT USAGE ON SCHEMA public TO etl_app;
```

- [ ] **Step 2: Re-create the postgres container so the init script runs**

```bash
docker-compose down postgres
docker volume rm etl_etl_pg
docker-compose up -d postgres
sleep 5
docker exec etl-postgres psql -U etl -d etl_catalog -c "\du etl_app"
```

Expected: a single row showing `etl_app` with `Cannot login` set to false and no extra attributes (no Superuser).

> ⚠️ This wipes the dev catalog. Re-run any seed scripts you depend on. The integration tests TRUNCATE on every run so they're fine.

- [ ] **Step 3: Commit**

```bash
git add db/postgres-init/01-app-role.sql
git commit -m "chore: etl_app non-superuser role for RLS"
```

---

## Task 2: Migration 0005 — `tenant_id` on `cdc_slots` + `stream_state`

**Files:**
- Create: `crates/catalog/migrations/0005_rls_prep.sql`

- [ ] **Step 1: Write the migration**

```sql
-- 0005_rls_prep.sql — denormalize tenant_id onto every tenant-scoped table.
-- RLS policies cannot reach through FKs, so each table needs the column.

ALTER TABLE cdc_slots
  ADD COLUMN IF NOT EXISTS tenant_id UUID;

UPDATE cdc_slots cs
SET tenant_id = p.tenant_id
FROM pipelines p
WHERE p.pipeline_id = cs.pipeline_id
  AND cs.tenant_id IS NULL;

ALTER TABLE cdc_slots
  ALTER COLUMN tenant_id SET NOT NULL,
  ADD CONSTRAINT cdc_slots_tenant_fk
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS cdc_slots_tenant_id_idx ON cdc_slots(tenant_id);

ALTER TABLE stream_state
  ADD COLUMN IF NOT EXISTS tenant_id UUID;

UPDATE stream_state ss
SET tenant_id = p.tenant_id
FROM pipelines p
WHERE p.pipeline_id = ss.pipeline_id
  AND ss.tenant_id IS NULL;

ALTER TABLE stream_state
  ALTER COLUMN tenant_id SET NOT NULL,
  ADD CONSTRAINT stream_state_tenant_fk
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS stream_state_tenant_id_idx ON stream_state(tenant_id);
```

- [ ] **Step 2: Apply + verify**

```bash
docker exec -i etl-postgres psql -U etl -d etl_catalog < crates/catalog/migrations/0005_rls_prep.sql
docker exec etl-postgres psql -U etl -d etl_catalog -c "\d cdc_slots" | head -15
docker exec etl-postgres psql -U etl -d etl_catalog -c "\d stream_state" | head -15
```

Expected: both tables show `tenant_id uuid NOT NULL` and the FK in the "Foreign-key constraints" block.

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0005_rls_prep.sql
git commit -m "feat(catalog): migration 0005 — tenant_id on cdc_slots + stream_state"
```

---

## Task 3: Migration 0006 — enable RLS + policies

**Files:**
- Create: `crates/catalog/migrations/0006_rls_policies.sql`

- [ ] **Step 1: Write the migration**

```sql
-- 0006_rls_policies.sql — enable RLS + define a single per-tenant policy
-- on every tenant-scoped table. The policy reads `app.tenant_id` from
-- the session; callers MUST `SET LOCAL app.tenant_id = '<uuid>'` inside
-- a transaction before issuing any DML.
--
-- The control-plane `tenants` table gets a wide-open policy keyed off
-- `app.tenant_id IS NULL OR tenant_id = current_setting(...)::uuid` so
-- a tenant can read its own row but not others, while admin (NULL
-- tenant) sees all.

-- 1. Grant catalog perms to etl_app.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_app;

-- 2. Helper: read app.tenant_id, return NULL if unset (admin mode).
CREATE OR REPLACE FUNCTION app_tenant_id()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
  SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid
$$;

-- 3. Per-table RLS.
DO $$
DECLARE
  tbl text;
  tables text[] := ARRAY[
    'connections',
    'pipelines',
    'runs',
    'workspaces',
    'streams',
    'schemas',
    'stream_state',
    'cdc_slots'
  ];
BEGIN
  FOREACH tbl IN ARRAY tables LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I
         USING (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
         WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)',
      tbl
    );
  END LOOP;
END
$$;

-- 4. tenants table: a tenant sees only its own row; admin (NULL) sees all.
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_self ON tenants
  USING (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
```

- [ ] **Step 2: Apply + verify**

```bash
docker exec -i etl-postgres psql -U etl -d etl_catalog < crates/catalog/migrations/0006_rls_policies.sql
docker exec etl-postgres psql -U etl -d etl_catalog -c \
  "SELECT tablename, rowsecurity, forcerowsecurity FROM pg_tables JOIN pg_class ON pg_class.relname = pg_tables.tablename WHERE schemaname='public' AND rowsecurity = true ORDER BY tablename"
```

Expected: 9 rows (`cdc_slots`, `connections`, `pipelines`, `runs`, `schemas`, `stream_state`, `streams`, `tenants`, `workspaces`) all with `rowsecurity = t` and `forcerowsecurity = t`.

- [ ] **Step 3: Adversarial smoke (still as superuser etl, RLS bypassed)**

```bash
docker exec etl-postgres psql -U etl_app -d etl_catalog -c \
  "SELECT count(*) FROM pipelines"
```

Expected: `0` — `etl_app` has no `app.tenant_id` set, NULL means admin → sees all rows. *(With seeded data this would be the count of all pipelines.)*

```bash
docker exec etl-postgres psql -U etl_app -d etl_catalog -c \
  "BEGIN; SET LOCAL app.tenant_id = '11111111-1111-1111-1111-111111111111'; SELECT count(*) FROM pipelines; COMMIT;"
```

Expected: only the dev tenant's pipelines.

- [ ] **Step 4: Commit**

```bash
git add crates/catalog/migrations/0006_rls_policies.sql
git commit -m "feat(catalog): migration 0006 — RLS policies on every tenant-scoped table"
```

---

## Task 4: `Catalog::connect_app` + `with_tenant` helper

**Files:**
- Modify: `crates/catalog/src/lib.rs`
- Create: `crates/catalog/src/tenant_context.rs`
- Modify: `crates/common-types/src/ids.rs`

- [ ] **Step 1: Add `TenantContext` to common-types**

In `crates/common-types/src/ids.rs`, append:

```rust
/// Identity carried through every cross-component call. For Phase II.1
/// it just wraps a TenantId; Phase II.2 adds principal/role/etc.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantContext {
    pub tenant_id: TenantId,
}

impl TenantContext {
    pub fn new(tenant_id: TenantId) -> Self { Self { tenant_id } }
    pub fn admin() -> Option<Self> { None }
}
```

- [ ] **Step 2: Re-export from catalog**

`crates/catalog/src/tenant_context.rs`:

```rust
pub use common_types::ids::TenantContext;
```

In `crates/catalog/src/lib.rs`, add `pub mod tenant_context;` and `pub use tenant_context::TenantContext;`.

- [ ] **Step 3: Add `Catalog::connect_app` + `with_tenant`**

In `crates/catalog/src/lib.rs`, find the existing `connect` function and add below it:

```rust
    /// Connect as the non-superuser `etl_app` role. RLS policies are
    /// enforced for this role.
    pub async fn connect_app(url_for_etl_app: &str) -> sqlx::Result<Self> {
        let pool = PgPool::connect(url_for_etl_app).await?;
        Ok(Self { pool })
    }

    /// Run `f` inside a transaction with `app.tenant_id` set. Pass
    /// `None` for admin mode (sees all tenants — tenant create/list
    /// only).
    pub async fn with_tenant<F, R>(
        &self,
        ctx: Option<TenantContext>,
        f: F,
    ) -> sqlx::Result<R>
    where
        F: for<'c> FnOnce(
                &'c mut sqlx::Transaction<'_, sqlx::Postgres>,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = sqlx::Result<R>> + Send + 'c>,
            > + Send,
        R: Send,
    {
        let mut tx = self.pool.begin().await?;
        let tenant_lit = ctx
            .map(|c| format!("'{}'", c.tenant_id.as_uuid()))
            .unwrap_or_else(|| "''".to_string());
        let stmt = format!("SET LOCAL app.tenant_id = {tenant_lit}");
        sqlx::query(&stmt).execute(&mut *tx).await?;
        let r = f(&mut tx).await?;
        tx.commit().await?;
        Ok(r)
    }
```

- [ ] **Step 4: Verify build (existing call sites unchanged for now)**

Run: `cargo build -p catalog`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/catalog/src/ crates/common-types/src/ids.rs
git commit -m "feat(catalog): TenantContext + connect_app + with_tenant"
```

---

## Task 5: Convert tenant + workspace + connection CRUD to use `with_tenant`

**Files:**
- Modify: `crates/catalog/src/tenant.rs`
- Modify: `crates/catalog/src/workspace.rs`
- Modify: `crates/catalog/src/connection.rs`
- Modify: `crates/catalog/src/lib.rs`

The catalog is currently a flat `&self` API. Convert each method to take `&mut Transaction` and add a `with_tenant`-wrapped public form on `Catalog`.

- [ ] **Step 1: tenant.rs — admin-mode-only ops**

Replace `crates/catalog/src/tenant.rs` body with:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::TenantId;
use sqlx::Postgres;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Tenant {
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

pub async fn create(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    name: &str,
) -> sqlx::Result<TenantId> {
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants(tenant_id, name) VALUES ($1,$2)")
        .bind(id)
        .bind(name)
        .execute(&mut **tx)
        .await?;
    Ok(TenantId::from_uuid_unchecked(id))
}

pub async fn list(
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> sqlx::Result<Vec<Tenant>> {
    let rows: Vec<(Uuid, String, DateTime<Utc>)> =
        sqlx::query_as("SELECT tenant_id, name, created_at FROM tenants ORDER BY created_at")
            .fetch_all(&mut **tx)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, created_at)| Tenant {
            tenant_id: TenantId::from_uuid_unchecked(id),
            name,
            created_at,
        })
        .collect())
}

pub async fn get_by_name(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    name: &str,
) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, created_at FROM tenants WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|(id, name, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(id),
        name,
        created_at,
    }))
}

pub async fn delete(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: TenantId,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
        .bind(id.as_uuid())
        .execute(&mut **tx)
        .await?;
    Ok(())
}
```

- [ ] **Step 2: Update `Catalog` API methods**

In `crates/catalog/src/lib.rs`, replace the existing `create_tenant` etc.:

```rust
    pub async fn create_tenant(&self, name: &str) -> sqlx::Result<TenantId> {
        // Admin mode — tenants table is RLS-policy permissive when app.tenant_id is NULL.
        self.with_tenant(None, |tx| {
            let name = name.to_string();
            Box::pin(async move { tenant::create(tx, &name).await })
        })
        .await
    }

    pub async fn list_tenants(&self) -> sqlx::Result<Vec<tenant::Tenant>> {
        self.with_tenant(None, |tx| Box::pin(async move { tenant::list(tx).await }))
            .await
    }

    pub async fn get_tenant_by_name(&self, name: &str) -> sqlx::Result<Option<tenant::Tenant>> {
        self.with_tenant(None, |tx| {
            let name = name.to_string();
            Box::pin(async move { tenant::get_by_name(tx, &name).await })
        })
        .await
    }

    pub async fn delete_tenant(&self, id: TenantId) -> sqlx::Result<()> {
        self.with_tenant(None, |tx| {
            Box::pin(async move { tenant::delete(tx, id).await })
        })
        .await
    }
```

- [ ] **Step 3: Convert workspace.rs the same way**

Replace `crates/catalog/src/workspace.rs` `ensure_default` to take `tx`:

```rust
pub async fn ensure_default(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: TenantId,
) -> sqlx::Result<WorkspaceId> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT workspace_id FROM workspaces WHERE tenant_id = $1 AND name = 'default'",
    )
    .bind(tenant_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((id,)) = row {
        return Ok(WorkspaceId::from_uuid_unchecked(id));
    }
    let new_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO workspaces(workspace_id, tenant_id, name) VALUES ($1,$2,'default') \
         ON CONFLICT (tenant_id, name) DO NOTHING",
    )
    .bind(new_id)
    .bind(tenant_id.as_uuid())
    .execute(&mut **tx)
    .await?;
    let row: (Uuid,) = sqlx::query_as(
        "SELECT workspace_id FROM workspaces WHERE tenant_id = $1 AND name = 'default'",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(&mut **tx)
    .await?;
    Ok(WorkspaceId::from_uuid_unchecked(row.0))
}
```

- [ ] **Step 4: Convert connection.rs**

`crates/catalog/src/connection.rs` — `create` becomes:

```rust
pub async fn create(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    new: NewConnection,
) -> sqlx::Result<ConnectionId> {
    let workspace_id = crate::workspace::ensure_default(tx, new.tenant_id).await?;
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO connections (connection_id, tenant_id, workspace_id, name, connector_ref, config) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(id)
    .bind(new.tenant_id.as_uuid())
    .bind(workspace_id.as_uuid())
    .bind(&new.name)
    .bind(&new.connector_ref)
    .bind(&new.config)
    .execute(&mut **tx)
    .await?;
    Ok(ConnectionId::from_uuid_unchecked(id))
}
```

`get` likewise:

```rust
pub async fn get(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: ConnectionId,
) -> sqlx::Result<Option<Connection>> {
    // ...existing body, with `&self.pool` replaced by `&mut **tx`.
}
```

Update `Catalog::create_connection` / `Catalog::get_connection` to wrap in `with_tenant`:

```rust
    pub async fn create_connection(&self, new: NewConnection) -> sqlx::Result<ConnectionId> {
        let ctx = TenantContext::new(new.tenant_id);
        self.with_tenant(Some(ctx), |tx| {
            Box::pin(async move { connection::create(tx, new).await })
        })
        .await
    }

    pub async fn get_connection(
        &self,
        ctx: TenantContext,
        id: ConnectionId,
    ) -> sqlx::Result<Option<connection::Connection>> {
        self.with_tenant(Some(ctx), |tx| {
            Box::pin(async move { connection::get(tx, id).await })
        })
        .await
    }
```

- [ ] **Step 5: Build (don't worry about other tables yet — they break in Task 6)**

Run: `cargo build -p catalog`
Expected: errors in pipeline/run/stream/schema/stream_state/cdc — those are Task 6.

Treat Task 5 + Task 6 as a single commit batch — proceed to Task 6 before committing.

---

## Task 6: Convert pipeline + run + stream + schema + stream_state + cdc CRUD

**Files:**
- Modify: `crates/catalog/src/pipeline.rs`
- Modify: `crates/catalog/src/run.rs`
- Modify: `crates/catalog/src/stream.rs`
- Modify: `crates/catalog/src/schema.rs`
- Modify: `crates/catalog/src/stream_state.rs`
- Modify: `crates/catalog/src/cdc.rs`
- Modify: `crates/catalog/src/lib.rs`

The pattern is identical for each file: free function takes `tx: &mut sqlx::Transaction<'_, sqlx::Postgres>`, replace `&self.pool` / `pool` with `&mut **tx`, and the public `Catalog::xxx` method wraps in `with_tenant`.

- [ ] **Step 1: pipeline.rs**

Apply the pattern to `create` and `get`. Then in `lib.rs`:

```rust
    pub async fn create_pipeline(&self, new: NewPipeline) -> sqlx::Result<PipelineId> {
        let ctx = TenantContext::new(new.tenant_id);
        self.with_tenant(Some(ctx), |tx| {
            Box::pin(async move { pipeline::create(tx, new).await })
        })
        .await
    }

    pub async fn get_pipeline(
        &self,
        ctx: TenantContext,
        id: PipelineId,
    ) -> sqlx::Result<Option<pipeline::Pipeline>> {
        self.with_tenant(Some(ctx), |tx| {
            Box::pin(async move { pipeline::get(tx, id).await })
        })
        .await
    }
```

- [ ] **Step 2: run.rs — same pattern**

Each public `Catalog` method (`create_run`, `mark_run_running`, `mark_run_completed`, `mark_run_failed`) gains a `ctx: TenantContext` first parameter. The `mark_run_*` methods need the tenant — fetch it from the runs row (one extra SELECT) or pass it from the workflow input. Pass from workflow input — cleaner:

```rust
    pub async fn mark_run_completed(
        &self,
        ctx: TenantContext,
        id: RunId,
    ) -> sqlx::Result<()> {
        self.with_tenant(Some(ctx), |tx| {
            Box::pin(async move { run::mark_completed(tx, id).await })
        })
        .await
    }
```

- [ ] **Step 3: stream.rs, schema.rs, stream_state.rs, cdc.rs**

Same conversion. `stream_state::upsert` and `cdc::upsert` take `&mut Transaction` instead of `&PgPool`. **`stream_state::upsert` needs to insert `tenant_id`** (added in migration 0005) — bind it from a new `tenant_id` parameter:

```rust
pub async fn upsert(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: common_types::ids::TenantId,
    pipeline_id: PipelineId,
    stream_name: &str,
    cursor: Option<CursorValue>,
    last_run_id: Option<RunId>,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO stream_state \
           (pipeline_id, stream_name, tenant_id, cursor_kind, cursor_value, last_run_id, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6, now()) \
         ON CONFLICT (pipeline_id, stream_name) DO UPDATE SET \
           cursor_kind  = EXCLUDED.cursor_kind, \
           cursor_value = EXCLUDED.cursor_value, \
           last_run_id  = COALESCE(EXCLUDED.last_run_id, stream_state.last_run_id), \
           updated_at   = now()",
    )
    .bind(pipeline_id.as_uuid())
    .bind(stream_name)
    .bind(tenant_id.as_uuid())
    .bind(cursor.as_ref().map(|c| match c.kind {
        common_types::cursor::CursorKind::Int64 => "int64",
        common_types::cursor::CursorKind::TimestampTz => "timestamptz",
        common_types::cursor::CursorKind::Lsn => "lsn",
    }))
    .bind(cursor.as_ref().map(|c| c.value.as_str()))
    .bind(last_run_id.map(|r| r.as_uuid()))
    .execute(&mut **tx)
    .await?;
    Ok(())
}
```

Same for `cdc::upsert` — accept `tenant_id` and bind it.

- [ ] **Step 4: Update `Catalog` public methods to accept `ctx: TenantContext`**

In `lib.rs`, every `pub async fn` that touches a tenant-scoped table grows a `ctx: TenantContext` first arg:

```rust
    pub async fn upsert_stream_state(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
        stream_name: &str,
        cursor: Option<CursorValue>,
        last_run_id: Option<RunId>,
    ) -> sqlx::Result<()> {
        self.with_tenant(Some(ctx), |tx| {
            Box::pin(async move {
                stream_state::upsert(tx, ctx.tenant_id, pipeline_id, stream_name, cursor, last_run_id).await
            })
        })
        .await
    }
```

Apply the analogous wrap to: `get_stream_state`, `ensure_stream`, `get_stream_by_name`, `get_latest_schema`, `record_schema`, `get_schema`, `cdc_upsert`, `cdc_get`, `cdc_update_confirmed_flush`.

- [ ] **Step 5: Build the catalog crate alone**

Run: `cargo build -p catalog`
Expected: clean.

- [ ] **Step 6: Commit Tasks 5+6 together**

```bash
git add crates/catalog/ crates/common-types/src/ids.rs
git commit -m "feat(catalog): TenantContext threaded through every CRUD path

Every catalog method now wraps its query in a transaction that issues
SET LOCAL app.tenant_id, so RLS policies enforce isolation. Free
functions take &mut Transaction; public Catalog methods take a
TenantContext arg and call with_tenant. Admin-only ops (tenant CRUD)
pass None."
```

---

## Task 7: Update every call site (worker activities, schema_evolution, CLI)

**Files:**
- Modify: every file that calls a `catalog::Catalog::*` method

The compile errors from Task 6 tell you exactly where. The pattern at every call site:

```rust
// Before:
catalog.get_pipeline(pipeline_id).await?

// After:
catalog.get_pipeline(ctx, pipeline_id).await?
```

Where `ctx` is constructed from the surrounding `tenant_id`:

```rust
let ctx = catalog::TenantContext::new(common_types::ids::TenantId::from_uuid_unchecked(tenant_id));
```

- [ ] **Step 1: Sweep call sites**

```bash
cargo build --workspace 2>&1 | grep -oE "[A-Za-z_]+\.rs:[0-9]+" | sort -u | head -30
```

For each location: open, grab `tenant_id` from the surrounding context, build a `TenantContext`, pass it.

Hot spots to expect (all have a `tenant_id` already in scope):
- `crates/worker/src/activities/sync/mod.rs::discover_stream` — `input.tenant_id`
- `crates/worker/src/activities/run_lifecycle.rs::start_run / complete_run / fail_run` — needs the tenant from somewhere. Add `tenant_id: Uuid` to `FailRunInput`; `start_run` and `complete_run` similarly take `tenant_id` (or look it up — but workflows already know it).
- `crates/worker/src/activities/cdc/mod.rs::ensure_slot / read_window / snapshot_chunk / release_slot` — inputs already have `pipeline_id`; add `tenant_id` to each input struct.
- `crates/worker/src/cdc_monitor.rs` — admin mode (None) so it can iterate all slots.
- `crates/worker/src/schema_evolution.rs::record_and_resolve` — already takes `tenant_id`; just thread to catalog calls.
- `crates/cli/src/main.rs` (`apply_cmd`, `pipeline_run`, `get_cmd`, etc.) — all use `ensure_dev_tenant`. Replace with `resolve_tenant(--tenant-flag)`.

- [ ] **Step 2: Add `tenant_id` to lifecycle activity inputs**

In `crates/worker/src/activities/run_lifecycle.rs`:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StartRunInput {
    pub run_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CompleteRunInput {
    pub run_id: Uuid,
    pub tenant_id: Uuid,
}
```

`start_run` and `complete_run` swap their `Uuid` arg for the new struct. Update `pipeline_run.rs` and `cdc_pipeline.rs` to construct them with the workflow's `input.tenant_id`.

- [ ] **Step 3: Add `tenant_id` to every CDC activity input**

```rust
// In crates/worker/src/activities/cdc/inputs.rs:
pub struct EnsureSlotInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    // ...existing fields
}
// (and the other three input structs)
```

In each activity, build a `TenantContext` and pass to catalog methods:

```rust
        let ctx = catalog::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        catalog::cdc::upsert(&self.catalog, ctx, ...).await?;
```

Wait — `catalog::cdc::upsert` is a free function, but the public path is now `catalog.cdc_upsert(ctx, ...)`. Use the public method.

- [ ] **Step 4: Build until clean**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/
git commit -m "refactor: thread TenantContext through every catalog call site

Every activity, workflow, and CLI command builds a TenantContext from
its surrounding tenant_id and passes it to the catalog. Activity
input structs gain a tenant_id field where they didn't already have
one (run_lifecycle, cdc::*)."
```

---

## Task 8: Wire `Catalog::connect_app` in worker + CLI

**Files:**
- Modify: `crates/worker/src/main.rs`
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/control-api/src/main.rs`
- Modify: `tests/integration/tests/*.rs` (test fixtures)

- [ ] **Step 1: Worker switches to etl_app**

In `crates/worker/src/main.rs`, change:

```rust
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Arc::new(Catalog::connect(&db_url).await?);
```

to:

```rust
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let app_url = std::env::var("DATABASE_URL_APP").unwrap_or_else(|_| {
        // Naively rewrite postgres://etl:etl@... → postgres://etl_app:etl_app@...
        db_url.replace("etl:etl@", "etl_app:etl_app@")
    });
    let catalog = Arc::new(Catalog::connect_app(&app_url).await?);
```

`migrate()` still needs superuser — keep a separate connection just for the migration run:

```rust
    let admin = Catalog::connect(&db_url).await?;
    admin.migrate().await?;
    drop(admin);
```

- [ ] **Step 2: Same swap in CLI + control-api**

Apply the same `connect_app` pattern to `crates/cli/src/main.rs::main`'s catalog construction (search for `Catalog::connect`). One-off admin commands (`tenant create | list | terminate`) keep using the superuser `connect()` because they need to bypass RLS to provision new rows.

- [ ] **Step 3: Update integration test fixtures**

Each integration test calls `Catalog::connect(&catalog_url())`. Two changes:
1. Use `connect_app` for the in-test catalog handle (so RLS is enforced in tests too — adversarial test catches regressions).
2. Add an `admin_url()` helper for migration + truncate (still superuser).

In `tests/integration/tests/*.rs`, replace:

```rust
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;
```

with:

```rust
    let admin = Catalog::connect(&catalog_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    drop(admin);
    let cat = Catalog::connect_app(&catalog_app_url()).await?;
```

Where the helpers in each test file are:

```rust
fn catalog_app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}
```

- [ ] **Step 4: Build + verify**

```bash
cargo build --workspace
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/ tests/
git commit -m "feat: worker + cli + tests connect as etl_app (RLS enforced)"
```

---

## Task 9: Per-tenant storage prefix

**Files:**
- Modify: `crates/worker/src/loaders/parquet_local.rs`
- Modify: `crates/worker/src/loaders/cdc_parquet.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs` (dead-letter path)
- Modify: `crates/worker/src/activities/sync/inputs.rs` (LoadBatchInput.tenant_id)
- Modify: `crates/worker/src/activities/cdc/mod.rs` (snapshot_chunk + read_window)
- Modify: `crates/worker/src/loaders/parquet_local.rs::tests`
- Modify: every integration test that reads `<tmp>/<pipeline_id>/`

- [ ] **Step 1: Loader signatures gain `tenant_id`**

In `crates/worker/src/loaders/parquet_local.rs`, the `LoadId` already has fields. Add tenant:

Search for `pub struct LoadId` in `loader-sdk`. Add `pub tenant_id: TenantId`. Rebuild call sites.

The path becomes:

```rust
fn target_path(spec: &LocalParquetSpec, load_id: &LoadId) -> PathBuf {
    let mut p = PathBuf::from(&spec.base_path);
    p.push(load_id.tenant_id.as_uuid().to_string());
    p.push(load_id.pipeline_id.as_uuid().to_string());
    p.push(format!("batch-{:05}.parquet", load_id.batch_seq));
    p
}
```

- [ ] **Step 2: CDC loader path**

In `crates/worker/src/loaders/cdc_parquet.rs::write`:

```rust
    let mut path = PathBuf::from(&base);
    path.push(tenant_id.to_string());
    path.push(pipeline_id.to_string());
    path.push("cdc");
    path.push(run_id.to_string());
```

Add `tenant_id: Uuid` arg to `CdcParquetLoader::write`.

- [ ] **Step 3: Dead-letter path**

In `crates/worker/src/activities/sync/mod.rs::load_batch`, the path computation:

```rust
                        let mut p = std::path::PathBuf::from(&s.base_path);
                        p.push(input.tenant_id.to_string());
                        p.push(load_id.pipeline_id.as_uuid().to_string());
                        p.push("dead-letter");
                        p.push(load_id.run_id.as_uuid().to_string());
```

Add `tenant_id: Uuid` to `LoadBatchInput`.

- [ ] **Step 4: Update integration tests' path expectations**

For each test that walks `tmp.path()/<pipeline_id_uuid>/`, update to `tmp.path()/<tenant_id_uuid>/<pipeline_id_uuid>/`. Since the dev tenant is `11111111-1111-1111-1111-111111111111` and tests use it, this is mechanical.

Do this for: `transforms_filter_mask.rs`, `transforms_dead_letter.rs`, `cdc_insert_update_delete.rs`, `cdc_snapshot_streaming_handoff.rs`.

`incremental_sync.rs`, `schema_evolution.rs`, `durability_midbatch.rs` — check whether they assert the path; if not, no change.

- [ ] **Step 5: Build + run unit tests**

Run: `cargo test --workspace --lib`
Expected: 78+ unit tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/ tests/
git commit -m "feat: storage prefix is now <base>/<tenant_id>/<pipeline_id>/...

LoadId, LoadBatchInput, and CdcParquetLoader all carry tenant_id.
Tests updated to walk the new path."
```

---

## Task 10: Tenant lifecycle CLI

**Files:**
- Create: `crates/cli/src/tenant.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Write the module**

`crates/cli/src/tenant.rs`:

```rust
use anyhow::Context;
use catalog::{Catalog, TenantContext};
use common_types::ids::TenantId;

/// Create a tenant: catalog row + Temporal namespace + storage prefix
/// will be created lazily on first use.
pub async fn create(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let id = admin.create_tenant(&name).await?;
    println!("created tenant {} ({})", name, id);
    register_temporal_namespace(&id).await?;
    Ok(())
}

pub async fn list() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let tenants = admin.list_tenants().await?;
    for t in tenants {
        println!("{}\t{}\t{}", t.tenant_id, t.name, t.created_at);
    }
    Ok(())
}

pub async fn suspend(name: String) -> anyhow::Result<()> {
    // MVP: rename tenant to "suspended:<name>" so the CLI can't resolve
    // it for new runs. Existing runs continue. Phase II.2 adds a
    // proper status column.
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let pool = admin.pool();
    sqlx::query("UPDATE tenants SET name = 'suspended:' || name WHERE name = $1 AND name NOT LIKE 'suspended:%'")
        .bind(&name)
        .execute(pool)
        .await?;
    println!("suspended tenant {}", name);
    Ok(())
}

pub async fn terminate(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin
        .get_tenant_by_name(&name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", name))?;
    // Catalog rows: ON DELETE CASCADE on every FK takes care of
    // pipelines/runs/streams/schemas/cdc_slots/stream_state.
    admin.delete_tenant(t.tenant_id).await?;
    println!("terminated tenant {} ({})", name, t.tenant_id);
    // Storage cleanup: ./data/<tenant_id>/...
    let base = std::env::var("ETL_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let path = std::path::PathBuf::from(&base).join(t.tenant_id.as_uuid().to_string());
    if path.exists() {
        std::fs::remove_dir_all(&path)
            .with_context(|| format!("removing {}", path.display()))?;
        println!("removed {}", path.display());
    }
    // Temporal namespace: deprecate (Temporal doesn't support delete-namespace by default).
    println!("note: Temporal namespace etl-{} deprecated — manual purge required for full cleanup",
             t.tenant_id.as_uuid().simple());
    Ok(())
}

async fn register_temporal_namespace(id: &TenantId) -> anyhow::Result<()> {
    use temporalio_client::{
        protos::temporal::api::workflowservice::v1::RegisterNamespaceRequest,
        WorkflowService,
    };
    let cfg = worker::temporal::TemporalConfig::from_env()?;
    let mut client = worker::temporal::make_client(&cfg).await?;
    let ns = format!("etl-{}", id.as_uuid().simple());
    let req = RegisterNamespaceRequest {
        namespace: ns.clone(),
        description: format!("Per-tenant namespace for {id}"),
        workflow_execution_retention_period: Some(prost_wkt_types::Duration {
            seconds: 7 * 24 * 3600,
            nanos: 0,
        }),
        ..Default::default()
    };
    match client.workflow_service().register_namespace(req).await {
        Ok(_) => println!("registered Temporal namespace {ns}"),
        Err(s) if s.code() == tonic::Code::AlreadyExists => {
            println!("Temporal namespace {ns} already exists")
        }
        Err(s) => return Err(anyhow::anyhow!("register_namespace: {s}")),
    }
    Ok(())
}
```

- [ ] **Step 2: Wire subcommand**

In `crates/cli/src/main.rs`:

```rust
mod tenant;
```

Inside `enum Cmd`:

```rust
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
```

Below `enum WorkflowCmd`:

```rust
#[derive(Subcommand)]
enum TenantCmd {
    Create { name: String },
    List,
    Suspend { name: String },
    Terminate { name: String },
}
```

In the match arm:

```rust
        Cmd::Tenant { cmd } => match cmd {
            TenantCmd::Create { name } => tenant::create(name).await,
            TenantCmd::List => tenant::list().await,
            TenantCmd::Suspend { name } => tenant::suspend(name).await,
            TenantCmd::Terminate { name } => tenant::terminate(name).await,
        },
```

- [ ] **Step 3: Smoke**

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  TEMPORAL_ADDRESS=127.0.0.1:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TASK_QUEUE=pipeline-default \
  ./target/debug/platform tenant create acme
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ./target/debug/platform tenant list
```

Expected: "created tenant acme (UUID)" + "registered Temporal namespace etl-…", then list shows acme + dev.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/
git commit -m "feat(cli): platform tenant create | list | suspend | terminate"
```

---

## Task 11: Worker reads `--tenant` / `TEMPORAL_NAMESPACE` per pipeline

**Files:**
- Modify: `crates/cli/src/main.rs::pipeline_run`
- Modify: `crates/worker/src/temporal.rs`

For Phase II.1 MVP we keep the worker on a single namespace at boot time and let the **CLI** override the start-workflow namespace per call. Multi-namespace workers are deferred.

- [ ] **Step 1: `make_client_for_namespace`**

In `crates/worker/src/temporal.rs`, add:

```rust
pub async fn make_client_for_namespace(
    cfg: &TemporalConfig,
    namespace: &str,
) -> anyhow::Result<temporalio_client::Client> {
    let mut cfg = cfg.clone();
    cfg.namespace = namespace.to_string();
    make_client(&cfg).await
}
```

- [ ] **Step 2: CLI uses tenant's namespace**

In `crates/cli/src/main.rs::pipeline_run`, after fetching the pipeline:

```rust
    let namespace = format!("etl-{}", pipeline.tenant_id.as_uuid().simple());
    let cfg = TemporalConfig::from_env()?;
    let client = worker::temporal::make_client_for_namespace(&cfg, &namespace).await?;
```

(Replace the existing `make_client(&cfg).await?`.)

- [ ] **Step 3: Worker registers under each known namespace**

In `crates/worker/src/main.rs`, after migrate but before constructing the worker, list tenants and start one Temporal worker per namespace:

```rust
    use catalog::Catalog;
    let tenants = catalog.list_tenants().await?;
    let runtime = make_runtime()?;
    let task_queue = cfg.task_queue.clone();

    let mut workers = Vec::new();
    for t in tenants {
        let ns = format!("etl-{}", t.tenant_id.as_uuid().simple());
        let mut ns_cfg = cfg.clone();
        ns_cfg.namespace = ns.clone();
        let ns_client = make_client(&ns_cfg).await?;
        // Build same WorkerOptions as before but with this client/namespace:
        let opts = WorkerOptions::new(task_queue.clone())
            .task_types(WorkerTaskTypes::all())
            .deployment_options(/* ... */)
            .register_activities(lifecycle.clone())
            .register_activities(sync.clone())
            .register_activities(cdc.clone())
            .register_workflow::<PipelineRunWorkflow>()
            .register_workflow::<CdcPipelineWorkflow>()
            .build();
        let mut w = Worker::new(&runtime, ns_client, opts)
            .map_err(|e| anyhow::anyhow!("Worker::new[{ns}]: {e}"))?;
        tracing::info!(%ns, "worker polling namespace");
        workers.push(tokio::spawn(async move {
            let _ = w.run().await;
        }));
    }
    futures::future::join_all(workers).await;
```

> Note: `RunLifecycleActivities`, `SyncActivities`, `CdcActivities` are not currently `Clone`. Wrap the inner state in `Arc` and add `#[derive(Clone)]`. Each activity struct already holds `Arc<Catalog>` — cloning is cheap.

- [ ] **Step 4: Build + smoke**

Run a pipeline against the dev tenant; the worker should log "worker polling namespace etl-1111…1111".

- [ ] **Step 5: Commit**

```bash
git add crates/cli/ crates/worker/
git commit -m "feat: per-tenant Temporal namespace; worker polls one per known tenant"
```

---

## Task 12: Tenant_id labels on metrics

**Files:**
- Modify: `crates/worker/src/activities/run_lifecycle.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/cdc/mod.rs`
- Modify: `crates/worker/src/cdc_monitor.rs`
- Modify: `ops/grafana/dashboards/etl-overview.json`

- [ ] **Step 1: Add the label everywhere**

Every `metrics::counter!(NAME)` call becomes:

```rust
metrics::counter!(NAME, "tenant_id" => input.tenant_id.to_string()).increment(1);
```

Hot spots:
- `run_lifecycle::start_run / complete_run / fail_run` — input now has `tenant_id`.
- `sync::read_batch / load_batch` — input now has `tenant_id`.
- `cdc::snapshot_chunk / read_window` — input has `tenant_id` (added in Task 7).

Slot-lag gauge already has `pipeline_id`; add `tenant_id`:

```rust
metrics::gauge!(
    crate::metrics::CDC_SLOT_LAG_BYTES,
    "pipeline_id" => pipeline_id.to_string(),
    "tenant_id" => tenant_id.to_string(),
)
.set(lag as f64);
```

The poller's catalog query needs to fetch `tenant_id`:

```rust
"SELECT pipeline_id, slot_name, tenant_id FROM cdc_slots WHERE state = 'active'"
```

- [ ] **Step 2: Grafana variable**

Edit `ops/grafana/dashboards/etl-overview.json` and add a `templating.list[]` entry:

```json
"templating": {
  "list": [
    {
      "name": "tenant",
      "label": "Tenant",
      "type": "query",
      "datasource": "Prometheus",
      "query": "label_values(etl_runs_started_total, tenant_id)",
      "includeAll": true,
      "multi": true
    }
  ]
},
```

Update each panel's `targets[].expr` to filter by `{tenant_id=~"$tenant"}`:

```json
{"expr": "sum(etl_runs_started_total{tenant_id=~\"$tenant\"})", "refId": "A"}
```

(Apply to all six panels.)

- [ ] **Step 3: Reload Grafana**

```bash
docker-compose restart grafana
sleep 4
```

- [ ] **Step 4: Commit**

```bash
git add crates/ ops/grafana/
git commit -m "feat(observability): tenant_id label on every metric + Grafana variable"
```

---

## Task 13: Adversarial cross-tenant test

**Files:**
- Create: `tests/integration/tests/tenant_isolation.rs`

- [ ] **Step 1: Write the test**

```rust
//! Phase II.1: adversarial cross-tenant isolation.
//!
//! Two tenants. Each creates a connection + pipeline. Verifies:
//! - Tenant A's catalog query (with TenantContext A) cannot see B's rows
//! - Direct `psql` as etl_app with `app.tenant_id` set to A returns
//!   only A's rows, even with explicit WHERE-on-B-id queries

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, TenantContext};
use serde_json::json;

fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn cross_tenant_reads_blocked() -> anyhow::Result<()> {
    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    drop(admin);

    let admin = Catalog::connect(&admin_url()).await?;
    let tenant_a = admin.create_tenant("acme").await?;
    let tenant_b = admin.create_tenant("globex").await?;

    let cat = Catalog::connect_app(&app_url()).await?;
    let ctx_a = TenantContext::new(tenant_a);
    let ctx_b = TenantContext::new(tenant_b);

    // Each tenant creates one connection + one pipeline.
    let conn_a = cat
        .create_connection(NewConnection {
            tenant_id: tenant_a,
            name: "src-a".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": "postgres://x" }),
        })
        .await?;
    let pipe_a = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_a,
            name: "pipe-a".into(),
            source_conn_id: conn_a,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    let conn_b = cat
        .create_connection(NewConnection {
            tenant_id: tenant_b,
            name: "src-b".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": "postgres://y" }),
        })
        .await?;
    let pipe_b = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_b,
            name: "pipe-b".into(),
            source_conn_id: conn_b,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    // Tenant A's context must NOT see tenant B's pipeline, even by ID.
    let leak = cat.get_pipeline(ctx_a, pipe_b).await?;
    assert!(leak.is_none(), "tenant A read tenant B's pipeline {pipe_b}");

    // Tenant B's context must NOT see tenant A's pipeline.
    let leak = cat.get_pipeline(ctx_b, pipe_a).await?;
    assert!(leak.is_none(), "tenant B read tenant A's pipeline {pipe_a}");

    // Sanity: each tenant sees its own.
    assert!(cat.get_pipeline(ctx_a, pipe_a).await?.is_some());
    assert!(cat.get_pipeline(ctx_b, pipe_b).await?.is_some());

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn cross_tenant_updates_blocked() -> anyhow::Result<()> {
    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;

    let tenant_a = admin.create_tenant("acme").await?;
    let tenant_b = admin.create_tenant("globex").await?;

    let cat = Catalog::connect_app(&app_url()).await?;
    let ctx_b = TenantContext::new(tenant_b);

    let conn_a = cat
        .create_connection(NewConnection {
            tenant_id: tenant_a,
            name: "src-a".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": "postgres://x" }),
        })
        .await?;
    let pipe_a = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_a,
            name: "pipe-a".into(),
            source_conn_id: conn_a,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    // Tenant B tries to mark a tenant-A run failed via direct SQL through the
    // catalog handle. RLS should reject (UPDATE returns 0 rows).
    let pool = cat.pool();
    let rows = sqlx::query(
        "BEGIN; \
         SET LOCAL app.tenant_id = $1; \
         UPDATE pipelines SET name = 'hijacked' WHERE pipeline_id = $2; \
         COMMIT;",
    )
    .bind(tenant_b.as_uuid())
    .bind(pipe_a.as_uuid())
    .execute(pool)
    .await
    .context("multi-statement update")?;
    assert_eq!(rows.rows_affected(), 0, "RLS allowed cross-tenant UPDATE");

    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test tenant_isolation -- --ignored --nocapture
```

Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/tenant_isolation.rs
git commit -m "test(integration): cross-tenant reads + updates blocked by RLS"
```

---

## Task 14: Tenant lifecycle integration test

**Files:**
- Create: `tests/integration/tests/tenant_lifecycle.rs`

- [ ] **Step 1: Test code**

```rust
//! Phase II.1: tenant lifecycle. create → seed pipeline → run →
//! verify Parquet under <tenant_id>/<pipeline_id>/ → terminate →
//! verify catalog rows + storage gone.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, TenantContext};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn admin_url() -> String { std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into()) }
fn app_url() -> String   { std::env::var("DATABASE_URL_APP").unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into()) }
fn workspace_root() -> PathBuf { let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR")); p.pop(); p.pop(); p }

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn tenant_lifecycle_provisions_and_terminates() -> anyhow::Result<()> {
    let st = Command::new("cargo").current_dir(workspace_root()).args(["build","--workspace"]).status().await?;
    assert!(st.success());

    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    let tenant_id = admin.create_tenant("lifecycle-test").await?;

    let cat = Catalog::connect_app(&app_url()).await?;
    let ctx = TenantContext::new(tenant_id);

    let src = cat.create_connection(NewConnection {
        tenant_id, name: "src".into(),
        connector_ref: "postgres@0.1.0".into(),
        config: json!({ "url": "postgres://etl:etl@localhost:5432/etl_source_demo" }),
    }).await?;

    let tmp = tempfile::tempdir()?;
    let spec = json!({
        "source": { "type":"postgres","schema":"public","table":"customers",
                    "cursor_column":"updated_at","cursor_kind":"timestamp_tz","pk_columns":["id"] },
        "destination": { "type":"local_parquet","base_path": tmp.path().to_string_lossy() },
        "batch_size": 4,
        "evolution_policy": "propagate_additive",
    });
    let pipe = cat.create_pipeline(NewPipeline {
        tenant_id, name: "lifecycle-pipe".into(),
        source_conn_id: src, dest_conn_id: None, spec,
    }).await?;

    // Verify storage path: <tmp>/<tenant_id>/<pipeline_id>/ does NOT yet exist.
    let mut tenant_dir = tmp.path().to_path_buf();
    tenant_dir.push(tenant_id.as_uuid().to_string());
    assert!(!tenant_dir.exists());

    // Worker + run.
    let mut w = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", admin_url())
        .env("DATABASE_URL_APP", app_url())
        .env("TEMPORAL_ADDRESS","127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", format!("etl-{}", tenant_id.as_uuid().simple()))
        .env("TEMPORAL_TASK_QUEUE","pipeline-default")
        .current_dir(workspace_root())
        .spawn().context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    Command::new(cargo_bin("platform"))
        .args(["pipeline","run", &pipe.to_string()])
        .env("DATABASE_URL", admin_url())
        .env("DATABASE_URL_APP", app_url())
        .env("TEMPORAL_ADDRESS","127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", format!("etl-{}", tenant_id.as_uuid().simple()))
        .env("TEMPORAL_TASK_QUEUE","pipeline-default")
        .current_dir(workspace_root())
        .output().await?;

    // Wait for ≥1 parquet under the new prefix.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if walkdir::WalkDir::new(&tenant_dir).into_iter().flatten()
            .any(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(tenant_dir.exists() && tenant_dir.read_dir()?.next().is_some(),
            "no parquet at {}", tenant_dir.display());

    w.kill().await?; w.wait().await?;

    // Terminate via CLI.
    let out = Command::new(cargo_bin("platform"))
        .args(["tenant","terminate","lifecycle-test"])
        .env("DATABASE_URL", admin_url())
        .env("ETL_DATA_DIR", tmp.path().to_string_lossy().into_owned())
        .env("TEMPORAL_ADDRESS","127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE","default")
        .env("TEMPORAL_TASK_QUEUE","pipeline-default")
        .current_dir(workspace_root())
        .output().await?;
    assert!(out.status.success(), "terminate failed: {}", String::from_utf8_lossy(&out.stderr));

    // Catalog row gone.
    let admin2 = Catalog::connect(&admin_url()).await?;
    assert!(admin2.get_tenant_by_name("lifecycle-test").await?.is_none());
    // Storage gone.
    assert!(!tenant_dir.exists());

    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test tenant_lifecycle -- --ignored --nocapture
```

Expected: pass within 90s.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/tenant_lifecycle.rs
git commit -m "test(integration): tenant lifecycle (create → run → terminate)"
```

---

## Task 15: README + completion log + regression sweep

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-25-phase-2-1-multi-tenancy.md` (append log)

- [ ] **Step 1: README section**

Replace the current "Phase" line with:

```markdown
Currently: **Phase II.1 — multi-tenancy turned real (complete)**. Next: **Phase II.2 — secrets, auth, security**.

## Multi-tenancy (Phase II.1)

```bash
# Provision a tenant — catalog row + Temporal namespace etl-<uuid> + storage prefix
cargo run --bin platform -- tenant create acme

# List
cargo run --bin platform -- tenant list

# Wind down (catalog cascade + ./data/<tenant_id>/ deletion)
cargo run --bin platform -- tenant terminate acme
```

Each tenant's pipelines run in a Temporal namespace named `etl-<tenant_id_simple>`, write Parquet under `./data/<tenant_id>/<pipeline_id>/...`, and catalog rows are isolated by Postgres RLS (`app_tenant_id()` policy). The worker connects as the non-superuser `etl_app` role; admin paths (migrations, tenant CRUD) keep using the superuser `etl`.

Grafana dashboard gains a `tenant` template variable filtering every panel.
```

- [ ] **Step 2: Append completion log**

To `docs/superpowers/plans/2026-04-25-phase-2-1-multi-tenancy.md`:

```markdown
---

## Phase II.1 Completion Log

Completed YYYY-MM-DD on branch `phase-2-1-multi-tenancy`.

- [x] Task 1  — etl_app non-superuser role
- [x] Task 2  — Migration 0005 (tenant_id on cdc_slots + stream_state)
- [x] Task 3  — Migration 0006 (RLS policies on every tenant-scoped table)
- [x] Task 4  — TenantContext + connect_app + with_tenant
- [x] Tasks 5–6 — every catalog CRUD function uses with_tenant
- [x] Task 7  — every call site threads TenantContext
- [x] Task 8  — worker + cli + tests connect as etl_app
- [x] Task 9  — storage prefix <base>/<tenant_id>/<pipeline_id>/...
- [x] Task 10 — platform tenant create | list | suspend | terminate
- [x] Task 11 — per-tenant Temporal namespace
- [x] Task 12 — tenant_id metric labels + Grafana variable
- [x] Task 13 — cross-tenant adversarial test (reads + updates blocked)
- [x] Task 14 — tenant lifecycle integration test
- [x] Task 15 — README + this log

### Exit criterion — MET

- Adversarial test passes: tenant A's TenantContext cannot read or
  modify tenant B's rows even with explicit ID. RLS enforced at the
  DB layer via the `app_tenant_id()` policy and a non-superuser app role.
- Tenant lifecycle works end-to-end: create → run → terminate, with
  catalog cascade + storage deletion.
- Per-tenant Temporal namespace verified by `tenant_lifecycle` test.
- Per-tenant storage prefix verified by every existing integration test
  (each updated to walk `<tenant_id>/<pipeline_id>/`).
- Metrics carry `tenant_id` label; Grafana panels filter by it.
- All 80+ unit + 12 integration tests green.

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.2

Phase II.2 (secrets, auth, security) picks up from:
- `TenantContext` is the seam — extend with `principal_id` + `roles`.
- `etl_app` role is in place; auth swaps the bearer for a per-request user.
- RLS is the line of defence; auth ensures the right session var is set.

Era II open after II.1:
- Auth: JWT, sessions, RBAC (II.2)
- Secrets backend: env-var → sealed-secrets → Vault (II.2)
- Connector + destination expansion (II.3)
- Customer-facing observability + lineage (II.5)
```

- [ ] **Step 3: Regression sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo -c \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
docker exec etl-temporal-postgres psql -U temporal -d temporal -c \
  "DELETE FROM executions WHERE namespace_id IN (SELECT id FROM namespaces WHERE name='default');" || true

cargo test --workspace --lib    # 80+ unit tests
cargo test -p cli              # cli unit tests
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-25-phase-2-1-multi-tenancy.md
git commit -m "docs: Phase II.1 README + completion log"
```

Then use the finishing-a-development-branch skill to push and open a PR.

---

## Appendix A — Operational notes

**RLS bypass when something looks like it should be filtered.** Two ways
this happens:
1. Connection is `etl` (superuser, RLS-bypassed). Use `\du etl_app` to
   confirm, and check the `Catalog::connect` vs `connect_app` call.
2. `app.tenant_id` is unset — policy treats NULL as admin. Verify with
   `SELECT current_setting('app.tenant_id', true)` inside your transaction.

**`SET LOCAL` is per-transaction.** `with_tenant` opens a transaction;
DDL or auto-commit operations bypass it. If you see RLS misfiring, you're
probably outside a tx.

**`FORCE ROW LEVEL SECURITY` matters.** Without it, the table owner
(usually `etl`) bypasses policies even when reading via etl_app. We
keep `etl` as owner so migrations work; `FORCE` makes the policy
apply to the owner too.

**Temporal namespace deletion is asynchronous.** `terminate` deprecates
the namespace; full purge requires `tctl namespace delete` and is
manual in MVP.

**Workspace dependency on Postgres init order.** The new role script
runs only on a fresh container. Re-running migrations against an
existing container won't re-create the role — `Task 1` documents the
volume nuke.

**Test parallelism + RLS.** Integration tests run with `--test-threads=1`
because `truncate_all_for_tests` and tenant creation collide otherwise.

## Appendix B — What's deferred to later phases

- Auth (JWT, RBAC, scoped tokens) — Phase II.2
- Secrets backend — Phase II.2
- Tenant signup UI — Phase III
- Multi-region tenant placement — Phase III
- Quota/billing — Phase II.5 / III
- Tenant-tier WASM resource limits (different fuel/memory caps per tier) — Phase III
- Multi-namespace single worker that hot-reconfigures on tenant create — Phase II.4
  (MVP: worker boots once and polls each known namespace)
- Tenant suspension as a proper status column (not name-prefix hack) — Phase II.2
- True Temporal namespace deletion in `tenant terminate` — Phase II.2

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-25-phase-2-1-multi-tenancy.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**

---

## Execution log — split into II.1.a + II.1.b

After landing T1–T3 inline (RLS foundation), the remaining catalog API refactor (T4–T8) was estimated at 3–5 hours of mechanical work touching ~30 files. Rather than push through in one session and risk a long compile-error rabbit hole, the plan split:

- **Phase II.1.a** (this branch, `phase-2-1a-rls-foundation`): T1–T3 + adversarial SQL-level test. RLS protective at the DB layer, validated end-to-end.
- **Phase II.1.b** (future plan): T4–T15 — `TenantContext` threading through catalog API, worker + CLI switch to `etl_app`, per-tenant storage prefix, per-tenant Temporal namespace, tenant CLI, metric labels, lifecycle test.

### Phase II.1.a Completion Log

Completed 2026-04-25 on branch `phase-2-1a-rls-foundation`.

- [x] T1 — etl_app non-superuser role on first container boot (init script `db/postgres-init/00-app-role.sql`)
- [x] T2 — Migration 0005: tenant_id NOT NULL on cdc_slots + stream_state
- [x] T3 — Migration 0006: RLS policies + `app_tenant_id()` function on every tenant-scoped table; etl_app GRANTs
- [x] Adversarial integration test: 3 scenarios (cross-tenant SELECT blocked, cross-tenant UPDATE affects 0 rows, INSERT with mismatched tenant_id rejected by WITH CHECK)
- [x] README + this log

### Exit criterion for II.1.a — MET

- 9 tenant-scoped tables have `rowsecurity = t` and `forcerowsecurity = t`.
- `etl_app` role exists with NOSUPERUSER + NOBYPASSRLS.
- `cargo test -p integration-tests --test rls_cross_tenant` proves RLS works at the SQL layer (3 tests pass).
- No regression: existing integration tests still connect as `etl` (superuser, RLS bypassed) and pass — they will be migrated to `etl_app` in Phase II.1.b.

### Why split

- App code currently connects as `etl` (superuser), so RLS is bypassed on the app path. **The RLS layer is dormant for production paths until Phase II.1.b lands.** It is fully active for any code path that uses `etl_app` (the new test).
- Splitting lets II.1.a ship as a focused PR (3 commits, 1 new test) instead of a 30-file refactor.

### Handoff to Phase II.1.b

Start a fresh plan that picks up from T4 (TenantContext + connect_app + with_tenant). The migration files and the test file are already in place — II.1.b just changes how app code interacts with them.
