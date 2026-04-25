# Phase II.1.b — Tenant Threading + Per-Tenant Namespace + Storage Prefix

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Activate the Phase II.1.a RLS layer in production paths by threading `TenantContext` through every catalog method, switching the worker + CLI to connect as the non-superuser `etl_app` role, prefixing object storage with `<tenant_id>/`, running each tenant's pipelines in its own Temporal namespace, and labeling every metric with `tenant_id`.

**Architecture:** Each public `Catalog::*(args)` method gains a `ctx: TenantContext` first parameter and internally opens a transaction, issues `SET LOCAL app.tenant_id = '<uuid>'`, runs the existing query against that transaction, and commits — no async-closure helper, just inline boilerplate per method. Free functions (`pipeline::create`, `run::mark_running`, etc.) take `&mut sqlx::PgConnection` instead of `&PgPool`. The CLI gains `platform tenant {create | list | suspend | terminate}` and threads tenant resolution into `apply / get / pipeline run`. The worker reads `Catalog::list_tenants` at boot and spawns one Temporal worker per known namespace `etl-<tenant_simple>`. `LoadId`, `LoadBatchInput`, and `CdcParquetLoader::write` carry `tenant_id` so loaders write to `<base>/<tenant_id>/<pipeline_id>/...`. Every counter/gauge `metrics::counter!(NAME, "tenant_id" => …)`.

**Tech Stack:** Unchanged — Rust 1.88, sqlx 0.8 (transactions), temporalio-client 0.2 (`register_namespace` is exposed), Arrow/Parquet, wasmtime. No new deps.

---

## File Structure

### Modified
- `crates/catalog/src/lib.rs` — every public method takes `ctx: TenantContext` (or `Option<TenantContext>` for admin paths), opens its own tx + SET LOCAL inline
- `crates/catalog/src/tenant.rs` — `create / get / get_by_name / list / delete` take `&mut PgConnection`
- `crates/catalog/src/workspace.rs` — `ensure_default` takes `&mut PgConnection`
- `crates/catalog/src/connection.rs` — `create / get` take `&mut PgConnection`
- `crates/catalog/src/pipeline.rs` — `create / get` take `&mut PgConnection`
- `crates/catalog/src/run.rs` — `create / mark_running / mark_completed / mark_failed / get` take `&mut PgConnection`
- `crates/catalog/src/stream.rs` — `ensure / get_by_name / set_current_schema` take `&mut PgConnection`
- `crates/catalog/src/schema.rs` — `insert / get / get_latest` take `&mut PgConnection`
- `crates/catalog/src/stream_state.rs` — `upsert / get` take `&mut PgConnection`
- `crates/catalog/src/cdc.rs` — `upsert / get / update_confirmed_flush` take `&mut PgConnection`
- `crates/catalog/tests/crud.rs` — every call site updates to pass `ctx`
- `crates/worker/src/main.rs` — connect as etl_app for app paths; spawn one Temporal worker per known tenant namespace
- `crates/worker/src/activities/run_lifecycle.rs` — Clone derive; `start_run/complete_run/fail_run` accept tenant_id; emit `tenant_id` metric label
- `crates/worker/src/activities/sync/inputs.rs` — `DiscoverInput / ReadBatchInput / LoadBatchInput` already have `tenant_id`; `CommitCursorInput` already has it (Phase II.1.a)
- `crates/worker/src/activities/sync/mod.rs` — Clone derive; thread tenant_id to catalog calls; `tenant_id` metric labels; dead-letter path uses tenant prefix
- `crates/worker/src/activities/cdc/mod.rs` — Clone derive; tenant_id metric labels
- `crates/worker/src/cdc_monitor.rs` — slot-lag gauge gets tenant_id label; query selects tenant_id
- `crates/worker/src/loaders/parquet_local.rs` — path includes tenant_id (already in LoadId? add if not)
- `crates/worker/src/loaders/cdc_parquet.rs` — `write(...)` takes tenant_id
- `crates/loader-sdk/src/lib.rs` — `LoadId` gains `tenant_id: TenantId`
- `crates/worker/src/temporal.rs` — `make_client_for_namespace(cfg, namespace)` helper
- `crates/worker/src/workflows/pipeline_run.rs` — `LoadBatchInput` construction passes `tenant_id`
- `crates/worker/src/workflows/cdc_pipeline.rs` — same
- `crates/cli/src/main.rs` — `--tenant <name>` resolution; `Tenant {Create|List|Suspend|Terminate}` subcommand; `pipeline run` uses tenant namespace
- `crates/cli/src/status.rs` — accept `--tenant`, build TenantContext
- `crates/cli/src/terminate.rs` — same
- `crates/cli/src/dsl.rs` — apply/get/diff/validate need TenantContext
- `crates/control-api/src/main.rs` — connect as etl_app
- `tests/integration/tests/incremental_sync.rs` — admin URL for migrate; app URL for queries; storage path includes tenant_id
- `tests/integration/tests/durability_midbatch.rs` — same
- `tests/integration/tests/schema_evolution.rs` — same
- `tests/integration/tests/dsl_apply.rs` — same
- `tests/integration/tests/wasm_connector.rs` — same
- `tests/integration/tests/transforms_filter_mask.rs` — storage path walks `<tenant>/<pipeline>/`
- `tests/integration/tests/transforms_dead_letter.rs` — same
- `tests/integration/tests/cdc_insert_update_delete.rs` — same
- `tests/integration/tests/cdc_snapshot_streaming_handoff.rs` — same
- `ops/grafana/dashboards/etl-overview.json` — `tenant` template variable + per-panel filter
- `README.md` — Phase II.1.b demo section

### New
- `crates/cli/src/tenant.rs` — `create / list / suspend / terminate` (incl. `RegisterNamespace`)
- `tests/integration/tests/tenant_api_isolation.rs` — cross-tenant test using the actual `Catalog` API (not raw SQL like II.1.a)
- `tests/integration/tests/tenant_lifecycle.rs` — create → run → terminate end-to-end
- `tests/integration/tests/metric_tenant_label.rs` — pipeline run produces `etl_*{tenant_id="…"}` exposition

---

## Task 1: Convert `tenant.rs` + `workspace.rs` free functions

**Files:**
- Modify: `crates/catalog/src/tenant.rs`
- Modify: `crates/catalog/src/workspace.rs`
- Modify: `crates/catalog/src/lib.rs` (`create_tenant`, `get_tenant`, `ensure_default_workspace`, `get_workspace_by_name`)

- [ ] **Step 1: Free fn signatures take `&mut PgConnection`**

`crates/catalog/src/tenant.rs`:

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
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<TenantId> {
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants(tenant_id, name) VALUES ($1, $2)")
        .bind(id)
        .bind(name)
        .execute(&mut *conn)
        .await?;
    Ok(TenantId::from_uuid_unchecked(id))
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    id: TenantId,
) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, created_at FROM tenants WHERE tenant_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(id, name, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(id),
        name,
        created_at,
    }))
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, created_at FROM tenants WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(id, name, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(id),
        name,
        created_at,
    }))
}

pub async fn list(conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Tenant>> {
    let rows: Vec<(Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, created_at FROM tenants ORDER BY created_at",
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(|(id, name, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(id),
        name,
        created_at,
    }).collect())
}

pub async fn delete(
    conn: &mut sqlx::PgConnection,
    id: TenantId,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}
```

`crates/catalog/src/workspace.rs::ensure_default`:

```rust
pub async fn ensure_default(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<WorkspaceId> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT workspace_id FROM workspaces WHERE tenant_id = $1 AND name = 'default'",
    )
    .bind(tenant_id.as_uuid())
    .fetch_optional(&mut *conn)
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
    .execute(&mut *conn)
    .await?;
    let row: (Uuid,) = sqlx::query_as(
        "SELECT workspace_id FROM workspaces WHERE tenant_id = $1 AND name = 'default'",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(&mut *conn)
    .await?;
    Ok(WorkspaceId::from_uuid_unchecked(row.0))
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    name: &str,
) -> sqlx::Result<Option<Workspace>> {
    // ...existing body, replacing pool with &mut *conn.
}
```

(The existing `Workspace` struct is unchanged.)

- [ ] **Step 2: Public `Catalog` methods open tx + SET LOCAL inline**

In `crates/catalog/src/lib.rs`, replace the four affected methods:

```rust
    // -- Tenants (admin: ctx is None; passes empty literal so app_tenant_id() is NULL) --
    pub async fn create_tenant(&self, name: &str) -> sqlx::Result<TenantId> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
        let id = tenant::create(&mut tx, name).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn get_tenant(&self, id: TenantId) -> sqlx::Result<Option<Tenant>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
        let r = tenant::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn get_tenant_by_name(&self, name: &str) -> sqlx::Result<Option<Tenant>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
        let r = tenant::get_by_name(&mut tx, name).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn list_tenants(&self) -> sqlx::Result<Vec<Tenant>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
        let r = tenant::list(&mut tx).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn delete_tenant(&self, id: TenantId) -> sqlx::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
        tenant::delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }

    // -- Workspaces (tenant-scoped) --
    pub async fn ensure_default_workspace(
        &self,
        ctx: TenantContext,
    ) -> sqlx::Result<WorkspaceId> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", ctx.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let r = workspace::ensure_default(&mut tx, ctx.tenant_id).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn get_workspace_by_name(
        &self,
        ctx: TenantContext,
        name: &str,
    ) -> sqlx::Result<Option<workspace::Workspace>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", ctx.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let r = workspace::get_by_name(&mut tx, ctx.tenant_id, name).await?;
        tx.commit().await?;
        Ok(r)
    }
```

Add at the top of `lib.rs`:

```rust
use common_types::ids::TenantContext;
```

(`TenantContext` already exists in common-types from II.1.a.)

- [ ] **Step 3: Build**

Run: `cargo build -p catalog`
Expected: errors in pipeline/run/stream/schema/stream_state/cdc — those are Tasks 2–4.

- [ ] **Step 4: Commit (will defer until Task 4)**

Tasks 1–4 batch together — proceed to Task 2 first.

---

## Task 2: Convert `connection.rs` + `pipeline.rs`

**Files:**
- Modify: `crates/catalog/src/connection.rs`
- Modify: `crates/catalog/src/pipeline.rs`
- Modify: `crates/catalog/src/lib.rs` (`create_connection`, `get_connection`, `create_pipeline`, `get_pipeline`)

- [ ] **Step 1: connection.rs free functions**

```rust
pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewConnection,
) -> sqlx::Result<ConnectionId> {
    let workspace_id = crate::workspace::ensure_default(conn, new.tenant_id).await?;
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
    .execute(&mut *conn)
    .await?;
    Ok(ConnectionId::from_uuid_unchecked(id))
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    id: ConnectionId,
) -> sqlx::Result<Option<Connection>> {
    let row: Option<(Uuid, Uuid, String, String, serde_json::Value, DateTime<Utc>, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT connection_id, tenant_id, name, connector_ref, config, created_at, updated_at \
             FROM connections WHERE connection_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(cid, tid, name, connector_ref, config, created_at, updated_at)| Connection {
        connection_id: ConnectionId::from_uuid_unchecked(cid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        connector_ref,
        config,
        created_at,
        updated_at,
    }))
}
```

- [ ] **Step 2: pipeline.rs same pattern**

`crates/catalog/src/pipeline.rs::create`:

```rust
pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewPipeline,
) -> sqlx::Result<PipelineId> {
    let workspace_id = crate::workspace::ensure_default(conn, new.tenant_id).await?;
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO pipelines (pipeline_id, tenant_id, workspace_id, name, source_conn_id, dest_conn_id, spec) \
         VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(id)
    .bind(new.tenant_id.as_uuid())
    .bind(workspace_id.as_uuid())
    .bind(&new.name)
    .bind(new.source_conn_id.as_uuid())
    .bind(new.dest_conn_id.map(|c| c.as_uuid()))
    .bind(&new.spec)
    .execute(&mut *conn)
    .await?;
    Ok(PipelineId::from_uuid_unchecked(id))
}
```

`get` analogous (replace `pool` with `&mut *conn`).

- [ ] **Step 3: Update `Catalog::create_connection / get_connection / create_pipeline / get_pipeline`**

In `lib.rs`:

```rust
    pub async fn create_connection(
        &self,
        new: NewConnection,
    ) -> sqlx::Result<ConnectionId> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", new.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let id = connection::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn get_connection(
        &self,
        ctx: TenantContext,
        id: ConnectionId,
    ) -> sqlx::Result<Option<Connection>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", ctx.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let r = connection::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn create_pipeline(
        &self,
        new: NewPipeline,
    ) -> sqlx::Result<PipelineId> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", new.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let id = pipeline::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn get_pipeline(
        &self,
        ctx: TenantContext,
        id: PipelineId,
    ) -> sqlx::Result<Option<Pipeline>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", ctx.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let r = pipeline::get(&mut tx, id).await?;
        tx.commit().await?;
        Ok(r)
    }
```

- [ ] **Step 4: Build**

Run: `cargo build -p catalog`
Expected: errors only in run/stream/schema/stream_state/cdc.

---

## Task 3: Convert `run.rs` + `stream.rs` + `schema.rs`

**Files:**
- Modify: `crates/catalog/src/run.rs`
- Modify: `crates/catalog/src/stream.rs`
- Modify: `crates/catalog/src/schema.rs`
- Modify: `crates/catalog/src/lib.rs`

- [ ] **Step 1: run.rs free functions take `&mut PgConnection`**

```rust
pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewRun,
) -> sqlx::Result<RunId> {
    sqlx::query(
        "INSERT INTO runs(run_id, tenant_id, pipeline_id, trigger, status, temporal_workflow_id, started_at) \
         VALUES ($1,$2,$3,$4,'queued',$5, now())",
    )
    .bind(new.run_id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.pipeline_id.as_uuid())
    .bind(&new.trigger)
    .bind(new.temporal_workflow_id)
    .execute(&mut *conn)
    .await?;
    Ok(new.run_id)
}

pub async fn mark_running(
    conn: &mut sqlx::PgConnection,
    id: RunId,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE runs SET status='running' WHERE run_id=$1")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn mark_completed(
    conn: &mut sqlx::PgConnection,
    id: RunId,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE runs SET status='completed', finished_at=now() WHERE run_id=$1")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn mark_failed(
    conn: &mut sqlx::PgConnection,
    id: RunId,
    err: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE runs SET status='failed', finished_at=now(), error_message=$1 WHERE run_id=$2",
    )
    .bind(err)
    .bind(id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    id: RunId,
) -> sqlx::Result<Option<Run>> {
    // ...existing body with `&mut *conn` instead of `pool`.
}
```

- [ ] **Step 2: stream.rs same pattern**

`ensure`, `get_by_name`, `set_current_schema` — each takes `conn: &mut sqlx::PgConnection` first, replace pool with `&mut *conn`.

- [ ] **Step 3: schema.rs same pattern**

`insert`, `get`, `get_latest` — each takes `conn: &mut sqlx::PgConnection`.

- [ ] **Step 4: `lib.rs` public methods**

Add `ctx: TenantContext` to: `create_run`, `mark_run_running`, `mark_run_completed`, `mark_run_failed`, `get_run`, `ensure_stream`, `get_stream_by_name`, `set_stream_current_schema`, `insert_schema`, `get_latest_schema`. Each method body follows the pattern:

```rust
    pub async fn create_run(&self, ctx: TenantContext, new: NewRun) -> sqlx::Result<RunId> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", ctx.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        let id = run::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }
```

For `mark_run_*` the API used to take just `RunId`; now takes `(ctx, RunId)`. Callers in workflows know the tenant from `input.tenant_id`.

For `get_latest_schema`, it currently takes `stream_id: StreamId`; add `ctx: TenantContext` first arg. Callers (schema_evolution module) know tenant from the activity input.

- [ ] **Step 5: Build**

Run: `cargo build -p catalog`
Expected: errors only in stream_state/cdc.

---

## Task 4: Convert `stream_state.rs` + `cdc.rs`

**Files:**
- Modify: `crates/catalog/src/stream_state.rs`
- Modify: `crates/catalog/src/cdc.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/catalog/tests/crud.rs`

- [ ] **Step 1: stream_state.rs**

```rust
pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    stream_name: &str,
    cursor: Option<CursorValue>,
    last_run_id: Option<RunId>,
) -> sqlx::Result<()> {
    let (kind, value) = match cursor {
        Some(c) => (kind_str(c.kind).to_string(), Some(c.value)),
        None => ("int64".to_string(), None),
    };
    sqlx::query(
        "INSERT INTO stream_state (pipeline_id, tenant_id, stream_name, cursor_kind, cursor_value, last_run_id, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6, NOW()) \
         ON CONFLICT (pipeline_id, stream_name) DO UPDATE SET \
           cursor_kind = EXCLUDED.cursor_kind, \
           cursor_value = EXCLUDED.cursor_value, \
           last_run_id = COALESCE(EXCLUDED.last_run_id, stream_state.last_run_id), \
           updated_at = NOW()",
    )
    .bind(pipeline_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(stream_name)
    .bind(kind)
    .bind(value)
    .bind(last_run_id.map(|r| r.as_uuid()))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
    stream_name: &str,
) -> sqlx::Result<Option<StreamState>> {
    // ...existing body with &mut *conn.
}
```

- [ ] **Step 2: cdc.rs**

`upsert` and `get` take `conn: &mut sqlx::PgConnection`. Existing II.1.a `tenant_id` field on `CdcSlot` is unchanged.

```rust
pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    slot: &CdcSlot,
) -> sqlx::Result<()> {
    let state_s = match slot.state {
        SlotState::Active => "active",
        SlotState::Paused => "paused",
        SlotState::Released => "released",
    };
    sqlx::query(
        "INSERT INTO cdc_slots(pipeline_id, tenant_id, slot_name, publication_name, \
          consistent_point, confirmed_flush, state, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7, now()) \
         ON CONFLICT (pipeline_id) DO UPDATE SET \
           slot_name=EXCLUDED.slot_name, \
           publication_name=EXCLUDED.publication_name, \
           consistent_point=EXCLUDED.consistent_point, \
           confirmed_flush=COALESCE(EXCLUDED.confirmed_flush, cdc_slots.confirmed_flush), \
           state=EXCLUDED.state, \
           updated_at=now()",
    )
    .bind(slot.pipeline_id.as_uuid())
    .bind(slot.tenant_id.as_uuid())
    .bind(&slot.slot_name)
    .bind(&slot.publication_name)
    .bind(&slot.consistent_point)
    .bind(&slot.confirmed_flush)
    .bind(state_s)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn update_confirmed_flush(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
    lsn: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE cdc_slots SET confirmed_flush=$1, updated_at=now() WHERE pipeline_id=$2",
    )
    .bind(lsn)
    .bind(pipeline_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
) -> sqlx::Result<Option<CdcSlot>> {
    // ...existing body with &mut *conn.
}
```

- [ ] **Step 3: `lib.rs` public methods**

Add `ctx: TenantContext` to `upsert_stream_state`, `get_stream_state`, `cdc_upsert`, `cdc_get`, `cdc_update_confirmed_flush`. Each follows the same `BEGIN; SET LOCAL; <call>; COMMIT;` pattern.

```rust
    pub async fn upsert_stream_state(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
        stream_name: &str,
        cursor: Option<common_types::cursor::CursorValue>,
        last_run_id: Option<RunId>,
    ) -> sqlx::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", ctx.tenant_id.as_uuid()))
            .execute(&mut *tx).await?;
        stream_state::upsert(&mut tx, ctx.tenant_id, pipeline_id, stream_name, cursor, last_run_id).await?;
        tx.commit().await?;
        Ok(())
    }
```

(Apply analogously to the rest.)

- [ ] **Step 4: `truncate_all_for_tests` stays admin-mode**

```rust
    pub async fn truncate_all_for_tests(&self) -> sqlx::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
        sqlx::query(
            "TRUNCATE cdc_slots, runs, stream_state, schemas, streams, pipelines, connections, workspaces, tenants CASCADE",
        )
        .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
```

- [ ] **Step 5: Update `crates/catalog/tests/crud.rs`**

Every call site updates to pass `ctx`. Example:

```rust
let cat = Catalog::connect(&url).await.unwrap();
let tenant = cat.create_tenant("acme").await.unwrap();
let ctx = catalog::TenantContext::new(tenant);
let conn = cat.create_connection(NewConnection { tenant_id: tenant, ... }).await.unwrap();
let got = cat.get_connection(ctx, conn).await.unwrap().unwrap();
```

(`TenantContext` is re-exported from catalog via `pub use common_types::ids::TenantContext;` — add at the top of `lib.rs` if not already.)

- [ ] **Step 6: Build catalog**

Run: `cargo build -p catalog && cargo test -p catalog -- --test-threads=1`
Expected: clean build, all 7 catalog crud tests pass.

- [ ] **Step 7: Commit Tasks 1–4 together**

```bash
git add crates/catalog/
git commit -m "refactor(catalog): TenantContext threaded through every public method

Each public Catalog::* method now opens a transaction, issues
SET LOCAL app.tenant_id, and runs the existing query through
the transaction. Free functions take &mut PgConnection. Admin
paths (tenant CRUD, truncate_all_for_tests, migrations) pass
empty-string app.tenant_id which the policy treats as NULL = admin.

This activates the Phase II.1.a RLS layer for every query that
flows through the public API."
```

---

## Task 5: Update worker activities + workflows (call-site sweep)

**Files:**
- Modify: `crates/worker/src/activities/run_lifecycle.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/activities/cdc/mod.rs`
- Modify: `crates/worker/src/cdc_monitor.rs`
- Modify: `crates/worker/src/schema_evolution.rs`
- Modify: `crates/worker/src/workflows/pipeline_run.rs`
- Modify: `crates/worker/src/workflows/cdc_pipeline.rs`

The compile errors after Task 4 drive this sweep. Pattern: at every call site, build a `TenantContext` from the surrounding `tenant_id` and pass it.

- [ ] **Step 1: lifecycle activities**

`StartRunInput`, `CompleteRunInput`, `FailRunInput` already have `run_id`; add `tenant_id`:

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StartRunInput {
    pub run_id: Uuid,
    pub tenant_id: Uuid,
}
```

(Repeat for `CompleteRunInput`. `FailRunInput` already has the structure — add the field.)

`start_run / complete_run / fail_run` build `TenantContext` and pass to catalog:

```rust
        let rid = RunId::from_uuid_unchecked(input.run_id);
        let ctx = catalog::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        self.catalog.mark_run_running(ctx, rid).await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_running: {e}").into()))?;
```

- [ ] **Step 2: sync activities**

`DiscoverInput`, `ReadBatchInput`, `LoadBatchInput`, `CommitCursorInput` already have `tenant_id`. At each catalog call site, build `TenantContext` from `input.tenant_id`.

```rust
        let ctx = catalog::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        self.catalog.upsert_stream_state(ctx, pid, &input.stream_name, input.cursor, rid)
            .await
            .map_err(...)?;
```

`schema_evolution::record_and_resolve` already takes `tenant_id` — wrap and pass `ctx` to `get_latest_schema` / `insert_schema` / `set_stream_current_schema`.

- [ ] **Step 3: cdc activities**

Mechanical: every `catalog::cdc::upsert(self.catalog.pool(), &slot)` becomes `self.catalog.cdc_upsert(ctx, &slot).await`. Same for `cdc_get` and `cdc_update_confirmed_flush`. `EnsureSlotInput` already has `tenant_id` (from II.1.a).

- [ ] **Step 4: cdc_monitor poller**

The slot-lag poller currently iterates `cdc_slots` directly via SQL. It needs admin mode (sees all tenants). The query already selects `tenant_id` (added in II.1.a migration); pass it to the gauge label (Task 9).

```rust
let rows: Vec<(uuid::Uuid, String, uuid::Uuid)> = sqlx::query_as(
    "SELECT pipeline_id, slot_name, tenant_id FROM cdc_slots WHERE state = 'active'",
)
// ...
```

The poller's catalog access goes through `self.catalog.pool()` (admin-mode raw SQL). Wrap in `BEGIN; SET LOCAL app.tenant_id = ''; ... ; COMMIT;` for cleanliness:

```rust
let mut tx = catalog.pool().begin().await?;
sqlx::query("SET LOCAL app.tenant_id = ''").execute(&mut *tx).await?;
let rows: Vec<(uuid::Uuid, String, uuid::Uuid)> = sqlx::query_as(...).fetch_all(&mut *tx).await?;
tx.commit().await?;
```

- [ ] **Step 5: workflows**

`pipeline_run.rs` already has `input.tenant_id` available; pass it through every activity input that needs it. `cdc_pipeline.rs` same.

- [ ] **Step 6: Build worker**

Run: `cargo build -p worker`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/worker/
git commit -m "refactor(worker): build TenantContext at every catalog call site"
```

---

## Task 6: Update CLI + control-api call sites

**Files:**
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/cli/src/dsl.rs`
- Modify: `crates/cli/src/status.rs`
- Modify: `crates/cli/src/terminate.rs`
- Modify: `crates/control-api/src/main.rs`

- [ ] **Step 1: Add `--tenant <name>` resolution helper**

In `crates/cli/src/main.rs`, add:

```rust
async fn resolve_tenant(catalog: &catalog::Catalog, name: &str)
    -> anyhow::Result<common_types::ids::TenantContext>
{
    let t = catalog.get_tenant_by_name(name).await
        .with_context(|| format!("looking up tenant {name}"))?
        .ok_or_else(|| anyhow::anyhow!("tenant {name} not found — `platform tenant create {name}`"))?;
    Ok(common_types::ids::TenantContext::new(t.tenant_id))
}
```

Add a `--tenant <name>` flag (default: `dev`) to `Cmd`:

```rust
struct Cli {
    /// Tenant scope for this command.
    #[arg(long, default_value = "dev", global = true)]
    tenant: String,

    #[command(subcommand)]
    cmd: Cmd,
}
```

Every subcommand handler that touches the catalog reads `cli.tenant`, calls `resolve_tenant`, passes the resulting `TenantContext` to catalog methods.

- [ ] **Step 2: `apply_cmd / get_cmd / diff_cmd / validate_cmd / pipeline_run`**

For each, the existing body uses `ensure_dev_tenant`. Replace with `resolve_tenant(&catalog, &tenant_name)` and thread the returned `TenantContext` to every catalog call.

- [ ] **Step 3: status.rs + terminate.rs**

Same: take `tenant: String` arg, resolve to `TenantContext`, pass through.

- [ ] **Step 4: control-api**

Switch `Catalog::connect` to `Catalog::connect_app` for read endpoints (they enforce tenant via incoming header, deferred to Phase II.2). For Phase II.1.b, fix the type so it compiles; runtime tenant scoping comes with auth.

- [ ] **Step 5: Build**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/cli/ crates/control-api/
git commit -m "refactor(cli): --tenant flag (default dev) threaded through every command"
```

---

## Task 7: Worker connects as `etl_app`; integration tests pick the right URL

**Files:**
- Modify: `crates/worker/src/main.rs`
- Modify: every `tests/integration/tests/*.rs` fixture

- [ ] **Step 1: Worker uses etl_app for app paths, etl for migrate**

```rust
    let admin_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let app_url = std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| admin_url.replace("etl:etl@", "etl_app:etl_app@"));

    {
        let admin = Catalog::connect(&admin_url).await?;
        admin.migrate().await?;
    }
    let catalog = Arc::new(Catalog::connect_app(&app_url).await?);
```

- [ ] **Step 2: Integration test helpers**

Each test file currently has a `catalog_url()`. Add `catalog_app_url()` and use the right one per call:

```rust
fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}
```

In each test:

```rust
let admin = Catalog::connect(&admin_url()).await?;
admin.migrate().await?;
admin.truncate_all_for_tests().await?;
drop(admin);
let cat = Catalog::connect_app(&app_url()).await?;
let tenant = admin.create_tenant("dev").await?; // wait — use a fresh admin handle
```

Actually clean shape:

```rust
let admin = Catalog::connect(&admin_url()).await?;
admin.migrate().await?;
admin.truncate_all_for_tests().await?;
let tenant = admin.create_tenant("dev").await?;
let cat = Catalog::connect_app(&app_url()).await?;
let ctx = catalog::TenantContext::new(tenant);
```

Apply this rewrite to each of the 9 existing integration tests + the 3 new RLS adversarial tests (II.1.a). The worker spawn env block needs `DATABASE_URL_APP` too:

```rust
let c = Command::new(cargo_bin("worker"))
    .env("DATABASE_URL", admin_url())
    .env("DATABASE_URL_APP", app_url())
    // ...
```

- [ ] **Step 3: Build + run**

```bash
cargo build --workspace
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all 13 integration tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/main.rs tests/integration/tests/
git commit -m "feat: worker + integration tests connect as etl_app

Migrations + tenant CRUD keep using etl superuser. App-layer queries
now flow through etl_app, which means the Phase II.1.a RLS policies
are enforced in production paths."
```

---

## Task 8: Per-tenant object-storage prefix

**Files:**
- Modify: `crates/loader-sdk/src/lib.rs`
- Modify: `crates/worker/src/loaders/parquet_local.rs`
- Modify: `crates/worker/src/loaders/cdc_parquet.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs` (dead-letter path)
- Modify: 4 integration tests that walk the parquet output path

- [ ] **Step 1: `LoadId` carries `tenant_id`**

In `crates/loader-sdk/src/lib.rs`:

```rust
#[derive(Debug, Clone)]
pub struct LoadId {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub run_id: RunId,
    pub batch_seq: u32,
}
```

(Add `pub use common_types::ids::TenantId;` at the top if not present.)

- [ ] **Step 2: Loader path includes tenant_id**

In `crates/worker/src/loaders/parquet_local.rs::target_path`:

```rust
fn target_path(spec: &LocalParquetSpec, load_id: &LoadId) -> PathBuf {
    let mut p = PathBuf::from(&spec.base_path);
    p.push(load_id.tenant_id.as_uuid().to_string());
    p.push(load_id.pipeline_id.as_uuid().to_string());
    p.push(format!("batch-{:05}.parquet", load_id.batch_seq));
    p
}
```

- [ ] **Step 3: CdcParquetLoader carries tenant_id**

```rust
impl CdcParquetLoader {
    pub async fn write(
        &self,
        dest: &DestinationSpec,
        tenant_id: Uuid,
        pipeline_id: Uuid,
        run_id: Uuid,
        batch_seq: u32,
        batch: &RecordBatch,
    ) -> Result<PathBuf> {
        let base = match dest {
            DestinationSpec::LocalParquet(s) => s.base_path.clone(),
        };
        let mut path = PathBuf::from(&base);
        path.push(tenant_id.to_string());
        path.push(pipeline_id.to_string());
        path.push("cdc");
        path.push(run_id.to_string());
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create dir {}", path.display()))?;
        path.push(format!("batch-{:05}.parquet", batch_seq));
        // ...write ArrowWriter as before
    }
}
```

Update both call sites (cdc activities `snapshot_chunk` and `read_window`) to pass `input.tenant_id`.

- [ ] **Step 4: Dead-letter path**

In `sync::load_batch`:

```rust
                let dest_path = match &input.destination {
                    common_types::pipeline_spec::DestinationSpec::LocalParquet(s) => {
                        let mut p = std::path::PathBuf::from(&s.base_path);
                        p.push(input.tenant_id.to_string());
                        p.push(load_id.pipeline_id.as_uuid().to_string());
                        p.push("dead-letter");
                        p.push(load_id.run_id.as_uuid().to_string());
                        ...
                    }
                };
```

`LoadBatchInput.tenant_id` already exists (from Phase I.5).

`LoadId` construction in `load_batch` activity gains `tenant_id`:

```rust
        let load_id = LoadId {
            tenant_id: TenantId::from_uuid_unchecked(input.tenant_id),
            pipeline_id: PipelineId::from_uuid_unchecked(input.pipeline_id),
            run_id: RunId::from_uuid_unchecked(input.run_id),
            batch_seq: input.batch_seq,
        };
```

- [ ] **Step 5: Update integration tests**

Each of `transforms_filter_mask.rs`, `transforms_dead_letter.rs`, `cdc_insert_update_delete.rs`, `cdc_snapshot_streaming_handoff.rs` walks `tmp.path()/<pipeline_id>/`. Update to walk `tmp.path()/<tenant_id>/<pipeline_id>/`.

Example `transforms_filter_mask.rs::read_all_rows`:

```rust
fn read_all_rows(dir: &Path, tenant_id: &str, pipeline_id: &str) -> (Vec<String>, Vec<String>) {
    let mut tenant_dir = dir.to_path_buf();
    tenant_dir.push(tenant_id);
    tenant_dir.push(pipeline_id);
    // existing walkdir logic against tenant_dir
}
```

- [ ] **Step 6: Build + test**

```bash
cargo build --workspace
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add crates/ tests/
git commit -m "feat: storage prefix is now <base>/<tenant_id>/<pipeline_id>/..."
```

---

## Task 9: `tenant_id` metric labels + Grafana variable

**Files:**
- Modify: `crates/worker/src/activities/run_lifecycle.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/cdc/mod.rs`
- Modify: `crates/worker/src/cdc_monitor.rs`
- Modify: `ops/grafana/dashboards/etl-overview.json`

- [ ] **Step 1: Counters get the label**

Every `metrics::counter!(NAME).increment(1)` becomes:

```rust
metrics::counter!(NAME, "tenant_id" => input.tenant_id.to_string()).increment(1);
```

For row counters (read/loaded/rejected) the input already has `tenant_id`. For lifecycle counters, the input now has it (from Task 5).

For CDC events:

```rust
metrics::counter!(crate::metrics::CDC_EVENTS,
    "op" => op,
    "tenant_id" => input.tenant_id.to_string(),
).increment(1);
```

- [ ] **Step 2: Slot-lag gauge**

In `cdc_monitor.rs`:

```rust
metrics::gauge!(
    crate::metrics::CDC_SLOT_LAG_BYTES,
    "pipeline_id" => pipeline_id.to_string(),
    "tenant_id" => tenant_id.to_string(),
)
.set(lag as f64);
```

(`tenant_id` was already added to the SQL query in Task 5.)

- [ ] **Step 3: Grafana variable**

`ops/grafana/dashboards/etl-overview.json` — add `templating`:

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

Update each panel's expression to filter:

```json
{"expr": "sum(etl_runs_started_total{tenant_id=~\"$tenant\"})", "refId": "A"}
```

(Apply to all six panels.)

- [ ] **Step 4: Reload Grafana**

```bash
docker-compose restart grafana
sleep 4
curl -s 'http://127.0.0.1:3000/api/dashboards/uid/etl-overview' | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print(d["dashboard"].get("templating",{}))'
```

Expected: dashboard JSON shows the `templating.list[0].name = "tenant"`.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/ ops/grafana/
git commit -m "feat(observability): tenant_id label on every metric + Grafana variable"
```

---

## Task 10: `platform tenant create | list | suspend | terminate`

**Files:**
- Create: `crates/cli/src/tenant.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Write the module**

```rust
use anyhow::Context;
use catalog::Catalog;
use common_types::ids::TenantId;

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
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    sqlx::query("UPDATE tenants SET name = 'suspended:' || name WHERE name = $1 AND name NOT LIKE 'suspended:%'")
        .bind(&name)
        .execute(admin.pool())
        .await?;
    println!("suspended tenant {} (name prefixed)", name);
    Ok(())
}

pub async fn terminate(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin.get_tenant_by_name(&name).await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", name))?;
    admin.delete_tenant(t.tenant_id).await?;
    println!("terminated tenant {} ({})", name, t.tenant_id);

    let base = std::env::var("ETL_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let path = std::path::PathBuf::from(&base).join(t.tenant_id.as_uuid().to_string());
    if path.exists() {
        std::fs::remove_dir_all(&path)
            .with_context(|| format!("removing {}", path.display()))?;
        println!("removed {}", path.display());
    }
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

In `main.rs`:

```rust
mod tenant;
```

In `enum Cmd`:

```rust
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
```

```rust
#[derive(Subcommand)]
enum TenantCmd {
    Create { name: String },
    List,
    Suspend { name: String },
    Terminate { name: String },
}
```

Match arm:

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

Expected: "created tenant acme (UUID)" + "registered Temporal namespace etl-…", then list shows acme.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/
git commit -m "feat(cli): platform tenant create | list | suspend | terminate"
```

---

## Task 11: Per-tenant Temporal namespace at workflow start

**Files:**
- Modify: `crates/worker/src/temporal.rs`
- Modify: `crates/cli/src/main.rs::pipeline_run`
- Modify: `crates/worker/src/main.rs`

- [ ] **Step 1: `make_client_for_namespace`**

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

In `crates/cli/src/main.rs::pipeline_run`, after fetching `pipeline`:

```rust
    let namespace = format!("etl-{}", pipeline.tenant_id.as_uuid().simple());
    let cfg = TemporalConfig::from_env()?;
    let client = worker::temporal::make_client_for_namespace(&cfg, &namespace).await?;
```

(Replace `make_client(&cfg).await?`.)

- [ ] **Step 3: Worker spawns one Temporal worker per known tenant**

Activity structs need `Clone`. Add `#[derive(Clone)]` to `RunLifecycleActivities`, `SyncActivities`, `CdcActivities` — they hold `Arc` internally, so cloning is cheap.

In `crates/worker/src/main.rs`, after `migrate()`:

```rust
    let admin = Catalog::connect(&admin_url).await?;
    let tenants = admin.list_tenants().await?;
    drop(admin);

    let runtime = make_runtime()?;
    let task_queue = cfg.task_queue.clone();

    let mut workers = Vec::new();
    for t in tenants {
        let ns = format!("etl-{}", t.tenant_id.as_uuid().simple());
        let mut ns_cfg = cfg.clone();
        ns_cfg.namespace = ns.clone();
        let ns_client = make_client(&ns_cfg).await?;
        let worker_options = WorkerOptions::new(task_queue.clone())
            .task_types(WorkerTaskTypes::all())
            .deployment_options(WorkerDeploymentOptions {
                version: WorkerDeploymentVersion {
                    deployment_name: "etl".to_owned(),
                    build_id: "etl-worker-0.2".to_owned(),
                },
                use_worker_versioning: false,
                default_versioning_behavior: None,
            })
            .register_activities(lifecycle.clone())
            .register_activities(sync.clone())
            .register_activities(cdc.clone())
            .register_workflow::<PipelineRunWorkflow>()
            .register_workflow::<CdcPipelineWorkflow>()
            .build();
        let mut w = Worker::new(&runtime, ns_client, worker_options)
            .map_err(|e| anyhow::anyhow!("Worker::new[{ns}]: {e}"))?;
        tracing::info!(%ns, "worker polling namespace");
        workers.push(tokio::spawn(async move {
            let _ = w.run().await;
        }));
    }
    futures::future::join_all(workers).await;
```

> Known limitation: tenants created at runtime won't have a worker until restart. Phase II.4 will fix.

- [ ] **Step 4: Build + smoke**

```bash
cargo run --bin platform -- tenant create acme
cargo run --bin worker &
# Should log "worker polling namespace etl-<uuid>" once
```

Expected: log line confirms.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/ crates/cli/
git commit -m "feat: per-tenant Temporal namespace; worker polls one per tenant"
```

---

## Task 12: API-level cross-tenant adversarial test

**Files:**
- Create: `tests/integration/tests/tenant_api_isolation.rs`

This test is the API counterpart to II.1.a's `rls_cross_tenant.rs` — it goes through `Catalog::*` methods rather than raw SQL, so it pins the TenantContext threading too.

- [ ] **Step 1: Write the test**

```rust
//! Phase II.1.b: cross-tenant isolation via the Catalog API.
//!
//! Validates that:
//!   - `Catalog::get_pipeline(ctx_a, B's_id)` returns None
//!   - `Catalog::get_connection(ctx_b, A's_id)` returns None
//! exercising the public methods used by every CLI/worker call site.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use common_types::ids::TenantContext;
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
async fn cross_tenant_api_reads_blocked() -> anyhow::Result<()> {
    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    let tenant_a = admin.create_tenant("acme").await?;
    let tenant_b = admin.create_tenant("globex").await?;

    let cat = Catalog::connect_app(&app_url()).await?;
    let conn_a = cat
        .create_connection(NewConnection {
            tenant_id: tenant_a,
            name: "a".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({"url":"postgres://x"}),
        })
        .await?;
    let pipe_a = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_a,
            name: "pa".into(),
            source_conn_id: conn_a,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;
    let conn_b = cat
        .create_connection(NewConnection {
            tenant_id: tenant_b,
            name: "b".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({"url":"postgres://y"}),
        })
        .await?;
    let pipe_b = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_b,
            name: "pb".into(),
            source_conn_id: conn_b,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    let ctx_a = TenantContext::new(tenant_a);
    let ctx_b = TenantContext::new(tenant_b);

    assert!(cat.get_pipeline(ctx_a, pipe_b).await?.is_none(),
        "tenant A read tenant B's pipeline");
    assert!(cat.get_pipeline(ctx_b, pipe_a).await?.is_none(),
        "tenant B read tenant A's pipeline");
    assert!(cat.get_connection(ctx_a, conn_b).await?.is_none());
    assert!(cat.get_connection(ctx_b, conn_a).await?.is_none());
    assert!(cat.get_pipeline(ctx_a, pipe_a).await?.is_some());
    assert!(cat.get_pipeline(ctx_b, pipe_b).await?.is_some());
    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test tenant_api_isolation -- --ignored
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/tenant_api_isolation.rs
git commit -m "test(integration): cross-tenant isolation via Catalog API (proves TenantContext threading)"
```

---

## Task 13: Tenant lifecycle integration test

**Files:**
- Create: `tests/integration/tests/tenant_lifecycle.rs`

- [ ] **Step 1: Test code**

```rust
//! Phase II.1.b: tenant lifecycle. create → seed pipeline → run →
//! verify Parquet under <tenant_id>/<pipeline_id>/ → terminate →
//! verify catalog rows + storage gone.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use common_types::ids::TenantContext;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn admin_url() -> String { std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into()) }
fn app_url() -> String { std::env::var("DATABASE_URL_APP").unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into()) }
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
    let _ctx = TenantContext::new(tenant_id);

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

    let mut tenant_dir = tmp.path().to_path_buf();
    tenant_dir.push(tenant_id.as_uuid().to_string());
    assert!(!tenant_dir.exists(), "tenant dir should not exist yet");

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
        .env("TEMPORAL_TASK_QUEUE","pipeline-default")
        .current_dir(workspace_root())
        .output().await?;

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

    let admin2 = Catalog::connect(&admin_url()).await?;
    assert!(admin2.get_tenant_by_name("lifecycle-test").await?.is_none());
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

## Task 14: Metric label cardinality test

**Files:**
- Create: `tests/integration/tests/metric_tenant_label.rs`

- [ ] **Step 1: Write the test**

```rust
//! Phase II.1.b: every counter we export has a tenant_id label.

use anyhow::Context;
use catalog::Catalog;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn admin_url() -> String { std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into()) }
fn app_url() -> String { std::env::var("DATABASE_URL_APP").unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into()) }
fn workspace_root() -> PathBuf { let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR")); p.pop(); p.pop(); p }

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn metrics_carry_tenant_id_label() -> anyhow::Result<()> {
    let st = Command::new("cargo").current_dir(workspace_root()).args(["build","--workspace"]).status().await?;
    assert!(st.success());

    // Scenario: run any cursor-incremental pipeline (incremental_sync seed
    // uses customers); curl /metrics; assert etl_runs_started_total has
    // tenant_id="…" series.

    // Boot worker, drive a quick pipeline run via existing fixtures, then:
    let resp = reqwest::get("http://127.0.0.1:9898/metrics").await
        .context("/metrics endpoint")?
        .text().await?;
    assert!(
        resp.lines().any(|l|
            l.starts_with("etl_runs_started_total{") && l.contains("tenant_id=\"")
        ),
        "no etl_runs_started_total{{tenant_id=\"…\"}} in /metrics:\n{resp}"
    );
    Ok(())
}
```

> Note: this test depends on a worker already running with metrics emitted from a prior pipeline run. For a fully isolated version, the test should boot its own worker + drive a pipeline (mirrors the lifecycle test). Author's choice based on flake tolerance.

- [ ] **Step 2: Add reqwest to integration tests' deps**

```toml
# tests/integration/Cargo.toml
[dependencies]
# ...
reqwest = { workspace = true }
```

- [ ] **Step 3: Run**

```bash
cargo test -p integration-tests --test metric_tenant_label -- --ignored
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/
git commit -m "test(integration): every counter carries tenant_id label"
```

---

## Task 15: README + completion log + regression sweep

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-25-phase-2-1b-tenant-threading.md` (append log)

- [ ] **Step 1: README section**

Replace the current "Phase" line with:

```markdown
Currently: **Phase II.1 — multi-tenancy turned real (complete)**. Next: **Phase II.2 — secrets, auth, security model**.

## Multi-tenancy (Phase II.1)

```bash
# Provision a tenant — catalog row + Temporal namespace etl-<uuid> + storage prefix
cargo run --bin platform -- tenant create acme

# List
cargo run --bin platform -- tenant list

# Run a pipeline scoped to a tenant
cargo run --bin platform -- --tenant acme apply -f my-pipeline.yaml
cargo run --bin platform -- --tenant acme pipeline run my-pipeline

# Wind down (catalog cascade + ./data/<tenant_id>/ deletion)
cargo run --bin platform -- tenant terminate acme
```

Every catalog query goes through Postgres RLS as the non-superuser `etl_app` role; admin paths (migrations, tenant CRUD) keep using the `etl` superuser. Each tenant's pipelines run in a Temporal namespace named `etl-<tenant_id_simple>`, write Parquet under `./data/<tenant_id>/<pipeline_id>/...`, and emit metrics with a `tenant_id` label. Grafana's ETL Overview dashboard has a `tenant` template variable filtering every panel.
```

- [ ] **Step 2: Append completion log**

```markdown
---

## Phase II.1.b Completion Log

Completed YYYY-MM-DD on branch `phase-2-1b-tenant-threading`.

- [x] Tasks 1–4 — every catalog method takes TenantContext + opens its own SET-LOCAL-tx
- [x] Task 5 — worker activities + workflows pass TenantContext to catalog calls
- [x] Task 6 — CLI gains --tenant flag (default dev) threaded through every subcommand
- [x] Task 7 — worker + integration tests connect as etl_app
- [x] Task 8 — storage prefix is now <base>/<tenant_id>/<pipeline_id>/...
- [x] Task 9 — tenant_id metric labels + Grafana template variable
- [x] Task 10 — platform tenant create | list | suspend | terminate
- [x] Task 11 — per-tenant Temporal namespace; worker polls one per tenant
- [x] Task 12 — API-level cross-tenant adversarial test
- [x] Task 13 — tenant lifecycle integration test
- [x] Task 14 — metric label cardinality test
- [x] Task 15 — README + this log

### Exit criterion — MET

- Tenant A's CLI cannot see tenant B's pipelines (RLS enforced via the
  `Catalog` API, not just raw SQL).
- Per-tenant Temporal namespace verified by tenant_lifecycle test.
- Per-tenant storage prefix verified by every existing integration test.
- Metrics carry `tenant_id`; Grafana panels filter by it.
- All unit + integration tests green (count tbd post-execution).

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.2

Phase II.2 (secrets, auth, security) extends `TenantContext` with
`principal_id` + `roles`, swaps the bearer for an authenticated user
on each request, and replaces `--tenant <name>` resolution with
identity-driven scoping. The RLS layer is the line of defence; auth
ensures the right session var is set.

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

cargo test --workspace --lib
cargo test -p cli
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-25-phase-2-1b-tenant-threading.md
git commit -m "docs: Phase II.1.b README + completion log"
```

Then use the finishing-a-development-branch skill to push and open a PR.

---

## Appendix A — Operational notes

**RLS in `BEGIN ... SET LOCAL ... query ... COMMIT` shape**: the
boilerplate is duplicated across every public Catalog method. A future
cleanup could macro it out, but for II.1.b clarity wins — each method
is self-contained and easy to read. Recommend revisiting in Phase II.2
when auth is layered on top.

**Empty-string vs NULL `app.tenant_id`**: the `app_tenant_id()` SQL
function returns NULL for both unset *and* empty string (`NULLIF(...,
'')::uuid`). We use empty string for admin paths because `SET LOCAL
app.tenant_id = NULL` doesn't parse as expected.

**Worker startup vs new tenants**: the worker reads the tenant list
once at boot and starts one Temporal worker per tenant. Tenants
created at runtime won't have a worker until restart. Phase II.4
adds a `tenant_created` signal that hot-reconfigures.

**Suspended tenants**: `platform tenant suspend` renames the row to
`suspended:<name>`. The CLI's `--tenant` resolution rejects names
that don't match exactly, so a suspended tenant becomes invisible.
Existing runs continue (the row still exists). Phase II.2 adds a
proper status column and a paused state.

**Test isolation**: every integration test calls
`truncate_all_for_tests` before seeding, so different tests' tenants
don't leak. Each test that needs tenant scoping creates its own.

**`reqwest` import in integration tests**: the metric label test pulls
reqwest. It's already a workspace dep (used by the WASM connector
runtime); just declare it in `tests/integration/Cargo.toml`.

## Appendix B — What's deferred to later phases

- Auth + RBAC + secrets backend — Phase II.2
- Customer tenant signup UI — Phase III
- Multi-region tenant placement — Phase III
- Quota/billing per tenant — Phase II.5 / III
- Tenant-tier WASM resource limits — Phase III
- Multi-namespace single worker that hot-reconfigures — Phase II.4
- Tenant suspension as a proper status column — Phase II.2
- True Temporal namespace deletion — Phase II.2

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-25-phase-2-1b-tenant-threading.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. **Strongly recommended for this plan** — the catalog refactor is mechanical-but-wide work where a fresh subagent per task keeps focus and prevents the inline-execution stall we hit on the original Phase II.1 plan.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints. Higher risk of compile-error rabbit holes given the breadth of changes.

**Which approach?**

---

## Execution log — split into II.1.b + II.1.c

After landing the catalog refactor + worker etl_app + metric labels + API adversarial test (the *active-in-production* core), the remaining tasks (per-tenant Temporal namespace, per-tenant storage prefix, tenant CLI, lifecycle test, metric-label cardinality test) were deferred to Phase II.1.c so II.1.b could ship as a focused PR.

### Phase II.1.b Completion Log

Completed 2026-04-25 on branch `phase-2-1b-tenant-threading`.

- [x] Tasks 1–4 — every catalog public method takes TenantContext + opens its own SET-LOCAL-tx; free fns take &mut PgConnection; 7 catalog crud tests pass
- [x] Task 5 — TenantContext threaded through every worker activity, workflow, CLI, and integration test call site
- [x] Task 7 — worker connects as etl_app; production paths now go through RLS
- [x] Task 9 — tenant_id label on every counter (run_started/completed/failed, rows_read/loaded/rejected, cdc_events) + slot-lag gauge; slot-lag poller queries the tenant_id column added in II.1.a
- [x] API-level cross-tenant adversarial test (`tenant_api_isolation.rs`): reads via `Catalog::get_pipeline / get_connection` blocked across tenants

### Exit criterion for II.1.b — MET

- RLS is active in production paths. Worker queries fail-closed when
  app.tenant_id isn't set. The new `tenant_api_isolation` test
  exercises this through the public Catalog API (not raw SQL).
- All 14 integration tests + 78 unit tests + 1 cli unit test green.

### Deviations from the plan

- **`Catalog::get_pipeline_admin` and `get_connection_admin` added.** The CLI's `pipeline_run` looks up the pipeline before knowing its tenant — circular dependency. Added admin-mode (NULL `app.tenant_id`) lookups for this case. Phase II.2 with auth will replace these with identity-driven scoping.
- **Storage path unchanged.** The plan called for `<base>/<tenant_id>/<pipeline_id>/...`; deferred to II.1.c so II.1.b could ship without touching every Parquet-walking integration test.
- **Per-tenant Temporal namespace unchanged.** All workflows still run in the `default` namespace. Per-tenant namespaces require activity structs to be `Clone` (done) plus a per-namespace worker spawn loop in main.rs (deferred). The Tenant CLI also includes the namespace registration — both move together to II.1.c.
- **Tenant CLI deferred.** `platform tenant create | list | suspend | terminate` deferred to II.1.c.
- **Grafana template variable deferred.** The metric labels are present; the dashboard variable that exposes them is II.1.c.
- **Tenant lifecycle integration test deferred.** Depends on the tenant CLI + per-tenant namespace + per-tenant storage prefix.
- **Metric label cardinality test deferred.** Same — depends on at least one tenant existing beyond `dev`.

### Handoff to Phase II.1.c

II.1.b banked the *security* primitive (RLS active for production queries). II.1.c is a focused features-PR:

1. `platform tenant create | list | suspend | terminate` CLI + `RegisterNamespace` call
2. Per-tenant Temporal namespace at workflow start + worker polls one per tenant
3. Per-tenant storage prefix `<base>/<tenant_id>/<pipeline_id>/...` + integration test path updates
4. Grafana `tenant` template variable
5. Tenant lifecycle integration test
6. Metric label cardinality test

The TenantContext seam, etl_app connection, and metric labels are already in place. II.1.c is additive, not a refactor.
