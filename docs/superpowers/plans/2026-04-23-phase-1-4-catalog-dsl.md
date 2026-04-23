# Phase I.4 — Full Catalog Entities + YAML DSL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Phase I.2 `stream_state`-only shortcut with the real RFC-10 catalog shape (`workspaces`, `streams`, `schemas` with append-only versioning + BLAKE3 fingerprints + typed change-kind diffing + evolution policies) and give operators a YAML DSL (`platform apply -f`, `get`, `diff`, `validate`) that is 1:1 with the catalog entities.

**Architecture:** Pure-function schema tooling (fingerprint → diff → apply_policy) sits in `worker::schema_evolution`, composed together by a `record_and_resolve()` entry point called from the existing `SyncActivities::discover_stream` activity. Schema entities are append-only; each sync-time discovery either matches the stream's current `schemas.fingerprint` (no-op) or produces a new `schemas` row linked by `parent_schema_id`, with the `Vec<ChangeKind>` stored as `change_summary`. YAML resources in `apiVersion: platform.etl/v0` serialize to and from the existing catalog via serde_yaml + upsert-by-(tenant, name). Default workspace is auto-created per-tenant; workspace_id is denormalized onto connections/pipelines but uniqueness constraints stay at (tenant_id, name) for now.

**Tech Stack:** Unchanged — Rust 1.88, sqlx/Postgres, arrow 53, temporalio-sdk 0.2, clap. New deps: `blake3` (fingerprinting), `serde_yaml` (DSL parsing), `similar` (optional — pretty diffing; defer if it adds friction).

---

## File Structure

### Modified
- `Cargo.toml` (root) — add `blake3`, `serde_yaml` to `[workspace.dependencies]`
- `crates/common-types/Cargo.toml` — pull in `blake3`
- `crates/common-types/src/lib.rs` — expose new modules
- `crates/catalog/src/lib.rs` — add new module declarations + re-exports + new methods on `Catalog`
- `crates/worker/src/lib.rs` — expose `schema_evolution` module
- `crates/worker/src/activities/sync/mod.rs` — call `schema_evolution::record_and_resolve` from `discover_stream`
- `crates/worker/src/activities/sync/inputs.rs` — `DiscoverOutput` gains `schema_id` (+ backward-compatible serde)
- `crates/cli/Cargo.toml` — add `serde_yaml`
- `crates/cli/src/main.rs` — new `apply`, `get`, `diff`, `validate` subcommands
- `README.md` — Phase I.4 section

### New
- `crates/catalog/migrations/0003_workspaces_streams_schemas.sql`
- `crates/catalog/src/workspace.rs` — Workspace CRUD
- `crates/catalog/src/stream.rs` — Stream CRUD
- `crates/catalog/src/schema.rs` — Schema CRUD (append-only)
- `crates/common-types/src/schema_fingerprint.rs` — `SchemaFingerprint` newtype wrapping BLAKE3 hex
- `crates/common-types/src/evolution.rs` — `EvolutionPolicy`, `ChangeKind`
- `crates/common-types/src/dsl.rs` — `ResourceEnvelope`, `ResourceKind`, `Metadata`, `ConnectionSpec`, `PipelineDslSpec`
- `crates/worker/src/schema_evolution/mod.rs` — public entry
- `crates/worker/src/schema_evolution/fingerprint.rs` — canonical Arrow schema → BLAKE3
- `crates/worker/src/schema_evolution/diff.rs` — old vs. new → `Vec<ChangeKind>`
- `crates/worker/src/schema_evolution/policy.rs` — applies `EvolutionPolicy` to a diff
- `crates/worker/src/schema_evolution/recorder.rs` — composes catalog + fingerprint + diff + policy
- `crates/cli/src/dsl.rs` — parse YAML files, resolve references, call catalog
- `examples/dsl/customers-sync.yaml` — demo multi-resource file
- `tests/integration/tests/schema_evolution.rs` — Postgres add-column end-to-end
- `tests/integration/tests/dsl_apply.rs` — YAML apply idempotency + get/diff flows

### Deliberately deferred (per the scope — do NOT add in this plan)
- `Workspace` CRUD UI — default workspace auto-created; Phase II.1
- Schedules — manual `platform pipeline run` stays
- CDC mode stream entities (RFC-10 Stream `sync_mode = cdc` with `cdc_slot_config`) — Phase I.6
- Lineage graph rows — Phase II.5
- `propagate_all` breaking-change handling / automatic column renames — Phase I.5+
- Postgres-in-WASM — still deferred (WIT host interface stable since I.3)
- Transformation DSL authoring — Phase I.5

---

## Key Type Contracts

All types derive `Clone, Debug, Serialize, Deserialize` unless noted.

```rust
// common-types/src/schema_fingerprint.rs
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaFingerprint(String); // lowercase BLAKE3 hex, 64 chars
impl SchemaFingerprint {
    pub fn from_hex(s: impl Into<String>) -> Self;
    pub fn as_hex(&self) -> &str;
}

// common-types/src/evolution.rs
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionPolicy {
    PropagateAdditive, // default — additive changes flow through; breaking → fail
    Freeze,            // ignore all drift; keep current schema
    Strict,            // fail on any change
}
impl Default for EvolutionPolicy {
    fn default() -> Self { Self::PropagateAdditive }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChangeKind {
    AddColumn { name: String, data_type: String, nullable: bool },
    DropColumn { name: String },
    RenameColumn { from: String, to: String },
    WidenType { name: String, from: String, to: String },       // Phase I.4 scope: int32→int64, float32→float64, utf8-widening
    NarrowType { name: String, from: String, to: String },      // flagged as breaking
    MakeNullable { name: String },
    MakeNonNullable { name: String },                           // breaking
    ReorderColumns { before: Vec<String>, after: Vec<String> }, // non-breaking for Phase I.4
}

// common-types/src/dsl.rs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceEnvelope {
    pub api_version: String,      // "platform.etl/v0"
    pub kind: ResourceKind,
    pub metadata: Metadata,
    pub spec: serde_json::Value,  // dispatched on kind
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ResourceKind { Connection, Pipeline }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionSpec {
    pub connector_ref: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineDslSpec {
    pub source_connection: String,     // reference by metadata.name
    pub source: crate::pipeline_spec::SourceSpec,
    pub destination: crate::pipeline_spec::DestinationSpec,
    pub batch_size: usize,
    #[serde(default)]
    pub evolution_policy: EvolutionPolicy,
}
```

```rust
// catalog entities (simplified; see migration SQL for the exact columns)
pub struct Workspace { pub workspace_id: WorkspaceId, pub tenant_id: TenantId, pub name: String, pub created_at: ... }
pub struct Stream {
    pub stream_id: StreamId, pub tenant_id: TenantId, pub pipeline_id: PipelineId,
    pub name: String, pub sync_mode: String, pub cursor_config: serde_json::Value,
    pub pk_config: serde_json::Value, pub destination_table: Option<String>,
    pub current_schema_id: Option<SchemaId>, pub created_at: ..., pub updated_at: ...,
}
pub struct Schema {
    pub schema_id: SchemaId, pub tenant_id: TenantId, pub stream_id: StreamId,
    pub version: i32, pub parent_schema_id: Option<SchemaId>,
    pub fingerprint: SchemaFingerprint,
    pub arrow_schema_json: serde_json::Value,
    pub change_summary: Vec<ChangeKind>,
    pub detected_at: ..., pub applied_to_destination_at: Option<...>,
}
```

Arrow's `arrow::datatypes::Schema` implements `serde::Serialize`/`Deserialize` — store via `serde_json::to_value(schema)`.

```rust
// worker/src/schema_evolution/mod.rs
pub struct ResolvedSchema {
    pub schema: arrow::datatypes::SchemaRef,
    pub schema_id: common_types::ids::SchemaId,
    pub created_new_version: bool,
}

pub async fn record_and_resolve(
    catalog: &catalog::Catalog,
    stream_id: common_types::ids::StreamId,
    policy: EvolutionPolicy,
    incoming: arrow::datatypes::SchemaRef,
) -> anyhow::Result<ResolvedSchema>;
```

---

## Task 1: Dep additions + BLAKE3

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/common-types/Cargo.toml`
- Modify: `crates/cli/Cargo.toml`

- [ ] **Step 1: Root workspace deps**

Edit root `Cargo.toml` `[workspace.dependencies]`, add:

```toml
blake3 = "1"
serde_yaml = "0.9"
```

- [ ] **Step 2: common-types picks up blake3**

Edit `crates/common-types/Cargo.toml` `[dependencies]`:

```toml
blake3 = { workspace = true }
```

- [ ] **Step 3: cli picks up serde_yaml**

Edit `crates/cli/Cargo.toml` `[dependencies]`:

```toml
serde_yaml = { workspace = true }
```

- [ ] **Step 4: Build**

Run: `cargo build --workspace`
Expected: clean build.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: add blake3 + serde_yaml for Phase I.4

blake3 for schema fingerprinting (common-types); serde_yaml for the
pipeline DSL parser (cli).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `common-types` — fingerprint, evolution, DSL types

**Files:**
- Create: `crates/common-types/src/schema_fingerprint.rs`
- Create: `crates/common-types/src/evolution.rs`
- Create: `crates/common-types/src/dsl.rs`
- Modify: `crates/common-types/src/lib.rs`
- Modify: `crates/common-types/src/ids.rs` — add `StreamId`, `SchemaId`, `WorkspaceId`

- [ ] **Step 1: Write the newtype file**

Create `crates/common-types/src/schema_fingerprint.rs`:

```rust
use serde::{Deserialize, Serialize};

/// BLAKE3-hex fingerprint of a normalized Arrow schema.
/// 64 lowercase hex chars (256-bit).
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaFingerprint(String);

impl SchemaFingerprint {
    pub fn from_hex(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_hex(&self) -> &str {
        &self.0
    }
    pub fn into_hex(self) -> String {
        self.0
    }
}

impl std::fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_serde() {
        let f = SchemaFingerprint::from_hex(
            "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
        );
        let j = serde_json::to_string(&f).unwrap();
        assert_eq!(
            j,
            "\"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234\""
        );
        let back: SchemaFingerprint = serde_json::from_str(&j).unwrap();
        assert_eq!(back, f);
    }
}
```

- [ ] **Step 2: Evolution enums**

Create `crates/common-types/src/evolution.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionPolicy {
    /// Default: additive changes (new nullable columns, type widening, make-nullable)
    /// flow through; breaking changes fail the run.
    PropagateAdditive,
    /// Ignore all schema drift; stick with the current stored schema.
    Freeze,
    /// Any schema change fails the run.
    Strict,
}

impl Default for EvolutionPolicy {
    fn default() -> Self {
        Self::PropagateAdditive
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChangeKind {
    AddColumn { name: String, data_type: String, nullable: bool },
    DropColumn { name: String },
    RenameColumn { from: String, to: String },
    WidenType { name: String, from: String, to: String },
    NarrowType { name: String, from: String, to: String },
    MakeNullable { name: String },
    MakeNonNullable { name: String },
    ReorderColumns { before: Vec<String>, after: Vec<String> },
}

impl ChangeKind {
    /// True if this change is safe to auto-apply under `propagate_additive`.
    pub fn is_additive(&self) -> bool {
        matches!(
            self,
            ChangeKind::AddColumn { nullable: true, .. }
                | ChangeKind::MakeNullable { .. }
                | ChangeKind::WidenType { .. }
                | ChangeKind::ReorderColumns { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_nullable_column_is_additive() {
        assert!(ChangeKind::AddColumn {
            name: "age".into(),
            data_type: "int64".into(),
            nullable: true,
        }
        .is_additive());
    }

    #[test]
    fn drop_column_is_breaking() {
        assert!(!ChangeKind::DropColumn { name: "email".into() }.is_additive());
    }

    #[test]
    fn widen_int32_to_int64_is_additive() {
        assert!(ChangeKind::WidenType {
            name: "id".into(),
            from: "int32".into(),
            to: "int64".into(),
        }
        .is_additive());
    }

    #[test]
    fn make_non_nullable_is_breaking() {
        assert!(!ChangeKind::MakeNonNullable { name: "name".into() }.is_additive());
    }
}
```

- [ ] **Step 3: DSL types**

Create `crates/common-types/src/dsl.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::evolution::EvolutionPolicy;
use crate::pipeline_spec::{DestinationSpec, SourceSpec};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceEnvelope {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: ResourceKind,
    pub metadata: Metadata,
    pub spec: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceKind {
    Connection,
    Pipeline,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionSpec {
    pub connector_ref: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineDslSpec {
    /// Reference by `metadata.name` of a Connection resource.
    pub source_connection: String,
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    pub batch_size: usize,
    #[serde(default)]
    pub evolution_policy: EvolutionPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_envelope_roundtrips_via_yaml() {
        let yaml = r#"
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: source-demo
spec:
  connector_ref: postgres@0.1.0
  config:
    url: postgres://etl:etl@localhost:5432/etl_source_demo
"#;
        let env: ResourceEnvelope = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(env.api_version, "platform.etl/v0");
        assert_eq!(env.kind, ResourceKind::Connection);
        assert_eq!(env.metadata.name, "source-demo");
        let spec: ConnectionSpec = serde_json::from_value(env.spec.clone()).unwrap();
        assert_eq!(spec.connector_ref, "postgres@0.1.0");
    }
}
```

Add `serde_yaml` as a dev-dep in `crates/common-types/Cargo.toml`:

```toml
[dev-dependencies]
serde_json = { workspace = true }
serde_yaml = { workspace = true }
```

- [ ] **Step 4: ID newtypes for Stream/Schema/Workspace**

Edit `crates/common-types/src/ids.rs`. Append four new `define_id!` invocations near the existing ones:

```rust
define_id!(WorkspaceId, "ws");
define_id!(StreamId, "stream");
define_id!(SchemaId, "sch");
```

- [ ] **Step 5: Wire modules**

Edit `crates/common-types/src/lib.rs`:

```rust
//! Shared newtype identifiers and primitive types for the platform.
pub mod connection_config;
pub mod cursor;
pub mod dsl;
pub mod evolution;
pub mod ids;
pub mod pipeline_spec;
pub mod schema_fingerprint;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p common-types`
Expected: all previous tests pass plus 4 new ones (1 fingerprint + 4 evolution + 1 dsl) = **12 passed** (previously 8).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(common-types): SchemaFingerprint, EvolutionPolicy, ChangeKind, DSL types

Types shared across catalog (schema entity fields), worker
(schema-evolution decision code), and CLI (DSL parsing). ChangeKind
carries an is_additive() helper used by the policy engine. DSL
ResourceEnvelope is the apiVersion/kind/metadata/spec envelope;
PipelineDslSpec references connections by name (not UUID).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Catalog migration 0003 — workspaces, streams, schemas

**Files:**
- Create: `crates/catalog/migrations/0003_workspaces_streams_schemas.sql`

- [ ] **Step 1: Write the migration**

Create `crates/catalog/migrations/0003_workspaces_streams_schemas.sql`:

```sql
-- Phase I.4 catalog elaboration per RFC-10.

-- Workspaces: one "default" workspace auto-created per tenant.
CREATE TABLE workspaces (
    workspace_id  UUID PRIMARY KEY,
    tenant_id     UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX workspaces_tenant_id_idx ON workspaces(tenant_id);

-- Backfill: one "default" workspace per existing tenant.
INSERT INTO workspaces (workspace_id, tenant_id, name)
SELECT gen_random_uuid(), tenant_id, 'default' FROM tenants;

-- Denormalize workspace_id onto connections + pipelines.
ALTER TABLE connections ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE;
UPDATE connections c
SET workspace_id = (
    SELECT workspace_id FROM workspaces w
    WHERE w.tenant_id = c.tenant_id AND w.name = 'default'
);
ALTER TABLE connections ALTER COLUMN workspace_id SET NOT NULL;
CREATE INDEX connections_workspace_id_idx ON connections(workspace_id);

ALTER TABLE pipelines ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE;
UPDATE pipelines p
SET workspace_id = (
    SELECT workspace_id FROM workspaces w
    WHERE w.tenant_id = p.tenant_id AND w.name = 'default'
);
ALTER TABLE pipelines ALTER COLUMN workspace_id SET NOT NULL;
CREATE INDEX pipelines_workspace_id_idx ON pipelines(workspace_id);

-- Streams: per-pipeline logical entity. Phase I.4: one row per source table/stream.
CREATE TABLE streams (
    stream_id          UUID PRIMARY KEY,
    tenant_id          UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id        UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    name               TEXT NOT NULL,
    sync_mode          TEXT NOT NULL DEFAULT 'incremental'
                         CHECK (sync_mode IN ('full_refresh','incremental','cdc')),
    cursor_config      JSONB NOT NULL,           -- { column, kind }
    pk_config          JSONB NOT NULL DEFAULT '[]'::jsonb,
    destination_table  TEXT,
    current_schema_id  UUID,                     -- FK added after schemas table exists
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, name)
);
CREATE INDEX streams_tenant_id_idx ON streams(tenant_id);

-- Schemas: append-only, versioned.
CREATE TABLE schemas (
    schema_id                    UUID PRIMARY KEY,
    tenant_id                    UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    stream_id                    UUID NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
    version                      INT NOT NULL,
    parent_schema_id             UUID REFERENCES schemas(schema_id) ON DELETE SET NULL,
    fingerprint                  TEXT NOT NULL,           -- BLAKE3 hex
    arrow_schema_json            JSONB NOT NULL,
    change_summary               JSONB NOT NULL DEFAULT '[]'::jsonb,  -- Vec<ChangeKind>
    detected_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_to_destination_at    TIMESTAMPTZ,
    UNIQUE (stream_id, version)
);
CREATE INDEX schemas_stream_id_idx ON schemas(stream_id);
CREATE INDEX schemas_fingerprint_idx ON schemas(fingerprint);

-- Now add the FK from streams.current_schema_id → schemas.schema_id.
ALTER TABLE streams
  ADD CONSTRAINT streams_current_schema_fk
  FOREIGN KEY (current_schema_id) REFERENCES schemas(schema_id) ON DELETE SET NULL;
```

- [ ] **Step 2: Apply manually to verify SQL**

Run: `docker exec -i etl-postgres psql -U etl -d etl_catalog < crates/catalog/migrations/0003_workspaces_streams_schemas.sql`
Expected: `CREATE TABLE` × 3, `ALTER TABLE` × 5, `INSERT` (tenants backfill), `UPDATE` × 2, `CREATE INDEX` × 6, `ALTER TABLE` (FK).

Then roll back for sqlx:

Run: `docker exec -i etl-postgres psql -U etl -d etl_catalog -c "DROP TABLE schemas, streams, workspaces CASCADE; ALTER TABLE connections DROP COLUMN IF EXISTS workspace_id; ALTER TABLE pipelines DROP COLUMN IF EXISTS workspace_id;"`

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0003_workspaces_streams_schemas.sql
git commit -m "feat(catalog): migration 0003 — workspaces, streams, schemas

RFC-10 entity elaboration. workspaces auto-populated with 'default'
per tenant. connections + pipelines gain workspace_id (denormalized;
UNIQUE(tenant_id, name) preserved for now). streams is per-pipeline
with sync_mode + cursor_config + pk_config + current_schema_id.
schemas is append-only versioned with BLAKE3 fingerprint + parent
link + change_summary (Vec<ChangeKind> as JSONB) + detected_at.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Catalog CRUD — Workspace

**Files:**
- Create: `crates/catalog/src/workspace.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/catalog/tests/crud.rs`

- [ ] **Step 1: Write `workspace.rs`**

Create `crates/catalog/src/workspace.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::{TenantId, WorkspaceId};
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Workspace {
    pub workspace_id: WorkspaceId,
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

pub async fn ensure_default(pool: &PgPool, tenant_id: TenantId) -> sqlx::Result<WorkspaceId> {
    if let Some(existing) = get_by_name(pool, tenant_id, "default").await? {
        return Ok(existing.workspace_id);
    }
    let id = WorkspaceId::new();
    sqlx::query(
        "INSERT INTO workspaces (workspace_id, tenant_id, name) VALUES ($1, $2, 'default')",
    )
    .bind(id.as_uuid())
    .bind(tenant_id.as_uuid())
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn get_by_name(
    pool: &PgPool,
    tenant_id: TenantId,
    name: &str,
) -> sqlx::Result<Option<Workspace>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT workspace_id, tenant_id, name, created_at \
         FROM workspaces WHERE tenant_id = $1 AND name = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(w, t, name, ts)| Workspace {
        workspace_id: WorkspaceId::from_uuid_unchecked(w),
        tenant_id: TenantId::from_uuid_unchecked(t),
        name,
        created_at: ts,
    }))
}
```

- [ ] **Step 2: Expose on `Catalog`**

Edit `crates/catalog/src/lib.rs`. Add `pub mod workspace;` near the existing module declarations. Append methods inside `impl Catalog`:

```rust
    // Workspaces
    pub async fn ensure_default_workspace(
        &self,
        tenant_id: TenantId,
    ) -> sqlx::Result<WorkspaceId> {
        workspace::ensure_default(&self.pool, tenant_id).await
    }
    pub async fn get_workspace_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> sqlx::Result<Option<workspace::Workspace>> {
        workspace::get_by_name(&self.pool, tenant_id, name).await
    }
```

Add `WorkspaceId` to the import at the top:

```rust
use common_types::ids::{ConnectionId, PipelineId, RunId, TenantId, WorkspaceId};
```

Extend `truncate_all_for_tests` to include workspaces:

```rust
        sqlx::query("TRUNCATE runs, stream_state, schemas, streams, pipelines, connections, workspaces, tenants CASCADE")
```

- [ ] **Step 3: Add test**

Append to `crates/catalog/tests/crud.rs`:

```rust
#[tokio::test]
async fn default_workspace_is_idempotent() {
    let cat = test_catalog().await;
    let t = cat.create_tenant("acme").await.unwrap();
    let w1 = cat.ensure_default_workspace(t).await.unwrap();
    let w2 = cat.ensure_default_workspace(t).await.unwrap();
    assert_eq!(w1, w2, "ensure_default_workspace must return the same id on repeat");
    let got = cat.get_workspace_by_name(t, "default").await.unwrap().unwrap();
    assert_eq!(got.workspace_id, w1);
}
```

- [ ] **Step 4: Run**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p catalog -- --test-threads=1`
Expected: **5 passed** (4 existing + new `default_workspace_is_idempotent`).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(catalog): workspace CRUD — ensure_default + get_by_name

Phase I.4 shape: one row per (tenant_id, 'default') auto-created on
demand by ensure_default_workspace (idempotent). get_by_name returns
Option<Workspace>. truncate helper extended.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Catalog CRUD — Stream

**Files:**
- Create: `crates/catalog/src/stream.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/catalog/tests/crud.rs`

- [ ] **Step 1: Write `stream.rs`**

Create `crates/catalog/src/stream.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::{PipelineId, SchemaId, StreamId, TenantId};
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Stream {
    pub stream_id: StreamId,
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub name: String,
    pub sync_mode: String,
    pub cursor_config: Value,
    pub pk_config: Value,
    pub destination_table: Option<String>,
    pub current_schema_id: Option<SchemaId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewStream {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub name: String,
    pub sync_mode: String,
    pub cursor_config: Value,
    pub pk_config: Value,
    pub destination_table: Option<String>,
}

/// Idempotent: if a stream with (pipeline_id, name) exists, returns its id.
pub async fn ensure(pool: &PgPool, new: NewStream) -> sqlx::Result<StreamId> {
    if let Some(existing) = get_by_name(pool, new.pipeline_id, &new.name).await? {
        return Ok(existing.stream_id);
    }
    let id = StreamId::new();
    sqlx::query(
        "INSERT INTO streams \
           (stream_id, tenant_id, pipeline_id, name, sync_mode, cursor_config, pk_config, destination_table) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.pipeline_id.as_uuid())
    .bind(&new.name)
    .bind(&new.sync_mode)
    .bind(&new.cursor_config)
    .bind(&new.pk_config)
    .bind(&new.destination_table)
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn get_by_name(
    pool: &PgPool,
    pipeline_id: PipelineId,
    name: &str,
) -> sqlx::Result<Option<Stream>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        Value,
        Value,
        Option<String>,
        Option<uuid::Uuid>,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT stream_id, tenant_id, pipeline_id, name, sync_mode, cursor_config, \
                pk_config, destination_table, current_schema_id, created_at, updated_at \
         FROM streams WHERE pipeline_id = $1 AND name = $2",
    )
    .bind(pipeline_id.as_uuid())
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(sid, tid, pid, name, mode, cur, pk, dest, cs, c, u)| Stream {
        stream_id: StreamId::from_uuid_unchecked(sid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        name,
        sync_mode: mode,
        cursor_config: cur,
        pk_config: pk,
        destination_table: dest,
        current_schema_id: cs.map(SchemaId::from_uuid_unchecked),
        created_at: c,
        updated_at: u,
    }))
}

pub async fn set_current_schema(
    pool: &PgPool,
    stream_id: StreamId,
    schema_id: SchemaId,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE streams SET current_schema_id = $1, updated_at = NOW() WHERE stream_id = $2",
    )
    .bind(schema_id.as_uuid())
    .bind(stream_id.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}
```

- [ ] **Step 2: Expose**

Edit `crates/catalog/src/lib.rs`. Add `pub mod stream;` module decl, and the following methods in `impl Catalog`:

```rust
    pub async fn ensure_stream(&self, new: stream::NewStream) -> sqlx::Result<common_types::ids::StreamId> {
        stream::ensure(&self.pool, new).await
    }
    pub async fn get_stream_by_name(
        &self,
        pipeline_id: common_types::ids::PipelineId,
        name: &str,
    ) -> sqlx::Result<Option<stream::Stream>> {
        stream::get_by_name(&self.pool, pipeline_id, name).await
    }
    pub async fn set_stream_current_schema(
        &self,
        stream_id: common_types::ids::StreamId,
        schema_id: common_types::ids::SchemaId,
    ) -> sqlx::Result<()> {
        stream::set_current_schema(&self.pool, stream_id, schema_id).await
    }
```

- [ ] **Step 3: Test**

Append to `crates/catalog/tests/crud.rs`:

```rust
#[tokio::test]
async fn ensure_stream_is_idempotent() {
    let cat = test_catalog().await;
    let t = cat.create_tenant("acme").await.unwrap();
    let src = cat
        .create_connection(NewConnection {
            tenant_id: t, name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await
        .unwrap();
    let p = cat
        .create_pipeline(NewPipeline {
            tenant_id: t, name: "demo".into(),
            source_conn_id: src, dest_conn_id: None, spec: json!({}),
        })
        .await
        .unwrap();
    let s1 = cat
        .ensure_stream(catalog::stream::NewStream {
            tenant_id: t, pipeline_id: p, name: "customers".into(),
            sync_mode: "incremental".into(),
            cursor_config: json!({"column":"updated_at","kind":"timestamp_tz"}),
            pk_config: json!(["id"]),
            destination_table: None,
        })
        .await
        .unwrap();
    let s2 = cat
        .ensure_stream(catalog::stream::NewStream {
            tenant_id: t, pipeline_id: p, name: "customers".into(),
            sync_mode: "incremental".into(),
            cursor_config: json!({}), // ignored — already exists
            pk_config: json!([]),
            destination_table: None,
        })
        .await
        .unwrap();
    assert_eq!(s1, s2);
}
```

Run: `DATABASE_URL=... cargo test -p catalog -- --test-threads=1`
Expected: **6 passed**.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(catalog): Stream CRUD (ensure + get_by_name + set_current_schema)

ensure_stream is idempotent by (pipeline_id, name). current_schema_id
updated separately once a schema row exists (FK resolves to it).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Catalog CRUD — Schema

**Files:**
- Create: `crates/catalog/src/schema.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/catalog/tests/crud.rs`

- [ ] **Step 1: Write `schema.rs`**

Create `crates/catalog/src/schema.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::evolution::ChangeKind;
use common_types::ids::{SchemaId, StreamId, TenantId};
use common_types::schema_fingerprint::SchemaFingerprint;
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Schema {
    pub schema_id: SchemaId,
    pub tenant_id: TenantId,
    pub stream_id: StreamId,
    pub version: i32,
    pub parent_schema_id: Option<SchemaId>,
    pub fingerprint: SchemaFingerprint,
    pub arrow_schema_json: Value,
    pub change_summary: Vec<ChangeKind>,
    pub detected_at: DateTime<Utc>,
    pub applied_to_destination_at: Option<DateTime<Utc>>,
}

pub struct NewSchema {
    pub tenant_id: TenantId,
    pub stream_id: StreamId,
    pub parent_schema_id: Option<SchemaId>,
    pub fingerprint: SchemaFingerprint,
    pub arrow_schema_json: Value,
    pub change_summary: Vec<ChangeKind>,
}

pub async fn insert(pool: &PgPool, new: NewSchema) -> sqlx::Result<SchemaId> {
    // Compute next version inside a transaction to avoid races.
    let mut tx = pool.begin().await?;
    let next_version: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM schemas WHERE stream_id = $1",
    )
    .bind(new.stream_id.as_uuid())
    .fetch_one(&mut *tx)
    .await?;

    let id = SchemaId::new();
    let change_summary_json = serde_json::to_value(&new.change_summary).expect("serialize Vec<ChangeKind>");
    sqlx::query(
        "INSERT INTO schemas \
           (schema_id, tenant_id, stream_id, version, parent_schema_id, fingerprint, arrow_schema_json, change_summary) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.stream_id.as_uuid())
    .bind(next_version)
    .bind(new.parent_schema_id.map(|p| p.as_uuid()))
    .bind(new.fingerprint.as_hex())
    .bind(&new.arrow_schema_json)
    .bind(&change_summary_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id)
}

pub async fn get_latest(
    pool: &PgPool,
    stream_id: StreamId,
) -> sqlx::Result<Option<Schema>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        uuid::Uuid,
        i32,
        Option<uuid::Uuid>,
        String,
        Value,
        Value,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    )> = sqlx::query_as(
        "SELECT schema_id, tenant_id, stream_id, version, parent_schema_id, fingerprint, \
                arrow_schema_json, change_summary, detected_at, applied_to_destination_at \
         FROM schemas WHERE stream_id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(stream_id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(sid, tid, stid, v, parent, fp, j, chg, d, app)| Schema {
        schema_id: SchemaId::from_uuid_unchecked(sid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        stream_id: StreamId::from_uuid_unchecked(stid),
        version: v,
        parent_schema_id: parent.map(SchemaId::from_uuid_unchecked),
        fingerprint: SchemaFingerprint::from_hex(fp),
        arrow_schema_json: j,
        change_summary: serde_json::from_value(chg).unwrap_or_default(),
        detected_at: d,
        applied_to_destination_at: app,
    }))
}
```

- [ ] **Step 2: Expose**

Edit `crates/catalog/src/lib.rs`. Add `pub mod schema;` module decl and:

```rust
    pub async fn insert_schema(&self, new: schema::NewSchema) -> sqlx::Result<common_types::ids::SchemaId> {
        schema::insert(&self.pool, new).await
    }
    pub async fn get_latest_schema(
        &self,
        stream_id: common_types::ids::StreamId,
    ) -> sqlx::Result<Option<schema::Schema>> {
        schema::get_latest(&self.pool, stream_id).await
    }
```

- [ ] **Step 3: Test**

Append to `crates/catalog/tests/crud.rs`:

```rust
use common_types::schema_fingerprint::SchemaFingerprint;

#[tokio::test]
async fn schema_insert_assigns_versions_sequentially() {
    let cat = test_catalog().await;
    let t = cat.create_tenant("acme").await.unwrap();
    let src = cat
        .create_connection(NewConnection {
            tenant_id: t, name: "src".into(),
            connector_ref: "postgres@0.1.0".into(), config: json!({}),
        })
        .await.unwrap();
    let p = cat
        .create_pipeline(NewPipeline {
            tenant_id: t, name: "demo".into(),
            source_conn_id: src, dest_conn_id: None, spec: json!({}),
        })
        .await.unwrap();
    let s = cat
        .ensure_stream(catalog::stream::NewStream {
            tenant_id: t, pipeline_id: p, name: "customers".into(),
            sync_mode: "incremental".into(),
            cursor_config: json!({}), pk_config: json!([]), destination_table: None,
        })
        .await.unwrap();

    let s1 = cat.insert_schema(catalog::schema::NewSchema {
        tenant_id: t, stream_id: s, parent_schema_id: None,
        fingerprint: SchemaFingerprint::from_hex("aaaa".repeat(16)),
        arrow_schema_json: json!({"fields":[]}),
        change_summary: vec![],
    }).await.unwrap();

    let s2 = cat.insert_schema(catalog::schema::NewSchema {
        tenant_id: t, stream_id: s, parent_schema_id: Some(s1),
        fingerprint: SchemaFingerprint::from_hex("bbbb".repeat(16)),
        arrow_schema_json: json!({"fields":[{"name":"x"}]}),
        change_summary: vec![],
    }).await.unwrap();

    let latest = cat.get_latest_schema(s).await.unwrap().unwrap();
    assert_eq!(latest.schema_id, s2);
    assert_eq!(latest.version, 2);
    assert_eq!(latest.parent_schema_id, Some(s1));
}
```

Run: `DATABASE_URL=... cargo test -p catalog -- --test-threads=1`
Expected: **7 passed**.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(catalog): Schema CRUD — append-only with auto version

insert() runs in a transaction, computes next version via
SELECT MAX(version)+1, inserts the row. get_latest() returns the
highest-versioned row for a stream. change_summary (Vec<ChangeKind>)
stored as JSONB; fingerprint as TEXT.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Schema fingerprinting (pure function + tests)

**Files:**
- Create: `crates/worker/src/schema_evolution/mod.rs`
- Create: `crates/worker/src/schema_evolution/fingerprint.rs`
- Modify: `crates/worker/src/lib.rs`

- [ ] **Step 1: Wire module**

Edit `crates/worker/src/lib.rs`. Append:

```rust
pub mod schema_evolution;
```

Create `crates/worker/src/schema_evolution/mod.rs`:

```rust
//! Schema evolution: fingerprint → diff → policy.
pub mod fingerprint;

pub use fingerprint::fingerprint_schema;
```

- [ ] **Step 2: Implement fingerprinting**

Create `crates/worker/src/schema_evolution/fingerprint.rs`:

```rust
use arrow::datatypes::{DataType, Field, Schema};
use blake3::Hasher;
use common_types::schema_fingerprint::SchemaFingerprint;

/// Produce a BLAKE3 fingerprint of a schema's structural shape.
///
/// Canonical form:
///   - Fields are sorted by name (order-insensitive; reorder is a separate change_kind).
///   - Each field contributes: <name>\0<type>\0<nullable>\0<metadata-sorted>\0
///   - Metadata entries are sorted by key, joined as `k=v`, separated by `,`.
pub fn fingerprint_schema(schema: &Schema) -> SchemaFingerprint {
    let mut fields: Vec<&Field> = schema.fields().iter().map(|f| f.as_ref()).collect();
    fields.sort_by(|a, b| a.name().cmp(b.name()));

    let mut hasher = Hasher::new();
    for f in fields {
        hasher.update(f.name().as_bytes());
        hasher.update(b"\0");
        hasher.update(datatype_string(f.data_type()).as_bytes());
        hasher.update(b"\0");
        hasher.update(if f.is_nullable() { b"1" } else { b"0" });
        hasher.update(b"\0");
        hasher.update(metadata_string(f.metadata()).as_bytes());
        hasher.update(b"\0");
    }
    // Include top-level schema metadata too.
    hasher.update(metadata_string(schema.metadata()).as_bytes());
    let digest = hasher.finalize();
    SchemaFingerprint::from_hex(digest.to_hex().to_string())
}

/// Canonical string form of an Arrow DataType.
/// Uses Debug for now (stable across arrow 53's minor versions for Phase I.4 types).
fn datatype_string(dt: &DataType) -> String {
    format!("{dt:?}")
}

fn metadata_string(md: &std::collections::HashMap<String, String>) -> String {
    let mut entries: Vec<(&String, &String)> = md.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let joined: Vec<String> = entries.iter().map(|(k, v)| format!("{k}={v}")).collect();
    joined.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    #[test]
    fn identical_schemas_fingerprint_equal() {
        let a = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let b = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert_eq!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn column_order_does_not_affect_fingerprint() {
        let a = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let b = schema(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]);
        assert_eq!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn nullability_change_affects_fingerprint() {
        let a = schema(vec![Field::new("x", DataType::Int64, false)]);
        let b = schema(vec![Field::new("x", DataType::Int64, true)]);
        assert_ne!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn type_change_affects_fingerprint() {
        let a = schema(vec![Field::new("x", DataType::Int32, false)]);
        let b = schema(vec![Field::new("x", DataType::Int64, false)]);
        assert_ne!(fingerprint_schema(&a), fingerprint_schema(&b));
    }

    #[test]
    fn added_column_affects_fingerprint() {
        let a = schema(vec![Field::new("x", DataType::Int64, false)]);
        let b = schema(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, true),
        ]);
        assert_ne!(fingerprint_schema(&a), fingerprint_schema(&b));
    }
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p worker schema_evolution::fingerprint`
Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/schema_evolution): BLAKE3 fingerprint of canonicalized Arrow schema

Fields sorted by name (reorder is a separate change_kind, not a
fingerprint bump). Each field contributes name + type (Debug-form) +
nullability + sorted metadata. Schema-level metadata also folded in.
5 tests cover: identical → equal, reorder → equal, nullability → ≠,
type → ≠, add-column → ≠.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Schema diffing (pure function + tests)

**Files:**
- Create: `crates/worker/src/schema_evolution/diff.rs`
- Modify: `crates/worker/src/schema_evolution/mod.rs`

- [ ] **Step 1: Implement diff**

Create `crates/worker/src/schema_evolution/diff.rs`:

```rust
use arrow::datatypes::{DataType, Schema};
use common_types::evolution::ChangeKind;
use std::collections::{BTreeMap, HashSet};

/// Compute the ordered list of changes from `old` → `new`.
pub fn diff_schemas(old: &Schema, new: &Schema) -> Vec<ChangeKind> {
    let mut changes = Vec::new();

    let old_fields: BTreeMap<&str, &arrow::datatypes::Field> =
        old.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();
    let new_fields: BTreeMap<&str, &arrow::datatypes::Field> =
        new.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();

    let old_names: HashSet<&str> = old_fields.keys().copied().collect();
    let new_names: HashSet<&str> = new_fields.keys().copied().collect();

    // Added columns.
    for name in new_names.difference(&old_names) {
        let f = new_fields[name];
        changes.push(ChangeKind::AddColumn {
            name: name.to_string(),
            data_type: datatype_string(f.data_type()),
            nullable: f.is_nullable(),
        });
    }
    // Dropped columns.
    for name in old_names.difference(&new_names) {
        changes.push(ChangeKind::DropColumn { name: name.to_string() });
    }
    // Shared columns: type / nullability change?
    for name in old_names.intersection(&new_names) {
        let o = old_fields[name];
        let n = new_fields[name];
        let o_ty = datatype_string(o.data_type());
        let n_ty = datatype_string(n.data_type());
        if o_ty != n_ty {
            if is_widening(o.data_type(), n.data_type()) {
                changes.push(ChangeKind::WidenType {
                    name: name.to_string(),
                    from: o_ty,
                    to: n_ty,
                });
            } else {
                changes.push(ChangeKind::NarrowType {
                    name: name.to_string(),
                    from: o_ty,
                    to: n_ty,
                });
            }
        }
        if o.is_nullable() != n.is_nullable() {
            if n.is_nullable() {
                changes.push(ChangeKind::MakeNullable { name: name.to_string() });
            } else {
                changes.push(ChangeKind::MakeNonNullable { name: name.to_string() });
            }
        }
    }

    // Column reorder (order-sensitive compare on shared columns).
    let old_order: Vec<String> = old
        .fields()
        .iter()
        .filter(|f| new_names.contains(f.name().as_str()))
        .map(|f| f.name().clone())
        .collect();
    let new_order: Vec<String> = new
        .fields()
        .iter()
        .filter(|f| old_names.contains(f.name().as_str()))
        .map(|f| f.name().clone())
        .collect();
    if old_order != new_order && !old_order.is_empty() {
        changes.push(ChangeKind::ReorderColumns {
            before: old_order,
            after: new_order,
        });
    }

    changes
}

fn datatype_string(dt: &DataType) -> String {
    format!("{dt:?}")
}

fn is_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    matches!(
        (from, to),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64)
            | (UInt16, UInt32 | UInt64)
            | (UInt32, UInt64)
            | (Float32, Float64)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn identical_schemas_no_changes() {
        let s = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(diff_schemas(&s, &s).is_empty());
    }

    #[test]
    fn add_nullable_column() {
        let old = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let new = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int64, true),
        ]);
        let c = diff_schemas(&old, &new);
        assert_eq!(c.len(), 1);
        assert!(matches!(
            &c[0],
            ChangeKind::AddColumn { name, nullable: true, .. } if name == "age"
        ));
    }

    #[test]
    fn drop_column() {
        let old = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let new = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let c = diff_schemas(&old, &new);
        assert!(c.iter().any(|ck| matches!(ck, ChangeKind::DropColumn { name } if name == "email")));
    }

    #[test]
    fn widen_int32_to_int64() {
        let old = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let new = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let c = diff_schemas(&old, &new);
        assert_eq!(c.len(), 1);
        assert!(matches!(&c[0], ChangeKind::WidenType { name, .. } if name == "x"));
    }

    #[test]
    fn narrow_int64_to_int32() {
        let old = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let new = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let c = diff_schemas(&old, &new);
        assert!(c.iter().any(|ck| matches!(ck, ChangeKind::NarrowType { .. })));
    }

    #[test]
    fn make_nullable() {
        let old = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let new = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let c = diff_schemas(&old, &new);
        assert!(c.iter().any(|ck| matches!(ck, ChangeKind::MakeNullable { .. })));
    }
}
```

- [ ] **Step 2: Expose**

Edit `crates/worker/src/schema_evolution/mod.rs`:

```rust
//! Schema evolution: fingerprint → diff → policy.
pub mod diff;
pub mod fingerprint;

pub use diff::diff_schemas;
pub use fingerprint::fingerprint_schema;
```

- [ ] **Step 3: Run**

Run: `cargo test -p worker schema_evolution::diff`
Expected: 6 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/schema_evolution): structured diff producing Vec<ChangeKind>

Compares two Arrow Schemas field-by-field, producing add/drop/widen/
narrow/make-nullable/make-non-nullable/reorder ChangeKind entries.
Widening recognized for int families + float32→float64. Reorder only
emitted when shared-column order actually differs. Six unit tests.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Evolution policy engine

**Files:**
- Create: `crates/worker/src/schema_evolution/policy.rs`
- Modify: `crates/worker/src/schema_evolution/mod.rs`

- [ ] **Step 1: Write policy**

Create `crates/worker/src/schema_evolution/policy.rs`:

```rust
use common_types::evolution::{ChangeKind, EvolutionPolicy};

/// Decision from the evolution policy engine.
pub enum PolicyOutcome {
    /// No changes; use the old schema unchanged.
    NoOp,
    /// All changes accepted; the new schema should be adopted.
    Accept,
    /// Changes reviewed under `Freeze`; old schema retained as canonical.
    /// Caller should project incoming batches onto the old schema.
    RetainOld,
    /// Changes include at least one breaking change under `Strict` or
    /// `PropagateAdditive`; run should fail.
    Reject { reason: String },
}

pub fn apply_policy(policy: EvolutionPolicy, changes: &[ChangeKind]) -> PolicyOutcome {
    if changes.is_empty() {
        return PolicyOutcome::NoOp;
    }
    match policy {
        EvolutionPolicy::Freeze => PolicyOutcome::RetainOld,
        EvolutionPolicy::Strict => PolicyOutcome::Reject {
            reason: format!("strict policy rejects {} change(s)", changes.len()),
        },
        EvolutionPolicy::PropagateAdditive => {
            let non_additive: Vec<&ChangeKind> =
                changes.iter().filter(|c| !c.is_additive()).collect();
            if non_additive.is_empty() {
                PolicyOutcome::Accept
            } else {
                PolicyOutcome::Reject {
                    reason: format!(
                        "propagate_additive rejects {} breaking change(s): {}",
                        non_additive.len(),
                        non_additive
                            .iter()
                            .map(|c| format!("{c:?}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_nullable_col() -> ChangeKind {
        ChangeKind::AddColumn {
            name: "age".into(),
            data_type: "Int64".into(),
            nullable: true,
        }
    }
    fn drop_col() -> ChangeKind {
        ChangeKind::DropColumn { name: "email".into() }
    }

    #[test]
    fn no_changes_is_noop() {
        assert!(matches!(
            apply_policy(EvolutionPolicy::PropagateAdditive, &[]),
            PolicyOutcome::NoOp
        ));
    }

    #[test]
    fn strict_rejects_any_change() {
        let out = apply_policy(EvolutionPolicy::Strict, &[add_nullable_col()]);
        assert!(matches!(out, PolicyOutcome::Reject { .. }));
    }

    #[test]
    fn freeze_retains_old_on_any_change() {
        let out = apply_policy(EvolutionPolicy::Freeze, &[add_nullable_col(), drop_col()]);
        assert!(matches!(out, PolicyOutcome::RetainOld));
    }

    #[test]
    fn propagate_additive_accepts_additive_only() {
        let out = apply_policy(EvolutionPolicy::PropagateAdditive, &[add_nullable_col()]);
        assert!(matches!(out, PolicyOutcome::Accept));
    }

    #[test]
    fn propagate_additive_rejects_breaking() {
        let out = apply_policy(
            EvolutionPolicy::PropagateAdditive,
            &[add_nullable_col(), drop_col()],
        );
        match out {
            PolicyOutcome::Reject { reason } => assert!(reason.contains("breaking change")),
            _ => panic!("expected Reject"),
        }
    }
}
```

- [ ] **Step 2: Expose**

Edit `crates/worker/src/schema_evolution/mod.rs`:

```rust
//! Schema evolution: fingerprint → diff → policy.
pub mod diff;
pub mod fingerprint;
pub mod policy;

pub use diff::diff_schemas;
pub use fingerprint::fingerprint_schema;
pub use policy::{apply_policy, PolicyOutcome};
```

- [ ] **Step 3: Run**

Run: `cargo test -p worker schema_evolution::policy`
Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/schema_evolution): policy engine turns Vec<ChangeKind> into a decision

PolicyOutcome variants: NoOp (empty diff), Accept (all additive under
propagate_additive), RetainOld (freeze — project incoming onto old),
Reject { reason } (strict → any change, propagate_additive → any
breaking). Five unit tests per policy × 2 change profiles.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: `record_and_resolve` — composes catalog + schema-evolution

**Files:**
- Create: `crates/worker/src/schema_evolution/recorder.rs`
- Modify: `crates/worker/src/schema_evolution/mod.rs`

- [ ] **Step 1: Implement**

Create `crates/worker/src/schema_evolution/recorder.rs`:

```rust
use anyhow::{Context, bail};
use arrow::datatypes::{Schema, SchemaRef};
use catalog::Catalog;
use common_types::evolution::EvolutionPolicy;
use common_types::ids::StreamId;
use std::sync::Arc;

use super::diff::diff_schemas;
use super::fingerprint::fingerprint_schema;
use super::policy::{apply_policy, PolicyOutcome};

pub struct ResolvedSchema {
    pub schema: SchemaRef,
    pub schema_id: common_types::ids::SchemaId,
    pub created_new_version: bool,
}

/// Discover-time entry point. Given an incoming Arrow schema:
/// 1. Fingerprint it
/// 2. Load the stream's current schema (if any)
/// 3. If same fingerprint → return current schema (no-op)
/// 4. If different or no prior → diff + apply policy
/// 5. On `Reject` → bail; on `RetainOld` → return stored schema; on
///    `Accept` or first-ever → insert new `schemas` row and set
///    `streams.current_schema_id`
pub async fn record_and_resolve(
    catalog: &Catalog,
    tenant_id: common_types::ids::TenantId,
    stream_id: StreamId,
    policy: EvolutionPolicy,
    incoming: SchemaRef,
) -> anyhow::Result<ResolvedSchema> {
    let fp = fingerprint_schema(&incoming);

    let existing = catalog.get_latest_schema(stream_id).await?;

    // Fast-path: fingerprint matches, no-op.
    if let Some(ref prev) = existing {
        if prev.fingerprint == fp {
            let schema = decode_arrow_schema(&prev.arrow_schema_json)?;
            return Ok(ResolvedSchema {
                schema,
                schema_id: prev.schema_id,
                created_new_version: false,
            });
        }
    }

    // Compute diff (empty if first-ever).
    let changes = match existing.as_ref() {
        None => Vec::new(),
        Some(prev) => {
            let old = decode_arrow_schema(&prev.arrow_schema_json)?;
            diff_schemas(&old, &incoming)
        }
    };

    // Apply policy.
    let outcome = apply_policy(policy, &changes);
    match outcome {
        PolicyOutcome::Reject { reason } => bail!("schema evolution rejected: {reason}"),
        PolicyOutcome::RetainOld => {
            let prev = existing.expect("RetainOld only makes sense when prior exists");
            let schema = decode_arrow_schema(&prev.arrow_schema_json)?;
            return Ok(ResolvedSchema {
                schema,
                schema_id: prev.schema_id,
                created_new_version: false,
            });
        }
        PolicyOutcome::NoOp | PolicyOutcome::Accept => {
            // fall through — insert new version
        }
    }

    let arrow_json = serde_json::to_value(incoming.as_ref())
        .context("serializing Arrow schema to JSON")?;
    let parent = existing.as_ref().map(|p| p.schema_id);
    let new_id = catalog
        .insert_schema(catalog::schema::NewSchema {
            tenant_id,
            stream_id,
            parent_schema_id: parent,
            fingerprint: fp,
            arrow_schema_json: arrow_json,
            change_summary: changes,
        })
        .await
        .context("inserting schema")?;
    catalog
        .set_stream_current_schema(stream_id, new_id)
        .await
        .context("updating streams.current_schema_id")?;

    Ok(ResolvedSchema {
        schema: incoming,
        schema_id: new_id,
        created_new_version: true,
    })
}

fn decode_arrow_schema(v: &serde_json::Value) -> anyhow::Result<SchemaRef> {
    let schema: Schema = serde_json::from_value(v.clone())
        .context("deserializing Arrow schema from JSONB")?;
    Ok(Arc::new(schema))
}
```

- [ ] **Step 2: Expose**

Edit `crates/worker/src/schema_evolution/mod.rs`:

```rust
//! Schema evolution: fingerprint → diff → policy.
pub mod diff;
pub mod fingerprint;
pub mod policy;
pub mod recorder;

pub use diff::diff_schemas;
pub use fingerprint::fingerprint_schema;
pub use policy::{apply_policy, PolicyOutcome};
pub use recorder::{record_and_resolve, ResolvedSchema};
```

- [ ] **Step 3: Build**

Run: `cargo build -p worker`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/schema_evolution): record_and_resolve composes the pipeline

Fingerprint match → no-op (returns stored schema).
Differ → diff + policy; Reject bails, RetainOld returns stored,
Accept/first-ever inserts new version + updates streams.current_schema_id.
Arrow schema stored as JSONB via serde_json::to_value (arrow 53's
serde impls).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Wire `record_and_resolve` into `discover_stream`

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/workflows/pipeline_run.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Add fields to DiscoverInput + DiscoverOutput**

Edit `crates/worker/src/activities/sync/inputs.rs`. Update `DiscoverInput`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub connector_ref: String,
    pub tenant_id: uuid::Uuid,
    pub stream_name: String,
    pub pipeline_id: uuid::Uuid,
    pub cursor_column: String,
    pub cursor_kind: common_types::cursor::CursorKind,
    pub pk_columns: Vec<String>,
    pub evolution_policy: common_types::evolution::EvolutionPolicy,
}
```

And `DiscoverOutput`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverOutput {
    pub columns: Vec<String>,
    pub schema_id: uuid::Uuid,
    pub created_new_version: bool,
}
```

- [ ] **Step 2: Update `discover_stream` activity**

Edit `crates/worker/src/activities/sync/mod.rs`. Replace the `discover_stream` method body:

```rust
    #[activity]
    pub async fn discover_stream(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverInput,
    ) -> Result<DiscoverOutput, ActivityError> {
        let connector =
            build_source_connector(&input.connector_ref, Some(self.wasm_runtime.clone()))
                .map_err(to_retryable)?;
        let schema = connector
            .discover(
                &ConnectionConfig { url: input.source_url.clone() },
                &input.source,
            )
            .await
            .map_err(to_retryable)?;

        // Ensure the stream record exists.
        use common_types::ids::{PipelineId as PId, TenantId};
        let tenant_id = TenantId::from_uuid_unchecked(input.tenant_id);
        let pipeline_id = PId::from_uuid_unchecked(input.pipeline_id);
        let cursor_config = serde_json::json!({
            "column": input.cursor_column,
            "kind": input.cursor_kind,
        });
        let pk_config = serde_json::to_value(&input.pk_columns).unwrap_or(serde_json::json!([]));
        let stream_id = self
            .catalog
            .ensure_stream(catalog::stream::NewStream {
                tenant_id,
                pipeline_id,
                name: input.stream_name.clone(),
                sync_mode: "incremental".into(),
                cursor_config,
                pk_config,
                destination_table: None,
            })
            .await
            .map_err(|e| to_retryable(anyhow::anyhow!("ensure_stream: {e}")))?;

        // Record / resolve schema under the policy.
        let resolved = crate::schema_evolution::record_and_resolve(
            &self.catalog,
            tenant_id,
            stream_id,
            input.evolution_policy,
            schema.clone(),
        )
        .await
        .map_err(to_retryable)?;

        let columns = resolved.schema.fields().iter().map(|f| f.name().clone()).collect();
        Ok(DiscoverOutput {
            columns,
            schema_id: resolved.schema_id.as_uuid(),
            created_new_version: resolved.created_new_version,
        })
    }
```

- [ ] **Step 3: Thread new fields through the workflow**

Edit `crates/worker/src/workflows/pipeline_run.rs`. Extend `PipelineRunInput`:

```rust
    pub evolution_policy: common_types::evolution::EvolutionPolicy,
    pub cursor_column: String,
    pub cursor_kind: common_types::cursor::CursorKind,
    pub pk_columns: Vec<String>,
    pub tenant_id: Uuid,
```

Mirror those on the `#[workflow]` struct. Extend the `#[init]` method to copy them, and the `run` method's `ctx.state(|s| ...)` tuple to include them.

In the `DiscoverInput` construction inside the workflow, pass all the new fields:

```rust
            DiscoverInput {
                source: spec.source.clone(),
                source_url: conn.url.clone(),
                connector_ref: connector_ref.clone(),
                tenant_id,
                stream_name: stream_name.clone(),
                pipeline_id,
                cursor_column: cursor_column.clone(),
                cursor_kind,
                pk_columns: pk_columns.clone(),
                evolution_policy,
            },
```

- [ ] **Step 4: CLI extracts new fields**

Edit `crates/cli/src/main.rs`. In `pipeline_run`, after parsing `spec`, derive the per-source values for the workflow input:

```rust
    use common_types::pipeline_spec::SourceSpec;
    let (cursor_column, cursor_kind, pk_columns) = match &spec.source {
        SourceSpec::Postgres(p) => (
            p.cursor_column.clone(),
            p.cursor_kind,
            p.pk_columns.clone(),
        ),
        SourceSpec::Wasm(_) => (
            "_row_index".to_string(),
            common_types::cursor::CursorKind::Int64,
            vec![],
        ),
    };

    // evolution_policy: pulled from pipelines.spec via serde_json (lives in PipelineSpec
    // in Phase I.4 — default to PropagateAdditive if missing).
    let evolution_policy = pipeline
        .spec
        .get("evolution_policy")
        .and_then(|v| serde_json::from_value::<common_types::evolution::EvolutionPolicy>(v.clone()).ok())
        .unwrap_or_default();
```

Extend the `PipelineRunInput`:

```rust
    let input = PipelineRunInput {
        run_id: run_id.as_uuid(),
        pipeline_id: pipeline_id.as_uuid(),
        spec,
        source_connection,
        initial_cursor,
        stream_name,
        connector_ref,
        evolution_policy,
        cursor_column,
        cursor_kind,
        pk_columns,
        tenant_id: pipeline.tenant_id.as_uuid(),
    };
```

- [ ] **Step 5: Build + rerun Phase I.3 integration test**

Run: `cargo build --workspace`
Expected: clean.

Run (full stack up): `DATABASE_URL=... cargo test -p integration-tests csv_wasm_connector_end_to_end -- --ignored --nocapture`
Expected: passes. WASM path should now also auto-create `stream` + `schema` rows on first run.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(worker): discover_stream now records Schema + resolves policy

Activity creates the Stream row (idempotent by pipeline,name) and
invokes schema_evolution::record_and_resolve with the stream's
EvolutionPolicy. First run creates Schema v1. Unchanged discovery is
a no-op (fingerprint match). Drift → new Schema version with
change_summary recorded in JSONB.

DiscoverInput + PipelineRunInput extended with the fields needed to
locate the stream and pick a policy; CLI derives them from the
pipeline spec.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: YAML DSL parser + apply engine

**Files:**
- Create: `crates/cli/src/dsl.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Write the parser/apply module**

Create `crates/cli/src/dsl.rs`:

```rust
//! Parse YAML resource files and apply them to the catalog idempotently.

use anyhow::{Context, bail};
use catalog::Catalog;
use common_types::dsl::{
    ConnectionSpec, Metadata, PipelineDslSpec, ResourceEnvelope, ResourceKind,
};
use common_types::ids::TenantId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct ParsedFile {
    pub path: PathBuf,
    pub resources: Vec<ResourceEnvelope>,
}

#[derive(Debug, Default)]
pub struct ApplyReport {
    pub connections_created: usize,
    pub connections_updated: usize,
    pub connections_unchanged: usize,
    pub pipelines_created: usize,
    pub pipelines_updated: usize,
    pub pipelines_unchanged: usize,
}

pub fn load_path(path: &Path) -> anyhow::Result<Vec<ParsedFile>> {
    let mut out = Vec::new();
    if path.is_file() {
        out.push(parse_file(path)?);
    } else if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("yaml")
                || p.extension().and_then(|e| e.to_str()) == Some("yml")
            {
                out.push(parse_file(&p)?);
            }
        }
    } else {
        bail!("path not found: {}", path.display());
    }
    Ok(out)
}

fn parse_file(path: &Path) -> anyhow::Result<ParsedFile> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let mut resources = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(&text) {
        let env: ResourceEnvelope = serde::Deserialize::deserialize(doc)
            .with_context(|| format!("parsing YAML doc in {}", path.display()))?;
        if env.api_version != "platform.etl/v0" {
            bail!(
                "unsupported apiVersion '{}' in {}",
                env.api_version,
                path.display()
            );
        }
        resources.push(env);
    }
    Ok(ParsedFile {
        path: path.to_path_buf(),
        resources,
    })
}

pub async fn apply(
    catalog: &Catalog,
    tenant_id: TenantId,
    files: &[ParsedFile],
) -> anyhow::Result<ApplyReport> {
    let mut report = ApplyReport::default();

    // Resolve default workspace.
    let _workspace_id = catalog.ensure_default_workspace(tenant_id).await?;

    // Two-pass apply: connections first (pipelines reference them by name).
    let mut connections: HashMap<String, ConnectionSpec> = HashMap::new();
    let mut pipelines: HashMap<String, (Metadata, PipelineDslSpec)> = HashMap::new();

    for file in files {
        for env in &file.resources {
            match env.kind {
                ResourceKind::Connection => {
                    let spec: ConnectionSpec = serde_json::from_value(env.spec.clone())
                        .with_context(|| format!("parsing Connection spec for {}", env.metadata.name))?;
                    connections.insert(env.metadata.name.clone(), spec);
                }
                ResourceKind::Pipeline => {
                    let spec: PipelineDslSpec = serde_json::from_value(env.spec.clone())
                        .with_context(|| format!("parsing Pipeline spec for {}", env.metadata.name))?;
                    pipelines.insert(env.metadata.name.clone(), (env.metadata.clone(), spec));
                }
            }
        }
    }

    // Apply connections.
    let mut conn_name_to_id = HashMap::new();
    for (name, spec) in &connections {
        let (id, action) = upsert_connection(catalog, tenant_id, name, spec).await?;
        conn_name_to_id.insert(name.clone(), id);
        match action {
            UpsertAction::Created => report.connections_created += 1,
            UpsertAction::Updated => report.connections_updated += 1,
            UpsertAction::Unchanged => report.connections_unchanged += 1,
        }
    }

    // Apply pipelines.
    for (name, (_meta, spec)) in &pipelines {
        let src_id = conn_name_to_id
            .get(&spec.source_connection)
            .copied()
            .with_context(|| {
                format!(
                    "pipeline '{name}' references connection '{}' which was not applied",
                    spec.source_connection
                )
            })?;
        let action = upsert_pipeline(catalog, tenant_id, name, src_id, spec).await?;
        match action {
            UpsertAction::Created => report.pipelines_created += 1,
            UpsertAction::Updated => report.pipelines_updated += 1,
            UpsertAction::Unchanged => report.pipelines_unchanged += 1,
        }
    }

    Ok(report)
}

#[derive(Debug, Clone, Copy)]
enum UpsertAction {
    Created,
    Updated,
    Unchanged,
}

async fn upsert_connection(
    catalog: &Catalog,
    tenant_id: TenantId,
    name: &str,
    spec: &ConnectionSpec,
) -> anyhow::Result<(common_types::ids::ConnectionId, UpsertAction)> {
    use sqlx::Row;
    let existing: Option<(uuid::Uuid, String, serde_json::Value)> = sqlx::query_as(
        "SELECT connection_id, connector_ref, config FROM connections \
         WHERE tenant_id = $1 AND name = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(name)
    .fetch_optional(catalog.pool())
    .await?;

    if let Some((cid, cur_ref, cur_config)) = existing {
        if cur_ref == spec.connector_ref && cur_config == spec.config {
            return Ok((
                common_types::ids::ConnectionId::from_uuid_unchecked(cid),
                UpsertAction::Unchanged,
            ));
        }
        sqlx::query(
            "UPDATE connections SET connector_ref = $1, config = $2, updated_at = NOW() \
             WHERE connection_id = $3",
        )
        .bind(&spec.connector_ref)
        .bind(&spec.config)
        .bind(cid)
        .execute(catalog.pool())
        .await?;
        return Ok((
            common_types::ids::ConnectionId::from_uuid_unchecked(cid),
            UpsertAction::Updated,
        ));
    }
    let id = catalog
        .create_connection(catalog::NewConnection {
            tenant_id,
            name: name.to_string(),
            connector_ref: spec.connector_ref.clone(),
            config: spec.config.clone(),
        })
        .await?;
    Ok((id, UpsertAction::Created))
}

async fn upsert_pipeline(
    catalog: &Catalog,
    tenant_id: TenantId,
    name: &str,
    source_conn_id: common_types::ids::ConnectionId,
    spec: &PipelineDslSpec,
) -> anyhow::Result<UpsertAction> {
    let spec_json = serde_json::json!({
        "source": spec.source,
        "destination": spec.destination,
        "batch_size": spec.batch_size,
        "evolution_policy": spec.evolution_policy,
    });

    let existing: Option<(uuid::Uuid, uuid::Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT pipeline_id, source_conn_id, spec FROM pipelines \
         WHERE tenant_id = $1 AND name = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(name)
    .fetch_optional(catalog.pool())
    .await?;

    if let Some((pid, cur_src, cur_spec)) = existing {
        if cur_src == source_conn_id.as_uuid() && cur_spec == spec_json {
            return Ok(UpsertAction::Unchanged);
        }
        sqlx::query(
            "UPDATE pipelines SET source_conn_id = $1, spec = $2, updated_at = NOW() \
             WHERE pipeline_id = $3",
        )
        .bind(source_conn_id.as_uuid())
        .bind(&spec_json)
        .bind(pid)
        .execute(catalog.pool())
        .await?;
        return Ok(UpsertAction::Updated);
    }
    catalog
        .create_pipeline(catalog::NewPipeline {
            tenant_id,
            name: name.to_string(),
            source_conn_id,
            dest_conn_id: None,
            spec: spec_json,
        })
        .await?;
    Ok(UpsertAction::Created)
}
```

- [ ] **Step 2: Wire into CLI**

Edit `crates/cli/src/main.rs`. Add the `mod dsl;` declaration at the top.

- [ ] **Step 3: Build**

Run: `cargo build -p cli`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(cli): YAML DSL parser + apply engine

dsl.rs handles multi-document YAML with apiVersion platform.etl/v0,
parsing Connection and Pipeline resources. apply() does two-pass
(connections first, pipelines second) upsert-by-(tenant_id, name),
reporting created/updated/unchanged counts. Idempotent: second apply
with identical YAML returns all-unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: CLI `platform apply`, `get`, `validate` subcommands

**Files:**
- Modify: `crates/cli/src/main.rs`
- Create: `examples/dsl/customers-sync.yaml`

- [ ] **Step 1: Extend the Cmd enum**

Edit `crates/cli/src/main.rs`. Add the new subcommands:

```rust
#[derive(Subcommand)]
enum Cmd {
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },
    Connector {
        #[command(subcommand)]
        cmd: ConnectorCmd,
    },
    /// Apply YAML resources (Connection, Pipeline) to the catalog.
    Apply {
        #[arg(short, long)]
        file: String,
    },
    /// Print a catalog resource.
    Get {
        kind: String,
        name: String,
    },
    /// Parse YAML and resolve references without writing to the catalog.
    Validate {
        #[arg(short, long)]
        file: String,
    },
}
```

And add the dispatch arms:

```rust
    match cli.cmd {
        Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => pipeline_run(id).await,
        Cmd::Connector {
            cmd: ConnectorCmd::Build { path, name, version, out },
        } => connector_build(path, name, version, out).await,
        Cmd::Apply { file } => apply(file).await,
        Cmd::Get { kind, name } => get(kind, name).await,
        Cmd::Validate { file } => validate(file).await,
    }
```

- [ ] **Step 2: Implement the three new functions**

Append to `crates/cli/src/main.rs`:

```rust
async fn apply(file: String) -> anyhow::Result<()> {
    use std::path::PathBuf;
    let path = PathBuf::from(&file);
    let files = dsl::load_path(&path)?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;

    // Phase I.4: apply into a "dev" tenant by default. Phase II.1 takes this from auth.
    let tenant_id = ensure_dev_tenant(&catalog).await?;
    let report = dsl::apply(&catalog, tenant_id, &files).await?;

    println!(
        "applied:\n  connections: {} created, {} updated, {} unchanged\n  pipelines:   {} created, {} updated, {} unchanged",
        report.connections_created,
        report.connections_updated,
        report.connections_unchanged,
        report.pipelines_created,
        report.pipelines_updated,
        report.pipelines_unchanged,
    );
    Ok(())
}

async fn validate(file: String) -> anyhow::Result<()> {
    use std::path::PathBuf;
    let path = PathBuf::from(&file);
    let files = dsl::load_path(&path)?;
    let mut conn_names = std::collections::HashSet::new();
    let mut pipes = Vec::new();
    for f in &files {
        for env in &f.resources {
            match env.kind {
                common_types::dsl::ResourceKind::Connection => {
                    conn_names.insert(env.metadata.name.clone());
                }
                common_types::dsl::ResourceKind::Pipeline => {
                    let spec: common_types::dsl::PipelineDslSpec =
                        serde_json::from_value(env.spec.clone())?;
                    pipes.push((env.metadata.name.clone(), spec));
                }
            }
        }
    }
    for (name, spec) in &pipes {
        if !conn_names.contains(&spec.source_connection) {
            anyhow::bail!(
                "pipeline '{name}' references connection '{}' which is not declared",
                spec.source_connection
            );
        }
    }
    println!(
        "validated {} file(s): {} connection(s), {} pipeline(s)",
        files.len(),
        conn_names.len(),
        pipes.len()
    );
    Ok(())
}

async fn get(kind: String, name: String) -> anyhow::Result<()> {
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    let tenant_id = ensure_dev_tenant(&catalog).await?;

    match kind.as_str() {
        "connection" => {
            let row: Option<(uuid::Uuid, String, String, serde_json::Value)> = sqlx::query_as(
                "SELECT connection_id, name, connector_ref, config \
                 FROM connections WHERE tenant_id = $1 AND name = $2",
            )
            .bind(tenant_id.as_uuid())
            .bind(&name)
            .fetch_optional(catalog.pool())
            .await?;
            let (_id, name, connector_ref, config) = row.with_context(|| {
                format!("connection '{}' not found", name)
            })?;
            let env = serde_yaml::to_string(&common_types::dsl::ResourceEnvelope {
                api_version: "platform.etl/v0".into(),
                kind: common_types::dsl::ResourceKind::Connection,
                metadata: common_types::dsl::Metadata {
                    name,
                    workspace: Some("default".into()),
                    labels: Default::default(),
                },
                spec: serde_json::json!({
                    "connector_ref": connector_ref,
                    "config": config,
                }),
            })?;
            print!("{env}");
        }
        "pipeline" => {
            let row: Option<(uuid::Uuid, String, uuid::Uuid, serde_json::Value)> = sqlx::query_as(
                "SELECT pipeline_id, name, source_conn_id, spec \
                 FROM pipelines WHERE tenant_id = $1 AND name = $2",
            )
            .bind(tenant_id.as_uuid())
            .bind(&name)
            .fetch_optional(catalog.pool())
            .await?;
            let (_id, pname, src, spec) = row.with_context(|| {
                format!("pipeline '{}' not found", name)
            })?;
            let src_name: String = sqlx::query_scalar(
                "SELECT name FROM connections WHERE connection_id = $1",
            )
            .bind(src)
            .fetch_one(catalog.pool())
            .await?;
            let env = serde_yaml::to_string(&common_types::dsl::ResourceEnvelope {
                api_version: "platform.etl/v0".into(),
                kind: common_types::dsl::ResourceKind::Pipeline,
                metadata: common_types::dsl::Metadata {
                    name: pname,
                    workspace: Some("default".into()),
                    labels: Default::default(),
                },
                spec: serde_json::json!({
                    "source_connection": src_name,
                    "source": spec.get("source").cloned().unwrap_or(serde_json::json!({})),
                    "destination": spec.get("destination").cloned().unwrap_or(serde_json::json!({})),
                    "batch_size": spec.get("batch_size").cloned().unwrap_or(serde_json::json!(100)),
                    "evolution_policy": spec.get("evolution_policy").cloned().unwrap_or(serde_json::json!("propagate_additive")),
                }),
            })?;
            print!("{env}");
        }
        other => anyhow::bail!("unknown kind: {other} (expected 'connection' or 'pipeline')"),
    }
    Ok(())
}

async fn ensure_dev_tenant(catalog: &Catalog) -> anyhow::Result<common_types::ids::TenantId> {
    // Phase I.4: single implicit tenant. Phase II.1 replaces this with auth.
    const DEV_TENANT_UUID: &str = "11111111-1111-1111-1111-111111111111";
    let uuid = uuid::Uuid::parse_str(DEV_TENANT_UUID)?;
    let tid = common_types::ids::TenantId::from_uuid_unchecked(uuid);
    if catalog.get_tenant(tid).await?.is_none() {
        sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, 'dev') ON CONFLICT DO NOTHING")
            .bind(uuid)
            .execute(catalog.pool())
            .await?;
    }
    Ok(tid)
}
```

- [ ] **Step 3: Example YAML**

Create `examples/dsl/customers-sync.yaml`:

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: source-demo
spec:
  connector_ref: postgres@0.1.0
  config:
    url: postgres://etl:etl@localhost:5432/etl_source_demo
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: customers-sync
spec:
  source_connection: source-demo
  source:
    type: postgres
    schema: public
    table: customers
    cursor_column: updated_at
    cursor_kind: timestamp_tz
    pk_columns: [id]
  destination:
    type: local_parquet
    base_path: ./data
  batch_size: 4
  evolution_policy: propagate_additive
```

- [ ] **Step 4: Build + run live**

Run: `cargo build -p cli`
Expected: clean.

Run: `cargo run --bin platform -- validate -f examples/dsl/customers-sync.yaml`
Expected: `validated 1 file(s): 1 connection(s), 1 pipeline(s)`.

Run: `cargo run --bin platform -- apply -f examples/dsl/customers-sync.yaml`
Expected: first run `connections: 1 created, 0 updated, 0 unchanged; pipelines: 1 created ...`.

Run: `cargo run --bin platform -- apply -f examples/dsl/customers-sync.yaml`
Expected: second run all `unchanged`.

Run: `cargo run --bin platform -- get connection source-demo`
Expected: YAML output matches the original file's Connection section.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cli): apply/get/validate subcommands + demo YAML

platform apply -f <file-or-dir> applies Connection + Pipeline
resources two-pass. platform get <kind> <name> serializes the catalog
row back out as YAML. platform validate -f parses + resolves
references without writing. dev tenant auto-created on first apply.

examples/dsl/customers-sync.yaml is the canonical demo that
round-trips through all three commands.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: CLI `platform diff` — show catalog vs. file

**Files:**
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/cli/src/dsl.rs`

- [ ] **Step 1: Add `diff` function to `dsl.rs`**

Append to `crates/cli/src/dsl.rs`:

```rust
#[derive(Debug, Clone)]
pub enum DiffRow {
    Create { kind: ResourceKind, name: String },
    Update { kind: ResourceKind, name: String, fields: Vec<String> },
    Unchanged { kind: ResourceKind, name: String },
}

pub async fn diff(
    catalog: &Catalog,
    tenant_id: TenantId,
    files: &[ParsedFile],
) -> anyhow::Result<Vec<DiffRow>> {
    let mut out = Vec::new();
    for file in files {
        for env in &file.resources {
            match env.kind {
                ResourceKind::Connection => {
                    let spec: ConnectionSpec = serde_json::from_value(env.spec.clone())?;
                    let existing: Option<(uuid::Uuid, String, serde_json::Value)> = sqlx::query_as(
                        "SELECT connection_id, connector_ref, config FROM connections \
                         WHERE tenant_id = $1 AND name = $2",
                    )
                    .bind(tenant_id.as_uuid())
                    .bind(&env.metadata.name)
                    .fetch_optional(catalog.pool())
                    .await?;
                    match existing {
                        None => out.push(DiffRow::Create {
                            kind: ResourceKind::Connection,
                            name: env.metadata.name.clone(),
                        }),
                        Some((_id, cur_ref, cur_cfg)) => {
                            let mut fields = Vec::new();
                            if cur_ref != spec.connector_ref { fields.push("connector_ref".into()); }
                            if cur_cfg != spec.config { fields.push("config".into()); }
                            if fields.is_empty() {
                                out.push(DiffRow::Unchanged {
                                    kind: ResourceKind::Connection,
                                    name: env.metadata.name.clone(),
                                });
                            } else {
                                out.push(DiffRow::Update {
                                    kind: ResourceKind::Connection,
                                    name: env.metadata.name.clone(),
                                    fields,
                                });
                            }
                        }
                    }
                }
                ResourceKind::Pipeline => {
                    let spec: PipelineDslSpec = serde_json::from_value(env.spec.clone())?;
                    let existing: Option<serde_json::Value> = sqlx::query_scalar(
                        "SELECT spec FROM pipelines WHERE tenant_id = $1 AND name = $2",
                    )
                    .bind(tenant_id.as_uuid())
                    .bind(&env.metadata.name)
                    .fetch_optional(catalog.pool())
                    .await?;
                    let new_spec = serde_json::json!({
                        "source": spec.source,
                        "destination": spec.destination,
                        "batch_size": spec.batch_size,
                        "evolution_policy": spec.evolution_policy,
                    });
                    match existing {
                        None => out.push(DiffRow::Create {
                            kind: ResourceKind::Pipeline,
                            name: env.metadata.name.clone(),
                        }),
                        Some(cur) => {
                            if cur == new_spec {
                                out.push(DiffRow::Unchanged {
                                    kind: ResourceKind::Pipeline,
                                    name: env.metadata.name.clone(),
                                });
                            } else {
                                let fields = diff_json_fields(&cur, &new_spec);
                                out.push(DiffRow::Update {
                                    kind: ResourceKind::Pipeline,
                                    name: env.metadata.name.clone(),
                                    fields,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

fn diff_json_fields(a: &serde_json::Value, b: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let (Some(ao), Some(bo)) = (a.as_object(), b.as_object()) {
        for k in ao.keys().chain(bo.keys()).collect::<std::collections::BTreeSet<_>>() {
            if ao.get(k) != bo.get(k) {
                out.push(k.clone());
            }
        }
    }
    out
}
```

- [ ] **Step 2: Wire into CLI**

Edit `crates/cli/src/main.rs`. Add to `Cmd`:

```rust
    /// Show what would change if this file were applied.
    Diff {
        #[arg(short, long)]
        file: String,
    },
```

Match arm:

```rust
        Cmd::Diff { file } => diff_cmd(file).await,
```

And the function:

```rust
async fn diff_cmd(file: String) -> anyhow::Result<()> {
    use std::path::PathBuf;
    let path = PathBuf::from(&file);
    let files = dsl::load_path(&path)?;
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;
    let tenant_id = ensure_dev_tenant(&catalog).await?;
    let rows = dsl::diff(&catalog, tenant_id, &files).await?;
    for row in rows {
        match row {
            dsl::DiffRow::Create { kind, name } => println!("+ {kind:?}/{name}"),
            dsl::DiffRow::Update { kind, name, fields } => {
                println!("~ {kind:?}/{name} ({})", fields.join(", "))
            }
            dsl::DiffRow::Unchanged { kind, name } => println!("= {kind:?}/{name}"),
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Live-test**

Run: `cargo run --bin platform -- diff -f examples/dsl/customers-sync.yaml`
Expected: on applied state, output contains `= Connection/source-demo` and `= Pipeline/customers-sync`.

Modify `batch_size` in the YAML from 4 to 8, save, re-run diff:
Expected: `~ Pipeline/customers-sync (batch_size)`.

Revert before committing.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(cli): platform diff -f <file> — show catalog vs. YAML

Per-resource: + Create (not in catalog), ~ Update with list of changed
fields, = Unchanged. JSON diff for pipeline.spec top-level keys.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: Integration test — Postgres schema evolution end-to-end

**Files:**
- Create: `tests/integration/tests/schema_evolution.rs`

Workflow: seed source, run pipeline (Schema v1), ALTER TABLE to add a column, rerun, verify Schema v2 + change_summary + Parquet includes new column.

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/schema_evolution.rs`:

```rust
//! Postgres schema evolution end-to-end.
//! Run 1: baseline schema captured as v1.
//! ALTER TABLE adds nullable column.
//! Run 2: Schema v2 created with AddColumn change_kind; Parquet includes new column.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn source_url() -> String {
    std::env::var("SOURCE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); p.pop();
    p
}

async fn reseed_source() -> anyhow::Result<()> {
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());
    Ok(())
}

async fn run_sql(db: &str, sql: &str) -> anyhow::Result<()> {
    let mut child = Command::new("docker")
        .args(["exec", "-i", "etl-postgres", "psql", "-U", "etl", "-d", db])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()?;
    if let Some(mut s) = child.stdin.take() {
        s.write_all(sql.as_bytes()).await?;
        s.shutdown().await?;
    }
    let status = child.wait().await?;
    assert!(status.success(), "psql: {sql}");
    Ok(())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

async fn run_cli(pipe: common_types::ids::PipelineId) -> anyhow::Result<()> {
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success(), "cli: {}", String::from_utf8_lossy(&out.stderr));
    Ok(())
}

async fn wait_for_last_run_completed(cat: &Catalog, timeout: Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool())
                .await?;
        if let Some((s,)) = row {
            if RunStatus::parse(&s) == Some(RunStatus::Completed) { return Ok(()); }
            if RunStatus::parse(&s) == Some(RunStatus::Failed) { anyhow::bail!("run failed"); }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("timeout waiting for completion");
}

fn parquet_column_names(dir: &Path) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
            for batch in reader {
                let b = batch.unwrap();
                for field in b.schema().fields() {
                    out.insert(field.name().clone());
                }
            }
        }
    }
    out
}

#[tokio::test]
#[ignore = "requires docker stack + source demo; adds then drops column"]
async fn schema_evolution_adds_column_on_second_run() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status().await?;
    assert!(status.success());

    reseed_source().await?;

    // Drop nickname if leftover from prior runs.
    let _ = run_sql("etl_source_demo", "ALTER TABLE customers DROP COLUMN IF EXISTS nickname;").await;

    let tmp = tempfile::tempdir()?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat.create_connection(NewConnection {
        tenant_id: tenant, name: "source-demo".into(),
        connector_ref: "postgres@0.1.0".into(),
        config: json!({ "url": source_url() }),
    }).await?;
    let spec = json!({
        "source": {"type":"postgres","schema":"public","table":"customers",
                   "cursor_column":"updated_at","cursor_kind":"timestamp_tz","pk_columns":["id"]},
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 4,
        "evolution_policy": "propagate_additive",
    });
    let pipe = cat.create_pipeline(NewPipeline {
        tenant_id: tenant, name: "customers-sync".into(),
        source_conn_id: src, dest_conn_id: None, spec,
    }).await?;

    let mut w = spawn_worker().await?;

    // Run 1 — baseline.
    run_cli(pipe).await?;
    wait_for_last_run_completed(&cat, Duration::from_secs(60)).await?;

    // Verify Schema v1 exists for the stream.
    let stream = cat.get_stream_by_name(pipe, "customers").await?.unwrap();
    let v1 = cat.get_latest_schema(stream.stream_id).await?.unwrap();
    assert_eq!(v1.version, 1);
    assert!(v1.parent_schema_id.is_none());
    assert!(v1.change_summary.is_empty());

    // Alter source: add nickname column (nullable).
    run_sql("etl_source_demo", "ALTER TABLE customers ADD COLUMN nickname TEXT;").await?;
    // Touch all rows so cursor advances and the table is re-read.
    run_sql("etl_source_demo",
            "UPDATE customers SET updated_at = updated_at + interval '1 day';").await?;

    // Run 2 — should detect add_column and record Schema v2.
    run_cli(pipe).await?;
    wait_for_last_run_completed(&cat, Duration::from_secs(60)).await?;

    let v2 = cat.get_latest_schema(stream.stream_id).await?.unwrap();
    assert_eq!(v2.version, 2);
    assert_eq!(v2.parent_schema_id, Some(v1.schema_id));
    assert!(
        v2.change_summary.iter().any(|c| matches!(
            c,
            common_types::evolution::ChangeKind::AddColumn { name, nullable: true, .. } if name == "nickname"
        )),
        "expected AddColumn(nickname, nullable=true) in change_summary, got {:?}",
        v2.change_summary
    );

    // Parquet files from the second run should include nickname.
    let cols = parquet_column_names(tmp.path());
    assert!(cols.contains("nickname"), "parquet missing 'nickname'; got {cols:?}");

    // Cleanup before exit.
    let _ = run_sql("etl_source_demo", "ALTER TABLE customers DROP COLUMN IF EXISTS nickname;").await;

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
```

- [ ] **Step 2: Run**

Run: `DATABASE_URL=... cargo test -p integration-tests schema_evolution -- --ignored --nocapture`
Expected: 1 passed, ~90 s.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(integration): Postgres schema evolution end-to-end

Run 1 captures baseline schema as v1. ALTER TABLE ADD COLUMN
nickname, touch all rows so the cursor advances. Run 2 detects
AddColumn(nickname, nullable=true) under propagate_additive policy,
inserts Schema v2 linked to v1, and Parquet files from run 2 contain
the nickname column. Cleanup drops the column at the end.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 16: Integration test — DSL apply idempotency

**Files:**
- Create: `tests/integration/tests/dsl_apply.rs`

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/dsl_apply.rs`:

```rust
use anyhow::Context;
use catalog::Catalog;
use std::path::PathBuf;
use tokio::process::Command;

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}
fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); p.pop();
    p
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn apply_is_idempotent() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status().await?;
    assert!(status.success());

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // First apply.
    let out1 = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(out1.status.success(), "apply 1 failed: {}", String::from_utf8_lossy(&out1.stderr));
    let s1 = String::from_utf8_lossy(&out1.stdout);
    assert!(s1.contains("1 created"), "expected 1 created on first apply: {s1}");

    // Second apply — should all be unchanged.
    let out2 = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(out2.status.success());
    let s2 = String::from_utf8_lossy(&out2.stdout);
    assert!(
        s2.contains("0 created") && s2.contains("2 unchanged"),
        "expected everything unchanged on second apply: {s2}"
    );

    // Diff should report all unchanged.
    let out3 = Command::new(cargo_bin("platform"))
        .args(["diff", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(out3.status.success());
    let s3 = String::from_utf8_lossy(&out3.stdout);
    assert!(s3.lines().all(|l| l.starts_with('=') || l.is_empty()), "expected all =Unchanged lines:\n{s3}");

    Ok(())
}
```

- [ ] **Step 2: Run**

Run: `DATABASE_URL=... cargo test -p integration-tests apply_is_idempotent -- --ignored --nocapture`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(integration): DSL apply idempotency + diff clean

First apply creates 1 connection + 1 pipeline. Second apply reports
all-unchanged counts. diff -f on the same file produces only '='
lines (no +/~).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 17: Regression check + README + completion log

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-23-phase-1-4-catalog-dsl.md` (this file)

- [ ] **Step 1: Rerun all integration tests (Phase I.2, I.3, I.4) — regression check**

With docker stack up + source-demo seeded:

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  cargo test -p integration-tests -- --ignored --nocapture
```

Expected: all 6 integration tests pass (incremental_sync, sync_survives_worker_kill_midbatch, workflow_survives_worker_restart, csv_wasm_connector_end_to_end, schema_evolution_adds_column_on_second_run, apply_is_idempotent).

- [ ] **Step 2: Update README Phase line + demo section**

Edit `README.md`. Replace the `Currently: ...` line with:

```markdown
Currently: **Phase I.4 — Full Catalog + YAML DSL (complete)**. Next: Phase I.5 — Transformation DAG. See the roadmap spec for the four-era trajectory.

## Phase I.4 — YAML DSL + schema evolution demo

```bash
# 1. Validate the YAML
cargo run --bin platform -- validate -f examples/dsl/customers-sync.yaml

# 2. Apply (creates 1 connection + 1 pipeline idempotently)
cargo run --bin platform -- apply -f examples/dsl/customers-sync.yaml

# 3. Run the pipeline — Schema v1 auto-captured from the discovered columns
cargo run --bin worker &
bash scripts/seed-source-demo.sh
cargo run --bin platform -- pipeline run pipe-<uuid-from-apply>

# 4. Alter source schema; rerun; observe Schema v2 with AddColumn change_summary
docker exec -i etl-postgres psql -U etl -d etl_source_demo -c \
  "ALTER TABLE customers ADD COLUMN nickname TEXT;"
docker exec -i etl-postgres psql -U etl -d etl_source_demo -c \
  "UPDATE customers SET updated_at = updated_at + interval '1 day';"
cargo run --bin platform -- pipeline run pipe-<uuid-from-apply>
docker exec -i etl-postgres psql -U etl -d etl_catalog -c \
  "SELECT version, change_summary FROM schemas ORDER BY version;"

# 5. `get` round-trips a catalog row back to YAML
cargo run --bin platform -- get pipeline customers-sync

# 6. `diff -f` shows what would change if the local YAML were reapplied
cargo run --bin platform -- diff -f examples/dsl/customers-sync.yaml
```

Evolution policies: `propagate_additive` (default — additive flows through), `freeze` (retain old schema), `strict` (fail run on any change). Set in `PipelineDslSpec.evolution_policy`.
```

Extend the crate map in README:

```markdown
| `common-types` | IDs, `PipelineSpec`, `CursorValue`, `SchemaFingerprint`, `EvolutionPolicy`, DSL types | I.1 / I.2 / I.3 / I.4 |
| `catalog` | Metadata store — tenants, workspaces, connections, pipelines, streams, schemas, runs | I.1 → I.4 |
```

- [ ] **Step 3: Completion log**

Append to the bottom of this plan file:

```markdown

---

## Phase I.4 Completion Log

Completed 2026-04-24 on branch `phase-1-4-catalog-dsl`, 17 commits (one per task).

- [x] Task 1 — Dep additions
- [x] Task 2 — common-types (fingerprint, evolution, DSL)
- [x] Task 3 — Migration 0003
- [x] Task 4 — Workspace CRUD
- [x] Task 5 — Stream CRUD
- [x] Task 6 — Schema CRUD
- [x] Task 7 — Fingerprinting
- [x] Task 8 — Diffing
- [x] Task 9 — Policy engine
- [x] Task 10 — record_and_resolve
- [x] Task 11 — discover_stream integration
- [x] Task 12 — YAML parser + apply
- [x] Task 13 — apply/get/validate CLI
- [x] Task 14 — diff CLI
- [x] Task 15 — Schema evolution integration test
- [x] Task 16 — DSL idempotency integration test
- [x] Task 17 — This regression check + docs

### Exit criterion — MET

- Schema fingerprint + diff + policy engine: 16 unit tests pass (5 fingerprint + 6 diff + 5 policy)
- `schema_evolution_adds_column_on_second_run` passes: Schema v1 → ALTER TABLE → Schema v2 with AddColumn(nickname, nullable=true); Parquet from run 2 includes the new column
- `apply_is_idempotent` passes: second apply of same YAML reports all-unchanged; diff emits only `=` lines
- Phase I.2 + I.3 integration tests still green (6 total green integration tests)

### Deviations

(Fill in as encountered during execution.)

### Handoff to Phase I.5

Phase I.5 (transformation DAG) adds:
- Transformation stage between read and load (select, filter, project, cast, rename, mask, add-column, validate, dedupe, flatten)
- WASM UDF escape hatch reusing the Phase I.3 runtime with a tighter capability set (no network, no randomness, no wall-clock — just declared-in-WIT input → output)
- Static schema derivation for each operator (already supported by the Phase I.4 schema machinery)

The schema machinery from I.4 is stable: operators produce a derived schema that flows into the same `record_and_resolve` path.
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: Phase I.4 README + completion log

README shows the full DSL flow (validate → apply → run → alter
source → rerun → see v2 → get → diff). Crate map updated. Completion
log scaffolded.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Appendix A — Troubleshooting

**`cargo build` complains that `PipelineSpec` has no `evolution_policy` field.**
Phase I.4 keeps `PipelineSpec` (the internal type) unchanged; `evolution_policy` lives on `PipelineDslSpec` and the pipelines.spec JSONB. The CLI pulls it out of the JSONB at `pipeline_run` time via `pipeline.spec.get("evolution_policy")`.

**Arrow schema serde_json::to_value fails.**
`arrow::datatypes::Schema` implements `Serialize` in arrow 53. If you see `not implemented` errors, double-check the `arrow` version in Cargo.toml (should be workspace = true → 53). Earlier versions may lack the impl.

**`schemas.version` race on concurrent runs.**
Phase I.4 uses `SELECT MAX(version)+1` inside a transaction. For Phase I.4's single-worker, single-tenant target this is fine. Phase II.1 should switch to a sequence or unique-violation-retry when multi-worker runs land.

**`get_by_name` returns `None` for an existing stream.**
The lookup is keyed on `(pipeline_id, name)` where `name` is the source table name for Postgres sources (e.g. `customers`). If you're looking at a WASM source, it's the connector-name derived from `connector_ref` (e.g. `csv-source`).

**DSL `apply` succeeds but the workflow can't find the pipeline.**
`apply` writes into the "dev" tenant (UUID `1111…`). Ensure seed SQL uses the same tenant UUID, or run `platform get pipeline <name>` to confirm the pipeline lives where you expect.

## Appendix B — What's deferred to later phases

- Workspace CRUD UI (Phase II.1)
- Schedules (later)
- `propagate_all` with automatic column renames (Phase I.5+)
- Lineage entity rows (Phase II.5)
- Column-level evolution overrides (ignore / freeze per field) — RFC-10 calls for them; Phase I.4 stops at stream-level policy
- CDC sync_mode (Phase I.6)
- Multi-tenancy enforcement (Phase II.1)
- Postgres-in-WASM via host postgres-query capability (still deferred)

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-23-phase-1-4-catalog-dsl.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**

---

## Phase I.4 Completion Log

Completed 2026-04-23 on branch `phase-1-4-catalog-dsl`, 13 commits.

- [x] Task 1 — Dep additions (blake3, serde_yaml)
- [x] Task 2 — common-types (SchemaFingerprint, EvolutionPolicy, ChangeKind, DSL types, new IDs)
- [x] Task 3 — Migration 0003 (workspaces + streams + schemas + FK + backfill)
- [x] Tasks 4-6 — Workspace + Stream + Schema CRUD (3 integration tests)
- [x] Tasks 7-9 — Pure-function schema_evolution: fingerprint + diff + policy (16 unit tests)
- [x] Task 10 — record_and_resolve composer
- [x] Task 11 — discover_stream activity records Schema entities
- [x] Tasks 12-14 — YAML DSL parser + apply/get/diff/validate CLI
- [x] Task 15 — Postgres schema evolution end-to-end integration test
- [x] Task 16 — DSL apply idempotency integration test
- [x] Task 17 — README + this log + regression sweep

### Exit criterion — MET

- Schema fingerprint + diff + policy engine: **16 unit tests** pass (5 fingerprint + 6 diff + 5 policy)
- `schema_evolution_adds_column_on_second_run` passes (~56 s): ALTER TABLE → Schema v2 with `AddColumn(nickname, nullable=true)`; Parquet from the second run includes the new column
- `apply_is_idempotent` passes (~1.5 s): second apply of same YAML reports all-unchanged; `diff -f` emits only `=` lines
- Phase I.2 + I.3 integration tests still green (regression-clean)

### Deviations from the plan

- **arrow::Schema doesn't derive Serialize in arrow 53**. Plan assumed it. Worked around by storing the schema as base64-encoded Arrow IPC bytes inside a `{"ipc_b64": "..."}` JSON wrapper. Same IPC format we already use for batch transport; round-trip verified via StreamWriter (schema-only finish() produces header bytes) + StreamReader.
- **connection/pipeline create needed workspace_id NOT NULL backfill**. Migration adds the column as NOT NULL, so Phase I.1/I.2 catalog call sites broke until `connection::create` and `pipeline::create` were updated to auto-resolve the default workspace via `ensure_default_workspace`.
- **`Result::unwrap_err` on a non-Debug SourceConnector trait object** doesn't compile — used explicit `match` in dispatcher tests (pattern already established in Phase I.3).
- **`ensure_default` + `ensure_stream` race-safety** via `INSERT ... ON CONFLICT DO NOTHING` + read-back by unique key.
- **Phase I.1's `workflow_survives_worker_restart` test had been silently broken since Phase I.2** — it seeded `spec: json!({})` which no longer parses as PipelineSpec. Phase I.2/I.3 phase-completion sweeps only ran the specific new tests, not the old ones. Fixed in Phase I.4 to seed a valid PipelineSpec.
- **Tracing lines on stdout** tripped the DSL apply idempotency test's "all lines are `=`" assertion — filtered to lines starting with `+`, `~`, or `=`.
- **CLI needed `serde = { workspace = true }`** for `serde::Deserialize::deserialize` in the DSL parser — not picked up transitively.

### Handoff to Phase I.5

Phase I.5 (transformation DAG) adds:
- Declarative operators between read and load (select, filter, project, cast, rename, mask, add-column, validate, dedupe, flatten)
- WASM UDF escape hatch reusing the Phase I.3 runtime with a tighter capability set (no network, no randomness, no wall-clock)
- Static schema derivation for each operator — output flows into the same `record_and_resolve` path built in Phase I.4

The schema machinery from I.4 is stable: the only integration point for transforms is "emit the derived schema and pass it to `record_and_resolve`".
