# Phase I.1 — Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Rust workspace, Postgres catalog with tenant-scoped entities, Arrow dependency, and a durable Temporal `PipelineRunWorkflow` that can be submitted via a `platform pipeline run <id>` CLI — producing the foundation every later phase sits on.

**Architecture:** Rust cargo workspace of seven crates (`common-types`, `catalog`, `worker`, `control-api`, `connector-sdk`, `loader-sdk`, `cli`). Local Postgres 16 via docker-compose holds the catalog. Temporal Cloud runs the workflow engine; tests use the local `temporal server start-dev` dev server. The workflow is intentionally non-trivial (activity → timer → activity) so killing the worker mid-flight exercises Temporal durability. `TenantId`/`PipelineId`/`RunId` are non-forgeable newtypes from day one so later multi-tenancy retrofit is plumbing not redesign.

**Tech Stack:** Rust 1.82+, cargo workspaces, `arrow` 53, `sqlx` 0.8 (Postgres), `temporal-sdk-core` + `temporal-sdk` (Rust SDK), `tokio` 1, `clap` 4 derive, `uuid` 1, `serde`/`serde_json`, `tracing`, `anyhow`, `thiserror`. Local dev: Postgres 16 via Docker, `temporal` CLI for dev server.

---

## File Structure

```
etl/
├── Cargo.toml                          (workspace root)
├── rust-toolchain.toml                 (pin Rust 1.82)
├── .env.example
├── docker-compose.yml                  (Postgres 16)
├── README.md
├── crates/
│   ├── common-types/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  (re-exports)
│   │       └── ids.rs                  (TenantId, PipelineId, ConnectionId, RunId)
│   ├── catalog/
│   │   ├── Cargo.toml
│   │   ├── migrations/
│   │   │   └── 0001_initial.sql        (4 tables + indices)
│   │   └── src/
│   │       ├── lib.rs                  (Catalog struct, error type)
│   │       ├── db.rs                   (pool, migration runner)
│   │       ├── tenant.rs               (CRUD for tenants)
│   │       ├── connection.rs           (CRUD for connections)
│   │       ├── pipeline.rs             (CRUD for pipelines)
│   │       └── run.rs                  (CRUD for runs)
│   ├── worker/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs                 (binary: worker daemon)
│   │       ├── lib.rs                  (register workflows/activities)
│   │       ├── activities/
│   │       │   ├── mod.rs
│   │       │   └── run_lifecycle.rs    (start_run, complete_run activities)
│   │       └── workflows/
│   │           ├── mod.rs
│   │           └── pipeline_run.rs     (PipelineRunWorkflow)
│   ├── control-api/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs                 (binary, stub for now)
│   │       └── lib.rs                  (empty shell)
│   ├── connector-sdk/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── lib.rs                  (empty shell, docstring)
│   ├── loader-sdk/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── lib.rs                  (empty shell, docstring)
│   └── cli/
│       ├── Cargo.toml
│       └── src/
│           └── main.rs                 (binary: `platform` command)
└── tests/
    └── integration/
        ├── Cargo.toml
        └── tests/
            └── durability.rs           (end-to-end kill-restart test)
```

Seven crates chosen because the RFCs split concerns along these exact lines (RFC-2 control vs. data plane, RFC-20 SDK separation, RFC-10 catalog as its own entity). Keeping them as separate crates even when nearly empty at Phase I.1 means later phases never need a big refactor to introduce boundaries; they just fill in existing crates.

---

## Task 1: Workspace scaffolding & toolchain pin

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.gitignore`
- Create: `crates/common-types/Cargo.toml`
- Create: `crates/common-types/src/lib.rs`
- Create: `crates/catalog/Cargo.toml`
- Create: `crates/catalog/src/lib.rs`
- Create: `crates/worker/Cargo.toml`
- Create: `crates/worker/src/main.rs`
- Create: `crates/worker/src/lib.rs`
- Create: `crates/control-api/Cargo.toml`
- Create: `crates/control-api/src/main.rs`
- Create: `crates/control-api/src/lib.rs`
- Create: `crates/connector-sdk/Cargo.toml`
- Create: `crates/connector-sdk/src/lib.rs`
- Create: `crates/loader-sdk/Cargo.toml`
- Create: `crates/loader-sdk/src/lib.rs`
- Create: `crates/cli/Cargo.toml`
- Create: `crates/cli/src/main.rs`

- [ ] **Step 1: Create workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
    "crates/common-types",
    "crates/catalog",
    "crates/worker",
    "crates/control-api",
    "crates/connector-sdk",
    "crates/loader-sdk",
    "crates/cli",
]

[workspace.package]
edition = "2021"
rust-version = "1.82"
license = "UNLICENSED"

[workspace.dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# Errors
anyhow = "1"
thiserror = "1"

# Serde
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# IDs
uuid = { version = "1", features = ["v4", "v7", "serde"] }

# Logging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }

# CLI
clap = { version = "4", features = ["derive", "env"] }

# Time
chrono = { version = "0.4", features = ["serde"] }

# DB
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "uuid", "chrono", "migrate", "macros"] }

# Arrow
arrow = "53"

# Internal crates
common-types = { path = "crates/common-types" }
catalog = { path = "crates/catalog" }
worker = { path = "crates/worker" }
connector-sdk = { path = "crates/connector-sdk" }
loader-sdk = { path = "crates/loader-sdk" }

[profile.dev]
opt-level = 1
```

- [ ] **Step 2: Create `rust-toolchain.toml`**

```toml
[toolchain]
channel = "1.82"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Create `.gitignore`**

```
/target
/data
/staging
.env
*.log
.DS_Store
```

- [ ] **Step 4: Create every sub-crate's `Cargo.toml` with minimal deps**

Each sub-crate gets a `Cargo.toml`. Use this template, substituting `<NAME>`:

```toml
[package]
name = "<NAME>"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true
```

For `common-types`: add `serde.workspace = true`, `uuid.workspace = true`, `thiserror.workspace = true`.

For `catalog`: add `common-types.workspace = true`, `tokio.workspace = true`, `sqlx.workspace = true`, `serde.workspace = true`, `chrono.workspace = true`, `uuid.workspace = true`, `thiserror.workspace = true`, `tracing.workspace = true`, `anyhow.workspace = true`.

For `worker`: add `common-types.workspace = true`, `catalog.workspace = true`, `tokio.workspace = true`, `arrow.workspace = true`, `serde.workspace = true`, `serde_json.workspace = true`, `tracing.workspace = true`, `tracing-subscriber.workspace = true`, `anyhow.workspace = true`, `thiserror.workspace = true`, `chrono.workspace = true`, `uuid.workspace = true`. Also declare `[[bin]] name = "worker" path = "src/main.rs"` and `[lib] path = "src/lib.rs"`.

For `control-api`: add `common-types.workspace = true`, `catalog.workspace = true`, `tokio.workspace = true`, `anyhow.workspace = true`, `tracing.workspace = true`, `tracing-subscriber.workspace = true`. Bin + lib stubs.

For `connector-sdk` and `loader-sdk`: just `serde.workspace = true`.

For `cli`: add `common-types.workspace = true`, `catalog.workspace = true`, `clap.workspace = true`, `tokio.workspace = true`, `anyhow.workspace = true`, `tracing.workspace = true`, `tracing-subscriber.workspace = true`, `uuid.workspace = true`. Bin only.

- [ ] **Step 5: Create empty `src/lib.rs` for each library crate**

For `common-types/src/lib.rs`:
```rust
//! Shared newtype identifiers and primitive types for the platform.
pub mod ids;
```

For `catalog/src/lib.rs`:
```rust
//! Catalog: persistent metadata store (RFC-10).
```

For `worker/src/lib.rs`:
```rust
//! Worker library: workflow + activity registrations.
```

For `control-api/src/lib.rs`:
```rust
//! Control API: public HTTP/gRPC surface (stub, Phase III).
```

For `connector-sdk/src/lib.rs`:
```rust
//! Connector SDK: developer-facing helpers. Stub for Phase I.1; real implementation in Phase I.3 (RFC-20).
```

For `loader-sdk/src/lib.rs`:
```rust
//! Loader SDK: Rust-native loader trait. Stub for Phase I.1; real implementation in Phase II.3 (RFC-9).
```

- [ ] **Step 6: Create minimal `main.rs` for each binary crate**

For `worker/src/main.rs`:
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("worker starting");
    Ok(())
}
```

For `control-api/src/main.rs`:
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("control-api starting (stub)");
    Ok(())
}
```

For `cli/src/main.rs`:
```rust
use clap::Parser;

#[derive(Parser)]
#[command(name = "platform", version, about = "ETL platform CLI")]
struct Cli {}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();
    println!("platform CLI (stub)");
    Ok(())
}
```

- [ ] **Step 7: Verify workspace builds**

Run: `cargo build --workspace`
Expected: compiles cleanly, produces three binaries in `target/debug/`: `worker`, `control-api`, `cli`.

- [ ] **Step 8: Verify binaries run**

Run: `cargo run --bin cli`
Expected output: `platform CLI (stub)`

Run: `cargo run --bin worker`
Expected output: `worker starting` (via tracing) then exit.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: scaffold cargo workspace with 7 crates

Empty-shell structure for control-api, worker, connector-sdk,
loader-sdk, catalog, cli, common-types. Each compiles and the three
binaries run. Pins Rust 1.82 and shared dependency versions in the
workspace root.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Local Postgres via docker-compose

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16
    container_name: etl-postgres
    environment:
      POSTGRES_USER: etl
      POSTGRES_PASSWORD: etl
      POSTGRES_DB: etl_catalog
    ports:
      - "5432:5432"
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=10"
    volumes:
      - etl_pg:/var/lib/postgresql/data

volumes:
  etl_pg:
```

Note: `wal_level=logical` is not needed for Phase I.1 but costs nothing and avoids a restart when CDC arrives in Phase I.6.

- [ ] **Step 2: Create `.env.example`**

```bash
# Local catalog database
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog

# Temporal Cloud (production) — leave blank for local dev-server
TEMPORAL_ADDRESS=127.0.0.1:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=pipeline-default

# Logging
RUST_LOG=info,sqlx=warn
```

- [ ] **Step 3: Start Postgres and verify connectivity**

Run: `docker compose up -d postgres`
Expected: container `etl-postgres` running.

Run: `psql postgres://etl:etl@localhost:5432/etl_catalog -c "SELECT 1;"`
Expected: returns `1`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "chore: add local Postgres 16 via docker-compose

wal_level=logical set now to avoid reconfigure when CDC arrives in
Phase I.6.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `common-types` — non-forgeable ID newtypes

**Files:**
- Create: `crates/common-types/src/ids.rs`
- Test: `crates/common-types/src/ids.rs` (inline `#[cfg(test)]`)

Design: every ID is a tuple newtype wrapping `Uuid`, but the only public constructor is `from_uuid_unchecked` (explicitly labeled to prevent accidental bypass) plus `new()` which generates a v7 UUID. `FromStr` parses from canonical UUID string. This matches RFC-16's "`TenantId` type non-constructible arbitrarily" invariant — no `TenantId::from(42u64)` style footgun.

- [ ] **Step 1: Write the failing test**

Create `crates/common-types/src/ids.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a new identifier (UUIDv7 — time-ordered).
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Construct from an existing UUID. Name is explicit so callers
            /// cannot silently fabricate identities.
            pub fn from_uuid_unchecked(u: Uuid) -> Self {
                Self(u)
            }

            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}-{}", $prefix, self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let expected_prefix = concat!($prefix, "-");
                let rest = s
                    .strip_prefix(expected_prefix)
                    .ok_or(IdParseError::WrongPrefix)?;
                let uuid = Uuid::parse_str(rest).map_err(|_| IdParseError::BadUuid)?;
                Ok(Self(uuid))
            }
        }
    };
}

#[derive(thiserror::Error, Debug)]
pub enum IdParseError {
    #[error("identifier has the wrong prefix for its type")]
    WrongPrefix,
    #[error("identifier tail is not a valid UUID")]
    BadUuid,
}

define_id!(TenantId, "ten");
define_id!(ConnectionId, "conn");
define_id!(PipelineId, "pipe");
define_id!(RunId, "run");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_roundtrip() {
        let t = TenantId::new();
        let s = t.to_string();
        assert!(s.starts_with("ten-"));
        let parsed: TenantId = s.parse().unwrap();
        assert_eq!(t, parsed);
    }

    #[test]
    fn wrong_prefix_rejected() {
        let t = TenantId::new();
        let s = t.to_string().replace("ten-", "pipe-");
        let err = s.parse::<TenantId>().unwrap_err();
        matches!(err, IdParseError::WrongPrefix);
    }

    #[test]
    fn serde_roundtrip_is_bare_uuid() {
        let t = TenantId::new();
        let j = serde_json::to_string(&t).unwrap();
        // With #[serde(transparent)], the JSON form is just the UUID string.
        assert!(j.starts_with("\"") && j.ends_with("\""));
        let back: TenantId = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn id_types_are_distinct() {
        // Compile-time test: you cannot cross-assign PipelineId and TenantId.
        // This is verified by the absence of an `impl From<TenantId> for PipelineId`.
        let _t = TenantId::new();
        let _p = PipelineId::new();
    }
}
```

- [ ] **Step 2: Ensure `serde_json` is available as dev-dep**

Edit `crates/common-types/Cargo.toml`:
```toml
[dev-dependencies]
serde_json = { workspace = true }
```

- [ ] **Step 3: Run tests to verify they fail initially (they won't; this is greenfield)**

Run: `cargo test -p common-types`
Expected: tests pass (the code is complete in step 1 — this is greenfield so there's no prior code to fail against; we're writing test + impl together in this task because the newtype macro is tight).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(common-types): non-forgeable ID newtypes

TenantId/ConnectionId/PipelineId/RunId are tuple newtypes around UUID
with explicit constructors (new() generates v7, from_uuid_unchecked
is the only other entry). Display emits 'ten-<uuid>'-style strings;
FromStr enforces the prefix. Matches RFC-16's non-constructible-
arbitrarily invariant.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Catalog schema + migration

**Files:**
- Create: `crates/catalog/migrations/0001_initial.sql`

Four tables per RFC-10. Every row carries `tenant_id`. UUIDs stored as native `uuid` type. JSONB for flexible config fields. Append-only for `runs`.

- [ ] **Step 1: Create the migration**

Create `crates/catalog/migrations/0001_initial.sql`:

```sql
-- RFC-10 catalog minimal schema. Every row is tenant-scoped.
-- Later phases will add: workspaces, streams, schemas, transformations, secrets_refs, audit.

CREATE TABLE tenants (
    tenant_id    UUID PRIMARY KEY,
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE connections (
    connection_id  UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    connector_ref  TEXT NOT NULL,           -- e.g. "postgres@0.1.0"
    config         JSONB NOT NULL,          -- non-secret config
    secret_refs    JSONB NOT NULL DEFAULT '{}'::jsonb, -- placeholder for RFC-11
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX connections_tenant_id_idx ON connections(tenant_id);

CREATE TABLE pipelines (
    pipeline_id      UUID PRIMARY KEY,
    tenant_id        UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    source_conn_id   UUID NOT NULL REFERENCES connections(connection_id),
    dest_conn_id     UUID REFERENCES connections(connection_id), -- nullable for I.1
    spec             JSONB NOT NULL,        -- YAML DSL body after parse
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, name)
);
CREATE INDEX pipelines_tenant_id_idx ON pipelines(tenant_id);

CREATE TABLE runs (
    run_id              UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id         UUID NOT NULL REFERENCES pipelines(pipeline_id),
    status              TEXT NOT NULL CHECK (status IN ('queued','running','completed','failed','cancelled')),
    trigger             TEXT NOT NULL,         -- 'manual', 'schedule', 'signal'
    temporal_workflow_id TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error               TEXT
);
CREATE INDEX runs_tenant_pipeline_idx ON runs(tenant_id, pipeline_id, started_at DESC);
CREATE INDEX runs_status_idx ON runs(status) WHERE status IN ('queued','running');
```

- [ ] **Step 2: Verify the migration applies cleanly**

Run (from repo root): `psql $DATABASE_URL -f crates/catalog/migrations/0001_initial.sql`

Expected: `CREATE TABLE` × 4, `CREATE INDEX` × 4, no errors.

Run: `psql $DATABASE_URL -c "\dt"`
Expected: lists `tenants`, `connections`, `pipelines`, `runs`.

- [ ] **Step 3: Reset the DB for subsequent automated migration**

Run: `psql $DATABASE_URL -c "DROP TABLE runs, pipelines, connections, tenants CASCADE;"`

We'll let sqlx apply the migration from Rust in Task 5.

- [ ] **Step 4: Commit**

```bash
git add crates/catalog/migrations/0001_initial.sql
git commit -m "feat(catalog): initial schema with 4 tenant-scoped tables

tenants, connections, pipelines, runs. Every non-tenant row carries
tenant_id. UNIQUE(tenant_id, name) on user-named entities. Indices
for tenant-scoped reads and for filtering live runs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Catalog — pool, migration runner, entity CRUD

**Files:**
- Create: `crates/catalog/src/db.rs`
- Create: `crates/catalog/src/tenant.rs`
- Create: `crates/catalog/src/connection.rs`
- Create: `crates/catalog/src/pipeline.rs`
- Create: `crates/catalog/src/run.rs`
- Modify: `crates/catalog/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/catalog/tests/crud.rs`:

```rust
use catalog::{Catalog, NewConnection, NewPipeline, NewRun, RunStatus};
use common_types::ids::{ConnectionId, PipelineId, RunId, TenantId};
use serde_json::json;

async fn test_catalog() -> Catalog {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into());
    let cat = Catalog::connect(&url).await.unwrap();
    cat.migrate().await.unwrap();
    // Clean slate per test run (tests are serial in this crate).
    cat.truncate_all_for_tests().await.unwrap();
    cat
}

#[tokio::test]
async fn tenant_insert_and_get() {
    let cat = test_catalog().await;
    let id = cat.create_tenant("acme").await.unwrap();
    let t = cat.get_tenant(id).await.unwrap().unwrap();
    assert_eq!(t.name, "acme");
}

#[tokio::test]
async fn connection_insert_scoped_to_tenant() {
    let cat = test_catalog().await;
    let tenant = cat.create_tenant("acme").await.unwrap();
    let conn = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "main-pg".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({"host": "localhost", "port": 5432, "database": "src"}),
        })
        .await
        .unwrap();
    let got = cat.get_connection(conn).await.unwrap().unwrap();
    assert_eq!(got.name, "main-pg");
    assert_eq!(got.tenant_id, tenant);
}

#[tokio::test]
async fn pipeline_run_lifecycle() {
    let cat = test_catalog().await;
    let tenant = cat.create_tenant("acme").await.unwrap();
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await
        .unwrap();
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "demo".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await
        .unwrap();
    let run = cat
        .create_run(NewRun {
            tenant_id: tenant,
            pipeline_id: pipe,
            trigger: "manual".into(),
            temporal_workflow_id: Some("wf-abc".into()),
        })
        .await
        .unwrap();
    cat.mark_run_completed(run).await.unwrap();
    let got = cat.get_run(run).await.unwrap().unwrap();
    assert_eq!(got.status, RunStatus::Completed);
    assert!(got.completed_at.is_some());
}
```

Add dev-deps in `crates/catalog/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p catalog`
Expected: FAIL (unresolved imports: `Catalog`, `NewConnection`, etc.)

- [ ] **Step 3: Implement `db.rs`**

Create `crates/catalog/src/db.rs`:

```rust
use sqlx::postgres::{PgPool, PgPoolOptions};

pub async fn connect(url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(url)
        .await
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}
```

- [ ] **Step 4: Implement `tenant.rs`**

Create `crates/catalog/src/tenant.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::TenantId;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Tenant {
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

pub async fn create(pool: &PgPool, name: &str) -> sqlx::Result<TenantId> {
    let id = TenantId::new();
    sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
        .bind(id.as_uuid())
        .bind(name)
        .execute(pool)
        .await?;
    Ok(id)
}

pub async fn get(pool: &PgPool, id: TenantId) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(uuid::Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, created_at FROM tenants WHERE tenant_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(u, name, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(u),
        name,
        created_at,
    }))
}
```

- [ ] **Step 5: Implement `connection.rs`**

Create `crates/catalog/src/connection.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::{ConnectionId, TenantId};
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Connection {
    pub connection_id: ConnectionId,
    pub tenant_id: TenantId,
    pub name: String,
    pub connector_ref: String,
    pub config: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewConnection {
    pub tenant_id: TenantId,
    pub name: String,
    pub connector_ref: String,
    pub config: Value,
}

pub async fn create(pool: &PgPool, new: NewConnection) -> sqlx::Result<ConnectionId> {
    let id = ConnectionId::new();
    sqlx::query(
        "INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(&new.name)
    .bind(&new.connector_ref)
    .bind(&new.config)
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn get(pool: &PgPool, id: ConnectionId) -> sqlx::Result<Option<Connection>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        Value,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT connection_id, tenant_id, name, connector_ref, config, created_at, updated_at \
         FROM connections WHERE connection_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(cid, tid, name, connector_ref, config, c, u)| Connection {
        connection_id: ConnectionId::from_uuid_unchecked(cid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        connector_ref,
        config,
        created_at: c,
        updated_at: u,
    }))
}
```

- [ ] **Step 6: Implement `pipeline.rs`**

Create `crates/catalog/src/pipeline.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::{ConnectionId, PipelineId, TenantId};
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub pipeline_id: PipelineId,
    pub tenant_id: TenantId,
    pub name: String,
    pub source_conn_id: ConnectionId,
    pub dest_conn_id: Option<ConnectionId>,
    pub spec: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewPipeline {
    pub tenant_id: TenantId,
    pub name: String,
    pub source_conn_id: ConnectionId,
    pub dest_conn_id: Option<ConnectionId>,
    pub spec: Value,
}

pub async fn create(pool: &PgPool, new: NewPipeline) -> sqlx::Result<PipelineId> {
    let id = PipelineId::new();
    sqlx::query(
        "INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, dest_conn_id, spec) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(&new.name)
    .bind(new.source_conn_id.as_uuid())
    .bind(new.dest_conn_id.map(|d| d.as_uuid()))
    .bind(&new.spec)
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn get(pool: &PgPool, id: PipelineId) -> sqlx::Result<Option<Pipeline>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        String,
        uuid::Uuid,
        Option<uuid::Uuid>,
        Value,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT pipeline_id, tenant_id, name, source_conn_id, dest_conn_id, spec, created_at, updated_at \
         FROM pipelines WHERE pipeline_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(pid, tid, name, src, dst, spec, c, u)| Pipeline {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        source_conn_id: ConnectionId::from_uuid_unchecked(src),
        dest_conn_id: dst.map(ConnectionId::from_uuid_unchecked),
        spec,
        created_at: c,
        updated_at: u,
    }))
}
```

- [ ] **Step 7: Implement `run.rs`**

Create `crates/catalog/src/run.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::ids::{PipelineId, RunId, TenantId};
use sqlx::PgPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Queued => "queued",
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "queued" => RunStatus::Queued,
            "running" => RunStatus::Running,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Run {
    pub run_id: RunId,
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub status: RunStatus,
    pub trigger: String,
    pub temporal_workflow_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

pub struct NewRun {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub trigger: String,
    pub temporal_workflow_id: Option<String>,
}

pub async fn create(pool: &PgPool, new: NewRun) -> sqlx::Result<RunId> {
    let id = RunId::new();
    sqlx::query(
        "INSERT INTO runs (run_id, tenant_id, pipeline_id, status, trigger, temporal_workflow_id) \
         VALUES ($1, $2, $3, 'queued', $4, $5)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.pipeline_id.as_uuid())
    .bind(&new.trigger)
    .bind(new.temporal_workflow_id)
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn mark_running(pool: &PgPool, id: RunId) -> sqlx::Result<()> {
    sqlx::query("UPDATE runs SET status = 'running' WHERE run_id = $1")
        .bind(id.as_uuid())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn mark_completed(pool: &PgPool, id: RunId) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE runs SET status = 'completed', completed_at = NOW() WHERE run_id = $1",
    )
    .bind(id.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_failed(pool: &PgPool, id: RunId, err: &str) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE runs SET status = 'failed', completed_at = NOW(), error = $2 WHERE run_id = $1",
    )
    .bind(id.as_uuid())
    .bind(err)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get(pool: &PgPool, id: RunId) -> sqlx::Result<Option<Run>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        Option<String>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT run_id, tenant_id, pipeline_id, status, trigger, temporal_workflow_id, \
                started_at, completed_at, error \
         FROM runs WHERE run_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(rid, tid, pid, status, trigger, wf, started_at, completed_at, error)| Run {
        run_id: RunId::from_uuid_unchecked(rid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        status: RunStatus::parse(&status).expect("DB check constraint enforces valid values"),
        trigger,
        temporal_workflow_id: wf,
        started_at,
        completed_at,
        error,
    }))
}
```

- [ ] **Step 8: Expose the public surface in `lib.rs`**

Replace `crates/catalog/src/lib.rs` with:

```rust
//! Catalog: persistent metadata store (RFC-10).
//!
//! Phase I.1 scope: 4 tables (tenants, connections, pipelines, runs),
//! every row tenant-scoped, async CRUD via sqlx. Subsequent phases add
//! workspaces, streams, schemas, transformations, audit.

mod db;
pub mod connection;
pub mod pipeline;
pub mod run;
pub mod tenant;

pub use connection::{Connection, NewConnection};
pub use pipeline::{NewPipeline, Pipeline};
pub use run::{NewRun, Run, RunStatus};
pub use tenant::Tenant;

use common_types::ids::{ConnectionId, PipelineId, RunId, TenantId};
use sqlx::PgPool;

/// Wrapper that exposes catalog operations as methods.
#[derive(Clone)]
pub struct Catalog {
    pool: PgPool,
}

impl Catalog {
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = db::connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), sqlx::migrate::MigrateError> {
        db::migrate(&self.pool).await
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // Tenants
    pub async fn create_tenant(&self, name: &str) -> sqlx::Result<TenantId> {
        tenant::create(&self.pool, name).await
    }
    pub async fn get_tenant(&self, id: TenantId) -> sqlx::Result<Option<Tenant>> {
        tenant::get(&self.pool, id).await
    }

    // Connections
    pub async fn create_connection(&self, new: NewConnection) -> sqlx::Result<ConnectionId> {
        connection::create(&self.pool, new).await
    }
    pub async fn get_connection(&self, id: ConnectionId) -> sqlx::Result<Option<Connection>> {
        connection::get(&self.pool, id).await
    }

    // Pipelines
    pub async fn create_pipeline(&self, new: NewPipeline) -> sqlx::Result<PipelineId> {
        pipeline::create(&self.pool, new).await
    }
    pub async fn get_pipeline(&self, id: PipelineId) -> sqlx::Result<Option<Pipeline>> {
        pipeline::get(&self.pool, id).await
    }

    // Runs
    pub async fn create_run(&self, new: NewRun) -> sqlx::Result<RunId> {
        run::create(&self.pool, new).await
    }
    pub async fn mark_run_running(&self, id: RunId) -> sqlx::Result<()> {
        run::mark_running(&self.pool, id).await
    }
    pub async fn mark_run_completed(&self, id: RunId) -> sqlx::Result<()> {
        run::mark_completed(&self.pool, id).await
    }
    pub async fn mark_run_failed(&self, id: RunId, err: &str) -> sqlx::Result<()> {
        run::mark_failed(&self.pool, id, err).await
    }
    pub async fn get_run(&self, id: RunId) -> sqlx::Result<Option<Run>> {
        run::get(&self.pool, id).await
    }

    /// Truncates every table. Intended for test cleanup only.
    #[doc(hidden)]
    pub async fn truncate_all_for_tests(&self) -> sqlx::Result<()> {
        sqlx::query("TRUNCATE runs, pipelines, connections, tenants CASCADE")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `docker compose up -d postgres` (if not already)
Run: `export DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog`
Run: `cargo test -p catalog -- --test-threads=1`

Expected: 3 tests pass. (`--test-threads=1` because `truncate_all_for_tests` means tests must be serial.)

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(catalog): pool, migration runner, tenant-scoped CRUD

Catalog struct wraps PgPool and exposes async CRUD for tenants,
connections, pipelines, runs. sqlx migrations run on startup. Every
entity query is tenant-scoped by design (FKs cascade on tenant delete).
Integration tests pass against local Postgres.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Arrow sanity round-trip in `worker`

**Files:**
- Create: `crates/worker/src/arrow_smoke.rs`
- Modify: `crates/worker/src/lib.rs`

Intent: prove Arrow compiles, links, and round-trips through IPC on this Rust toolchain before Phase I.2 builds real batches. One test, small.

- [ ] **Step 1: Write the failing test**

Create `crates/worker/src/arrow_smoke.rs`:

```rust
//! Smoke test that Arrow compiles and round-trips via IPC. Deleted
//! once Phase I.2's real data path lands.

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn record_batch_roundtrips_through_ipc() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let reader = StreamReader::try_new(&*buf, None).unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);
    }
}
```

- [ ] **Step 2: Wire module into `lib.rs`**

Append to `crates/worker/src/lib.rs`:
```rust
#[cfg(test)]
mod arrow_smoke;
```

- [ ] **Step 3: Run test**

Run: `cargo test -p worker arrow_smoke`
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(worker): Arrow IPC round-trip smoke test

Proves arrow 53 compiles and links on this toolchain; deleted when
Phase I.2's real data path lands.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Temporal SDK wiring — client, worker bootstrap, no-op registration

**Files:**
- Modify: `crates/worker/Cargo.toml`
- Create: `crates/worker/src/temporal.rs`
- Modify: `crates/worker/src/main.rs`
- Modify: `crates/worker/src/lib.rs`

**Background:** The Rust Temporal SDK is `temporal-sdk` (Rust-facing types) plus `temporal-sdk-core` (the gRPC + state machine core). The typical setup: create a `ClientOptionsBuilder` → connect → build a `Worker` → register workflows + activities → `run()`.

Pin versions in `Cargo.toml`. APIs below reflect the SDK shape at time of writing; if the API has evolved, adapt signatures but keep the same structure (client → worker → register → run).

- [ ] **Step 1: Add Temporal dependencies**

Edit `crates/worker/Cargo.toml` to add:
```toml
temporal-sdk = "0.1"
temporal-sdk-core = "0.1"
temporal-sdk-core-api = "0.1"
temporal-sdk-core-protos = "0.1"
url = "2"
futures = "0.3"
```

- [ ] **Step 2: Create `temporal.rs` with client + worker helpers**

Create `crates/worker/src/temporal.rs`:

```rust
use anyhow::Context;
use std::sync::Arc;
use temporal_sdk::Worker as SdkWorker;
use temporal_sdk_core::{init_worker, CoreRuntime};
use temporal_sdk_core_api::worker::WorkerConfigBuilder;
use temporal_sdk_core::{ClientOptionsBuilder, WorkerConfig};

pub struct TemporalConfig {
    pub address: String,
    pub namespace: String,
    pub task_queue: String,
}

impl TemporalConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            address: std::env::var("TEMPORAL_ADDRESS")
                .unwrap_or_else(|_| "127.0.0.1:7233".into()),
            namespace: std::env::var("TEMPORAL_NAMESPACE")
                .unwrap_or_else(|_| "default".into()),
            task_queue: std::env::var("TEMPORAL_TASK_QUEUE")
                .unwrap_or_else(|_| "pipeline-default".into()),
        })
    }
}

/// Build a connected Temporal client and return it along with a CoreRuntime.
pub async fn connect(cfg: &TemporalConfig) -> anyhow::Result<(Arc<CoreRuntime>, Arc<dyn temporal_sdk_core_api::Client>)> {
    let runtime = Arc::new(CoreRuntime::new_assume_tokio(
        temporal_sdk_core::telemetry::TelemetryOptionsBuilder::default()
            .build()
            .unwrap(),
    )?);

    let url = if cfg.address.starts_with("http") {
        cfg.address.clone()
    } else {
        format!("http://{}", cfg.address)
    };

    let client_options = ClientOptionsBuilder::default()
        .target_url(url::Url::parse(&url)?)
        .client_name("etl-worker")
        .client_version(env!("CARGO_PKG_VERSION"))
        .build()
        .context("building Temporal client options")?;

    let client = client_options
        .connect(&cfg.namespace, None)
        .await
        .context("connecting to Temporal")?;

    Ok((runtime, Arc::new(client)))
}

pub fn build_worker(
    runtime: Arc<CoreRuntime>,
    client: Arc<dyn temporal_sdk_core_api::Client>,
    cfg: &TemporalConfig,
) -> anyhow::Result<SdkWorker> {
    let worker_cfg: WorkerConfig = WorkerConfigBuilder::default()
        .namespace(cfg.namespace.clone())
        .task_queue(cfg.task_queue.clone())
        .worker_build_id("etl-worker-0.1")
        .build()?;

    let core_worker = init_worker(&runtime, worker_cfg, client)?;
    Ok(SdkWorker::new_from_core(Arc::new(core_worker), cfg.task_queue.clone()))
}
```

Note: the exact `temporal-sdk-core` API surface (`init_worker` arguments, `Client` trait object shape) shifts between minor versions. If the SDK has updated, keep the structure (config → runtime → client → worker) and adapt call signatures. The downstream tasks depend only on `SdkWorker::new_from_core` returning something that has `.register_wf()`, `.register_activity()`, and `.run()` methods.

- [ ] **Step 3: Wire `main.rs` to bring up a worker that polls but registers nothing yet**

Replace `crates/worker/src/main.rs`:

```rust
use anyhow::Context;
use worker::{register, temporal::{connect, build_worker, TemporalConfig}};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    ).init();

    let cfg = TemporalConfig::from_env()?;
    tracing::info!(
        address = %cfg.address, namespace = %cfg.namespace, task_queue = %cfg.task_queue,
        "worker booting"
    );

    let (runtime, client) = connect(&cfg).await.context("Temporal connect")?;
    let mut w = build_worker(runtime, client, &cfg)?;
    register(&mut w).await?;

    tracing::info!("worker polling");
    w.run().await?;
    Ok(())
}
```

- [ ] **Step 4: Update `lib.rs` to expose `register` and `temporal` module**

Replace `crates/worker/src/lib.rs`:

```rust
//! Worker library: workflow + activity registrations.
pub mod temporal;
pub mod activities;
pub mod workflows;

#[cfg(test)]
mod arrow_smoke;

use temporal_sdk::Worker as SdkWorker;

pub async fn register(_w: &mut SdkWorker) -> anyhow::Result<()> {
    // Filled in by Task 8. For now, register nothing — worker polls and idles.
    Ok(())
}
```

Create `crates/worker/src/activities/mod.rs` (empty):
```rust
//! Activities: concrete units of work (RFC-4).
```

Create `crates/worker/src/workflows/mod.rs` (empty):
```rust
//! Workflows: orchestration logic (RFC-4).
```

- [ ] **Step 5: Install the Temporal dev server and verify worker connects**

Install: `brew install temporal` (or equivalent on your platform).
Run in a separate terminal: `temporal server start-dev`

Run in another terminal:
```bash
export TEMPORAL_ADDRESS=127.0.0.1:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_TASK_QUEUE=pipeline-default
cargo run --bin worker
```

Expected: logs `worker booting` and `worker polling`, stays running. Visit `http://localhost:8233` — the Temporal UI should show a worker polling the `pipeline-default` task queue under namespace `default`.

Ctrl-C to stop.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(worker): bring up Temporal SDK client and worker

Worker binary connects to Temporal (dev server at 127.0.0.1:7233 by
default; TEMPORAL_ADDRESS env var for Cloud) and polls the
'pipeline-default' task queue. No workflows registered yet — that
lands in the next task.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `PipelineRunWorkflow` + run-lifecycle activities

**Files:**
- Create: `crates/worker/src/activities/run_lifecycle.rs`
- Create: `crates/worker/src/workflows/pipeline_run.rs`
- Modify: `crates/worker/src/activities/mod.rs`
- Modify: `crates/worker/src/workflows/mod.rs`
- Modify: `crates/worker/src/lib.rs`
- Modify: `crates/worker/src/main.rs`

**Design:**
- Workflow takes `PipelineRunInput { run_id: Uuid }`.
- Workflow calls `start_run` activity → sleeps 30 seconds → calls `complete_run` activity → returns.
- The 30-second sleep exists specifically to make the durability test in Task 10 meaningful (kill worker during sleep, restart, workflow resumes and completes).
- Activities receive a shared `ActivityContext` holding a `Catalog` clone.

- [ ] **Step 1: Write activities**

Create `crates/worker/src/activities/run_lifecycle.rs`:

```rust
use catalog::Catalog;
use common_types::ids::RunId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use temporal_sdk::{ActContext, ActivityError};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ActivityDeps {
    pub catalog: Arc<Catalog>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunRef {
    pub run_id: Uuid,
}

pub async fn start_run(
    ctx: ActContext,
    input: RunRef,
) -> Result<(), ActivityError> {
    let deps: Arc<ActivityDeps> = ctx.app_data().ok_or_else(|| {
        ActivityError::NonRetryable(anyhow::anyhow!("activity deps not wired").into())
    })?;
    let run_id = RunId::from_uuid_unchecked(input.run_id);
    deps.catalog.mark_run_running(run_id).await.map_err(|e| {
        ActivityError::Retryable(anyhow::anyhow!("mark_running: {e}").into())
    })?;
    tracing::info!(%input.run_id, "run started");
    Ok(())
}

pub async fn complete_run(
    ctx: ActContext,
    input: RunRef,
) -> Result<(), ActivityError> {
    let deps: Arc<ActivityDeps> = ctx.app_data().ok_or_else(|| {
        ActivityError::NonRetryable(anyhow::anyhow!("activity deps not wired").into())
    })?;
    let run_id = RunId::from_uuid_unchecked(input.run_id);
    deps.catalog.mark_run_completed(run_id).await.map_err(|e| {
        ActivityError::Retryable(anyhow::anyhow!("mark_completed: {e}").into())
    })?;
    tracing::info!(%input.run_id, "run completed");
    Ok(())
}
```

Update `crates/worker/src/activities/mod.rs`:
```rust
//! Activities: concrete units of work (RFC-4).
pub mod run_lifecycle;
```

- [ ] **Step 2: Write the workflow**

Create `crates/worker/src/workflows/pipeline_run.rs`:

```rust
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporal_sdk::{ActivityOptions, WfContext, WorkflowResult};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineRunInput {
    pub run_id: Uuid,
}

pub async fn pipeline_run(ctx: WfContext, input: PipelineRunInput) -> WorkflowResult<()> {
    let run_ref = crate::activities::run_lifecycle::RunRef { run_id: input.run_id };

    ctx.activity(ActivityOptions {
        activity_type: "start_run".into(),
        input: run_ref.as_json_payload()?,
        start_to_close_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    })
    .await
    .into_result()?;

    // 30-second sleep exercises Temporal durability: a worker crash here must
    // not lose the run. The timer survives restart.
    ctx.timer(Duration::from_secs(30)).await;

    ctx.activity(ActivityOptions {
        activity_type: "complete_run".into(),
        input: run_ref.as_json_payload()?,
        start_to_close_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    })
    .await
    .into_result()?;

    Ok(().into())
}
```

Helper trait (if not already present in the SDK version you're on — create in worker lib):

Create `crates/worker/src/workflows/mod.rs`:
```rust
//! Workflows: orchestration logic (RFC-4).
pub mod pipeline_run;

use serde::Serialize;
use temporal_sdk_core_protos::coresdk::common::Payload;

pub trait AsJsonPayload: Serialize {
    fn as_json_payload(&self) -> Result<Payload, serde_json::Error>;
}

impl<T: Serialize> AsJsonPayload for T {
    fn as_json_payload(&self) -> Result<Payload, serde_json::Error> {
        Ok(Payload {
            metadata: [("encoding".into(), b"json/plain".to_vec())].into(),
            data: serde_json::to_vec(self)?,
        })
    }
}
```

Note: if the Temporal Rust SDK in your pinned version provides its own `as_json_payload` helper (it did in mid-2024), delete this shim and use the SDK's.

- [ ] **Step 3: Register the workflow and activities**

Replace `crates/worker/src/lib.rs`:

```rust
//! Worker library: workflow + activity registrations.
pub mod temporal;
pub mod activities;
pub mod workflows;

#[cfg(test)]
mod arrow_smoke;

use std::sync::Arc;
use temporal_sdk::Worker as SdkWorker;

use crate::activities::run_lifecycle::{complete_run, start_run, ActivityDeps};
use crate::workflows::pipeline_run::pipeline_run;

pub async fn register(w: &mut SdkWorker, deps: Arc<ActivityDeps>) -> anyhow::Result<()> {
    w.insert_app_data(deps);
    w.register_wf("PipelineRunWorkflow", pipeline_run);
    w.register_activity("start_run", start_run);
    w.register_activity("complete_run", complete_run);
    Ok(())
}
```

Replace `crates/worker/src/main.rs`:

```rust
use anyhow::Context;
use std::sync::Arc;

use catalog::Catalog;
use worker::{
    activities::run_lifecycle::ActivityDeps,
    register,
    temporal::{build_worker, connect, TemporalConfig},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    ).init();

    let db_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL must be set")?;
    let catalog = Arc::new(Catalog::connect(&db_url).await?);
    catalog.migrate().await?;

    let cfg = TemporalConfig::from_env()?;
    tracing::info!(
        address = %cfg.address, namespace = %cfg.namespace, task_queue = %cfg.task_queue,
        "worker booting"
    );

    let (runtime, client) = connect(&cfg).await.context("Temporal connect")?;
    let mut w = build_worker(runtime, client, &cfg)?;
    register(&mut w, Arc::new(ActivityDeps { catalog })).await?;

    tracing::info!("worker polling");
    w.run().await?;
    Ok(())
}
```

- [ ] **Step 4: Build and run the worker**

Run: `cargo build -p worker`
Expected: compiles clean.

Run (with Postgres and `temporal server start-dev` running):
```bash
export DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog
cargo run --bin worker
```

Expected: `worker booting` → `worker polling`. Stays running.

Leave it running for Task 9.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker): PipelineRunWorkflow + run-lifecycle activities

Workflow calls start_run activity, sleeps 30s, calls complete_run
activity. The sleep makes the durability test (Task 10) meaningful —
killing the worker mid-sleep must not lose the run.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: CLI `platform pipeline run <id>` — submits the workflow

**Files:**
- Modify: `crates/cli/Cargo.toml`
- Modify: `crates/cli/src/main.rs`

**Design:**
- `platform pipeline run <PIPELINE_ID>` inserts a new `runs` row (status=queued) and starts a Temporal workflow whose `workflow_id` = `run-<run-id>`. Prints the workflow id.
- CLI reuses the Temporal client code from the worker crate.
- Pipeline must exist in catalog; if not, error out with a clear message.

- [ ] **Step 1: Add deps**

Edit `crates/cli/Cargo.toml` to add:
```toml
worker = { path = "../worker" }
serde_json = { workspace = true }
```

- [ ] **Step 2: Replace `cli/src/main.rs`**

```rust
use anyhow::Context;
use catalog::{Catalog, NewRun};
use clap::{Parser, Subcommand};
use common_types::ids::{PipelineId, RunId};
use serde_json::json;
use std::time::Duration;
use worker::temporal::{connect, TemporalConfig};

#[derive(Parser)]
#[command(name = "platform", version, about = "ETL platform CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Pipeline commands.
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },
}

#[derive(Subcommand)]
enum PipelineCmd {
    /// Run a pipeline by id.
    Run {
        /// Pipeline id (e.g. "pipe-<uuid>") or bare UUID.
        id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    ).init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => pipeline_run(id).await,
    }
}

async fn pipeline_run(id_str: String) -> anyhow::Result<()> {
    let pipeline_id = parse_pipeline_id(&id_str)
        .with_context(|| format!("parsing pipeline id '{}'", id_str))?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;

    let pipeline = catalog
        .get_pipeline(pipeline_id)
        .await?
        .with_context(|| format!("pipeline {} not found", pipeline_id))?;

    let run_id = RunId::new();
    let workflow_id = format!("run-{}", run_id.as_uuid());

    catalog
        .create_run(NewRun {
            tenant_id: pipeline.tenant_id,
            pipeline_id,
            trigger: "manual".into(),
            temporal_workflow_id: Some(workflow_id.clone()),
        })
        .await?;

    // start the workflow
    let cfg = TemporalConfig::from_env()?;
    let (_runtime, client) = connect(&cfg).await.context("Temporal connect")?;

    use temporal_sdk_core_protos::coresdk::workflow_commands::WorkflowType;
    use temporal_sdk_core_protos::temporal::api::common::v1::{Payload, Payloads};
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;

    let input_json = serde_json::to_vec(&json!({ "run_id": run_id.as_uuid() }))?;
    let input_payload = Payload {
        metadata: [("encoding".into(), b"json/plain".to_vec())].into(),
        data: input_json,
    };

    let req = StartWorkflowExecutionRequest {
        namespace: cfg.namespace.clone(),
        workflow_id: workflow_id.clone(),
        workflow_type: Some(temporal_sdk_core_protos::temporal::api::common::v1::WorkflowType {
            name: "PipelineRunWorkflow".into(),
        }),
        task_queue: Some(temporal_sdk_core_protos::temporal::api::taskqueue::v1::TaskQueue {
            name: cfg.task_queue.clone(),
            kind: 0,
            normal_name: "".into(),
        }),
        input: Some(Payloads { payloads: vec![input_payload] }),
        workflow_execution_timeout: Some(Duration::from_secs(600).try_into()?),
        workflow_run_timeout: Some(Duration::from_secs(600).try_into()?),
        workflow_task_timeout: Some(Duration::from_secs(10).try_into()?),
        identity: "platform-cli".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    };

    // The Rust SDK's Client trait exposes `workflow_client()` for raw service calls.
    use temporal_sdk_core_api::Client;
    let mut svc = client.workflow_service().clone();
    svc.start_workflow_execution(req).await
        .context("starting workflow")?;

    println!("started workflow {}", workflow_id);
    Ok(())
}

fn parse_pipeline_id(s: &str) -> anyhow::Result<PipelineId> {
    if let Ok(p) = s.parse::<PipelineId>() {
        return Ok(p);
    }
    // Accept bare UUID for convenience.
    let u = uuid::Uuid::parse_str(s)?;
    Ok(PipelineId::from_uuid_unchecked(u))
}
```

**Note:** the exact path `client.workflow_service().clone()` and `StartWorkflowExecutionRequest` construction may differ between SDK minor versions. The shape is: use the raw Temporal gRPC stubs (re-exported by `temporal-sdk-core-protos`) via the connected client. If the SDK exposes a higher-level `Client::start_workflow(name, id, queue, input)` helper in your pinned version, prefer it over the raw gRPC request. Either way, the CLI's contract is unchanged.

- [ ] **Step 3: Seed a tenant + connection + pipeline**

Run:
```bash
psql $DATABASE_URL <<SQL
INSERT INTO tenants (tenant_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111',
          'dev-pg', 'postgres@0.1.0', '{}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111',
          'demo', '22222222-2222-2222-2222-222222222222', '{}'::jsonb);
SQL
```

- [ ] **Step 4: With worker + temporal dev server running, submit the pipeline**

Run:
```bash
cargo run --bin cli -- pipeline run pipe-33333333-3333-3333-3333-333333333333
```

Expected stdout:
```
started workflow run-<uuid>
```

Expected in worker logs: `run started`, then ~30s later, `run completed`.

Verify DB:
```bash
psql $DATABASE_URL -c "SELECT run_id, status, started_at, completed_at FROM runs ORDER BY started_at DESC LIMIT 1;"
```
Expected: `status = completed`, `completed_at IS NOT NULL`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cli): platform pipeline run <id> submits PipelineRunWorkflow

CLI inserts a runs row, starts the workflow via Temporal gRPC, and
prints the workflow id. Seed script in README shows tenant +
connection + pipeline setup.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Durability test — kill worker mid-workflow

**Files:**
- Create: `tests/integration/Cargo.toml`
- Create: `tests/integration/tests/durability.rs`
- Modify: `Cargo.toml` (add `tests/integration` to workspace)

**Design:** integration test that spawns the worker as a child process, submits a workflow via the CLI, kills the worker mid-sleep, restarts it, and asserts the run completes. Uses `tokio::process::Command` to manage the worker.

- [ ] **Step 1: Add the integration crate to the workspace**

Edit the root `Cargo.toml`'s `[workspace].members` to append:
```toml
    "tests/integration",
```

Create `tests/integration/Cargo.toml`:
```toml
[package]
name = "integration-tests"
version = "0.1.0"
edition.workspace = true
publish = false

[dependencies]
catalog = { workspace = true }
common-types = { workspace = true }
tokio = { workspace = true }
anyhow = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
sqlx = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 2: Write the durability test**

Create `tests/integration/tests/durability.rs`:

```rust
use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, NewRun, RunStatus};
use common_types::ids::TenantId;
use serde_json::json;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    // When this test runs, cargo puts target bins in `../../target/debug/<name>`.
    // CARGO_BIN_EXE_<name> is set for crates whose binaries are under test; here
    // we reference binaries in sibling crates via relative path.
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", db_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info")
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(child)
}

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://etl:etl@localhost:5432/etl_catalog".into()
    })
}

#[tokio::test]
#[ignore = "requires Postgres and Temporal dev server; run with --ignored"]
async fn workflow_survives_worker_restart() -> anyhow::Result<()> {
    // Build binaries first so cargo_bin() paths exist.
    let build = Command::new("cargo")
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(build.success());

    let cat = Catalog::connect(&db_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let conn = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await?;
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "demo".into(),
            source_conn_id: conn,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    // Spawn worker #1
    let mut w1 = spawn_worker().await?;

    // Submit a run via CLI
    let cli = Command::new(cargo_bin("cli"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", db_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .output()
        .await?;
    assert!(cli.status.success(), "cli failed: {}", String::from_utf8_lossy(&cli.stderr));

    // Wait for run to enter 'running' state
    let run_id = wait_for_status_change(&cat, tenant, RunStatus::Running).await?;

    // Kill the worker — the workflow is currently sleeping in ctx.timer(30s),
    // which is entirely server-side, so the kill should not affect it.
    w1.kill().await?;
    w1.wait().await?;

    // Restart worker #2
    let mut w2 = spawn_worker().await?;

    // Wait up to 90s for completion
    let done = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let run = cat.get_run(run_id).await?.expect("run row");
            if run.status == RunStatus::Completed {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    w2.kill().await?;
    w2.wait().await?;

    done??;
    Ok(())
}

async fn wait_for_status_change(
    cat: &Catalog,
    _tenant: TenantId,
    target: RunStatus,
) -> anyhow::Result<common_types::ids::RunId> {
    for _ in 0..30 {
        let row: Option<(uuid::Uuid, String)> = sqlx::query_as(
            "SELECT run_id, status FROM runs ORDER BY started_at DESC LIMIT 1",
        )
        .fetch_optional(cat.pool())
        .await?;
        if let Some((rid, status)) = row {
            let rid = common_types::ids::RunId::from_uuid_unchecked(rid);
            if RunStatus::parse(&status) == Some(target) {
                return Ok(rid);
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("run did not reach status {:?} within 30s", target);
}
```

- [ ] **Step 3: Run the durability test**

Prerequisites (all running):
- `docker compose up -d postgres`
- `temporal server start-dev` in a dedicated terminal

Run:
```bash
cargo test -p integration-tests -- --ignored --nocapture
```

Expected: test passes. Total runtime ~60–90s. You'll see the worker logs from both processes interleaved; workflow completes after restart.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(integration): workflow durability across worker kill-restart

Spawns worker, submits via CLI, kills worker while the 30s timer is
pending server-side, restarts worker, asserts run completes. Gated
with #[ignore] since it requires Postgres + Temporal dev server
running.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: README with bootstrap + run instructions

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write the README**

Create `README.md`:

````markdown
# ETL Platform

Rust + Temporal + WebAssembly ETL platform. RFCs in `docs/rfc/`, roadmap in `docs/superpowers/specs/`, Phase I.1 plan in `docs/superpowers/plans/`.

## Prerequisites

- Rust 1.82+ (`rustup install 1.82` or use `rust-toolchain.toml`)
- Docker + Docker Compose
- Temporal CLI: `brew install temporal` (macOS) or see <https://docs.temporal.io/cli>

## Local dev bootstrap

```bash
# 1. Start Postgres
docker compose up -d postgres

# 2. Start Temporal dev server (separate terminal)
temporal server start-dev

# 3. Env
cp .env.example .env
source .env

# 4. Build
cargo build --workspace

# 5. Apply catalog migrations (binary does this on startup, but you can also run):
psql $DATABASE_URL -f crates/catalog/migrations/0001_initial.sql

# 6. Seed one tenant + connection + pipeline for manual testing
psql $DATABASE_URL <<SQL
INSERT INTO tenants (tenant_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111',
          'dev-pg', 'postgres@0.1.0', '{}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111',
          'demo', '22222222-2222-2222-2222-222222222222', '{}'::jsonb);
SQL

# 7. Run the worker (separate terminal)
cargo run --bin worker

# 8. Submit a pipeline run
cargo run --bin cli -- pipeline run pipe-33333333-3333-3333-3333-333333333333
```

Then watch:
- Worker logs show `run started` → 30s pause → `run completed`
- Temporal UI at <http://localhost:8233> shows the workflow
- `psql $DATABASE_URL -c "SELECT run_id, status, started_at, completed_at FROM runs;"` shows completion

## Tests

Unit tests:
```bash
cargo test --workspace -- --test-threads=1
```

Integration test (requires Postgres + Temporal dev server running):
```bash
cargo test -p integration-tests -- --ignored --nocapture
```

## Crate map

| Crate | Role |
|---|---|
| `common-types` | Non-forgeable ID newtypes (TenantId, PipelineId, etc.) |
| `catalog` | Postgres-backed metadata store (RFC-10) |
| `worker` | Temporal worker + activities + workflows (RFC-4) |
| `control-api` | Public HTTP/gRPC surface (stub until Phase III) |
| `connector-sdk` | Developer-facing connector SDK (stub until Phase I.3) |
| `loader-sdk` | Rust-native loader trait (stub until Phase II.3) |
| `cli` | `platform` command-line tool (RFC-13) |
| `tests/integration` | End-to-end durability test |
````

- [ ] **Step 2: Verify README renders**

Run: `glow README.md` (optional) or just open it.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README with local-dev bootstrap and run instructions

Prereqs, seed SQL, build/run commands, tests, crate map.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Phase I.1 exit gate — document demo

**Files:**
- Modify: `docs/superpowers/plans/2026-04-22-phase-1-1-skeleton.md`

- [ ] **Step 1: Confirm all exit criteria from the roadmap are met**

Work through each with the actual running system:
- [ ] `cargo build --workspace` succeeds
- [ ] `cargo test --workspace -- --test-threads=1` all unit tests pass
- [ ] `cargo test -p integration-tests -- --ignored` passes (durability)
- [ ] Worker connects to Temporal dev server, polls task queue
- [ ] CLI submits PipelineRunWorkflow, completes end-to-end
- [ ] Runs row in Postgres reflects `completed` status after workflow success
- [ ] Kill worker mid-sleep, restart, run completes — **the Phase I.1 exit criterion**

- [ ] **Step 2: Append a "Phase I.1 Completion Log" section to this plan file**

At the end of `docs/superpowers/plans/2026-04-22-phase-1-1-skeleton.md`:

```markdown

---

## Phase I.1 Completion Log

- [x] Workspace scaffolding (Task 1)
- [x] Local Postgres (Task 2)
- [x] Non-forgeable IDs (Task 3)
- [x] Catalog migration (Task 4)
- [x] Catalog CRUD (Task 5)
- [x] Arrow wiring (Task 6)
- [x] Temporal client + worker (Task 7)
- [x] PipelineRunWorkflow (Task 8)
- [x] CLI submission (Task 9)
- [x] Durability integration test (Task 10)
- [x] README (Task 11)

Phase I.1 exit criterion met: workflow durable across worker restart.
Ready for Phase I.2 (first real pipeline: Postgres cursor-incremental → Parquet).
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-04-22-phase-1-1-skeleton.md
git commit -m "docs: Phase I.1 completion log

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Appendix A — Troubleshooting

**`cargo build` fails on `temporal-sdk-core` version resolution.**
The Rust Temporal SDK's crate names and versions have shifted over time. If the pinned versions in Task 7's `Cargo.toml` don't resolve, check <https://crates.io/crates/temporal-sdk> for the current release and update all four `temporal-*` crates to matching versions. The structural code (client → worker → register → run) does not depend on specific versions.

**Worker connects but workflow never runs.**
Check that `TEMPORAL_TASK_QUEUE` matches between the worker's env and the CLI's env. If they differ, the workflow sits queued forever. The Temporal UI shows this as "unassigned tasks".

**Integration test hangs in `wait_for_status_change`.**
Probable cause: worker not running, or wrong task queue. Check worker stdout for `worker polling` and verify the Temporal UI shows an active worker on the task queue.

**`temporal server start-dev` uses port already in use.**
Kill any existing dev server or use `--port 7234` and update `TEMPORAL_ADDRESS` accordingly.

**`sqlx` complains about offline mode at build time.**
This plan uses runtime-checked queries (`sqlx::query(...)`, not `sqlx::query!(...)`). If you later switch to the compile-time macros, run `cargo sqlx prepare --workspace` after any schema change.

## Appendix B — What's deferred to Phase I.2 and beyond

Not in Phase I.1, listed so you don't accidentally build them here:
- Actual connector reading rows from Postgres (Phase I.2)
- Loader writing Parquet (Phase I.2)
- Arrow RecordBatch flowing through the workflow (Phase I.2)
- WASM runtime / connector sandbox (Phase I.3)
- Full catalog (workspaces, streams, schemas) (Phase I.4)
- DSL YAML parser (Phase I.4)
- Transformations (Phase I.5)
- CDC (Phase I.6)
- Multi-tenancy beyond the TenantId plumbing (Phase II.1)
- Secrets backend (Phase II.2)
- Anything else in the roadmap
