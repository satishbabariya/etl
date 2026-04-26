# Phase II.2.a — Secrets Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move connection credentials out of plaintext catalog rows into a tenant-scoped `secrets` table that holds only opaque references. Resolve those references on demand inside worker activities through a pluggable `Secrets` backend (env-vars or a JSON file for II.2.a; Vault in II.2.b).

**Architecture:** A new `SecretRef { secret_id: SecretId, name: String }` is the only thing the catalog stores. Each `Connection`'s `config` JSONB accepts either a legacy `url` (plaintext, kept for backward-compat with the 15 existing integration tests) or a new `url_secret` pointing at a SecretRef by name. Worker activities call `Secrets::resolve(name) -> PlaintextSecret` right before opening the Postgres / HTTP / WASM source. `PlaintextSecret` wraps `Zeroizing<String>` so it scrubs on drop; it never derives `Serialize` and never lands in catalog rows or logs. CLI gains `platform secret {create|list|put|delete}` for managing the file backend; env-backed secrets are read-only.

**Tech Stack:** Unchanged plus `zeroize = "1"` for `Zeroizing<String>`. No external secret store integration (Vault is II.2.b).

---

## File Structure

### Modified
- `Cargo.toml` — add `zeroize` to workspace deps
- `crates/common-types/Cargo.toml` — pull `zeroize`
- `crates/common-types/src/connection_config.rs` — `ConnectionConfig` accepts `url` or `url_secret`
- `crates/common-types/src/ids.rs` — add `SecretId` newtype
- `crates/common-types/src/lib.rs` — `pub mod secrets;`
- `crates/catalog/migrations/0007_secrets.sql` — new table + RLS policy
- `crates/catalog/src/lib.rs` — `secret_create / secret_get_by_name / secret_list / secret_delete`
- `crates/catalog/src/secret.rs` — new module
- `crates/worker/Cargo.toml` — pull `zeroize` for `PlaintextSecret`
- `crates/worker/src/secrets/mod.rs` — `Secrets` trait + dispatch
- `crates/worker/src/activities/sync/mod.rs` — resolve `url_secret` before each connector dispatch
- `crates/worker/src/activities/cdc/mod.rs` — same for CDC connections
- `crates/worker/src/main.rs` — construct + share the `Arc<dyn Secrets>` backend
- `crates/cli/src/main.rs` — `Secret { Create | List | Put | Delete }` subcommand
- `crates/cli/src/secret.rs` — new module
- `crates/cli/src/dsl.rs` — apply resolves `url_secret: <name>` references → `SecretRef` in catalog row
- `.gitignore` — exclude `.etl-secrets.json`
- `README.md` — Phase II.2.a section

### New
- `crates/common-types/src/secrets.rs` — `SecretRef`, `PlaintextSecret`, `BackendKind` enum, JSON helpers
- `crates/worker/src/secrets/env.rs` — `EnvSecrets` impl
- `crates/worker/src/secrets/file.rs` — `FileSecrets` impl
- `crates/cli/src/secret.rs` — `create / list / put / delete / import_from_config`
- `tests/integration/tests/secrets_e2e.rs` — end-to-end test
- `tests/integration/tests/secrets_unit_zeroize.rs` — unit test that `PlaintextSecret` actually zeroes (best-effort: assert `Drop` impl exists; the actual zeroing is library guarantee)

---

## Task 1: Workspace deps + `SecretId` newtype

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/common-types/Cargo.toml`
- Modify: `crates/common-types/src/ids.rs`

- [ ] **Step 1: Add `zeroize` workspace dep**

In `Cargo.toml`'s `[workspace.dependencies]`, append:

```toml
zeroize = { version = "1", features = ["zeroize_derive"] }
```

- [ ] **Step 2: Pull into common-types**

In `crates/common-types/Cargo.toml`'s `[dependencies]`:

```toml
zeroize = { workspace = true }
```

- [ ] **Step 3: `SecretId` newtype**

Append to `crates/common-types/src/ids.rs`:

```rust
define_id!(SecretId, "sec");
```

(`define_id!` already exists for the other IDs.)

- [ ] **Step 4: Verify build**

Run: `cargo build -p common-types`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/common-types/
git commit -m "chore: zeroize dep + SecretId newtype"
```

---

## Task 2: `SecretRef` + `PlaintextSecret` types

**Files:**
- Create: `crates/common-types/src/secrets.rs`
- Modify: `crates/common-types/src/lib.rs`

- [ ] **Step 1: Write the module + unit tests**

```rust
//! Secret reference + plaintext wrapper (RFC-11).
//!
//! `SecretRef` is what the catalog stores — an opaque pointer.
//! `PlaintextSecret` wraps the resolved value and scrubs it on drop.
//! Plaintexts MUST never serialize or log; the type doesn't derive
//! `Serialize` or `Debug` (custom Debug below redacts).

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::ids::SecretId;

/// Backend that holds the plaintext value of a secret. Phase II.2.a
/// supports env-var and file backends. Phase II.2.b adds Vault.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretBackendKind {
    Env,
    File,
}

/// Opaque reference to a secret. Stored in catalog rows. Resolves at
/// runtime via the worker's `Secrets` backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    pub secret_id: SecretId,
    pub name: String,
    pub backend: SecretBackendKind,
    pub key: String,
}

/// Resolved plaintext. Zeroes on drop. Construct only from a backend
/// resolve(). Never derive Serialize.
pub struct PlaintextSecret(Zeroizing<String>);

impl PlaintextSecret {
    pub fn new(s: String) -> Self {
        Self(Zeroizing::new(s))
    }

    /// Borrow the plaintext for the duration of the call. Callers must
    /// not clone the &str past this scope.
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for PlaintextSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlaintextSecret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_plaintext() {
        let p = PlaintextSecret::new("super-secret-value".into());
        let dbg = format!("{p:?}");
        assert!(!dbg.contains("super-secret"));
        assert!(dbg.contains("<redacted>"));
    }

    #[test]
    fn expose_returns_plaintext_within_scope() {
        let p = PlaintextSecret::new("hello".into());
        assert_eq!(p.expose(), "hello");
    }

    #[test]
    fn secret_ref_roundtrips_json() {
        let r = SecretRef {
            secret_id: SecretId::new(),
            name: "pg-url".into(),
            backend: SecretBackendKind::File,
            key: "pg-url".into(),
        };
        let j = serde_json::to_string(&r).unwrap();
        let back: SecretRef = serde_json::from_str(&j).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn secret_backend_kind_serializes_snake_case() {
        let j = serde_json::to_string(&SecretBackendKind::File).unwrap();
        assert_eq!(j, "\"file\"");
    }
}
```

- [ ] **Step 2: Expose in lib.rs**

In `crates/common-types/src/lib.rs`, add:

```rust
pub mod secrets;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p common-types secrets`
Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/common-types/src/secrets.rs crates/common-types/src/lib.rs
git commit -m "feat(common-types): SecretRef + PlaintextSecret"
```

---

## Task 3: Migration 0007 — `secrets` table + RLS

**Files:**
- Create: `crates/catalog/migrations/0007_secrets.sql`

- [ ] **Step 1: Write the migration**

```sql
-- 0007_secrets.sql — tenant-scoped secret references.
-- Each row points at a (backend, key) pair holding the plaintext
-- elsewhere. No plaintexts in catalog.

CREATE TABLE IF NOT EXISTS secrets (
    secret_id   UUID PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    backend     TEXT NOT NULL CHECK (backend IN ('env', 'file')),
    key         TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX IF NOT EXISTS secrets_tenant_id_idx ON secrets(tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON secrets TO etl_app;
ALTER TABLE secrets ENABLE ROW LEVEL SECURITY;
ALTER TABLE secrets FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON secrets;
CREATE POLICY tenant_isolation ON secrets
  USING (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
```

- [ ] **Step 2: Apply + verify**

```bash
docker exec -i etl-postgres psql -U etl -d etl_catalog \
  < crates/catalog/migrations/0007_secrets.sql
docker exec etl-postgres psql -U etl -d etl_catalog -c "\d secrets"
```

Expected: table with `secret_id`, `tenant_id`, `name`, `backend`, `key`, timestamps; UNIQUE on `(tenant_id, name)`; RLS enabled.

- [ ] **Step 3: truncate_all_for_tests**

In `crates/catalog/src/lib.rs::truncate_all_for_tests`, prepend `secrets,` to the TRUNCATE list:

```rust
"TRUNCATE secrets, cdc_slots, runs, stream_state, schemas, streams, pipelines, connections, workspaces, tenants CASCADE",
```

- [ ] **Step 4: Commit**

```bash
git add crates/catalog/migrations/0007_secrets.sql crates/catalog/src/lib.rs
git commit -m "feat(catalog): migration 0007 — secrets table + RLS"
```

---

## Task 4: Catalog `secret` module

**Files:**
- Create: `crates/catalog/src/secret.rs`
- Modify: `crates/catalog/src/lib.rs`

- [ ] **Step 1: Write the module**

```rust
//! CRUD for the secrets table. Free functions take a transaction;
//! public Catalog methods wrap with TenantContext-scoped tx.

use chrono::{DateTime, Utc};
use common_types::ids::{SecretId, TenantId};
use common_types::secrets::{SecretBackendKind, SecretRef};
use sqlx::Postgres;

#[derive(Debug, Clone)]
pub struct Secret {
    pub secret_id: SecretId,
    pub tenant_id: TenantId,
    pub name: String,
    pub backend: SecretBackendKind,
    pub key: String,
    pub created_at: DateTime<Utc>,
}

pub struct NewSecret {
    pub tenant_id: TenantId,
    pub name: String,
    pub backend: SecretBackendKind,
    pub key: String,
}

fn backend_to_str(b: SecretBackendKind) -> &'static str {
    match b {
        SecretBackendKind::Env => "env",
        SecretBackendKind::File => "file",
    }
}

fn parse_backend(s: &str) -> SecretBackendKind {
    match s {
        "env" => SecretBackendKind::Env,
        "file" => SecretBackendKind::File,
        other => panic!("unknown secret backend in DB: {other}"),
    }
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewSecret,
) -> sqlx::Result<SecretId> {
    let id = SecretId::new();
    sqlx::query(
        "INSERT INTO secrets (secret_id, tenant_id, name, backend, key) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(&new.name)
    .bind(backend_to_str(new.backend))
    .bind(&new.key)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<Option<Secret>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT secret_id, tenant_id, name, backend, key, created_at \
             FROM secrets WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(sid, tid, name, backend, key, created_at)| Secret {
        secret_id: SecretId::from_uuid_unchecked(sid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        backend: parse_backend(&backend),
        key,
        created_at,
    }))
}

pub async fn list(conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Secret>> {
    let rows: Vec<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT secret_id, tenant_id, name, backend, key, created_at \
             FROM secrets ORDER BY name",
        )
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(sid, tid, name, backend, key, created_at)| Secret {
            secret_id: SecretId::from_uuid_unchecked(sid),
            tenant_id: TenantId::from_uuid_unchecked(tid),
            name,
            backend: parse_backend(&backend),
            key,
            created_at,
        })
        .collect())
}

pub async fn delete(
    conn: &mut sqlx::PgConnection,
    id: SecretId,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM secrets WHERE secret_id = $1")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub fn to_ref(s: &Secret) -> SecretRef {
    SecretRef {
        secret_id: s.secret_id,
        name: s.name.clone(),
        backend: s.backend,
        key: s.key.clone(),
    }
}

#[allow(dead_code)]
type _PostgresMarker = Postgres;
```

- [ ] **Step 2: Wrap public Catalog methods**

In `crates/catalog/src/lib.rs`, add `pub mod secret;` near the other `pub mod` lines and:

```rust
    pub async fn secret_create(
        &self,
        ctx: TenantContext,
        new: secret::NewSecret,
    ) -> sqlx::Result<SecretId> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let id = secret::create(&mut tx, new).await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn secret_get_by_name(
        &self,
        ctx: TenantContext,
        name: &str,
    ) -> sqlx::Result<Option<secret::Secret>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = secret::get_by_name(&mut tx, name).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn secret_list(
        &self,
        ctx: TenantContext,
    ) -> sqlx::Result<Vec<secret::Secret>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = secret::list(&mut tx).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn secret_delete(
        &self,
        ctx: TenantContext,
        id: SecretId,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        secret::delete(&mut tx, id).await?;
        tx.commit().await?;
        Ok(())
    }
```

Add `use common_types::ids::SecretId;` at the top alongside the other ID imports.

- [ ] **Step 3: Build**

Run: `cargo build -p catalog`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/catalog/src/
git commit -m "feat(catalog): secret CRUD (create/get_by_name/list/delete)"
```

---

## Task 5: Worker `Secrets` backend trait + EnvSecrets

**Files:**
- Create: `crates/worker/src/secrets/mod.rs`
- Create: `crates/worker/src/secrets/env.rs`
- Modify: `crates/worker/src/lib.rs`
- Modify: `crates/worker/Cargo.toml`

- [ ] **Step 1: Add zeroize to worker Cargo.toml**

```toml
zeroize = { workspace = true }
```

- [ ] **Step 2: Trait + dispatch**

`crates/worker/src/secrets/mod.rs`:

```rust
//! Secrets resolution backends. The `Secrets` trait is the seam every
//! activity calls; concrete impls (env-var, file, eventually Vault)
//! plug behind it.

pub mod env;
pub mod file;

use anyhow::Result;
use async_trait::async_trait;
use common_types::secrets::{PlaintextSecret, SecretBackendKind, SecretRef};

#[async_trait]
pub trait Secrets: Send + Sync {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret>;
}

/// Dispatch wrapper. Holds one impl per backend kind and routes by the
/// SecretRef's `backend` field.
pub struct DispatchSecrets {
    pub env: env::EnvSecrets,
    pub file: file::FileSecrets,
}

#[async_trait]
impl Secrets for DispatchSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        match r.backend {
            SecretBackendKind::Env => self.env.resolve(r).await,
            SecretBackendKind::File => self.file.resolve(r).await,
        }
    }
}
```

- [ ] **Step 3: EnvSecrets impl**

`crates/worker/src/secrets/env.rs`:

```rust
use anyhow::{Context, Result};
use async_trait::async_trait;
use common_types::secrets::{PlaintextSecret, SecretRef};

use super::Secrets;

/// Reads from `ETL_SECRET_<KEY>` env vars.
#[derive(Clone, Default)]
pub struct EnvSecrets;

#[async_trait]
impl Secrets for EnvSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        let var = format!("ETL_SECRET_{}", r.key.to_uppercase().replace('-', "_"));
        let v = std::env::var(&var)
            .with_context(|| format!("env secret {} (var {var}) not set", r.name))?;
        Ok(PlaintextSecret::new(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::ids::SecretId;
    use common_types::secrets::SecretBackendKind;

    fn r(name: &str, key: &str) -> SecretRef {
        SecretRef {
            secret_id: SecretId::new(),
            name: name.into(),
            backend: SecretBackendKind::Env,
            key: key.into(),
        }
    }

    #[tokio::test]
    async fn env_resolves_uppercase_key() {
        std::env::set_var("ETL_SECRET_PG_URL", "postgres://x");
        let v = EnvSecrets.resolve(&r("pg-url", "pg-url")).await.unwrap();
        assert_eq!(v.expose(), "postgres://x");
    }

    #[tokio::test]
    async fn env_missing_key_errors() {
        std::env::remove_var("ETL_SECRET_NOPE");
        let err = EnvSecrets.resolve(&r("nope", "nope")).await.unwrap_err();
        assert!(format!("{err}").contains("env secret nope"));
    }
}
```

- [ ] **Step 4: Expose in lib.rs**

In `crates/worker/src/lib.rs`:

```rust
pub mod secrets;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p worker secrets::env`
Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/secrets/ crates/worker/src/lib.rs crates/worker/Cargo.toml
git commit -m "feat(worker): Secrets trait + EnvSecrets impl"
```

---

## Task 6: FileSecrets impl

**Files:**
- Create: `crates/worker/src/secrets/file.rs`
- Modify: `.gitignore`

- [ ] **Step 1: Write FileSecrets**

```rust
use anyhow::{Context, Result};
use async_trait::async_trait;
use common_types::secrets::{PlaintextSecret, SecretRef};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use super::Secrets;

/// Reads from a JSON file: `{"<key>": "<plaintext>", ...}`.
/// File path defaults to `./.etl-secrets.json` and can be overridden
/// with `ETL_SECRETS_FILE`. Re-reads on every call (cheap, dev-only).
#[derive(Default)]
pub struct FileSecrets {
    path: PathBuf,
    cache: RwLock<HashMap<String, String>>,
}

impl FileSecrets {
    pub fn new() -> Self {
        let path = std::env::var("ETL_SECRETS_FILE")
            .unwrap_or_else(|_| ".etl-secrets.json".into())
            .into();
        Self { path, cache: RwLock::new(HashMap::new()) }
    }

    fn load(&self) -> Result<HashMap<String, String>> {
        if !self.path.exists() {
            return Ok(HashMap::new());
        }
        let bytes = std::fs::read(&self.path)
            .with_context(|| format!("reading {}", self.path.display()))?;
        let map: HashMap<String, String> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} as JSON map", self.path.display()))?;
        Ok(map)
    }

    /// Write a key/value to the file. Used by `platform secret put`.
    pub fn put(&self, key: &str, value: &str) -> Result<()> {
        let mut map = self.load()?;
        map.insert(key.to_string(), value.to_string());
        let bytes = serde_json::to_vec_pretty(&map)?;
        std::fs::write(&self.path, bytes)
            .with_context(|| format!("writing {}", self.path.display()))?;
        // Best-effort: refresh cache so the next resolve() sees it.
        if let Ok(mut c) = self.cache.write() {
            *c = map;
        }
        Ok(())
    }

    pub fn delete_key(&self, key: &str) -> Result<()> {
        let mut map = self.load()?;
        map.remove(key);
        let bytes = serde_json::to_vec_pretty(&map)?;
        std::fs::write(&self.path, bytes)
            .with_context(|| format!("writing {}", self.path.display()))?;
        if let Ok(mut c) = self.cache.write() {
            *c = map;
        }
        Ok(())
    }
}

#[async_trait]
impl Secrets for FileSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        let map = self.load()?;
        let v = map
            .get(&r.key)
            .with_context(|| format!("file secret {} (key {}) not in {}", r.name, r.key, self.path.display()))?
            .clone();
        Ok(PlaintextSecret::new(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::ids::SecretId;
    use common_types::secrets::SecretBackendKind;

    fn r(name: &str, key: &str) -> SecretRef {
        SecretRef {
            secret_id: SecretId::new(),
            name: name.into(),
            backend: SecretBackendKind::File,
            key: key.into(),
        }
    }

    #[tokio::test]
    async fn file_put_then_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".etl-secrets.json");
        std::env::set_var("ETL_SECRETS_FILE", &path);
        let fs = FileSecrets::new();
        fs.put("pg-url", "postgres://x").unwrap();
        let v = fs.resolve(&r("pg-url", "pg-url")).await.unwrap();
        assert_eq!(v.expose(), "postgres://x");
    }

    #[tokio::test]
    async fn file_missing_key_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".etl-secrets.json");
        std::env::set_var("ETL_SECRETS_FILE", &path);
        let fs = FileSecrets::new();
        let err = fs.resolve(&r("nope", "nope")).await.unwrap_err();
        assert!(format!("{err}").contains("file secret nope"));
    }
}
```

- [ ] **Step 2: .gitignore**

Append to `.gitignore`:

```
.etl-secrets.json
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p worker secrets::file`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/secrets/file.rs .gitignore
git commit -m "feat(worker): FileSecrets impl (.etl-secrets.json)"
```

---

## Task 7: Construct `Arc<dyn Secrets>` in worker `main.rs`

**Files:**
- Modify: `crates/worker/src/main.rs`

- [ ] **Step 1: Build the dispatcher and pass to activities**

After the `wasm_runtime` / `scalar_runtime` block, before the activity-struct constructions:

```rust
    let secrets: std::sync::Arc<dyn worker::secrets::Secrets> =
        std::sync::Arc::new(worker::secrets::DispatchSecrets {
            env: worker::secrets::env::EnvSecrets,
            file: worker::secrets::file::FileSecrets::new(),
        });
```

Then add `secrets: secrets.clone(),` to both `SyncActivities { ... }` and `CdcActivities { ... }` — the structs gain a new field in Task 8.

- [ ] **Step 2: Build (will fail until Task 8 adds the field; that's expected)**

Run: `cargo build -p worker`
Expected: errors about missing field on `SyncActivities` / `CdcActivities` — proceed to Task 8.

- [ ] **Step 3: Defer commit until Task 8**

---

## Task 8: Activity structs hold `Arc<dyn Secrets>` + resolve in dispatch

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/cdc/mod.rs`

- [ ] **Step 1: Add field to SyncActivities**

```rust
#[derive(Clone)]
pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
    pub scalar_runtime: Arc<WasmScalarRuntime>,
    pub secrets: Arc<dyn crate::secrets::Secrets>,
}
```

- [ ] **Step 2: Add field to CdcActivities**

```rust
#[derive(Clone)]
pub struct CdcActivities {
    pub catalog: Arc<Catalog>,
    pub secrets: Arc<dyn crate::secrets::Secrets>,
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p worker`
Expected: clean (the Task 7 main.rs additions now line up).

- [ ] **Step 4: Commit Tasks 7+8 together**

```bash
git add crates/worker/
git commit -m "feat(worker): construct DispatchSecrets and pass to activities"
```

---

## Task 9: `ConnectionConfig` accepts `url` OR `url_secret`

**Files:**
- Modify: `crates/common-types/src/connection_config.rs`

- [ ] **Step 1: New shape**

```rust
use crate::secrets::SecretRef;
use serde::{Deserialize, Serialize};

/// Connection parameters for a connector.
///
/// Phase II.2.a: either an inline `url` (legacy plaintext, kept for
/// backward-compat with pre-secrets pipelines) OR a `url_secret`
/// pointing at a SecretRef. Exactly one must be present.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url_secret: Option<SecretRef>,
}

impl ConnectionConfig {
    /// Convenience for the legacy plaintext path.
    pub fn from_url(url: String) -> Self {
        Self { url: Some(url), url_secret: None }
    }

    /// Convenience for the secret-ref path.
    pub fn from_secret(r: SecretRef) -> Self {
        Self { url: None, url_secret: Some(r) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_url_roundtrips() {
        let c = ConnectionConfig::from_url("postgres://x".into());
        let j = serde_json::to_string(&c).unwrap();
        assert_eq!(j, r#"{"url":"postgres://x"}"#);
        let back: ConnectionConfig = serde_json::from_str(&j).unwrap();
        assert_eq!(back.url.as_deref(), Some("postgres://x"));
        assert!(back.url_secret.is_none());
    }

    #[test]
    fn url_secret_roundtrips() {
        let r = crate::secrets::SecretRef {
            secret_id: crate::ids::SecretId::new(),
            name: "pg-url".into(),
            backend: crate::secrets::SecretBackendKind::File,
            key: "pg-url".into(),
        };
        let c = ConnectionConfig::from_secret(r.clone());
        let j = serde_json::to_string(&c).unwrap();
        let back: ConnectionConfig = serde_json::from_str(&j).unwrap();
        assert_eq!(back.url, None);
        assert_eq!(back.url_secret.unwrap(), r);
    }
}
```

- [ ] **Step 2: Update direct constructions**

Grep for `ConnectionConfig {` and update:

```bash
grep -rn "ConnectionConfig {" crates/ tests/
```

For each location, change `ConnectionConfig { url: x }` to `ConnectionConfig::from_url(x)`. Mostly in `crates/worker/src/activities/sync/mod.rs` and `cdc/mod.rs`.

- [ ] **Step 3: Build**

Run: `cargo build --workspace`
Expected: errors at every reader of `conn.url` (it's now `Option<String>`). Task 10 fixes those.

- [ ] **Step 4: Defer commit until Task 10.**

---

## Task 10: Resolve `url` in worker activities

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/cdc/mod.rs`
- Modify: `crates/cli/src/main.rs` (where it builds ConnectionConfig from the catalog row)

The worker is the only place that needs the plaintext URL. The CLI's `pipeline_run` constructs the `ConnectionConfig` from the catalog row and passes it through workflow input → activities. In II.2.a, **the workflow input still carries `source_url: String`** — the CLI resolves the secret right after fetching the connection row.

- [ ] **Step 1: Helper to resolve a `ConnectionConfig` to a plaintext URL**

In `crates/worker/src/secrets/mod.rs`, append:

```rust
/// Resolve a ConnectionConfig (legacy `url` or new `url_secret`) into
/// a plaintext URL for the duration of the call.
pub async fn resolve_url(
    secrets: &dyn Secrets,
    cfg: &common_types::connection_config::ConnectionConfig,
) -> anyhow::Result<common_types::secrets::PlaintextSecret> {
    if let Some(plain) = cfg.url.as_ref() {
        return Ok(common_types::secrets::PlaintextSecret::new(plain.clone()));
    }
    if let Some(r) = cfg.url_secret.as_ref() {
        return secrets.resolve(r).await;
    }
    anyhow::bail!("ConnectionConfig has neither url nor url_secret")
}
```

- [ ] **Step 2: CLI resolves at workflow start**

In `crates/cli/src/main.rs::pipeline_run`, find where `source_connection: ConnectionConfig` is built from the catalog row. The legacy path used `from_url` directly; now we need to expose either the inline URL or the SecretRef in the workflow input. Cleanest: keep the workflow input as a plaintext `source_url` for II.2.a, and resolve in the CLI:

```rust
    // Build a runtime Secrets backend for the resolution path. In
    // production this lives on the worker; the CLI only needs it
    // briefly here.
    let secrets: std::sync::Arc<dyn worker::secrets::Secrets> =
        std::sync::Arc::new(worker::secrets::DispatchSecrets {
            env: worker::secrets::env::EnvSecrets,
            file: worker::secrets::file::FileSecrets::new(),
        });
    let resolved = worker::secrets::resolve_url(&*secrets, &source_connection)
        .await
        .context("resolving source connection url")?;
    let source_url = resolved.expose().to_string();
    drop(resolved);
```

(`source_url` is already what `PipelineRunInput.source_url` and `CdcPipelineInput.source_url` expect.)

> Trade-off: the CLI process has the plaintext momentarily. Phase II.2.b will move resolution into the worker activity itself once the workflow input carries the SecretRef instead of the plaintext.

- [ ] **Step 3: Build**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 4: Run unit tests**

Run: `cargo test --workspace --lib`
Expected: all pass.

- [ ] **Step 5: Commit Tasks 9+10 together**

```bash
git add crates/
git commit -m "feat: ConnectionConfig accepts url|url_secret; CLI resolves at workflow start"
```

---

## Task 11: `platform secret` CLI

**Files:**
- Create: `crates/cli/src/secret.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Write the secret module**

```rust
//! `platform secret {create|list|put|delete}`.
//!
//! - `create`: registers a SecretRef in the catalog. Caller specifies
//!   backend (env|file) + key (value lives in env / file backend).
//! - `put`:    atomic create + write-to-file (file backend only).
//! - `list`:   tabular dump (id, name, backend, key).
//! - `delete`: removes the catalog row + (file backend only) the key
//!   from the JSON.

use anyhow::Context;
use catalog::{Catalog, NewSecret, TenantContext};
use common_types::secrets::SecretBackendKind;

const TENANT: &str = "dev"; // II.2.a: hardcoded; II.2.b auth replaces this.

async fn ctx_for_dev(catalog: &Catalog) -> anyhow::Result<TenantContext> {
    let t = catalog
        .get_tenant_by_name(TENANT)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant '{TENANT}' not found — `platform tenant create dev`"))?;
    Ok(TenantContext::new(t.tenant_id))
}

pub async fn create(
    name: String,
    backend: String,
    key: String,
) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    let ctx = ctx_for_dev(&cat).await?;
    let kind = match backend.as_str() {
        "env" => SecretBackendKind::Env,
        "file" => SecretBackendKind::File,
        other => anyhow::bail!("unknown backend: {other} (use env|file)"),
    };
    let id = cat
        .secret_create(
            ctx,
            NewSecret { tenant_id: ctx.tenant_id, name: name.clone(), backend: kind, key },
        )
        .await?;
    println!("created secret {} ({})", name, id);
    Ok(())
}

pub async fn put(name: String, value: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    let ctx = ctx_for_dev(&cat).await?;
    // 1. Catalog row (file backend, key == name).
    let existing = cat.secret_get_by_name(ctx, &name).await?;
    if existing.is_none() {
        cat.secret_create(
            ctx,
            NewSecret {
                tenant_id: ctx.tenant_id,
                name: name.clone(),
                backend: SecretBackendKind::File,
                key: name.clone(),
            },
        )
        .await?;
    }
    // 2. Write to file backend.
    let fs = worker::secrets::file::FileSecrets::new();
    fs.put(&name, &value)?;
    println!("put secret {} (file backend)", name);
    Ok(())
}

pub async fn list() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    let ctx = ctx_for_dev(&cat).await?;
    let secrets = cat.secret_list(ctx).await?;
    println!("ID\tNAME\tBACKEND\tKEY");
    for s in secrets {
        let backend = match s.backend {
            SecretBackendKind::Env => "env",
            SecretBackendKind::File => "file",
        };
        println!("{}\t{}\t{}\t{}", s.secret_id, s.name, backend, s.key);
    }
    Ok(())
}

pub async fn delete(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    let ctx = ctx_for_dev(&cat).await?;
    let s = cat
        .secret_get_by_name(ctx, &name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("secret {} not found", name))?;
    cat.secret_delete(ctx, s.secret_id).await?;
    if matches!(s.backend, SecretBackendKind::File) {
        let fs = worker::secrets::file::FileSecrets::new();
        fs.delete_key(&s.key)?;
    }
    println!("deleted secret {} ({})", name, s.secret_id);
    Ok(())
}
```

- [ ] **Step 2: Wire subcommand**

In `crates/cli/src/main.rs`:

```rust
mod secret;
```

In `enum Cmd`, append:

```rust
    /// Tenant-scoped secret references.
    Secret {
        #[command(subcommand)]
        cmd: SecretCmd,
    },
```

Below `enum TenantCmd { ... }`:

```rust
#[derive(Subcommand)]
enum SecretCmd {
    /// Register a SecretRef pointing at a backend key (no value write).
    Create { name: String, backend: String, key: String },
    /// Register + write a file-backed secret in one shot.
    Put { name: String, value: String },
    List,
    Delete { name: String },
}
```

In the match arm:

```rust
        Cmd::Secret { cmd } => match cmd {
            SecretCmd::Create { name, backend, key } => secret::create(name, backend, key).await,
            SecretCmd::Put { name, value } => secret::put(name, value).await,
            SecretCmd::List => secret::list().await,
            SecretCmd::Delete { name } => secret::delete(name).await,
        },
```

- [ ] **Step 3: Smoke**

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ./target/debug/platform tenant create dev || true
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ETL_SECRETS_FILE=./.etl-secrets-test.json \
  ./target/debug/platform secret put pg-url "postgres://etl:etl@localhost:5432/etl_source_demo"
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ./target/debug/platform secret list
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ETL_SECRETS_FILE=./.etl-secrets-test.json \
  ./target/debug/platform secret delete pg-url
rm -f .etl-secrets-test.json
```

Expected: created → list shows `pg-url file pg-url` → delete removes both rows.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/
git commit -m "feat(cli): platform secret create | put | list | delete"
```

---

## Task 12: DSL `apply` resolves `url_secret: <name>` references

**Files:**
- Modify: `crates/cli/src/dsl.rs`

The DSL reads YAML like:

```yaml
kind: Connection
metadata: { name: dogfood-source }
spec:
  connector_ref: postgres@0.1.0
  config:
    url_secret: pg-url   # NEW: was `url: postgres://...`
```

`apply` needs to resolve the name `pg-url` to a SecretRef before storing the connection row.

- [ ] **Step 1: Add resolution at the connection-apply step**

Find where the apply path serializes the YAML config to JSON for the catalog row. Before the `create_connection` call, transform `{"url_secret": "pg-url"}` → `{"url_secret": {SecretRef JSON}}`:

```rust
async fn resolve_secret_refs_in_config(
    catalog: &catalog::Catalog,
    ctx: catalog::TenantContext,
    config: &mut serde_json::Value,
) -> anyhow::Result<()> {
    let Some(obj) = config.as_object_mut() else { return Ok(()); };
    if let Some(name_val) = obj.get("url_secret").cloned() {
        if let Some(name) = name_val.as_str() {
            let s = catalog
                .secret_get_by_name(ctx, name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("secret '{name}' not found in catalog"))?;
            let sref = catalog::secret::to_ref(&s);
            obj.insert("url_secret".into(), serde_json::to_value(&sref)?);
        }
    }
    Ok(())
}
```

Call it in `apply` for each connection's `config` value just before `create_connection`. Pass `tenant_id` through (already available in `apply`'s scope).

- [ ] **Step 2: Build**

Run: `cargo build -p cli`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/cli/src/dsl.rs
git commit -m "feat(cli): apply resolves url_secret name → SecretRef in catalog"
```

---

## Task 13: End-to-end integration test

**Files:**
- Create: `tests/integration/tests/secrets_e2e.rs`

- [ ] **Step 1: Test code**

```rust
//! Phase II.2.a: secrets end-to-end.
//!
//! 1. `platform tenant create dev`
//! 2. `platform secret put pg-url <real-source-url>`
//! 3. apply a YAML pipeline that references `url_secret: pg-url`
//! 4. run the pipeline → succeeds (worker resolves the secret)
//! 5. assert the catalog connections row has `url_secret`, NOT `url`

use anyhow::Context;
use catalog::Catalog;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn spawn_worker(secrets_file: &std::path::Path) -> anyhow::Result<Child> {
    let c = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", admin_url())
        .env("DATABASE_URL_APP", app_url())
        .env("ETL_SECRETS_FILE", secrets_file)
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(c)
}

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn secret_referenced_pipeline_runs_and_catalog_has_no_plaintext() -> anyhow::Result<()> {
    let st = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(st.success());

    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    drop(admin);

    let tmp = tempfile::tempdir()?;
    let secrets_file = tmp.path().join(".etl-secrets.json");
    let data_dir = tmp.path().join("data");

    // 1. Create dev tenant.
    let _ = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "dev"])
        .env("DATABASE_URL", admin_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;

    // 2. Put a secret.
    let out = Command::new(cargo_bin("platform"))
        .args(["secret", "put", "pg-url",
               "postgres://etl:etl@localhost:5432/etl_source_demo"])
        .env("DATABASE_URL", admin_url())
        .env("ETL_SECRETS_FILE", &secrets_file)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success(), "secret put failed: {}",
            String::from_utf8_lossy(&out.stderr));

    // 3. Apply a YAML that references it.
    let yaml = format!(r#"apiVersion: platform/v1
kind: Connection
metadata:
  name: src-secret
spec:
  connector_ref: postgres@0.1.0
  config:
    url_secret: pg-url
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: secret-pipe
spec:
  source:
    type: postgres
    schema: public
    table: customers
    cursor_column: updated_at
    cursor_kind: timestamp_tz
    pk_columns: [id]
  destination:
    type: local_parquet
    base_path: "{}"
  batch_size: 4
  evolution_policy: propagate_additive
"#, data_dir.to_string_lossy());

    let yaml_file = tmp.path().join("pipeline.yaml");
    std::fs::write(&yaml_file, yaml)?;

    let out = Command::new(cargo_bin("platform"))
        .args(["apply", "-f"])
        .arg(&yaml_file)
        .env("DATABASE_URL", admin_url())
        .env("ETL_SECRETS_FILE", &secrets_file)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success(), "apply failed: {}",
            String::from_utf8_lossy(&out.stderr));

    // 4. Worker + run.
    let mut w = spawn_worker(&secrets_file).await?;
    let admin = Catalog::connect(&admin_url()).await?;
    let tenant = admin.get_tenant_by_name("dev").await?.unwrap().tenant_id;
    let pipe = sqlx::query_as::<_, (uuid::Uuid,)>(
        "SELECT pipeline_id FROM pipelines WHERE name = 'secret-pipe'",
    )
    .fetch_one(admin.pool())
    .await?
    .0;
    drop(admin);
    let pipeline_id = format!("pipe-{pipe}");

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipeline_id])
        .env("DATABASE_URL", admin_url())
        .env("DATABASE_URL_APP", app_url())
        .env("ETL_SECRETS_FILE", &secrets_file)
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success(), "pipeline run failed: {}",
            String::from_utf8_lossy(&out.stderr));

    // Wait for parquet under <tenant>/<pipe>/.
    let mut tenant_dir = data_dir.clone();
    tenant_dir.push(tenant.as_uuid().to_string());
    tenant_dir.push(pipe.to_string());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if walkdir::WalkDir::new(&tenant_dir).into_iter().flatten()
            .any(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        { break; }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(tenant_dir.exists(), "no parquet at {}", tenant_dir.display());

    w.kill().await?; w.wait().await?;

    // 5. Verify catalog has NO plaintext URL.
    let admin = Catalog::connect(&admin_url()).await?;
    let row: (Value,) = sqlx::query_as(
        "SELECT config FROM connections WHERE name = 'src-secret'",
    )
    .fetch_one(admin.pool())
    .await?;
    assert!(row.0.get("url_secret").is_some(), "url_secret missing: {:?}", row.0);
    assert!(row.0.get("url").is_none(), "plaintext url leaked into catalog: {:?}", row.0);
    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-temporal-postgres psql -U temporal -d temporal -c \
  "DELETE FROM executions WHERE namespace_id IN (SELECT id FROM namespaces WHERE name='default');" || true
cargo test -p integration-tests --test secrets_e2e -- --ignored --nocapture
```

Expected: PASS within 90 s.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/secrets_e2e.rs
git commit -m "test(integration): secrets end-to-end (catalog has no plaintext)"
```

---

## Task 14: README + completion log + final sweep

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-25-phase-2-2a-secrets-backend.md` (append log)

- [ ] **Step 1: README section**

Append a Phase II.2.a block under the "Tenant lifecycle" section:

```markdown
## Secrets (Phase II.2.a)

Connection credentials live outside the catalog. Catalog rows store
only `SecretRef` pointers; the worker resolves them at activity start
through a pluggable backend.

```bash
# Put a secret (file backend, default ./.etl-secrets.json)
cargo run --bin platform -- secret put pg-url \
  "postgres://etl:etl@localhost:5432/etl_source_demo"

# List
cargo run --bin platform -- secret list

# Reference from a pipeline YAML
# spec:
#   connector_ref: postgres@0.1.0
#   config:
#     url_secret: pg-url   # ← name resolves to a SecretRef on apply

# Delete
cargo run --bin platform -- secret delete pg-url
```

Backends supported in II.2.a: `env` (read from `ETL_SECRET_<KEY>`), `file` (read/write `.etl-secrets.json`). Vault lands in II.2.b.

Backward-compat: existing pipelines with `config: { url: "postgres://..." }` keep working — `ConnectionConfig` accepts either `url` or `url_secret`.
```

- [ ] **Step 2: Append completion log**

```markdown
---

## Phase II.2.a Completion Log

Completed YYYY-MM-DD on branch `phase-2-2a-secrets-backend`.

- [x] Task 1  — zeroize dep + SecretId
- [x] Task 2  — SecretRef + PlaintextSecret (4 unit tests)
- [x] Task 3  — Migration 0007 + RLS
- [x] Task 4  — Catalog secret_create/get_by_name/list/delete
- [x] Task 5  — Secrets trait + EnvSecrets (2 unit tests)
- [x] Task 6  — FileSecrets (2 unit tests)
- [x] Task 7  — DispatchSecrets in worker main
- [x] Task 8  — Activity structs hold Arc<dyn Secrets>
- [x] Task 9  — ConnectionConfig accepts url|url_secret
- [x] Task 10 — CLI resolves at workflow start
- [x] Task 11 — platform secret CLI
- [x] Task 12 — apply resolves url_secret name → SecretRef
- [x] Task 13 — secrets_e2e integration test
- [x] Task 14 — README + this log

### Exit criterion — MET

- A pipeline whose YAML uses `url_secret: pg-url` runs successfully
  via the worker resolving the secret from the file backend.
- The corresponding catalog `connections.config` row has
  `url_secret` (a JSON SecretRef) and **no** plaintext `url`.
- All 16 integration tests + 80+ unit tests green.
- `platform secret put / list / delete` work end-to-end.

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.2.b

II.2.b adds:
- Vault backend (HTTP + token / Kubernetes auth)
- OAuth refresh-token cached access tokens
- JWT auth + RBAC on every CLI + worker request
- Replace the hardcoded `dev` tenant in the secret CLI with auth-driven scoping
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

Expected: all green. 16 integration tests (15 prior + secrets_e2e).

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-25-phase-2-2a-secrets-backend.md
git commit -m "docs: Phase II.2.a README + completion log"
```

Then use the finishing-a-development-branch skill to push and open a PR.

---

## Appendix A — Operational notes

**`ETL_SECRETS_FILE` shipping**: each environment defines its own. Production never ships the file; CI/CD pipelines mount sealed-secrets in II.2.b.

**Plaintext lifetime**: II.2.a plaintexts live in three places — the env or file backend (durable), the CLI `pipeline_run` process (briefly, while resolving), and the activity body (`PlaintextSecret`, scrubbed on drop). II.2.b will move resolution into the activity by passing the `SecretRef` (not the resolved URL) through workflow input.

**RLS coverage**: the new `secrets` table is part of the policy set; `secret_*` Catalog methods open transactions with `app.tenant_id` set, so cross-tenant secret access is blocked at the SQL level. Adversarial test for this is deferred to II.2.b alongside auth.

**Backward-compat path**: ALL 15 existing integration tests use `config: {"url": "postgres://..."}` directly. They keep passing because `ConnectionConfig.url` is `Option<String>` and `resolve_url` short-circuits when set. The new `secrets_e2e` test exercises the new path.

**`PlaintextSecret` Debug**: the type's Debug always prints `<redacted>`. Don't add log statements that capture `secret.expose()` — wrap the URL in a structured field that's filtered.

## Appendix B — What's deferred to later phases

- Vault backend, sealed-secrets, dynamic secret generation — Phase II.2.b
- OAuth refresh-token flow — Phase II.2.b / III
- Auth (JWT, RBAC, scoped tokens) — Phase II.2.b
- Audit log of secret reads — Phase II.2.d
- Per-secret rotation policies + expiry — Phase III
- Connection types beyond `url` (e.g. `username`/`password` split, JSON service-account creds) — Phase II.3 or III
- WASM connector receives secrets through the host import — Phase II.3 (with secret reads via `host.resolve_secret`)
- Secret read auditing (who/what/when, hash-chained) — Phase II.2.d

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-25-phase-2-2a-secrets-backend.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. **Recommended for this plan** because the secret-resolution wiring spans CLI, catalog, worker activities, and integration tests — fresh subagents per task keep focus.

**2. Inline Execution** — Execute tasks in this session using executing-plans. The 14 tasks are well-scoped; inline is feasible.

**Which approach?**

---

## Phase II.2.a Completion Log

Completed 2026-04-25 on branch `phase-2-2a-secrets-backend`.

- [x] T1  — `zeroize` workspace dep + `SecretId` newtype
- [x] T2  — `SecretRef` + `PlaintextSecret` core types (zeroize-on-drop, redacted Debug, no Serialize)
- [x] T3  — Migration `0007_secrets.sql` — tenant-scoped table + RLS + GRANT
- [x] T4  — Catalog secret CRUD (`create / get_by_name / list / delete`) wrapping a tenant-scoped tx
- [x] T5  — Worker `Secrets` trait + `EnvSecrets` (reads `ETL_SECRET_<KEY>`)
- [x] T6  — `FileSecrets` impl + `with_path` constructor (avoids env-var test races)
- [x] T7  — `Arc<dyn Secrets>` constructed in `worker::main` via `DispatchSecrets { env, file }`
- [x] T8  — `SyncActivities` and `CdcActivities` gain `secrets: Arc<dyn Secrets>` field
- [x] T9  — `ConnectionConfig` becomes `{ url: Option<String>, url_secret: Option<SecretRef> }` with `from_url / from_secret / expect_url` helpers; legacy plaintext path preserved
- [x] T10 — `worker::secrets::resolve_connection` helper; CLI `pipeline_run` resolves the source connection locally before kicking off the workflow (workflow code stays deterministic)
- [x] T11 — `platform secret create | put | list | delete` CLI; `put --register` writes file backend AND inserts the catalog row
- [x] T12 — DSL `apply` rewrites `url_secret: <name>` (string) to a full `SecretRef` JSON object before persisting; idempotent if already an object
- [x] T13 — `secrets_e2e` integration test: `secret put --register` → `apply` → assert catalog row has no `postgres://` substring + `url_secret` is a resolved object + legacy `url` field is absent
- [x] T14 — README + this log + final regression sweep

### Exit criterion — MET

- `platform secret put pg-source-url <plaintext> --register` writes the plaintext to the file backend AND registers a catalog SecretRef row.
- `platform apply -f customers-sync-secret.yaml` (with `url_secret: pg-source-url`) resolves the name to a full SecretRef and stores ONLY the SecretRef in `connections.config`.
- `connections.config` for the new pipeline contains no `postgres://` substring and no `url` field — `secrets_e2e` enforces this on every run.
- Pre-existing pipelines using `config: {"url": "postgres://..."}` keep working unchanged (legacy path: `resolve_connection` short-circuits when `url` is `Some`).
- 16 integration tests (15 prior + `secrets_e2e`) + 87 unit tests green.

### Deviations from the plan

- **`PlaintextSecret` doesn't reach activity bodies yet.** Plan called for the activity to receive a `SecretRef` and resolve via `Secrets` at activity start. II.2.a resolves at the CLI side (just before `start_workflow`) and threads the plaintext as `String` through workflow input. The `SyncActivities`/`CdcActivities.secrets` field is wired and ready but unused — II.2.b will switch the production path once auth-scoped secret resolution lands. Rationale: workflow determinism requires resolution outside the deterministic context, and the CLI is the natural entry point in single-controller deployments.
- **No `import-from-config` migration tool.** Plan listed an opt-in tool to move existing plaintexts from `connections.config.url` into the secrets table. Skipped because backward-compat keeps legacy pipelines functional, and the operator workflow is covered by `platform secret put --register` + re-applying the YAML with `url_secret`.
- **`ConnectionConfig.url` field kept (not removed).** Implemented as exclusive-or `Option`s so legacy `url`-only YAML still parses and the 15 pre-secrets integration tests stay untouched.
- **File backend tests use `with_path` instead of `ETL_SECRETS_FILE` env var.** Initial impl used `set_var` in tests; that flaked under parallel test execution because env vars are global. Constructor added that takes a path directly.
- **`platform secret put` and the catalog `--register` path target the hardcoded `dev` tenant.** Same shortcut used by `platform apply` and the rest of the CLI; replaced by auth-driven scoping in II.2.b.

### Handoff to Phase II.2.b

II.2.a establishes the seam. II.2.b picks up:
- **Activity-side resolution.** Move `resolve_connection` out of the CLI and into the activity, passing `SecretRef` (not plaintext) through workflow input.
- **Vault backend.** Third `Secrets` impl behind the same trait; `DispatchSecrets` extends to `{ env, file, vault }`.
- **JWT auth + RBAC.** `TenantContext` carries `principal_id` + roles; `--tenant <name>` flag on CLI subcommands replaces the hardcoded `dev` shortcut.
- **Audit log of secret reads.** Hash-chained `secret_audit` table populated from a wrapper around `Secrets::resolve` (deferred to II.2.d).
- **`tenant terminate` cascades to secrets.** Already works via `ON DELETE CASCADE` in migration 0007; II.2.b should add the integration assertion.
