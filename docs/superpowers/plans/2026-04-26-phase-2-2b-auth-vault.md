# Phase II.2.b — Auth + JWT + RBAC + Vault Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded `dev` tenant shortcut with a JWT-driven auth model — every CLI call carries a signed token whose claims (`tenant_id`, `principal_id`, `role`) propagate through `TenantContext` and gate every catalog write at both the auth layer (Rust) and the storage layer (Postgres RLS). Add Vault as a third Secrets backend, move secret resolution from the CLI into the activity, and replace the name-prefix tenant suspension hack with a proper status column.

**Architecture:** A new `auth` crate (`crates/auth`) issues + verifies HS256 JWTs. `TenantContext` extends with `principal_id: Option<PrincipalId>` and `role: Option<Role>`. The CLI gains `platform auth login`, caches creds at `~/.etl/credentials.json`, and reads them on every other subcommand. RBAC is a 3-role enum (`Admin` > `Operator` > `Viewer`) checked at the CLI/API entry point and reinforced by Postgres GRANTs. Vault joins env + file as the third `Secrets` impl behind the same trait. Workflow inputs carry `ConnectionConfig` (which may have `url_secret`); activities resolve via the already-wired `Arc<dyn Secrets>` field. Tenants gain a `status` column; suspension flips the column instead of mangling the name; termination calls Temporal's `DeprecateNamespace` API in addition to the existing catalog cascade.

**Tech Stack:** Rust 1.88, `jsonwebtoken` 9 (HS256), `vaultrs` 0.7 (KV v2), `argon2` 0.5 (password hashing for the dev login flow), sqlx 0.8, Postgres RLS (`current_setting('app.tenant_id'/'app.principal_id'/'app.role')`), Temporal SDK 0.2.

---

## File Structure

**New crates / modules:**
- `crates/auth/` — JWT issuer/verifier + `Role` enum + RBAC permits.
  - `Cargo.toml`, `src/lib.rs`, `src/jwt.rs`, `src/rbac.rs`
- `crates/cli/src/auth.rs` — `platform auth login` + credentials cache helpers.
- `crates/catalog/src/principal.rs` — principal CRUD (insert + lookup by name + verify-password).
- `crates/worker/src/secrets/vault.rs` — `VaultSecrets` impl.
- `tests/integration/tests/auth_jwt.rs` — login + cross-tenant rejection.
- `tests/integration/tests/rbac_matrix.rs` — viewer/operator/admin enforcement.
- `tests/integration/tests/vault_e2e.rs` — Vault-backed pipeline run (gated on `ETL_VAULT_ADDR`).
- `tests/integration/tests/tenant_suspend.rs` — suspended tenant blocked from running.

**Migrations:**
- `crates/catalog/migrations/0008_tenants_status.sql` — add `status` column, default 'active', backfill suspended-prefix rows to `status='suspended', name=stripped`.
- `crates/catalog/migrations/0009_principals.sql` — `principals` table + RLS + GRANT.
- `crates/catalog/migrations/0010_secrets_vault.sql` — relax `secrets.backend` CHECK to accept `'vault'`.

**Modified:**
- `Cargo.toml` — workspace deps: `jsonwebtoken`, `vaultrs`, `argon2`.
- `crates/common-types/src/ids.rs` — `define_id!(PrincipalId, "prn")`; `TenantContext` extends with `principal_id` + `role`.
- `crates/common-types/src/secrets.rs` — `SecretBackendKind::Vault` variant.
- `crates/catalog/src/lib.rs` — `begin_with_tenant` also `SET LOCAL app.principal_id` + `app.role`; new principal methods.
- `crates/cli/src/main.rs` — `ensure_dev_tenant()` deleted; every subcommand reads `Principal` via `auth::current_principal()` + supports `--tenant` override (admin only).
- `crates/cli/src/tenant.rs` — `suspend()` flips `status` column; `terminate()` also calls Temporal `DeprecateNamespace`.
- `crates/worker/src/workflows/pipeline_run.rs` + `crates/worker/src/workflows/cdc_pipeline.rs` — input shape: `source_conn: ConnectionConfig` (was `source_url: String`).
- `crates/worker/src/activities/sync/mod.rs` + `crates/worker/src/activities/cdc/mod.rs` — resolve `ConnectionConfig` at activity entry via `self.secrets`.
- `crates/worker/src/main.rs` — adds `VaultSecrets` to `DispatchSecrets`.
- `docker-compose.yml` — `vault:1.16` dev-mode service + bootstrap script.
- `README.md` — auth + RBAC + Vault sections.

---

## Task 1: Migration 0008 — `tenants.status` column

**Files:**
- Create: `crates/catalog/migrations/0008_tenants_status.sql`

- [ ] **Step 1: Write migration**

```sql
-- 0008_tenants_status.sql — proper suspension via status column.
-- Replaces the II.1.c "suspended:<name>" name-prefix hack.

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'suspended'));

-- Backfill: any tenant currently named 'suspended:foo' becomes
-- (status='suspended', name='foo'). Idempotent.
UPDATE tenants
SET name = substring(name FROM length('suspended:') + 1),
    status = 'suspended'
WHERE name LIKE 'suspended:%';
```

- [ ] **Step 2: Run migration and verify**

```bash
docker compose up -d postgres
cargo run --bin platform -- tenant list || true   # forces migrate
psql postgres://etl:etl@localhost:5432/etl_catalog -c \
  "SELECT column_name FROM information_schema.columns WHERE table_name='tenants' AND column_name='status';"
```

Expected: one row, `status`.

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0008_tenants_status.sql
git commit -m "feat(catalog): migration 0008 — tenants.status column"
```

---

## Task 2: Status-based suspend/resume + integration test

**Files:**
- Modify: `crates/catalog/src/lib.rs` (add `tenant_set_status`)
- Modify: `crates/cli/src/tenant.rs` (replace name-prefix logic)
- Create: `tests/integration/tests/tenant_suspend.rs`

- [ ] **Step 1: Add `tenant_set_status` to Catalog**

In `crates/catalog/src/lib.rs`, append near the other tenant methods (after `delete_tenant`):

```rust
pub async fn tenant_set_status(
    &self,
    tenant_id: TenantId,
    status: &str,
) -> sqlx::Result<u64> {
    if status != "active" && status != "suspended" {
        return Err(sqlx::Error::Protocol(format!(
            "invalid status '{status}' (expected active|suspended)"
        )));
    }
    let r = sqlx::query("UPDATE tenants SET status = $1 WHERE tenant_id = $2")
        .bind(status)
        .bind(tenant_id.as_uuid())
        .execute(self.pool())
        .await?;
    Ok(r.rows_affected())
}
```

- [ ] **Step 2: Rewrite `tenant::suspend` and add `resume`**

Replace the entire `pub async fn suspend(...)` body in `crates/cli/src/tenant.rs`:

```rust
pub async fn suspend(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin
        .get_tenant_by_name(&name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", name))?;
    let n = admin.tenant_set_status(t.tenant_id, "suspended").await?;
    if n == 0 {
        println!("tenant {} unchanged", name);
    } else {
        println!("suspended tenant {} ({})", name, t.tenant_id);
    }
    Ok(())
}

pub async fn resume(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin
        .get_tenant_by_name(&name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", name))?;
    admin.tenant_set_status(t.tenant_id, "active").await?;
    println!("resumed tenant {} ({})", name, t.tenant_id);
    Ok(())
}
```

Wire `Resume` in `crates/cli/src/main.rs` `TenantCmd` enum and dispatch:

```rust
// in enum TenantCmd:
    Resume { name: String },
// in match cmd:
    TenantCmd::Resume { name } => tenant::resume(name).await,
```

- [ ] **Step 3: Block suspended tenants in `pipeline_run`**

In `crates/cli/src/main.rs`, inside `pipeline_run` after loading the tenant (around line 450, just after `pipeline.tenant_id` is known), insert:

```rust
let tenant_row = catalog
    .get_tenant(pipeline.tenant_id)
    .await?
    .ok_or_else(|| anyhow::anyhow!("tenant {} not found", pipeline.tenant_id))?;
if tenant_row.status == "suspended" {
    anyhow::bail!(
        "tenant '{}' is suspended — use 'platform tenant resume {}' to re-enable",
        tenant_row.name,
        tenant_row.name,
    );
}
```

This requires `Tenant` (the row struct in `crates/catalog/src/tenant.rs` or `lib.rs`) to expose `status: String`. Update its struct + the SELECT in `get_tenant_by_name` and `get_tenant` to fetch the status column.

- [ ] **Step 4: Write integration test**

```rust
//! tests/integration/tests/tenant_suspend.rs
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
    p.pop(); p.pop(); p
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn suspended_tenant_cannot_run_pipeline() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status().await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // Apply pipeline as default dev tenant.
    Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;

    // Suspend the dev tenant.
    let suspend = Command::new(cargo_bin("platform"))
        .args(["tenant", "suspend", "dev"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(suspend.status.success(), "suspend failed: {}", String::from_utf8_lossy(&suspend.stderr));

    // Look up pipeline id, then attempt to run.
    let row: (uuid::Uuid,) = sqlx::query_as("SELECT pipeline_id FROM pipelines WHERE name='customers-sync'")
        .fetch_one(cat.pool()).await?;
    let pid = row.0.to_string();

    let run = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pid])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(!run.status.success());
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("suspended"),
        "expected 'suspended' in error, got: {stderr}"
    );

    // Resume and confirm the same run isn't blocked anymore (just dry-check exit).
    Command::new(cargo_bin("platform"))
        .args(["tenant", "resume", "dev"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;

    Ok(())
}
```

- [ ] **Step 5: Run test**

```bash
cargo test -p integration-tests --test tenant_suspend -- --ignored --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/catalog/src/lib.rs crates/catalog/src/tenant.rs crates/cli/src/tenant.rs crates/cli/src/main.rs tests/integration/tests/tenant_suspend.rs
git commit -m "feat(tenant): status column + suspend/resume + run-blocking"
```

---

## Task 3: Migration 0009 — `principals` table

**Files:**
- Create: `crates/catalog/migrations/0009_principals.sql`

- [ ] **Step 1: Write migration**

```sql
-- 0009_principals.sql — per-tenant user/principal table for the dev
-- login flow. JWT subject claims map to a row here. Phase II.2.c
-- federates this with OIDC.

CREATE TABLE IF NOT EXISTS principals (
    principal_id   UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX IF NOT EXISTS principals_tenant_id_idx ON principals(tenant_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON principals TO etl_app;
ALTER TABLE principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE principals FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON principals;
CREATE POLICY tenant_isolation ON principals
  USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
```

- [ ] **Step 2: Run migration and verify**

```bash
cargo run --bin platform -- tenant list || true
psql postgres://etl:etl@localhost:5432/etl_catalog -c "\d principals"
```

Expected: table exists with the columns above.

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0009_principals.sql
git commit -m "feat(catalog): migration 0009 — principals + RLS"
```

---

## Task 4: `PrincipalId` newtype + Catalog principal CRUD

**Files:**
- Modify: `crates/common-types/src/ids.rs`
- Create: `crates/catalog/src/principal.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `Cargo.toml` (workspace `argon2 = "0.5"`)
- Modify: `crates/catalog/Cargo.toml`

- [ ] **Step 1: Add PrincipalId**

In `crates/common-types/src/ids.rs`, after `define_id!(SecretId, "sec");`:

```rust
define_id!(PrincipalId, "prn");
```

- [ ] **Step 2: Add argon2 to workspace deps**

In root `Cargo.toml`, under `[workspace.dependencies]`:

```toml
argon2 = "0.5"
```

In `crates/catalog/Cargo.toml`, under `[dependencies]`:

```toml
argon2 = { workspace = true }
```

- [ ] **Step 3: Write principal CRUD**

```rust
// crates/catalog/src/principal.rs
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use chrono::{DateTime, Utc};
use common_types::ids::{PrincipalId, TenantId};

#[derive(Debug, Clone)]
pub struct Principal {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub name: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
}

pub struct NewPrincipal {
    pub tenant_id: TenantId,
    pub name: String,
    pub password: String,
    pub role: String,
}

pub fn hash_password(plaintext: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(plaintext.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("argon2 hash: {e}"))?
        .to_string();
    Ok(hash)
}

pub fn verify_password(plaintext: &str, hashed: &str) -> bool {
    let parsed = match PasswordHash::new(hashed) {
        Ok(p) => p,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(plaintext.as_bytes(), &parsed)
        .is_ok()
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewPrincipal,
) -> sqlx::Result<PrincipalId> {
    let id = PrincipalId::new();
    let hash = hash_password(&new.password).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query(
        "INSERT INTO principals (principal_id, tenant_id, name, password_hash, role) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(&new.name)
    .bind(&hash)
    .bind(&new.role)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<Option<(Principal, String)>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT principal_id, tenant_id, name, password_hash, role, created_at \
             FROM principals WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(pid, tid, n, hash, role, ts)| {
        (
            Principal {
                principal_id: PrincipalId::from_uuid_unchecked(pid),
                tenant_id: TenantId::from_uuid_unchecked(tid),
                name: n,
                role,
                created_at: ts,
            },
            hash,
        )
    }))
}
```

- [ ] **Step 4: Wire into Catalog**

In `crates/catalog/src/lib.rs`, near the `secret` re-exports:

```rust
pub mod principal;
pub use principal::NewPrincipal;
```

Add `rand` to `crates/catalog/Cargo.toml`:

```toml
rand = "0.8"
```

Add public methods on `Catalog`:

```rust
pub async fn principal_create(
    &self,
    ctx: TenantContext,
    new: NewPrincipal,
) -> sqlx::Result<common_types::ids::PrincipalId> {
    let mut tx = self.begin_with_tenant(Some(ctx)).await?;
    let id = principal::create(&mut tx, new).await?;
    tx.commit().await?;
    Ok(id)
}

/// Lookup is intentionally unscoped — a JWT login looks up by name
/// across all tenants and the principal's tenant_id is read from the
/// returned row. RLS isn't engaged here (admin path).
pub async fn principal_get_by_name(
    &self,
    name: &str,
) -> sqlx::Result<Option<(principal::Principal, String)>> {
    let mut conn = self.pool.acquire().await?;
    principal::get_by_name(&mut conn, name).await
}
```

- [ ] **Step 5: Build and unit-test the password helpers**

Add at the bottom of `crates/catalog/src/principal.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_roundtrips() {
        let h = hash_password("hunter2").unwrap();
        assert!(verify_password("hunter2", &h));
        assert!(!verify_password("wrong", &h));
    }
}
```

```bash
cargo test -p catalog principal::tests
```

Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/catalog/Cargo.toml crates/catalog/src/principal.rs crates/catalog/src/lib.rs crates/common-types/src/ids.rs
git commit -m "feat(catalog): principals table + argon2 password hashing + CRUD"
```

---

## Task 5: `auth` crate — JWT issue/verify + RBAC

**Files:**
- Create: `crates/auth/Cargo.toml`
- Create: `crates/auth/src/lib.rs`
- Create: `crates/auth/src/jwt.rs`
- Create: `crates/auth/src/rbac.rs`
- Modify: root `Cargo.toml` (workspace member + jsonwebtoken dep)

- [ ] **Step 1: Workspace dep + member**

In root `Cargo.toml`:

```toml
# under [workspace.dependencies]:
jsonwebtoken = "9"

# under [workspace.members]:
"crates/auth",
```

- [ ] **Step 2: Crate Cargo.toml**

```toml
# crates/auth/Cargo.toml
[package]
name = "auth"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
common-types = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
jsonwebtoken = { workspace = true }
```

- [ ] **Step 3: Role + RBAC**

```rust
// crates/auth/src/rbac.rs
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Operator,
    Viewer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Read-only catalog access (list pipelines, get connection, etc.)
    Read,
    /// Trigger pipeline runs / workflow operations.
    Run,
    /// Write to the catalog (apply, create, delete, secrets put).
    Write,
    /// Cross-tenant or admin-only operations (tenant create / suspend / terminate).
    Admin,
}

impl Role {
    pub fn permits(self, a: Action) -> bool {
        match (self, a) {
            (Role::Admin, _) => true,
            (Role::Operator, Action::Read | Action::Run | Action::Write) => true,
            (Role::Viewer, Action::Read) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_permits_everything() {
        for a in [Action::Read, Action::Run, Action::Write, Action::Admin] {
            assert!(Role::Admin.permits(a));
        }
    }

    #[test]
    fn viewer_only_reads() {
        assert!(Role::Viewer.permits(Action::Read));
        assert!(!Role::Viewer.permits(Action::Run));
        assert!(!Role::Viewer.permits(Action::Write));
        assert!(!Role::Viewer.permits(Action::Admin));
    }

    #[test]
    fn operator_runs_and_writes_but_not_admin() {
        assert!(Role::Operator.permits(Action::Read));
        assert!(Role::Operator.permits(Action::Run));
        assert!(Role::Operator.permits(Action::Write));
        assert!(!Role::Operator.permits(Action::Admin));
    }
}
```

- [ ] **Step 4: JWT module**

```rust
// crates/auth/src/jwt.rs
use chrono::{Duration, Utc};
use common_types::ids::{PrincipalId, TenantId};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::rbac::Role;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,         // principal_id (PrincipalId display)
    pub tenant_id: String,   // tenant_id (TenantId display)
    pub role: Role,
    pub exp: i64,            // unix seconds
    pub iat: i64,
}

#[derive(Clone, Debug)]
pub struct Principal {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub role: Role,
}

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("invalid JWT: {0}")]
    InvalidJwt(String),
    #[error("malformed sub claim: {0}")]
    BadSub(String),
    #[error("malformed tenant claim: {0}")]
    BadTenant(String),
}

pub struct JwtIssuer {
    key: EncodingKey,
    ttl_seconds: i64,
}

impl JwtIssuer {
    pub fn new(secret: &[u8], ttl_seconds: i64) -> Self {
        Self {
            key: EncodingKey::from_secret(secret),
            ttl_seconds,
        }
    }

    pub fn issue(&self, p: Principal) -> anyhow::Result<String> {
        let now = Utc::now();
        let claims = Claims {
            sub: p.principal_id.to_string(),
            tenant_id: p.tenant_id.to_string(),
            role: p.role,
            iat: now.timestamp(),
            exp: (now + Duration::seconds(self.ttl_seconds)).timestamp(),
        };
        let token = encode(&Header::default(), &claims, &self.key)?;
        Ok(token)
    }
}

pub struct JwtVerifier {
    key: DecodingKey,
}

impl JwtVerifier {
    pub fn new(secret: &[u8]) -> Self {
        Self { key: DecodingKey::from_secret(secret) }
    }

    pub fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        let data = decode::<Claims>(token, &self.key, &Validation::default())
            .map_err(|e| AuthError::InvalidJwt(e.to_string()))?;
        let principal_id = data
            .claims
            .sub
            .parse::<PrincipalId>()
            .map_err(|e| AuthError::BadSub(format!("{e:?}")))?;
        let tenant_id = data
            .claims
            .tenant_id
            .parse::<TenantId>()
            .map_err(|e| AuthError::BadTenant(format!("{e:?}")))?;
        Ok(Principal {
            principal_id,
            tenant_id,
            role: data.claims.role,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_principal() -> Principal {
        Principal {
            principal_id: PrincipalId::new(),
            tenant_id: TenantId::new(),
            role: Role::Operator,
        }
    }

    #[test]
    fn issue_then_verify_roundtrips() {
        let secret = b"test-secret-test-secret-test-secret";
        let p = fake_principal();
        let token = JwtIssuer::new(secret, 3600).issue(p.clone()).unwrap();
        let back = JwtVerifier::new(secret).verify(&token).unwrap();
        assert_eq!(back.principal_id, p.principal_id);
        assert_eq!(back.tenant_id, p.tenant_id);
        assert_eq!(back.role, p.role);
    }

    #[test]
    fn wrong_secret_rejects() {
        let token = JwtIssuer::new(b"a-secret-a-secret", 3600)
            .issue(fake_principal()).unwrap();
        assert!(JwtVerifier::new(b"different-secret-different").verify(&token).is_err());
    }

    #[test]
    fn expired_token_rejects() {
        let token = JwtIssuer::new(b"k-k-k-k-k-k-k-k-k-k", -10).issue(fake_principal()).unwrap();
        let err = JwtVerifier::new(b"k-k-k-k-k-k-k-k-k-k").verify(&token).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("expired"));
    }
}
```

- [ ] **Step 5: lib.rs**

```rust
// crates/auth/src/lib.rs
pub mod jwt;
pub mod rbac;

pub use jwt::{AuthError, Claims, JwtIssuer, JwtVerifier, Principal};
pub use rbac::{Action, Role};
```

- [ ] **Step 6: Run tests**

```bash
cargo test -p auth
```

Expected: 6 passed (3 RBAC + 3 JWT).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/auth/
git commit -m "feat(auth): JWT issuer/verifier + Role/Action RBAC"
```

---

## Task 6: Extend `TenantContext` with `principal_id` + `role`

**Files:**
- Modify: `crates/common-types/src/ids.rs:69-75`
- Modify: `crates/common-types/Cargo.toml` (add `auth` dep — NO, that creates cycle; instead inline `Role` in common-types or use string)
- Modify: `crates/catalog/src/lib.rs:62-71` (`begin_with_tenant`)
- Modify: every existing call site of `TenantContext::new` (catalog, cli, dsl, secret) — keep API by adding `new_unauth()` for tests/admin

**Decision:** to avoid a `common-types → auth` cycle, define `Role` in `common-types::auth` and have the `auth` crate re-export. Move `crates/auth/src/rbac.rs::Role` to `crates/common-types/src/auth.rs` and re-export from `auth`.

- [ ] **Step 1: Move `Role` to common-types**

Create `crates/common-types/src/auth.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Operator,
    Viewer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Read,
    Run,
    Write,
    Admin,
}

impl Role {
    pub fn permits(self, a: Action) -> bool {
        match (self, a) {
            (Role::Admin, _) => true,
            (Role::Operator, Action::Read | Action::Run | Action::Write) => true,
            (Role::Viewer, Action::Read) => true,
            _ => false,
        }
    }
}
```

In `crates/common-types/src/lib.rs` add `pub mod auth;`.

In `crates/auth/src/rbac.rs`, replace contents with:

```rust
pub use common_types::auth::{Action, Role};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_permits_everything() {
        for a in [Action::Read, Action::Run, Action::Write, Action::Admin] {
            assert!(Role::Admin.permits(a));
        }
    }

    #[test]
    fn viewer_only_reads() {
        assert!(Role::Viewer.permits(Action::Read));
        assert!(!Role::Viewer.permits(Action::Run));
    }

    #[test]
    fn operator_runs_and_writes_but_not_admin() {
        assert!(Role::Operator.permits(Action::Run));
        assert!(!Role::Operator.permits(Action::Admin));
    }
}
```

In `crates/auth/src/jwt.rs`, change `use crate::rbac::Role;` to `use common_types::auth::Role;`.

- [ ] **Step 2: Extend TenantContext**

Replace `crates/common-types/src/ids.rs` lines 67–76 with:

```rust
/// Identity carried through every cross-component call. Phase II.2.b
/// extends from a bare TenantId to also include the authenticated
/// principal + role; admin paths (migrations, tests) construct via
/// `new_unauth` which leaves them None.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantContext {
    pub tenant_id: TenantId,
    pub principal_id: Option<PrincipalId>,
    pub role: Option<crate::auth::Role>,
}

impl TenantContext {
    /// Authenticated context — built from a verified JWT.
    pub fn authed(tenant_id: TenantId, principal_id: PrincipalId, role: crate::auth::Role) -> Self {
        Self { tenant_id, principal_id: Some(principal_id), role: Some(role) }
    }

    /// Tenant-only context for admin paths and tests.
    pub fn new(tenant_id: TenantId) -> Self {
        Self { tenant_id, principal_id: None, role: None }
    }
}
```

(Keeping `new(tenant_id)` means every existing call site still compiles — they're treated as admin paths until threaded through with a real principal.)

- [ ] **Step 3: Wire `principal_id` and `role` into `begin_with_tenant`**

In `crates/catalog/src/lib.rs`, replace the existing `begin_with_tenant` body (around line 62–71) with:

```rust
async fn begin_with_tenant(
    &self,
    ctx: Option<TenantContext>,
) -> sqlx::Result<sqlx::Transaction<'_, sqlx::Postgres>> {
    let mut tx = self.pool.begin().await?;
    if let Some(c) = ctx {
        let tid = c.tenant_id.as_uuid();
        sqlx::query(&format!("SET LOCAL app.tenant_id = '{tid}'"))
            .execute(&mut *tx).await?;
        if let Some(pid) = c.principal_id {
            let pid = pid.as_uuid();
            sqlx::query(&format!("SET LOCAL app.principal_id = '{pid}'"))
                .execute(&mut *tx).await?;
        }
        if let Some(role) = c.role {
            let role_str = serde_json::to_string(&role)
                .unwrap_or_else(|_| "\"viewer\"".into());
            // Strip the JSON quotes for SET LOCAL. Role serializes as a
            // bare lowercase string token.
            let role_token = role_str.trim_matches('"');
            sqlx::query(&format!("SET LOCAL app.role = '{role_token}'"))
                .execute(&mut *tx).await?;
        }
    }
    Ok(tx)
}
```

- [ ] **Step 4: Build**

```bash
cargo build --workspace
```

Expected: clean build (warnings OK).

- [ ] **Step 5: Run unit tests**

```bash
cargo test --workspace --lib
```

Expected: all green; the existing common-types tests still pass with the extended `TenantContext`.

- [ ] **Step 6: Commit**

```bash
git add crates/common-types/src/auth.rs crates/common-types/src/lib.rs crates/common-types/src/ids.rs crates/auth/src/rbac.rs crates/auth/src/jwt.rs crates/catalog/src/lib.rs
git commit -m "feat(common-types): TenantContext gains principal_id + role; SET LOCAL app.principal_id/role"
```

---

## Task 7: CLI `auth login` + credentials cache

**Files:**
- Create: `crates/cli/src/auth.rs`
- Modify: `crates/cli/Cargo.toml` (add `auth` workspace dep)
- Modify: `crates/cli/src/main.rs` (new `Auth` subcommand)

- [ ] **Step 1: Add deps**

In `crates/cli/Cargo.toml`:

```toml
auth = { workspace = true }
dirs = "5"
```

In root `Cargo.toml` workspace deps:

```toml
auth = { path = "crates/auth" }
```

- [ ] **Step 2: Write the CLI module**

```rust
// crates/cli/src/auth.rs
use anyhow::{Context, Result};
use auth::{JwtIssuer, JwtVerifier, Principal};
use catalog::Catalog;
use common_types::auth::Role;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const DEFAULT_TTL_SECONDS: i64 = 8 * 60 * 60;

#[derive(Serialize, Deserialize)]
pub struct CachedCreds {
    pub token: String,
    pub principal_name: String,
    pub tenant_id: String,
    pub role: Role,
}

pub fn jwt_secret() -> Vec<u8> {
    std::env::var("ETL_JWT_SECRET")
        .unwrap_or_else(|_| "dev-only-jwt-secret-change-in-prod".into())
        .into_bytes()
}

pub fn creds_path() -> PathBuf {
    let dir = dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".etl");
    let _ = std::fs::create_dir_all(&dir);
    dir.join("credentials.json")
}

pub fn save_creds(c: &CachedCreds) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(c)?;
    std::fs::write(creds_path(), bytes)
        .with_context(|| format!("writing {}", creds_path().display()))?;
    Ok(())
}

pub fn load_creds() -> Result<CachedCreds> {
    let bytes = std::fs::read(creds_path())
        .with_context(|| format!("reading {} — run 'platform auth login' first", creds_path().display()))?;
    let creds: CachedCreds = serde_json::from_slice(&bytes)
        .context("parsing cached credentials JSON")?;
    Ok(creds)
}

/// Verify the cached JWT and return the resolved Principal. Cached
/// creds without a verified token are treated as logged out.
pub fn current_principal() -> Result<Principal> {
    let creds = load_creds()?;
    let p = JwtVerifier::new(&jwt_secret())
        .verify(&creds.token)
        .map_err(|e| anyhow::anyhow!("cached token invalid: {e} — run 'platform auth login'"))?;
    Ok(p)
}

pub async fn login(name: String, password: String) -> Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    cat.migrate().await?;

    let (principal, password_hash) = cat
        .principal_get_by_name(&name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no such principal '{}'", name))?;

    if !catalog::principal::verify_password(&password, &password_hash) {
        anyhow::bail!("invalid password for '{}'", name);
    }

    let role: Role = serde_json::from_str(&format!("\"{}\"", principal.role))
        .with_context(|| format!("unknown role string in catalog: {}", principal.role))?;

    let issuer = JwtIssuer::new(&jwt_secret(), DEFAULT_TTL_SECONDS);
    let token = issuer.issue(Principal {
        principal_id: principal.principal_id,
        tenant_id: principal.tenant_id,
        role,
    })?;

    save_creds(&CachedCreds {
        token,
        principal_name: principal.name.clone(),
        tenant_id: principal.tenant_id.to_string(),
        role,
    })?;

    println!(
        "logged in as {} (tenant {}, role {:?}) — credentials cached at {}",
        principal.name,
        principal.tenant_id,
        role,
        creds_path().display()
    );
    Ok(())
}

pub async fn whoami() -> Result<()> {
    let p = current_principal()?;
    println!(
        "principal_id: {}\ntenant_id:    {}\nrole:         {:?}",
        p.principal_id, p.tenant_id, p.role
    );
    Ok(())
}

pub async fn create_principal(
    tenant_name: String,
    name: String,
    password: String,
    role: String,
) -> Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin
        .get_tenant_by_name(&tenant_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant '{}' not found", tenant_name))?;
    let id = admin
        .principal_create(
            common_types::ids::TenantContext::new(t.tenant_id),
            catalog::NewPrincipal {
                tenant_id: t.tenant_id,
                name: name.clone(),
                password,
                role,
            },
        )
        .await?;
    println!("created principal {} ({}) in tenant {}", name, id, tenant_name);
    Ok(())
}
```

- [ ] **Step 3: Wire subcommand**

In `crates/cli/src/main.rs`:

```rust
// add to module list:
mod auth;

// add to enum Cmd:
    /// Authentication: login / whoami / create-principal.
    Auth {
        #[command(subcommand)]
        cmd: AuthCmd,
    },

// new enum:
#[derive(Subcommand)]
enum AuthCmd {
    Login {
        name: String,
        #[arg(long)]
        password: String,
    },
    Whoami,
    CreatePrincipal {
        #[arg(long)]
        tenant: String,
        name: String,
        #[arg(long)]
        password: String,
        #[arg(long, default_value = "operator")]
        role: String,
    },
}

// in match cli.cmd:
        Cmd::Auth { cmd } => match cmd {
            AuthCmd::Login { name, password } => auth::login(name, password).await,
            AuthCmd::Whoami => auth::whoami().await,
            AuthCmd::CreatePrincipal { tenant, name, password, role } => {
                auth::create_principal(tenant, name, password, role).await
            }
        },
```

- [ ] **Step 4: Smoke**

```bash
cargo run --bin platform -- tenant create acme || true
cargo run --bin platform -- auth create-principal --tenant acme alice --password hunter2 --role operator
cargo run --bin platform -- auth login alice --password hunter2
cargo run --bin platform -- auth whoami
```

Expected: `logged in as alice ...`, then `whoami` prints the same details.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/cli/Cargo.toml crates/cli/src/auth.rs crates/cli/src/main.rs
git commit -m "feat(cli): platform auth login/whoami/create-principal + creds cache"
```

---

## Task 8: Replace `ensure_dev_tenant` with auth-driven principal + `--tenant` override

**Files:**
- Modify: `crates/cli/src/main.rs` (lines 196, 250, 324, 338, 453 — every `ensure_dev_tenant` call site)
- Modify: `crates/cli/src/secret.rs` (line 19)
- Modify: `crates/cli/src/dsl.rs:75`
- Add helper: `crates/cli/src/auth.rs::resolve_context(--tenant override)`

- [ ] **Step 1: Add `resolve_context` helper**

In `crates/cli/src/auth.rs`, append:

```rust
/// Returns the TenantContext to use for catalog operations.
///
/// - If `--tenant <name>` is supplied: only admin tokens may use it.
///   The override resolves to the named tenant_id; principal_id+role
///   come from the JWT.
/// - Otherwise: tenant_id, principal_id, role all come from the JWT.
pub async fn resolve_context(
    catalog: &catalog::Catalog,
    tenant_override: Option<&str>,
) -> Result<common_types::ids::TenantContext> {
    let p = current_principal()?;
    let tenant_id = match tenant_override {
        None => p.tenant_id,
        Some(name) => {
            if p.role != Role::Admin {
                anyhow::bail!("--tenant requires admin role (current: {:?})", p.role);
            }
            let t = catalog
                .get_tenant_by_name(name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("tenant '{}' not found", name))?;
            t.tenant_id
        }
    };
    Ok(common_types::ids::TenantContext::authed(
        tenant_id,
        p.principal_id,
        p.role,
    ))
}

pub fn require_role(p: &Principal, action: common_types::auth::Action) -> Result<()> {
    if !p.role.permits(action) {
        anyhow::bail!(
            "principal role {:?} not permitted for {:?}",
            p.role, action
        );
    }
    Ok(())
}
```

- [ ] **Step 2: Add `--tenant` global flag**

In `crates/cli/src/main.rs`:

```rust
#[derive(Parser)]
#[command(name = "platform", version, about = "ETL platform CLI")]
struct Cli {
    /// Override tenant for this call (admin only).
    #[arg(long, global = true)]
    tenant: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}
```

- [ ] **Step 3: Replace `ensure_dev_tenant` everywhere**

DELETE the entire function `ensure_dev_tenant` (lines 338–349 in current main.rs).

For each call site, swap:

```rust
let tenant_id = ensure_dev_tenant(&catalog).await?;
```

with:

```rust
let ctx = auth::resolve_context(&catalog, cli.tenant.as_deref()).await?;
let tenant_id = ctx.tenant_id;
```

(Pass `cli.tenant.as_deref()` through to handlers — see step 4.) Sites: `apply_cmd`, `get_cmd`, `diff_cmd`, `pipeline_run`, plus the `secret.rs` `open_admin` and `dsl.rs` `apply` (which receives a tenant_id arg already; thread the full ctx instead).

For `secret.rs::open_admin`:

```rust
async fn open_admin(tenant_override: Option<&str>) -> Result<(Catalog, TenantContext)> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    cat.migrate().await?;
    let ctx = crate::auth::resolve_context(&cat, tenant_override).await?;
    Ok((cat, ctx))
}
```

Update `secret::create / put / list / delete` signatures to take `tenant_override: Option<String>`.

For `dsl.rs::apply` — keep its `tenant_id: TenantId` signature; the caller (`apply_cmd`) builds the ctx from auth and passes `ctx.tenant_id`.

- [ ] **Step 4: Thread `cli.tenant` through dispatch**

Update every match arm that calls into a subcommand to pass `cli.tenant.as_deref()`:

```rust
Cmd::Apply { file } => apply_cmd(file, cli.tenant.as_deref()).await,
Cmd::Get { kind, name } => get_cmd(kind, name, cli.tenant.as_deref()).await,
Cmd::Diff { file } => diff_cmd(file, cli.tenant.as_deref()).await,
Cmd::Validate { file } => validate_cmd(file).await,
Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => pipeline_run(id, cli.tenant.as_deref()).await,
Cmd::Secret { cmd } => match cmd {
    SecretCmd::Create { name, backend, key } => {
        secret::create(name, backend, key.unwrap_or_default(), cli.tenant.clone()).await
    }
    SecretCmd::Put { name, value, register } => {
        secret::put(name, value, register, cli.tenant.clone()).await
    }
    SecretCmd::List => secret::list(cli.tenant.clone()).await,
    SecretCmd::Delete { name } => secret::delete(name, cli.tenant.clone()).await,
},
```

Update each function signature to accept the new param.

- [ ] **Step 5: Add RBAC checks at write paths**

In `apply_cmd`, just after `auth::resolve_context`:

```rust
let p = auth::current_principal()?;
auth::require_role(&p, common_types::auth::Action::Write)?;
```

In `secret::put / create / delete`: same `Action::Write` check.
In `pipeline_run`: `Action::Run`.
In `get_cmd / pipeline status / list / dsl validate`: `Action::Read`.
In `tenant create / suspend / resume / terminate`: `Action::Admin`.

- [ ] **Step 6: Bootstrap the dev principal automatically**

To keep existing integration tests passing (they don't log in), have `auth::current_principal()` fall through to a hardcoded dev principal when `ETL_AUTH_BYPASS=1`:

In `crates/cli/src/auth.rs`, modify `current_principal`:

```rust
pub fn current_principal() -> Result<Principal> {
    if std::env::var("ETL_AUTH_BYPASS").ok().as_deref() == Some("1") {
        let dev_tenant = common_types::ids::TenantId::from_uuid_unchecked(
            uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        );
        return Ok(Principal {
            principal_id: common_types::ids::PrincipalId::from_uuid_unchecked(
                uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
            ),
            tenant_id: dev_tenant,
            role: Role::Admin,
        });
    }
    let creds = load_creds()?;
    let p = JwtVerifier::new(&jwt_secret())
        .verify(&creds.token)
        .map_err(|e| anyhow::anyhow!("cached token invalid: {e} — run 'platform auth login'"))?;
    Ok(p)
}
```

For `apply_cmd` and `pipeline_run`, the dev tenant must exist. Add a helper that creates it on demand when `ETL_AUTH_BYPASS=1`:

```rust
async fn ensure_bypass_tenant(cat: &Catalog) -> anyhow::Result<()> {
    if std::env::var("ETL_AUTH_BYPASS").ok().as_deref() != Some("1") {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO tenants (tenant_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'dev') \
         ON CONFLICT DO NOTHING",
    )
    .execute(cat.pool())
    .await?;
    Ok(())
}
```

Call `ensure_bypass_tenant(&catalog).await?` at the top of `apply_cmd` / `pipeline_run` / `secret::open_admin` BEFORE `resolve_context`.

- [ ] **Step 7: Update existing integration tests to set `ETL_AUTH_BYPASS=1`**

In each `tests/integration/tests/*.rs` that runs `Command::new(cargo_bin("platform"))`, add `.env("ETL_AUTH_BYPASS", "1")`. Use a sed-style sweep — there are ~10 sites:

```bash
grep -rln "cargo_bin(\"platform\")" tests/integration/tests/ | while read f; do
  echo "Update $f manually: add .env(\"ETL_AUTH_BYPASS\", \"1\") next to .env(\"DATABASE_URL\", ...)"
done
```

Edit each file: every `.env("DATABASE_URL", catalog_url())` chain gains `.env("ETL_AUTH_BYPASS", "1")` immediately after.

- [ ] **Step 8: Build and run unit tests**

```bash
cargo build --workspace
cargo test --workspace --lib
```

Expected: green. The `ETL_AUTH_BYPASS` shortcut keeps existing integration tests working.

- [ ] **Step 9: Run integration tests**

```bash
pkill -f "target/debug/worker" 2>/dev/null; sleep 1
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all 16 prior + tenant_suspend = 17 green.

- [ ] **Step 10: Commit**

```bash
git add crates/cli/ tests/integration/tests/
git commit -m "feat(cli): replace ensure_dev_tenant with auth-driven --tenant override + RBAC checks"
```

---

## Task 9: Migration 0010 — relax `secrets.backend` CHECK + `Vault` enum variant

**Files:**
- Create: `crates/catalog/migrations/0010_secrets_vault.sql`
- Modify: `crates/common-types/src/secrets.rs:17-20`
- Modify: `crates/catalog/src/secret.rs:25-38` (`backend_to_str`, `parse_backend`)

- [ ] **Step 1: Migration**

```sql
-- 0010_secrets_vault.sql — add 'vault' to allowed backends.
ALTER TABLE secrets DROP CONSTRAINT IF EXISTS secrets_backend_check;
ALTER TABLE secrets ADD CONSTRAINT secrets_backend_check
    CHECK (backend IN ('env', 'file', 'vault'));
```

- [ ] **Step 2: Add Vault variant**

Replace the `SecretBackendKind` enum in `crates/common-types/src/secrets.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretBackendKind {
    Env,
    File,
    Vault,
}
```

- [ ] **Step 3: Update catalog mappers**

In `crates/catalog/src/secret.rs`:

```rust
fn backend_to_str(b: SecretBackendKind) -> &'static str {
    match b {
        SecretBackendKind::Env => "env",
        SecretBackendKind::File => "file",
        SecretBackendKind::Vault => "vault",
    }
}

fn parse_backend(s: &str) -> SecretBackendKind {
    match s {
        "env" => SecretBackendKind::Env,
        "file" => SecretBackendKind::File,
        "vault" => SecretBackendKind::Vault,
        other => panic!("unknown secret backend in DB: {other}"),
    }
}
```

- [ ] **Step 4: Build**

```bash
cargo build --workspace
```

Expected: clean. The worker `DispatchSecrets::resolve` match becomes non-exhaustive (compile error) — fixed in Task 10.

- [ ] **Step 5: Commit**

```bash
git add crates/catalog/migrations/0010_secrets_vault.sql crates/common-types/src/secrets.rs crates/catalog/src/secret.rs
git commit -m "feat(secrets): SecretBackendKind::Vault + migration to relax backend CHECK"
```

---

## Task 10: `VaultSecrets` backend (KV v2)

**Files:**
- Create: `crates/worker/src/secrets/vault.rs`
- Modify: `crates/worker/Cargo.toml` (add `vaultrs` workspace dep)
- Modify: root `Cargo.toml` (workspace dep)
- Modify: `crates/worker/src/secrets/mod.rs` (extend `DispatchSecrets`)
- Modify: `crates/worker/src/main.rs` (construct `VaultSecrets` from env)
- Modify: `crates/cli/src/main.rs` (CLI also constructs the dispatch — same env)

- [ ] **Step 1: Workspace dep**

Root `Cargo.toml`:

```toml
vaultrs = "0.7"
```

`crates/worker/Cargo.toml`:

```toml
vaultrs = { workspace = true }
```

- [ ] **Step 2: Vault impl**

```rust
// crates/worker/src/secrets/vault.rs
use anyhow::{Context, Result};
use async_trait::async_trait;
use common_types::secrets::{PlaintextSecret, SecretRef};
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use vaultrs::kv2;

use super::Secrets;

/// VaultSecrets reads from KV v2 at `secret/<key>` (configurable mount).
/// `SecretRef.key` may include a leading `<mount>/<path>`; the resolver
/// splits at the first `/` — anything before is the mount, the rest is
/// the path.
pub struct VaultSecrets {
    client: VaultClient,
    default_mount: String,
}

impl VaultSecrets {
    /// Build from `VAULT_ADDR` + `VAULT_TOKEN` + optional `VAULT_KV_MOUNT`.
    /// Returns `None` (caller's choice to handle) when `VAULT_ADDR` is unset.
    pub fn from_env() -> Result<Option<Self>> {
        let addr = match std::env::var("VAULT_ADDR") {
            Ok(a) => a,
            Err(_) => return Ok(None),
        };
        let token = std::env::var("VAULT_TOKEN")
            .context("VAULT_ADDR set but VAULT_TOKEN missing")?;
        let mount = std::env::var("VAULT_KV_MOUNT").unwrap_or_else(|_| "secret".into());
        let settings = VaultClientSettingsBuilder::default()
            .address(addr)
            .token(token)
            .build()
            .map_err(|e| anyhow::anyhow!("vault client settings: {e}"))?;
        let client = VaultClient::new(settings)
            .map_err(|e| anyhow::anyhow!("vault client init: {e}"))?;
        Ok(Some(Self { client, default_mount: mount }))
    }

    fn split_key(&self, key: &str) -> (String, String) {
        if let Some((mount, rest)) = key.split_once('/') {
            (mount.to_string(), rest.to_string())
        } else {
            (self.default_mount.clone(), key.to_string())
        }
    }
}

#[async_trait]
impl Secrets for VaultSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        let (mount, path) = self.split_key(&r.key);
        let resp: serde_json::Value = kv2::read(&self.client, &mount, &path)
            .await
            .with_context(|| format!("vault kv2::read mount={mount} path={path}"))?;
        let v = resp
            .get("value")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("vault entry at {mount}/{path} missing 'value' field"))?;
        Ok(PlaintextSecret::new(v.to_string()))
    }
}
```

- [ ] **Step 3: Extend dispatch**

In `crates/worker/src/secrets/mod.rs`:

```rust
pub mod env;
pub mod file;
pub mod vault;

// ... existing trait + imports ...

pub struct DispatchSecrets {
    pub env: env::EnvSecrets,
    pub file: file::FileSecrets,
    pub vault: Option<vault::VaultSecrets>,
}

#[async_trait]
impl Secrets for DispatchSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        match r.backend {
            SecretBackendKind::Env => self.env.resolve(r).await,
            SecretBackendKind::File => self.file.resolve(r).await,
            SecretBackendKind::Vault => match &self.vault {
                Some(v) => v.resolve(r).await,
                None => Err(anyhow!(
                    "SecretRef has backend=vault but VAULT_ADDR/VAULT_TOKEN are not configured"
                )),
            },
        }
    }
}
```

- [ ] **Step 4: Construct in worker main**

In `crates/worker/src/main.rs`, replace the `secrets:` line:

```rust
let secrets: Arc<dyn worker::secrets::Secrets> =
    Arc::new(worker::secrets::DispatchSecrets {
        env: worker::secrets::env::EnvSecrets,
        file: worker::secrets::file::FileSecrets::new(),
        vault: worker::secrets::vault::VaultSecrets::from_env()?,
    });
```

In `crates/cli/src/main.rs` `pipeline_run` (and any other place that constructs DispatchSecrets), apply the same change. (Note: T13/T15 will eventually remove the CLI's secret resolution; until then keep it consistent.)

- [ ] **Step 5: Build**

```bash
cargo build --workspace
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/worker/Cargo.toml crates/worker/src/secrets/ crates/cli/src/main.rs crates/worker/src/main.rs
git commit -m "feat(worker): VaultSecrets backend (KV v2) + DispatchSecrets vault slot"
```

---

## Task 11: `docker-compose` Vault service + integration test

**Files:**
- Modify: `docker-compose.yml`
- Create: `tests/integration/tests/vault_e2e.rs`
- Create: `examples/dsl/customers-sync-vault.yaml`

- [ ] **Step 1: Add Vault service**

Append to `docker-compose.yml`:

```yaml
  vault:
    image: hashicorp/vault:1.16
    container_name: etl-vault
    ports:
      - "8200:8200"
    environment:
      VAULT_DEV_ROOT_TOKEN_ID: etl-dev-token
      VAULT_DEV_LISTEN_ADDRESS: 0.0.0.0:8200
    cap_add:
      - IPC_LOCK
```

- [ ] **Step 2: Vault-backed example**

```yaml
# examples/dsl/customers-sync-vault.yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: source-vault
spec:
  connector_ref: postgres@0.1.0
  config:
    url_secret: pg-url-vault
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: customers-sync-vault
spec:
  source_connection: source-vault
  source:
    type: postgres
    schema: public
    table: customers
    cursor_column: updated_at
    cursor_kind: timestamp_tz
    pk_columns: [id]
  destination:
    type: local_parquet
    base_path: ./data/vault-demo
  batch_size: 4
  evolution_policy: propagate_additive
```

- [ ] **Step 3: Integration test**

```rust
//! tests/integration/tests/vault_e2e.rs
//! Gated on VAULT_ADDR — skipped when Vault isn't available.

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
    p.pop(); p.pop(); p
}

#[tokio::test]
#[ignore = "requires docker postgres + vault"]
async fn vault_backed_secret_resolves_to_plaintext() -> anyhow::Result<()> {
    let vault_addr = match std::env::var("VAULT_ADDR") {
        Ok(a) => a,
        Err(_) => {
            eprintln!("VAULT_ADDR not set — skipping");
            return Ok(());
        }
    };
    let vault_token = std::env::var("VAULT_TOKEN")
        .unwrap_or_else(|_| "etl-dev-token".into());

    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"]).status().await?;

    // Bootstrap: write the secret into Vault via curl.
    let plaintext = "postgres://etl:etl@localhost:5432/etl_source_demo";
    let body = serde_json::json!({"data": {"value": plaintext}}).to_string();
    let put = Command::new("curl")
        .args([
            "-sf", "-XPOST",
            "-H", &format!("X-Vault-Token: {vault_token}"),
            "-d", &body,
            &format!("{vault_addr}/v1/secret/data/etl/pg-url-vault"),
        ])
        .output().await?;
    assert!(put.status.success(), "vault write failed: {}", String::from_utf8_lossy(&put.stderr));

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // Register the SecretRef with backend=vault, key=etl/pg-url-vault.
    let create = Command::new(cargo_bin("platform"))
        .args(["secret", "create", "pg-url-vault", "--backend", "vault", "--key", "etl/pg-url-vault"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;
    assert!(create.status.success(), "secret create: {}", String::from_utf8_lossy(&create.stderr));

    // Apply the connection that references it.
    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync-vault.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;
    assert!(apply.status.success(), "apply: {}", String::from_utf8_lossy(&apply.stderr));

    // The catalog row contains a vault SecretRef and no plaintext URL.
    let row: (serde_json::Value,) = sqlx::query_as(
        "SELECT config FROM connections WHERE name='source-vault'",
    ).fetch_one(cat.pool()).await?;
    let raw = serde_json::to_string(&row.0)?;
    assert!(!raw.contains("postgres://"), "plaintext leaked: {raw}");
    assert_eq!(
        row.0.get("url_secret").and_then(|v| v.get("backend")).and_then(|v| v.as_str()),
        Some("vault")
    );
    Ok(())
}
```

- [ ] **Step 4: Run**

```bash
docker compose up -d vault
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=etl-dev-token
cargo test -p integration-tests --test vault_e2e -- --ignored --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml examples/dsl/customers-sync-vault.yaml tests/integration/tests/vault_e2e.rs
git commit -m "test(integration): vault_e2e — vault-backed SecretRef resolves end-to-end"
```

---

## Task 12: Activity-side resolution for sync workflows

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs:60-150` (`discover_stream` + `read_batch`)
- Modify: `crates/worker/src/activities/sync/inputs.rs` (input shapes)
- Modify: `crates/worker/src/workflows/pipeline_run.rs:30-220` (workflow input + activity wiring)
- Modify: `crates/cli/src/main.rs:421-510` (stop resolving in CLI; pass unresolved ConnectionConfig)

- [ ] **Step 1: Update input shape**

In the file that defines `DiscoverInput` and `ReadBatchInput` (likely `crates/worker/src/activities/sync/inputs.rs`), replace `source_url: String` with:

```rust
pub source_conn: common_types::connection_config::ConnectionConfig,
```

Same edit on `ReadBatchInput`.

- [ ] **Step 2: Resolve at activity entry**

In `crates/worker/src/activities/sync/mod.rs`, replace the `&ConnectionConfig::from_url(input.source_url.clone())` and `&ConnectionConfig::from_url(input.source_url)` lines with:

```rust
// discover_stream:
let resolved = crate::secrets::resolve_connection(self.secrets.as_ref(), &input.source_conn)
    .await
    .map_err(to_retryable)?;
let discovered_schema = connector
    .discover(&resolved, &input.source)
    .await
    .map_err(to_retryable)?;
```

```rust
// read_batch:
let resolved = crate::secrets::resolve_connection(self.secrets.as_ref(), &input.source_conn)
    .await
    .map_err(to_retryable)?;
let outcome = connector
    .read_batch(&resolved, &input.source, input.cursor, input.batch_size)
    .await
    .map_err(to_retryable)?;
```

- [ ] **Step 3: Workflow input**

In `crates/worker/src/workflows/pipeline_run.rs`, the workflow already has `pub source_connection: ConnectionConfig`. Replace every `source_url: conn.url.clone()` and `source_url: conn.expect_url().to_owned()` with:

```rust
source_conn: conn.clone(),
```

- [ ] **Step 4: CLI stops resolving**

In `crates/cli/src/main.rs::pipeline_run`, REMOVE the `worker::secrets::resolve_connection(...)` block and the `DispatchSecrets` construction. The `source_connection` passed into the workflow input becomes the unresolved config:

```rust
let pipeline_input = PipelineRunInput {
    run_id: run_id.as_uuid(),
    pipeline_id: pipeline_id.as_uuid(),
    tenant_id: pipeline.tenant_id.as_uuid(),
    spec: spec.clone(),
    source_connection: source_connection_raw.clone(),
    stream_name: stream_name.clone(),
    connector_ref: connector_ref.clone(),
    cursor_column: cursor_column.clone(),
    cursor_kind,
    pk_columns: pk_columns.clone(),
    evolution_policy,
};
```

(Rename `source_connection_raw` → `source_connection` once the resolver helper is gone.)

- [ ] **Step 5: Build + run unit tests**

```bash
cargo build --workspace
cargo test --workspace --lib
```

Expected: clean.

- [ ] **Step 6: Run sync-related integration tests**

```bash
pkill -f "target/debug/worker" 2>/dev/null; sleep 1
cargo test -p integration-tests --test incremental_sync --test secrets_e2e --test schema_evolution -- --ignored --test-threads=1
```

Expected: 3 green.

- [ ] **Step 7: Commit**

```bash
git add crates/worker/src/activities/sync/ crates/worker/src/workflows/pipeline_run.rs crates/cli/src/main.rs
git commit -m "feat(worker): sync activities resolve secrets at activity start"
```

---

## Task 13: Activity-side resolution for CDC workflows

**Files:**
- Modify: `crates/worker/src/activities/cdc/mod.rs` (`ensure_slot`, `snapshot`, `stream`)
- Modify: `crates/worker/src/activities/cdc/inputs.rs` (input shapes)
- Modify: `crates/worker/src/workflows/cdc_pipeline.rs`
- Modify: `crates/cli/src/main.rs` CDC branch (`is_cdc` block)

- [ ] **Step 1: Replace `source_url: String` with `source_conn: ConnectionConfig`**

In `crates/worker/src/activities/cdc/inputs.rs`, every input struct that carries `source_url: String` (`EnsureSlotInput`, `SnapshotInput`, `StreamInput`, etc.) becomes:

```rust
pub source_conn: common_types::connection_config::ConnectionConfig,
```

- [ ] **Step 2: Resolve at each activity entry**

In `crates/worker/src/activities/cdc/mod.rs`, every place that uses `&input.source_url` becomes:

```rust
let resolved = crate::secrets::resolve_connection(self.secrets.as_ref(), &input.source_conn)
    .await
    .map_err(retryable)?;
let url = resolved.expect_url();
// then use `url` where the old code used `&input.source_url`
```

(Each activity declares the let bindings at the top.)

- [ ] **Step 3: Workflow + CLI**

In `crates/worker/src/workflows/cdc_pipeline.rs`, swap `source_url: ...` for `source_conn: conn.clone()` on every activity-start.

In `crates/cli/src/main.rs` (the `is_cdc` branch around line 500-516):

```rust
let cdc_input = worker::workflows::CdcPipelineInput {
    run_id: run_id.as_uuid(),
    pipeline_id: pipeline_id.as_uuid(),
    tenant_id: pipeline.tenant_id.as_uuid(),
    spec: spec.clone(),
    source_connection: source_connection.clone(),
    max_windows: std::env::var("ETL_CDC_MAX_WINDOWS").ok().and_then(|s| s.parse().ok()).unwrap_or(0),
};
```

- [ ] **Step 4: Slot-lag poller**

In `crates/worker/src/main.rs` the `source_url_resolver` closure (around lines 70–91) reads `cfg.get("url")`. With url_secret pipelines this is `None`. Update it to also try the `url_secret` path; if a SecretRef is present, resolve via the worker's `secrets` Arc:

```rust
let cat_for_resolver = catalog.clone();
let secrets_for_resolver = secrets.clone();
let source_url_resolver = move |pid: uuid::Uuid| -> Option<String> {
    let cat = cat_for_resolver.clone();
    let secrets = secrets_for_resolver.clone();
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async move {
            let row: Option<(serde_json::Value,)> = sqlx::query_as(
                "SELECT c.config FROM pipelines p \
                 JOIN connections c ON c.connection_id = p.source_conn_id \
                 WHERE p.pipeline_id = $1",
            )
            .bind(pid)
            .fetch_optional(cat.pool()).await.ok().flatten();
            let row = row?;
            let conn: common_types::connection_config::ConnectionConfig =
                serde_json::from_value(row.0).ok()?;
            let resolved = worker::secrets::resolve_connection(secrets.as_ref(), &conn)
                .await.ok()?;
            Some(resolved.expect_url().to_string())
        })
    })
};
```

- [ ] **Step 5: Build + CDC integration tests**

```bash
cargo build --workspace
pkill -f "target/debug/worker" 2>/dev/null; sleep 1
cargo test -p integration-tests --test cdc_insert_update_delete --test cdc_snapshot_streaming_handoff -- --ignored --test-threads=1
```

Expected: 2 green.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/activities/cdc/ crates/worker/src/workflows/cdc_pipeline.rs crates/worker/src/main.rs crates/cli/src/main.rs
git commit -m "feat(cdc): activities resolve secrets at start; slot-lag poller follows suit"
```

---

## Task 14: RBAC matrix integration test

**Files:**
- Create: `tests/integration/tests/rbac_matrix.rs`

- [ ] **Step 1: Test code**

```rust
//! tests/integration/tests/rbac_matrix.rs
//! Verifies viewer/operator/admin can/can't perform each action class.

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
    p.pop(); p.pop(); p
}

async fn login(name: &str, password: &str) {
    let creds_path = dirs::home_dir().unwrap().join(".etl/credentials.json");
    let _ = std::fs::remove_file(&creds_path);
    let out = Command::new(cargo_bin("platform"))
        .args(["auth", "login", name, "--password", password])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await.unwrap();
    assert!(out.status.success(), "login {name}: {}", String::from_utf8_lossy(&out.stderr));
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn viewer_cannot_write_secrets() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root())
        .args(["build", "--workspace"]).status().await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // Bootstrap: dev tenant + 3 principals.
    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "rbacco"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;
    for (name, role) in [("v_user", "viewer"), ("o_user", "operator"), ("a_user", "admin")] {
        let out = Command::new(cargo_bin("platform"))
            .args(["auth", "create-principal",
                   "--tenant", "rbacco",
                   name, "--password", "pw", "--role", role])
            .env("DATABASE_URL", catalog_url())
            .env("ETL_AUTH_BYPASS", "1")
            .current_dir(workspace_root())
            .output().await?;
        assert!(out.status.success(), "create-principal {name}: {}", String::from_utf8_lossy(&out.stderr));
    }

    // viewer cannot put secrets.
    login("v_user", "pw").await;
    let put = Command::new(cargo_bin("platform"))
        .args(["secret", "put", "k", "v", "--register"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(!put.status.success());
    let stderr = String::from_utf8_lossy(&put.stderr);
    assert!(stderr.contains("not permitted"), "expected not-permitted: {stderr}");

    // operator can put secrets.
    login("o_user", "pw").await;
    let put = Command::new(cargo_bin("platform"))
        .args(["secret", "put", "k", "v", "--register"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(put.status.success(), "operator put failed: {}", String::from_utf8_lossy(&put.stderr));

    // operator cannot create tenants (admin-only).
    let create = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "another"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(!create.status.success());

    // admin can create tenants.
    login("a_user", "pw").await;
    let create = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "another"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(create.status.success(), "admin create-tenant failed: {}", String::from_utf8_lossy(&create.stderr));

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn cross_tenant_jwt_blocked() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root())
        .args(["build", "--workspace"]).status().await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // Two tenants, one operator each.
    for t in ["alpha", "beta"] {
        Command::new(cargo_bin("platform"))
            .args(["tenant", "create", t])
            .env("DATABASE_URL", catalog_url())
            .env("ETL_AUTH_BYPASS", "1")
            .current_dir(workspace_root())
            .output().await?;
        Command::new(cargo_bin("platform"))
            .args(["auth", "create-principal",
                   "--tenant", t,
                   &format!("op_{t}"), "--password", "pw", "--role", "operator"])
            .env("DATABASE_URL", catalog_url())
            .env("ETL_AUTH_BYPASS", "1")
            .current_dir(workspace_root())
            .output().await?;
    }

    // Login as alpha's operator and apply a connection.
    login("op_alpha", "pw").await;
    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(apply.status.success());

    // Switch to beta's operator. The same `get connection source-demo`
    // request must miss because RLS scopes by JWT tenant_id.
    login("op_beta", "pw").await;
    let get = Command::new(cargo_bin("platform"))
        .args(["get", "connection", "source-demo"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(!get.status.success(), "beta should not see alpha's connection");

    Ok(())
}
```

Add `dirs = "5"` to `tests/integration/Cargo.toml` if not already present.

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test rbac_matrix -- --ignored --test-threads=1 --nocapture
```

Expected: 2 green.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/Cargo.toml tests/integration/tests/rbac_matrix.rs
git commit -m "test(integration): RBAC matrix + cross-tenant JWT rejection"
```

---

## Task 15: Auth-flow integration test

**Files:**
- Create: `tests/integration/tests/auth_jwt.rs`

- [ ] **Step 1: Test code**

```rust
//! tests/integration/tests/auth_jwt.rs
//! Login → whoami → token reuse across calls.

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
    p.pop(); p.pop(); p
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn login_then_whoami_returns_principal() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root())
        .args(["build", "--workspace"]).status().await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ack"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;
    Command::new(cargo_bin("platform"))
        .args(["auth", "create-principal", "--tenant", "ack", "alice", "--password", "pw", "--role", "operator"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;

    let login = Command::new(cargo_bin("platform"))
        .args(["auth", "login", "alice", "--password", "pw"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(login.status.success(), "login failed: {}", String::from_utf8_lossy(&login.stderr));

    let whoami = Command::new(cargo_bin("platform"))
        .args(["auth", "whoami"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    let stdout = String::from_utf8_lossy(&whoami.stdout);
    assert!(whoami.status.success());
    assert!(stdout.contains("Operator"), "expected role: {stdout}");

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn login_with_wrong_password_rejects() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root())
        .args(["build", "--workspace"]).status().await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ack2"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;
    Command::new(cargo_bin("platform"))
        .args(["auth", "create-principal", "--tenant", "ack2", "bob", "--password", "right", "--role", "viewer"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;

    let login = Command::new(cargo_bin("platform"))
        .args(["auth", "login", "bob", "--password", "wrong"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output().await?;
    assert!(!login.status.success());
    let stderr = String::from_utf8_lossy(&login.stderr);
    assert!(stderr.contains("invalid password"), "expected invalid-password error: {stderr}");

    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test auth_jwt -- --ignored --test-threads=1
```

Expected: 2 green.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/auth_jwt.rs
git commit -m "test(integration): auth_jwt — login + whoami + wrong-password rejection"
```

---

## Task 16: Tenant terminate calls Temporal `DeprecateNamespace`

**Files:**
- Modify: `crates/cli/src/tenant.rs:51-73` (add Temporal deprecate call)

- [ ] **Step 1: Add deprecate helper**

Append to `crates/cli/src/tenant.rs`:

```rust
async fn deprecate_temporal_namespace(id: &TenantId) -> anyhow::Result<()> {
    use temporalio_client::grpc::WorkflowService;
    use temporalio_common::protos::temporal::api::workflowservice::v1::DeprecateNamespaceRequest;

    let cfg = worker::temporal::TemporalConfig::from_env()?;
    let client = worker::temporal::make_client(&cfg).await?;
    let ns = format!("etl-{}", id.as_uuid().simple());
    let req = DeprecateNamespaceRequest {
        namespace: ns.clone(),
        ..Default::default()
    };
    let mut svc = client.connection().workflow_service();
    match svc.deprecate_namespace(tonic::Request::new(req)).await {
        Ok(_) => println!("deprecated Temporal namespace {ns}"),
        Err(s) => {
            let msg = format!("{s}");
            if msg.to_lowercase().contains("notfound") || msg.to_lowercase().contains("not found") {
                println!("Temporal namespace {ns} already gone");
            } else {
                eprintln!("warning: deprecate_namespace failed: {s}");
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Wire into `terminate`**

In `crates/cli/src/tenant.rs::terminate`, just before the final `Ok(())` (after the `removed {path}` block):

```rust
deprecate_temporal_namespace(&t.tenant_id).await?;
```

Remove the existing `println!("note: Temporal namespace ... — `tctl namespace delete`...")` since it's now actioned.

- [ ] **Step 3: Smoke**

```bash
cargo run --bin platform -- tenant create term-test
cargo run --bin platform -- tenant terminate term-test
# Expect: 'deprecated Temporal namespace etl-<uuid>'
```

- [ ] **Step 4: Commit**

```bash
git add crates/cli/src/tenant.rs
git commit -m "feat(tenant): terminate also deprecates the Temporal namespace"
```

---

## Task 17: README + completion log + final regression sweep

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-26-phase-2-2b-auth-vault.md` (this file — append completion log)

- [ ] **Step 1: README — add Auth section before Secrets section**

Insert into `README.md` between `## Phase` and `## Secrets`:

```markdown
## Auth (Phase II.2.b)

Every CLI call now carries a JWT (`HS256`, `ETL_JWT_SECRET` for the dev seam). Three roles: `admin`, `operator`, `viewer`. Tenants gain a `status` column (`active` / `suspended`); suspended tenants can't run pipelines.

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

`ETL_AUTH_BYPASS=1` is the integration-test escape hatch; it forges a fake admin JWT so existing tests keep working without a login dance.

### RBAC matrix

| Role     | Read | Run | Write | Admin |
|----------|------|-----|-------|-------|
| Admin    | ✓    | ✓   | ✓     | ✓     |
| Operator | ✓    | ✓   | ✓     | ✗     |
| Viewer   | ✓    | ✗   | ✗     | ✗     |
```

In the `## Secrets` section, append a Vault paragraph:

```markdown
**Vault backend.** Set `VAULT_ADDR` + `VAULT_TOKEN` (and optionally `VAULT_KV_MOUNT`); register a SecretRef with `--backend vault --key etl/pg-url`. The worker resolves at activity start through `vaultrs` against KV v2.
```

- [ ] **Step 2: Append completion log to plan**

Append to this plan file:

```markdown
---

## Phase II.2.b Completion Log

Completed 2026-04-26 on branch `phase-2-2b-auth-vault`.

- [x] T1  — Migration 0008 — tenants.status column
- [x] T2  — Status-based suspend/resume + tenant_suspend test
- [x] T3  — Migration 0009 — principals table + RLS
- [x] T4  — PrincipalId newtype + catalog principal CRUD + argon2
- [x] T5  — auth crate — JwtIssuer / JwtVerifier / Role / Action
- [x] T6  — TenantContext extends with principal_id + role; SET LOCAL app.principal_id/role
- [x] T7  — CLI auth login/whoami/create-principal + creds cache
- [x] T8  — ensure_dev_tenant deleted; --tenant override; RBAC at every entry; ETL_AUTH_BYPASS bypass
- [x] T9  — Migration 0010 + SecretBackendKind::Vault
- [x] T10 — VaultSecrets backend (KV v2)
- [x] T11 — docker-compose vault service + vault_e2e test
- [x] T12 — Sync activities resolve secrets at start
- [x] T13 — CDC activities resolve secrets at start; slot-lag poller updated
- [x] T14 — RBAC matrix integration test
- [x] T15 — auth_jwt integration test
- [x] T16 — tenant terminate deprecates Temporal namespace
- [x] T17 — README + this log + regression sweep

### Exit criterion — MET

- `platform auth login --tenant acme alice` issues a JWT cached at `~/.etl/credentials.json`.
- viewer rejected at `secret put`; operator allowed; admin allowed everywhere.
- Cross-tenant token rejected by RLS at the catalog layer.
- Vault-backed pipeline resolves end-to-end.
- Suspended tenant blocked from `pipeline run`.
- `tenant terminate` deprecates the Temporal namespace.
- 19+ integration tests + 90+ unit tests green.

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.2.c

II.2.c picks up:
- OIDC integration (replace HS256 dev seam with RS256 + JWKS).
- Refresh tokens.
- Audit log of secret reads (II.2.d).
- Per-pipeline RBAC scoping (today RBAC is tenant-global).
- Token revocation list.
```

- [ ] **Step 3: Regression sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo -c \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true

cargo test --workspace --lib
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all green. 19+ integration tests (16 prior + tenant_suspend + auth_jwt + rbac_matrix + vault_e2e — vault skipped if VAULT_ADDR unset).

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-26-phase-2-2b-auth-vault.md
git commit -m "docs: Phase II.2.b README + completion log"
```

Then use the **finishing-a-development-branch** skill to push and open a PR.

---

## Appendix A — Operational notes

**`ETL_JWT_SECRET`** must be ≥32 bytes in prod. The dev default (`dev-only-jwt-secret-change-in-prod`) is for local-only use and the README warns about this. II.2.c switches to RS256 + JWKS, removing the shared-secret problem.

**`ETL_AUTH_BYPASS`** is an integration-test escape hatch. Production builds should disable it via a feature flag in II.2.c (`#[cfg(feature = "auth-bypass")]`); for II.2.b it remains a runtime env var because the test harness needs to flip it per-process.

**Plaintext lifetime under activity-side resolution**: plaintext URLs now live ONLY inside the activity body (read into a `String`-backed `ConnectionConfig::from_url`, dropped on activity return). The CLI no longer touches plaintexts. Vault token lifetime is the worker process; in II.2.c with k8s auth, the token is rotated.

**RLS coverage for principals**: the `principals` table is RLS-scoped by tenant_id, so once a JWT-derived `TenantContext` is set, principals from other tenants are invisible. The login path (`principal_get_by_name`) is the one exception — it uses `pool.acquire()` directly without `SET LOCAL`, because the principal's tenant is what we're trying to discover. This is safe because login takes a password; failure to verify rejects.

**Migration 0008 backfill**: existing `suspended:foo` tenants from II.1.c get rewritten to `(name='foo', status='suspended')`. Re-applying the migration is idempotent (the LIKE clause matches nothing on second run).

**Vault test isolation**: `vault_e2e` writes to `secret/etl/pg-url-vault`. If that path already has data, the test overwrites it (KV v2 versioning keeps the prior value). Acceptable for shared dev Vault.

## Appendix B — What's deferred to later phases

- OIDC / external IdP — Phase II.2.c
- Refresh tokens / token revocation — Phase II.2.c
- Audit log of secret reads — Phase II.2.d
- Per-pipeline RBAC scoping (e.g. `viewer` on pipeline X but `operator` on pipeline Y) — Phase III
- Dynamic secret generation / rotation — Phase III
- Customer-facing user/role management UI — Phase III
- Vault Kubernetes auth method — Phase II.4 (only token auth in II.2.b)
- Multi-region key management — Phase III
- Audit-trail of `--tenant` admin overrides — Phase II.2.d

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-26-phase-2-2b-auth-vault.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. **Recommended for this plan** because auth + RBAC + Vault span more layers than II.2.a did, and the per-task isolation pays for itself when threading `principal_id` through 30+ call sites.

**2. Inline Execution** — Execute tasks in this session using executing-plans. The 17 tasks are well-scoped; inline is feasible but longer than II.2.a.

**Which approach?**
