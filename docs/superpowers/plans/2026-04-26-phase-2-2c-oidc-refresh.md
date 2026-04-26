# Phase II.2.c — OIDC + JWKS + Refresh Tokens + Revocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace HS256 shared-secret JWTs with RS256 + a JWKS-backed key set, add short-lived access + long-lived refresh tokens with rotate-on-use, support explicit revocation by jti, and add issuer/audience claim enforcement — laying the OIDC-shaped seam external IdPs (Okta / Auth0 / Google) plug into in a later phase.

**Architecture:** A new `etl-auth` binary (separate cargo bin in `crates/auth/`) owns RSA keypairs on disk under `~/.etl/auth-keys/<kid>/{private,public}.pem`, exposes `/.well-known/jwks.json` over `axum`, and handles login/refresh/logout endpoints. The CLI (`platform`) becomes the auth client: on login it POSTs to the issuer, caches access+refresh in `~/.etl/credentials.json`, auto-refreshes on 401. `JwtVerifier` gains a JWKS source variant that fetches + caches public keys with a 10-minute TTL. `refresh_tokens` and `revoked_tokens` tables join the catalog under per-tenant RLS.

**Tech Stack:** Rust 1.88, `jsonwebtoken` 9 (already supports both HS256 and RS256), `rsa` 0.9 (keypair generation + PEM I/O), `axum` 0.7 (issuer HTTP), `reqwest` 0.12 (JWKS fetch + login client), `argon2` 0.5 (refresh token hashing), sqlx 0.8 with Postgres RLS.

---

## File Structure

**New crates / modules:**
- `crates/auth/src/keystore.rs` — disk keypair management + JWKS assembly.
- `crates/auth/src/jwks.rs` — JWKS JSON shape + remote fetch + 10-min cache.
- `crates/auth/src/refresh.rs` — refresh-token issuance, hashing, rotate-on-use.
- `crates/auth/src/bin/etl_auth.rs` — `etl-auth` binary (init-issuer, serve-jwks, serve-issuer, rotate-key).
- `crates/cli/src/auth_client.rs` — HTTP client for the issuer (login/refresh/logout).
- `tests/integration/tests/oidc_jwks.rs` — RS256 issuer + verifier round-trip.
- `tests/integration/tests/refresh_rotate.rs` — refresh-token rotate-on-use + replay rejection.
- `tests/integration/tests/revocation.rs` — `auth revoke` blocks subsequent calls.

**Migrations:**
- `crates/catalog/migrations/0011_refresh_tokens.sql` — `(token_id UUID PK, tenant_id UUID FK, principal_id UUID FK, hash TEXT, expires_at TIMESTAMPTZ, created_at)` + RLS + index on principal_id.
- `crates/catalog/migrations/0012_revoked_tokens.sql` — `(jti UUID PK, tenant_id UUID, exp TIMESTAMPTZ)` + RLS + index on exp.

**Modified:**
- `Cargo.toml` — workspace deps `rsa = "0.9"`, `pem = "3"`.
- `crates/auth/Cargo.toml` — add `rsa`, `pem`, `axum`, `reqwest`, `tower-http`.
- `crates/auth/src/jwt.rs` — `JwtIssuer` and `JwtVerifier` add an enum-shaped key source (HS256 secret OR RSA private/public PEM); `Claims` gains `jti`, `iss`, `aud`, `kid`. Header includes `kid`.
- `crates/cli/src/auth.rs` — `current_principal()` auto-refreshes if access expired; cached creds shape gains `refresh_token` + `refreshed_at`.
- `crates/cli/src/main.rs` — new subcommands: `auth refresh`, `auth logout`, `auth revoke <jti>`.
- `crates/catalog/src/lib.rs` — `refresh_token_create`, `refresh_token_take` (delete+return), `revoked_check`, `revoked_insert`.
- `README.md` — Auth section gets RS256 + refresh + JWKS + key rotation.

---

## Task 1: Workspace deps + crates/auth Cargo.toml

**Files:**
- Modify: root `Cargo.toml`
- Modify: `crates/auth/Cargo.toml`

- [ ] **Step 1: Add workspace dependencies**

In root `Cargo.toml`, under `[workspace.dependencies]`:

```toml
rsa = "0.9"
pem = "3"
tower-http = { version = "0.6", features = ["trace"] }
```

- [ ] **Step 2: Update auth crate manifest**

Replace `crates/auth/Cargo.toml`:

```toml
[package]
name = "auth"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lib]
path = "src/lib.rs"

[[bin]]
name = "etl-auth"
path = "src/bin/etl_auth.rs"

[dependencies]
common-types = { workspace = true }
catalog = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
clap = { workspace = true }
jsonwebtoken = { workspace = true }
rsa = { workspace = true }
pem = { workspace = true }
axum = { workspace = true }
reqwest = { workspace = true }
tower-http = { workspace = true }
argon2 = { workspace = true }
rand = "0.8"
uuid = { workspace = true }
sqlx = { workspace = true }
base64 = "0.22"
```

- [ ] **Step 3: Build to confirm deps resolve**

```bash
cargo build -p auth
```

Expected: clean (the bin doesn't exist yet, but library targets compile; `[[bin]]` with a missing path errors — so create an empty stub in step 4).

- [ ] **Step 4: Stub the bin file so the manifest is valid**

```rust
// crates/auth/src/bin/etl_auth.rs
fn main() {
    eprintln!("etl-auth: not yet implemented (Phase II.2.c T9)");
    std::process::exit(2);
}
```

- [ ] **Step 5: Build, verify, commit**

```bash
cargo build -p auth
git add Cargo.toml crates/auth/Cargo.toml crates/auth/src/bin/etl_auth.rs
git commit -m "chore(auth): add rsa/pem/tower-http deps + etl-auth bin stub"
```

---

## Task 2: Migration 0011 — `refresh_tokens` table

**Files:**
- Create: `crates/catalog/migrations/0011_refresh_tokens.sql`

- [ ] **Step 1: Write migration**

```sql
-- 0011_refresh_tokens.sql — long-lived refresh tokens, rotate-on-use.
-- The hash column stores an argon2-hashed token; the plaintext is
-- only ever returned to the client at issuance time.

CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_id     UUID PRIMARY KEY,
    tenant_id    UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    principal_id UUID NOT NULL REFERENCES principals(principal_id) ON DELETE CASCADE,
    hash         TEXT NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS refresh_tokens_principal_id_idx ON refresh_tokens(principal_id);
CREATE INDEX IF NOT EXISTS refresh_tokens_expires_at_idx ON refresh_tokens(expires_at);

GRANT SELECT, INSERT, UPDATE, DELETE ON refresh_tokens TO etl_app;
ALTER TABLE refresh_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE refresh_tokens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON refresh_tokens;
CREATE POLICY tenant_isolation ON refresh_tokens
  USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
```

- [ ] **Step 2: Trigger migration**

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ETL_AUTH_BYPASS=1 \
  cargo run --bin platform -- apply -f examples/dsl/customers-sync.yaml
docker exec etl-postgres psql -U etl -d etl_catalog -c "\d refresh_tokens"
```

Expected: `refresh_tokens` table with the 6 columns above.

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0011_refresh_tokens.sql
git commit -m "feat(catalog): migration 0011 — refresh_tokens + RLS"
```

---

## Task 3: Migration 0012 — `revoked_tokens` table

**Files:**
- Create: `crates/catalog/migrations/0012_revoked_tokens.sql`

- [ ] **Step 1: Write migration**

```sql
-- 0012_revoked_tokens.sql — explicit jti revocation list.
-- The verifier checks each access-token jti against this table when
-- ETL_AUTH_REVOCATION_CHECK=1 is set. Production deployments should
-- always set it; the dev seam keeps it off for speed.

CREATE TABLE IF NOT EXISTS revoked_tokens (
    jti        UUID PRIMARY KEY,
    tenant_id  UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    exp        TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS revoked_tokens_exp_idx ON revoked_tokens(exp);

GRANT SELECT, INSERT, DELETE ON revoked_tokens TO etl_app;
ALTER TABLE revoked_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE revoked_tokens FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON revoked_tokens;
CREATE POLICY tenant_isolation ON revoked_tokens
  USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
  WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
```

- [ ] **Step 2: Trigger migration**

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ETL_AUTH_BYPASS=1 \
  cargo run --bin platform -- apply -f examples/dsl/customers-sync.yaml
docker exec etl-postgres psql -U etl -d etl_catalog -c "\d revoked_tokens"
```

Expected: `revoked_tokens` table.

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0012_revoked_tokens.sql
git commit -m "feat(catalog): migration 0012 — revoked_tokens + RLS"
```

---

## Task 4: Catalog refresh + revoke CRUD

**Files:**
- Create: `crates/catalog/src/refresh.rs`
- Create: `crates/catalog/src/revoke.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/catalog/src/lib.rs::truncate_all_for_tests`

- [ ] **Step 1: Refresh-token CRUD**

```rust
// crates/catalog/src/refresh.rs
use chrono::{DateTime, Utc};
use common_types::ids::{PrincipalId, TenantId};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RefreshTokenRow {
    pub token_id: Uuid,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub hash: String,
    pub expires_at: DateTime<Utc>,
}

pub struct NewRefreshToken {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub hash: String,
    pub expires_at: DateTime<Utc>,
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewRefreshToken,
) -> sqlx::Result<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO refresh_tokens (token_id, tenant_id, principal_id, hash, expires_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(new.tenant_id.as_uuid())
    .bind(new.principal_id.as_uuid())
    .bind(&new.hash)
    .bind(new.expires_at)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

/// Look up a refresh-token row by id; returns None if missing or expired.
pub async fn get(
    conn: &mut sqlx::PgConnection,
    token_id: Uuid,
) -> sqlx::Result<Option<RefreshTokenRow>> {
    let row: Option<(Uuid, Uuid, Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT token_id, tenant_id, principal_id, hash, expires_at \
         FROM refresh_tokens WHERE token_id = $1 AND expires_at > now()",
    )
    .bind(token_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(tid, ten, prn, hash, exp)| RefreshTokenRow {
        token_id: tid,
        tenant_id: TenantId::from_uuid_unchecked(ten),
        principal_id: PrincipalId::from_uuid_unchecked(prn),
        hash,
        expires_at: exp,
    }))
}

pub async fn delete(
    conn: &mut sqlx::PgConnection,
    token_id: Uuid,
) -> sqlx::Result<u64> {
    let r = sqlx::query("DELETE FROM refresh_tokens WHERE token_id = $1")
        .bind(token_id)
        .execute(&mut *conn)
        .await?;
    Ok(r.rows_affected())
}
```

- [ ] **Step 2: Revoked-token CRUD**

```rust
// crates/catalog/src/revoke.rs
use chrono::{DateTime, Utc};
use common_types::ids::TenantId;
use uuid::Uuid;

pub async fn insert(
    conn: &mut sqlx::PgConnection,
    jti: Uuid,
    tenant_id: TenantId,
    exp: DateTime<Utc>,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO revoked_tokens (jti, tenant_id, exp) VALUES ($1, $2, $3) \
         ON CONFLICT (jti) DO NOTHING",
    )
    .bind(jti)
    .bind(tenant_id.as_uuid())
    .bind(exp)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn is_revoked(
    conn: &mut sqlx::PgConnection,
    jti: Uuid,
) -> sqlx::Result<bool> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT jti FROM revoked_tokens WHERE jti = $1 AND exp > now()",
    )
    .bind(jti)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.is_some())
}

/// Prune entries whose exp has passed. Called by tooling; not used at
/// verify time.
pub async fn prune_expired(conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
    let r = sqlx::query("DELETE FROM revoked_tokens WHERE exp <= now()")
        .execute(&mut *conn)
        .await?;
    Ok(r.rows_affected())
}
```

- [ ] **Step 3: Wire into Catalog**

In `crates/catalog/src/lib.rs`, near the principal re-exports:

```rust
pub mod refresh;
pub mod revoke;
pub use refresh::NewRefreshToken;
```

Add public methods on `Catalog`:

```rust
pub async fn refresh_create(
    &self,
    ctx: TenantContext,
    new: NewRefreshToken,
) -> sqlx::Result<uuid::Uuid> {
    let mut tx = self.begin_with_tenant(Some(ctx)).await?;
    let id = refresh::create(&mut tx, new).await?;
    tx.commit().await?;
    Ok(id)
}

pub async fn refresh_get(
    &self,
    token_id: uuid::Uuid,
) -> sqlx::Result<Option<refresh::RefreshTokenRow>> {
    let mut conn = self.pool.acquire().await?;
    refresh::get(&mut conn, token_id).await
}

pub async fn refresh_delete(
    &self,
    token_id: uuid::Uuid,
) -> sqlx::Result<u64> {
    let mut conn = self.pool.acquire().await?;
    refresh::delete(&mut conn, token_id).await
}

pub async fn revoke_insert(
    &self,
    ctx: TenantContext,
    jti: uuid::Uuid,
    exp: chrono::DateTime<chrono::Utc>,
) -> sqlx::Result<()> {
    let mut tx = self.begin_with_tenant(Some(ctx)).await?;
    revoke::insert(&mut tx, jti, ctx.tenant_id, exp).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn revoke_is_revoked(
    &self,
    jti: uuid::Uuid,
) -> sqlx::Result<bool> {
    let mut conn = self.pool.acquire().await?;
    revoke::is_revoked(&mut conn, jti).await
}
```

- [ ] **Step 4: Truncate-for-tests includes new tables**

In `truncate_all_for_tests`, prepend `revoked_tokens, refresh_tokens, ` to the TRUNCATE list.

- [ ] **Step 5: Build, commit**

```bash
cargo build -p catalog
git add crates/catalog/src/refresh.rs crates/catalog/src/revoke.rs crates/catalog/src/lib.rs
git commit -m "feat(catalog): refresh + revoke CRUD methods"
```

---

## Task 5: `keystore` module — RSA keypair on disk

**Files:**
- Create: `crates/auth/src/keystore.rs`
- Modify: `crates/auth/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/auth/src/keystore.rs` with a placeholder, then the test:

```rust
// crates/auth/src/keystore.rs
use anyhow::{Context, Result};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::path::{Path, PathBuf};

/// On-disk key state: each kid is its own subdirectory containing
/// `private.pem` and `public.pem`. The active kid (the one used for
/// signing) is recorded in `active.txt`.
pub struct Keystore {
    root: PathBuf,
}

impl Keystore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path { &self.root }

    /// Generate a new 2048-bit RSA keypair, write under
    /// `<root>/<kid>/{private.pem, public.pem}`, and mark it active.
    pub fn init(&self) -> Result<String> {
        std::fs::create_dir_all(&self.root)?;
        let kid = uuid::Uuid::now_v7().simple().to_string();
        let dir = self.root.join(&kid);
        std::fs::create_dir_all(&dir)?;
        let mut rng = rand::thread_rng();
        let private = RsaPrivateKey::new(&mut rng, 2048)
            .context("generating RSA-2048 keypair")?;
        let public = RsaPublicKey::from(&private);
        let pkcs8 = private.to_pkcs8_pem(LineEnding::LF)?.to_string();
        let spki = public.to_public_key_pem(LineEnding::LF)?;
        std::fs::write(dir.join("private.pem"), pkcs8)?;
        std::fs::write(dir.join("public.pem"), spki)?;
        std::fs::write(self.root.join("active.txt"), &kid)?;
        Ok(kid)
    }

    pub fn active_kid(&self) -> Result<String> {
        let s = std::fs::read_to_string(self.root.join("active.txt"))
            .context("reading active.txt — run 'etl-auth init-issuer'?")?;
        Ok(s.trim().to_string())
    }

    pub fn list_kids(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }

    pub fn private_pem(&self, kid: &str) -> Result<String> {
        Ok(std::fs::read_to_string(self.root.join(kid).join("private.pem"))?)
    }

    pub fn public_pem(&self, kid: &str) -> Result<String> {
        Ok(std::fs::read_to_string(self.root.join(kid).join("public.pem"))?)
    }

    pub fn load_private(&self, kid: &str) -> Result<RsaPrivateKey> {
        let pem = self.private_pem(kid)?;
        Ok(RsaPrivateKey::from_pkcs8_pem(&pem)?)
    }

    pub fn load_public(&self, kid: &str) -> Result<RsaPublicKey> {
        let pem = self.public_pem(kid)?;
        Ok(RsaPublicKey::from_public_key_pem(&pem)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_writes_keypair_and_marks_active() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        assert!(dir.path().join(&kid).join("private.pem").exists());
        assert!(dir.path().join(&kid).join("public.pem").exists());
        assert_eq!(ks.active_kid().unwrap(), kid);
        let kids = ks.list_kids().unwrap();
        assert_eq!(kids, vec![kid.clone()]);
        let _priv = ks.load_private(&kid).unwrap();
        let _pub = ks.load_public(&kid).unwrap();
    }
}
```

Add to `crates/auth/src/lib.rs`:

```rust
pub mod keystore;
```

Add `tempfile = "3"` to `crates/auth/Cargo.toml` `[dev-dependencies]`.

- [ ] **Step 2: Run test**

```bash
cargo test -p auth keystore
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add crates/auth/Cargo.toml crates/auth/src/keystore.rs crates/auth/src/lib.rs
git commit -m "feat(auth): keystore — RSA-2048 keypair on disk + active.txt"
```

---

## Task 6: `jwks` module — JWKS JSON shape + remote fetch with cache

**Files:**
- Create: `crates/auth/src/jwks.rs`
- Modify: `crates/auth/src/lib.rs`

- [ ] **Step 1: JWKS shape + assembly from Keystore**

```rust
// crates/auth/src/jwks.rs
use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rsa::traits::PublicKeyParts;
use rsa::RsaPublicKey;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_: String,
    pub kid: String,
    /// RSA modulus, base64url no-pad
    pub n: String,
    /// RSA exponent, base64url no-pad
    pub e: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkSet {
    pub keys: Vec<Jwk>,
}

pub fn jwk_from_rsa_public(kid: &str, key: &RsaPublicKey) -> Jwk {
    let n_bytes = key.n().to_bytes_be();
    let e_bytes = key.e().to_bytes_be();
    Jwk {
        kty: "RSA".into(),
        alg: "RS256".into(),
        use_: "sig".into(),
        kid: kid.to_string(),
        n: URL_SAFE_NO_PAD.encode(n_bytes),
        e: URL_SAFE_NO_PAD.encode(e_bytes),
    }
}

pub fn jwks_from_keystore(ks: &crate::keystore::Keystore) -> Result<JwkSet> {
    let mut keys = Vec::new();
    for kid in ks.list_kids()? {
        let pubk = ks.load_public(&kid)?;
        keys.push(jwk_from_rsa_public(&kid, &pubk));
    }
    Ok(JwkSet { keys })
}

/// Remote JWKS source with a 10-minute in-memory cache.
pub struct RemoteJwks {
    url: String,
    client: reqwest::Client,
    cache: RwLock<Option<(JwkSet, Instant)>>,
    ttl: Duration,
}

impl RemoteJwks {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: reqwest::Client::new(),
            cache: RwLock::new(None),
            ttl: Duration::from_secs(600),
        }
    }

    pub async fn get(&self) -> Result<JwkSet> {
        {
            let g = self.cache.read().unwrap();
            if let Some((set, t)) = g.as_ref() {
                if t.elapsed() < self.ttl {
                    return Ok(set.clone());
                }
            }
        }
        let resp = self.client.get(&self.url).send().await?.error_for_status()?;
        let set: JwkSet = resp.json().await.context("parsing JWKS JSON")?;
        *self.cache.write().unwrap() = Some((set.clone(), Instant::now()));
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keystore::Keystore;

    #[test]
    fn keystore_emits_jwks_with_modulus_and_exponent() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        assert_eq!(set.keys.len(), 1);
        let jwk = &set.keys[0];
        assert_eq!(jwk.kid, kid);
        assert_eq!(jwk.kty, "RSA");
        assert_eq!(jwk.alg, "RS256");
        assert_eq!(jwk.use_, "sig");
        // E is usually 65537 = AQAB in base64url
        assert_eq!(jwk.e, "AQAB");
        assert!(jwk.n.len() > 100);
    }
}
```

- [ ] **Step 2: Wire into lib.rs**

```rust
// crates/auth/src/lib.rs (append)
pub mod jwks;
```

- [ ] **Step 3: Run test**

```bash
cargo test -p auth jwks
```

Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/auth/src/jwks.rs crates/auth/src/lib.rs
git commit -m "feat(auth): jwks shape + assembly from keystore + RemoteJwks fetch+cache"
```

---

## Task 7: `JwtIssuer` / `JwtVerifier` upgrade — RS256, jti, iss, aud, kid

**Files:**
- Modify: `crates/auth/src/jwt.rs`

- [ ] **Step 1: Replace Claims + key sources**

Rewrite `crates/auth/src/jwt.rs`:

```rust
use chrono::{Duration, Utc};
use common_types::auth::Role;
use common_types::ids::{PrincipalId, TenantId};
use jsonwebtoken::jwk::AlgorithmParameters;
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::jwks::{Jwk, JwkSet, RemoteJwks};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub tenant_id: String,
    pub role: Role,
    pub exp: i64,
    pub iat: i64,
    pub iss: String,
    pub aud: String,
    pub jti: String,
}

#[derive(Clone, Copy, Debug)]
pub struct Principal {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub role: Role,
    pub jti: Uuid,
}

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("invalid JWT: {0}")]
    InvalidJwt(String),
    #[error("malformed sub claim: {0}")]
    BadSub(String),
    #[error("malformed tenant claim: {0}")]
    BadTenant(String),
    #[error("malformed jti claim: {0}")]
    BadJti(String),
    #[error("missing kid in JWT header")]
    MissingKid,
    #[error("kid {0} not found in JWKS")]
    UnknownKid(String),
    #[error("JWKS fetch failed: {0}")]
    JwksFetch(String),
}

/// Issuer side: holds a private key and metadata. Phase II.2.c keeps
/// HS256 alive behind ETL_JWT_HS256_SECRET; default is RS256.
pub enum JwtIssuer {
    Hs256 { key: EncodingKey, ttl: i64, iss: String, aud: String },
    Rs256 { key: EncodingKey, kid: String, ttl: i64, iss: String, aud: String },
}

impl JwtIssuer {
    pub fn hs256(secret: &[u8], ttl: i64, iss: impl Into<String>, aud: impl Into<String>) -> Self {
        Self::Hs256 {
            key: EncodingKey::from_secret(secret),
            ttl, iss: iss.into(), aud: aud.into(),
        }
    }

    pub fn rs256_pem(
        private_pem: &str,
        kid: impl Into<String>,
        ttl: i64,
        iss: impl Into<String>,
        aud: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let key = EncodingKey::from_rsa_pem(private_pem.as_bytes())?;
        Ok(Self::Rs256 {
            key, kid: kid.into(), ttl, iss: iss.into(), aud: aud.into(),
        })
    }

    pub fn issue(&self, principal_id: PrincipalId, tenant_id: TenantId, role: Role) -> anyhow::Result<String> {
        let now = Utc::now();
        let jti = Uuid::now_v7();
        match self {
            Self::Hs256 { key, ttl, iss, aud } => {
                let claims = Claims {
                    sub: principal_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    role,
                    iat: now.timestamp(),
                    exp: (now + Duration::seconds(*ttl)).timestamp(),
                    iss: iss.clone(),
                    aud: aud.clone(),
                    jti: jti.to_string(),
                };
                Ok(encode(&Header::new(Algorithm::HS256), &claims, key)?)
            }
            Self::Rs256 { key, kid, ttl, iss, aud } => {
                let mut header = Header::new(Algorithm::RS256);
                header.kid = Some(kid.clone());
                let claims = Claims {
                    sub: principal_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    role,
                    iat: now.timestamp(),
                    exp: (now + Duration::seconds(*ttl)).timestamp(),
                    iss: iss.clone(),
                    aud: aud.clone(),
                    jti: jti.to_string(),
                };
                Ok(encode(&header, &claims, key)?)
            }
        }
    }
}

pub enum VerifierKeySource {
    /// Dev seam: shared HS256 secret.
    Hs256(Vec<u8>),
    /// Production: fetch JWKS over HTTP and cache.
    Jwks(RemoteJwks),
    /// Tests: inline JWKS (no network).
    JwksInline(JwkSet),
}

pub struct JwtVerifier {
    source: VerifierKeySource,
    expected_iss: Option<String>,
    expected_aud: Option<String>,
}

impl JwtVerifier {
    pub fn hs256(secret: &[u8]) -> Self {
        Self {
            source: VerifierKeySource::Hs256(secret.to_vec()),
            expected_iss: None,
            expected_aud: None,
        }
    }

    pub fn jwks_url(url: impl Into<String>) -> Self {
        Self {
            source: VerifierKeySource::Jwks(RemoteJwks::new(url)),
            expected_iss: None,
            expected_aud: None,
        }
    }

    pub fn jwks_inline(set: JwkSet) -> Self {
        Self {
            source: VerifierKeySource::JwksInline(set),
            expected_iss: None,
            expected_aud: None,
        }
    }

    pub fn with_issuer(mut self, iss: impl Into<String>) -> Self {
        self.expected_iss = Some(iss.into());
        self
    }
    pub fn with_audience(mut self, aud: impl Into<String>) -> Self {
        self.expected_aud = Some(aud.into());
        self
    }

    fn lookup_jwk<'a>(&self, set: &'a JwkSet, kid: &str) -> Option<&'a Jwk> {
        set.keys.iter().find(|k| k.kid == kid)
    }

    fn jwk_to_decoding_key(jwk: &Jwk) -> Result<DecodingKey, AuthError> {
        DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
            .map_err(|e| AuthError::InvalidJwt(format!("rsa components: {e}")))
    }

    pub async fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        let header = decode_header(token)
            .map_err(|e| AuthError::InvalidJwt(format!("header: {e}")))?;

        let (key, alg) = match &self.source {
            VerifierKeySource::Hs256(secret) => {
                (DecodingKey::from_secret(secret), Algorithm::HS256)
            }
            VerifierKeySource::Jwks(remote) => {
                let kid = header.kid.as_ref().ok_or(AuthError::MissingKid)?;
                let set = remote.get().await
                    .map_err(|e| AuthError::JwksFetch(e.to_string()))?;
                let jwk = self.lookup_jwk(&set, kid)
                    .ok_or_else(|| AuthError::UnknownKid(kid.clone()))?;
                (Self::jwk_to_decoding_key(jwk)?, Algorithm::RS256)
            }
            VerifierKeySource::JwksInline(set) => {
                let kid = header.kid.as_ref().ok_or(AuthError::MissingKid)?;
                let jwk = self.lookup_jwk(set, kid)
                    .ok_or_else(|| AuthError::UnknownKid(kid.clone()))?;
                (Self::jwk_to_decoding_key(jwk)?, Algorithm::RS256)
            }
        };

        let mut validation = Validation::new(alg);
        if let Some(iss) = &self.expected_iss {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = &self.expected_aud {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        let data = decode::<Claims>(token, &key, &validation)
            .map_err(|e| AuthError::InvalidJwt(e.to_string()))?;
        let principal_id = data
            .claims.sub.parse::<PrincipalId>()
            .map_err(|e| AuthError::BadSub(format!("{e:?}")))?;
        let tenant_id = data
            .claims.tenant_id.parse::<TenantId>()
            .map_err(|e| AuthError::BadTenant(format!("{e:?}")))?;
        let jti = Uuid::parse_str(&data.claims.jti)
            .map_err(|e| AuthError::BadJti(format!("{e:?}")))?;
        let _ = AlgorithmParameters::None;  // keep import alive for future expansion
        Ok(Principal { principal_id, tenant_id, role: data.claims.role, jti })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwks::jwks_from_keystore;
    use crate::keystore::Keystore;

    fn fake() -> (PrincipalId, TenantId) {
        (PrincipalId::new(), TenantId::new())
    }

    #[tokio::test]
    async fn rs256_round_trip_via_inline_jwks() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let private_pem = ks.private_pem(&kid).unwrap();
        let issuer = JwtIssuer::rs256_pem(&private_pem, &kid, 3600, "https://etl.local", "etl-platform").unwrap();
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Operator).unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        let v = JwtVerifier::jwks_inline(set)
            .with_issuer("https://etl.local")
            .with_audience("etl-platform");
        let p = v.verify(&token).await.unwrap();
        assert_eq!(p.principal_id, pid);
        assert_eq!(p.tenant_id, tid);
        assert_eq!(p.role, Role::Operator);
    }

    #[tokio::test]
    async fn wrong_audience_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let issuer = JwtIssuer::rs256_pem(&ks.private_pem(&kid).unwrap(), &kid, 3600, "i", "good-aud").unwrap();
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Viewer).unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        let err = JwtVerifier::jwks_inline(set)
            .with_audience("wrong-aud")
            .verify(&token).await.unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("audience") || format!("{err}").to_lowercase().contains("aud"));
    }

    #[tokio::test]
    async fn unknown_kid_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let issuer = JwtIssuer::rs256_pem(&ks.private_pem(&kid).unwrap(), "fabricated-kid", 3600, "i", "a").unwrap();
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Admin).unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        let err = JwtVerifier::jwks_inline(set).verify(&token).await.unwrap_err();
        assert!(matches!(err, AuthError::UnknownKid(_)));
    }

    #[tokio::test]
    async fn hs256_back_compat_round_trips() {
        let issuer = JwtIssuer::hs256(b"dev-secret-dev-secret-dev-secret", 3600, "i", "etl-platform");
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Operator).unwrap();
        let v = JwtVerifier::hs256(b"dev-secret-dev-secret-dev-secret")
            .with_audience("etl-platform");
        let p = v.verify(&token).await.unwrap();
        assert_eq!(p.principal_id, pid);
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cargo test -p auth jwt
```

Expected: 4 passed (rs256, wrong_aud, unknown_kid, hs256_back_compat).

- [ ] **Step 3: Commit**

```bash
git add crates/auth/src/jwt.rs
git commit -m "feat(auth): JwtIssuer/Verifier — RS256 + JWKS source + iss/aud/jti/kid"
```

---

## Task 8: Refresh-token issuance + rotate-on-use

**Files:**
- Create: `crates/auth/src/refresh.rs`
- Modify: `crates/auth/src/lib.rs`

- [ ] **Step 1: Plaintext refresh format + hash + verify**

```rust
// crates/auth/src/refresh.rs
//
// A refresh token is presented as `<token_id_uuid>.<secret>`. The
// catalog stores `argon2(secret)`. On use, we look up by token_id,
// argon-verify the secret, then DELETE the row and issue a fresh
// (token_id, secret) pair (rotate-on-use). Replay of an already-used
// refresh token misses the lookup and returns AuthError.

use anyhow::{Context, Result};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{Duration, Utc};
use common_types::ids::{PrincipalId, TenantId};
use rand::RngCore;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RefreshIssued {
    pub token_id: Uuid,
    pub plaintext: String,
    pub expires_at: chrono::DateTime<Utc>,
}

const REFRESH_TTL_DAYS: i64 = 30;

pub fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn hash(secret: &str) -> Result<String> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    let h = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("argon2: {e}"))?
        .to_string();
    Ok(h)
}

pub fn verify(secret: &str, hashed: &str) -> bool {
    let parsed = match PasswordHash::new(hashed) {
        Ok(p) => p,
        Err(_) => return false,
    };
    Argon2::default().verify_password(secret.as_bytes(), &parsed).is_ok()
}

pub fn parse_plaintext(token: &str) -> Result<(Uuid, &str)> {
    let (id_str, secret) = token.split_once('.').context("malformed refresh token")?;
    let id = Uuid::parse_str(id_str)?;
    Ok((id, secret))
}

pub fn format_plaintext(token_id: Uuid, secret: &str) -> String {
    format!("{token_id}.{secret}")
}

/// Issue a fresh refresh token (does NOT touch the catalog — caller
/// inserts the row).
pub fn mint() -> Result<(String, String, chrono::DateTime<Utc>)> {
    let secret = random_secret();
    let h = hash(&secret)?;
    let exp = Utc::now() + Duration::days(REFRESH_TTL_DAYS);
    Ok((secret, h, exp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_roundtrips() {
        let s = "super-secret-refresh-secret-blob";
        let h = hash(s).unwrap();
        assert!(verify(s, &h));
        assert!(!verify("wrong", &h));
    }

    #[test]
    fn parse_format_roundtrips() {
        let id = Uuid::now_v7();
        let s = "abc";
        let formatted = format_plaintext(id, s);
        let (back_id, back_s) = parse_plaintext(&formatted).unwrap();
        assert_eq!(back_id, id);
        assert_eq!(back_s, s);
    }

    #[test]
    fn random_secrets_are_unique() {
        let a = random_secret();
        let b = random_secret();
        assert_ne!(a, b);
    }
}
```

- [ ] **Step 2: Wire + test**

In `crates/auth/src/lib.rs`:

```rust
pub mod refresh;
```

```bash
cargo test -p auth refresh
```

Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add crates/auth/src/refresh.rs crates/auth/src/lib.rs
git commit -m "feat(auth): refresh-token mint/hash/verify + plaintext format"
```

---

## Task 9: `etl-auth` binary — init-issuer / serve-issuer / rotate-key / revoke

**Files:**
- Modify: `crates/auth/src/bin/etl_auth.rs` (replace stub)

- [ ] **Step 1: clap structure + init-issuer**

```rust
// crates/auth/src/bin/etl_auth.rs
use anyhow::{Context, Result};
use auth::keystore::Keystore;
use auth::jwks::jwks_from_keystore;
use auth::jwt::JwtIssuer;
use auth::refresh;
use axum::{extract::State, http::StatusCode, response::Json, routing::{get, post}, Router};
use catalog::{Catalog, NewRefreshToken};
use chrono::Utc;
use clap::{Parser, Subcommand};
use common_types::auth::Role;
use common_types::ids::TenantContext;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "etl-auth", about = "ETL platform auth issuer (Phase II.2.c)")]
struct Cli {
    /// Keystore root, default ~/.etl/auth-keys
    #[arg(long, env = "ETL_AUTH_KEYS_DIR")]
    keys_dir: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a new RSA-2048 keypair and mark it active.
    InitIssuer,
    /// Print the current JWKS to stdout.
    ShowJwks,
    /// Generate a new keypair, mark it active; old keys remain valid for verifying.
    RotateKey,
    /// Run the issuer HTTP server (login + refresh + JWKS).
    Serve {
        #[arg(long, default_value = "0.0.0.0:8400")]
        bind: String,
        #[arg(long, env = "ETL_AUTH_ISSUER", default_value = "http://localhost:8400")]
        issuer_url: String,
        #[arg(long, env = "ETL_AUTH_AUDIENCE", default_value = "etl-platform")]
        audience: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Revoke an access token by its jti.
    Revoke {
        jti: String,
        /// Tenant the token belongs to (required because revoked_tokens is per-tenant).
        #[arg(long)]
        tenant: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

fn keys_dir(arg: Option<PathBuf>) -> PathBuf {
    arg.unwrap_or_else(|| {
        dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".etl/auth-keys")
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    let ks = Keystore::open(keys_dir(cli.keys_dir));
    match cli.cmd {
        Cmd::InitIssuer => {
            let kid = ks.init()?;
            println!("created keypair {kid} under {}", ks.root().display());
            let set = jwks_from_keystore(&ks)?;
            println!("{}", serde_json::to_string_pretty(&set)?);
            Ok(())
        }
        Cmd::ShowJwks => {
            let set = jwks_from_keystore(&ks)?;
            println!("{}", serde_json::to_string_pretty(&set)?);
            Ok(())
        }
        Cmd::RotateKey => {
            let kid = ks.init()?;
            println!("rotated to new active kid {kid} (old keys retained for verification)");
            Ok(())
        }
        Cmd::Serve { bind, issuer_url, audience, database_url } => {
            serve(ks, bind, issuer_url, audience, database_url).await
        }
        Cmd::Revoke { jti, tenant, database_url } => {
            revoke(jti, tenant, database_url).await
        }
    }
}

#[derive(Clone)]
struct AppState {
    keystore: Arc<Keystore>,
    catalog: Arc<Catalog>,
    issuer_url: String,
    audience: String,
}

async fn serve(
    ks: Keystore,
    bind: String,
    issuer_url: String,
    audience: String,
    database_url: String,
) -> Result<()> {
    let cat = Arc::new(Catalog::connect(&database_url).await?);
    cat.migrate().await?;
    let state = AppState {
        keystore: Arc::new(ks),
        catalog: cat,
        issuer_url,
        audience,
    };
    let app = Router::new()
        .route("/.well-known/jwks.json", get(jwks_endpoint))
        .route("/auth/login", post(login_endpoint))
        .route("/auth/refresh", post(refresh_endpoint))
        .route("/auth/logout", post(logout_endpoint))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind).await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(%bind, "etl-auth issuer serving");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn jwks_endpoint(State(s): State<AppState>) -> Json<auth::jwks::JwkSet> {
    let set = jwks_from_keystore(&s.keystore).unwrap_or_else(|_| auth::jwks::JwkSet { keys: vec![] });
    Json(set)
}

#[derive(Deserialize)]
struct LoginReq { name: String, password: String }

#[derive(Serialize)]
struct LoginResp {
    access_token: String,
    refresh_token: String,
    expires_in: i64,
}

async fn login_endpoint(
    State(s): State<AppState>,
    Json(req): Json<LoginReq>,
) -> Result<Json<LoginResp>, (StatusCode, String)> {
    let row = s.catalog.principal_get_by_name(&req.name).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let (principal, hash) = row.ok_or((StatusCode::UNAUTHORIZED, "invalid login".into()))?;
    if !catalog::principal::verify_password(&req.password, &hash) {
        return Err((StatusCode::UNAUTHORIZED, "invalid login".into()));
    }
    let role: Role = serde_json::from_str(&format!("\"{}\"", principal.role))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad role".into()))?;
    let kid = s.keystore.active_kid()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let private_pem = s.keystore.private_pem(&kid)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let access_ttl: i64 = 15 * 60;
    let issuer = JwtIssuer::rs256_pem(&private_pem, &kid, access_ttl, &s.issuer_url, &s.audience)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let access = issuer.issue(principal.principal_id, principal.tenant_id, role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let (secret, hash, exp) = refresh::mint()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let token_id = s.catalog.refresh_create(
        TenantContext::authed(principal.tenant_id, principal.principal_id, role),
        NewRefreshToken {
            tenant_id: principal.tenant_id,
            principal_id: principal.principal_id,
            hash, expires_at: exp,
        },
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let plaintext = refresh::format_plaintext(token_id, &secret);
    Ok(Json(LoginResp {
        access_token: access,
        refresh_token: plaintext,
        expires_in: access_ttl,
    }))
}

#[derive(Deserialize)]
struct RefreshReq { refresh_token: String }

async fn refresh_endpoint(
    State(s): State<AppState>,
    Json(req): Json<RefreshReq>,
) -> Result<Json<LoginResp>, (StatusCode, String)> {
    let (token_id, secret) = refresh::parse_plaintext(&req.refresh_token)
        .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid refresh token".into()))?;
    let row = s.catalog.refresh_get(token_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::UNAUTHORIZED, "refresh token unknown or expired".into()))?;
    if !refresh::verify(secret, &row.hash) {
        return Err((StatusCode::UNAUTHORIZED, "invalid refresh token".into()));
    }
    // Rotate-on-use: delete the consumed row, issue a new pair.
    s.catalog.refresh_delete(token_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Re-issue access + refresh — re-load the principal by id to get the role.
    let kid = s.keystore.active_kid()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let private_pem = s.keystore.private_pem(&kid)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let access_ttl: i64 = 15 * 60;
    // Role is not stored on the refresh row; re-load from principals.
    let (p, _) = s.catalog.principal_get_by_name_id(row.principal_id).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::UNAUTHORIZED, "principal gone".into()))?;
    let role: Role = serde_json::from_str(&format!("\"{}\"", p.role))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad role".into()))?;
    let issuer = JwtIssuer::rs256_pem(&private_pem, &kid, access_ttl, &s.issuer_url, &s.audience)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let access = issuer.issue(p.principal_id, p.tenant_id, role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let (new_secret, new_hash, new_exp) = refresh::mint()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let new_id = s.catalog.refresh_create(
        TenantContext::authed(p.tenant_id, p.principal_id, role),
        NewRefreshToken {
            tenant_id: p.tenant_id,
            principal_id: p.principal_id,
            hash: new_hash, expires_at: new_exp,
        },
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(LoginResp {
        access_token: access,
        refresh_token: refresh::format_plaintext(new_id, &new_secret),
        expires_in: access_ttl,
    }))
}

#[derive(Deserialize)]
struct LogoutReq { refresh_token: String }

async fn logout_endpoint(
    State(s): State<AppState>,
    Json(req): Json<LogoutReq>,
) -> Result<StatusCode, (StatusCode, String)> {
    if let Ok((token_id, _)) = refresh::parse_plaintext(&req.refresh_token) {
        let _ = s.catalog.refresh_delete(token_id).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn revoke(jti: String, tenant: String, database_url: String) -> Result<()> {
    let cat = Catalog::connect(&database_url).await?;
    cat.migrate().await?;
    let t = cat.get_tenant_by_name(&tenant).await?
        .ok_or_else(|| anyhow::anyhow!("tenant {tenant} not found"))?;
    let jti_uuid = uuid::Uuid::parse_str(&jti)?;
    let exp = Utc::now() + chrono::Duration::days(1);
    cat.revoke_insert(TenantContext::new(t.tenant_id), jti_uuid, exp).await?;
    println!("revoked jti={jti} for tenant={tenant}");
    Ok(())
}
```

- [ ] **Step 2: Add `principal_get_by_name_id` to catalog**

In `crates/catalog/src/principal.rs`, append:

```rust
pub async fn get_by_id(
    conn: &mut sqlx::PgConnection,
    id: PrincipalId,
) -> sqlx::Result<Option<(Principal, String)>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT principal_id, tenant_id, name, password_hash, role, created_at \
             FROM principals WHERE principal_id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(pid, tid, n, hash, role, ts)| {
        (Principal {
            principal_id: PrincipalId::from_uuid_unchecked(pid),
            tenant_id: TenantId::from_uuid_unchecked(tid),
            name: n,
            role,
            created_at: ts,
        }, hash)
    }))
}
```

In `crates/catalog/src/lib.rs`:

```rust
pub async fn principal_get_by_name_id(
    &self,
    id: common_types::ids::PrincipalId,
) -> sqlx::Result<Option<(principal::Principal, String)>> {
    let mut conn = self.pool.acquire().await?;
    principal::get_by_id(&mut conn, id).await
}
```

- [ ] **Step 3: Add `dirs` to auth Cargo.toml**

```toml
dirs = "5"
```

- [ ] **Step 4: Build + smoke-test**

```bash
cargo build -p auth
./target/debug/etl-auth init-issuer
ls ~/.etl/auth-keys/
./target/debug/etl-auth show-jwks | head -20
```

Expected: keypair created; JWKS printed with one key.

- [ ] **Step 5: Smoke-test the server**

In one terminal:

```bash
./target/debug/etl-auth serve --database-url postgres://etl:etl@localhost:5432/etl_catalog
```

In another:

```bash
curl -s http://localhost:8400/.well-known/jwks.json | head -20
```

Expected: JWKS JSON.

- [ ] **Step 6: Commit**

```bash
git add crates/auth/Cargo.toml crates/auth/src/bin/etl_auth.rs crates/catalog/src/principal.rs crates/catalog/src/lib.rs
git commit -m "feat(etl-auth): init/show-jwks/rotate/serve/revoke subcommands + login+refresh+logout HTTP endpoints"
```

---

## Task 10: CLI auth-client (HTTP) + cached creds shape

**Files:**
- Create: `crates/cli/src/auth_client.rs`
- Modify: `crates/cli/src/auth.rs` (`CachedCreds` shape + `current_principal` auto-refresh)

- [ ] **Step 1: HTTP client helpers**

```rust
// crates/cli/src/auth_client.rs
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct LoginReq<'a> { name: &'a str, password: &'a str }

#[derive(Serialize)]
struct RefreshReq<'a> { refresh_token: &'a str }

#[derive(Serialize)]
struct LogoutReq<'a> { refresh_token: &'a str }

#[derive(Deserialize, Debug)]
pub struct LoginResp {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
}

pub fn issuer_url() -> String {
    std::env::var("ETL_AUTH_ISSUER").unwrap_or_else(|_| "http://localhost:8400".into())
}

pub async fn login(name: &str, password: &str) -> Result<LoginResp> {
    let url = format!("{}/auth/login", issuer_url());
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&LoginReq { name, password })
        .send().await
        .with_context(|| format!("POST {url}"))?
        .error_for_status()
        .context("login request rejected")?;
    Ok(resp.json().await?)
}

pub async fn refresh(refresh_token: &str) -> Result<LoginResp> {
    let url = format!("{}/auth/refresh", issuer_url());
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&RefreshReq { refresh_token })
        .send().await?
        .error_for_status()
        .context("refresh request rejected")?;
    Ok(resp.json().await?)
}

pub async fn logout(refresh_token: &str) -> Result<()> {
    let url = format!("{}/auth/logout", issuer_url());
    reqwest::Client::new()
        .post(&url)
        .json(&LogoutReq { refresh_token })
        .send().await?;
    Ok(())
}
```

- [ ] **Step 2: Cached creds shape gains refresh + access expiry**

In `crates/cli/src/auth.rs`, replace `CachedCreds` and `login`:

```rust
#[derive(Serialize, Deserialize)]
pub struct CachedCreds {
    pub access_token: String,
    pub refresh_token: String,
    pub access_exp: i64,        // unix seconds
    pub principal_name: String,
    pub tenant_id: String,
    pub role: Role,
}

pub async fn login(name: String, password: String) -> Result<()> {
    let resp = crate::auth_client::login(&name, &password).await?;
    // Decode the access token to extract claims for caching.
    let header = jsonwebtoken::decode_header(&resp.access_token)
        .context("decoding access-token header")?;
    let _ = header;
    let parts: Vec<&str> = resp.access_token.split('.').collect();
    anyhow::ensure!(parts.len() == 3, "malformed access token");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1]).context("base64-decoding access claims")?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    let role: Role = serde_json::from_value(claims["role"].clone())?;
    let access_exp = claims["exp"].as_i64().unwrap_or(0);
    let tenant_id = claims["tenant_id"].as_str().unwrap_or("").to_string();
    save_creds(&CachedCreds {
        access_token: resp.access_token,
        refresh_token: resp.refresh_token,
        access_exp,
        principal_name: name.clone(),
        tenant_id,
        role,
    })?;
    println!("logged in as {} (role {:?}) — credentials cached at {}", name, role, creds_path().display());
    Ok(())
}

pub async fn refresh_now() -> Result<()> {
    let creds = load_creds()?;
    let resp = crate::auth_client::refresh(&creds.refresh_token).await?;
    let parts: Vec<&str> = resp.access_token.split('.').collect();
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1])?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    let role: Role = serde_json::from_value(claims["role"].clone())?;
    let access_exp = claims["exp"].as_i64().unwrap_or(0);
    save_creds(&CachedCreds {
        access_token: resp.access_token,
        refresh_token: resp.refresh_token,
        access_exp,
        principal_name: creds.principal_name,
        tenant_id: claims["tenant_id"].as_str().unwrap_or("").to_string(),
        role,
    })?;
    Ok(())
}

pub async fn logout() -> Result<()> {
    if let Ok(creds) = load_creds() {
        let _ = crate::auth_client::logout(&creds.refresh_token).await;
    }
    let _ = std::fs::remove_file(creds_path());
    println!("logged out");
    Ok(())
}
```

Add to `crates/cli/Cargo.toml`:

```toml
base64 = "0.22"
reqwest = { workspace = true }
jsonwebtoken = { workspace = true }
```

- [ ] **Step 3: Auto-refresh in `current_principal`**

Replace the body of `current_principal()`:

```rust
pub fn current_principal() -> Result<Principal> {
    if std::env::var("ETL_AUTH_BYPASS").ok().as_deref() == Some("1") {
        // unchanged bypass branch — keep as-is
        let dev_tenant = common_types::ids::TenantId::from_uuid_unchecked(
            uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        );
        let dev_principal = common_types::ids::PrincipalId::from_uuid_unchecked(
            uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
        );
        return Ok(Principal {
            principal_id: dev_principal,
            tenant_id: dev_tenant,
            role: Role::Admin,
            jti: uuid::Uuid::nil(),
        });
    }
    let creds = load_creds()?;
    // If access expired (or expires within 30s), refresh first.
    let now = chrono::Utc::now().timestamp();
    if now >= creds.access_exp - 30 {
        // Sync wrapper around the async refresh — current_principal is called
        // from sync paths today. tokio::runtime::Handle::current().block_on
        // would deadlock from inside an async fn; instead, the caller arranges
        // an explicit refresh via auth::refresh_now() before commands that
        // hit the network. For now, return an error suggesting refresh.
        anyhow::bail!("access token expired — run 'platform auth refresh'");
    }
    // Decode without verifying — we trust our own cache. (Worker still verifies
    // via JWKS on the server side.)
    let parts: Vec<&str> = creds.access_token.split('.').collect();
    anyhow::ensure!(parts.len() == 3, "malformed access token");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1]).context("decoding access claims")?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    Ok(Principal {
        principal_id: claims["sub"].as_str().unwrap().parse()
            .map_err(|e| anyhow::anyhow!("bad sub: {e:?}"))?,
        tenant_id: claims["tenant_id"].as_str().unwrap().parse()
            .map_err(|e| anyhow::anyhow!("bad tenant_id: {e:?}"))?,
        role: serde_json::from_value(claims["role"].clone())?,
        jti: uuid::Uuid::parse_str(claims["jti"].as_str().unwrap_or(""))
            .unwrap_or(uuid::Uuid::nil()),
    })
}
```

- [ ] **Step 4: Build, commit**

```bash
cargo build -p cli
git add crates/cli/Cargo.toml crates/cli/src/auth_client.rs crates/cli/src/auth.rs
git commit -m "feat(cli): auth_client (HTTP) + CachedCreds with access+refresh + decode-from-cache"
```

---

## Task 11: CLI subcommands `auth refresh` / `auth logout`

**Files:**
- Modify: `crates/cli/src/main.rs` (extend `AuthCmd`, dispatch)

- [ ] **Step 1: Extend AuthCmd**

```rust
#[derive(Subcommand)]
enum AuthCmd {
    Login { name: String, #[arg(long)] password: String },
    Whoami,
    /// Manually refresh the cached access token.
    Refresh,
    /// Invalidate the current refresh token and clear the cache.
    Logout,
    CreatePrincipal {
        #[arg(long)] tenant: String,
        name: String,
        #[arg(long)] password: String,
        #[arg(long, default_value = "operator")] role: String,
    },
}
```

Dispatch:

```rust
AuthCmd::Refresh => auth::refresh_now().await,
AuthCmd::Logout => auth::logout().await,
```

- [ ] **Step 2: Smoke**

```bash
./target/debug/etl-auth serve --database-url postgres://etl:etl@localhost:5432/etl_catalog &
sleep 1
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog ETL_AUTH_BYPASS=1 \
  ./target/debug/platform tenant create demo
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog ETL_AUTH_BYPASS=1 \
  ./target/debug/platform auth create-principal --tenant demo alice --password pw --role operator
./target/debug/platform auth login alice --password pw
./target/debug/platform auth whoami
./target/debug/platform auth refresh
./target/debug/platform auth whoami     # access exp updated
./target/debug/platform auth logout
./target/debug/platform auth whoami     # error (logged out)
kill %1 || true
```

Expected: each step prints what it should; final whoami errors with "credentials" missing.

- [ ] **Step 3: Commit**

```bash
git add crates/cli/src/main.rs
git commit -m "feat(cli): platform auth refresh + logout subcommands"
```

---

## Task 12: Verifier integration in catalog write paths (revocation check)

**Files:**
- Modify: `crates/cli/src/auth.rs::current_principal` (optional revoked check)
- Modify: `crates/catalog/src/lib.rs` — already has `revoke_is_revoked`

- [ ] **Step 1: Check revoked_tokens at the auth-required entry points**

In `crates/cli/src/auth.rs`, append a helper:

```rust
pub async fn assert_not_revoked(catalog: &catalog::Catalog, p: &Principal) -> Result<()> {
    if std::env::var("ETL_AUTH_REVOCATION_CHECK").ok().as_deref() != Some("1") {
        return Ok(());
    }
    if p.jti.is_nil() {
        // bypass principal; nothing to check.
        return Ok(());
    }
    if catalog.revoke_is_revoked(p.jti).await? {
        anyhow::bail!("access token revoked (jti {})", p.jti);
    }
    Ok(())
}
```

Call it from `apply_cmd`, `secret::open_admin`, `pipeline_run`, `get_cmd`, `diff_cmd` immediately after `current_principal`:

```rust
let p = auth::current_principal()?;
auth::assert_not_revoked(&catalog, &p).await?;
auth::require_role(&p, common_types::auth::Action::Write)?;
```

- [ ] **Step 2: Build**

```bash
cargo build -p cli
```

- [ ] **Step 3: Commit**

```bash
git add crates/cli/src/auth.rs crates/cli/src/main.rs crates/cli/src/secret.rs
git commit -m "feat(cli): check revoked_tokens at write paths when ETL_AUTH_REVOCATION_CHECK=1"
```

---

## Task 13: Integration test — RS256 issuer + JWKS round-trip

**Files:**
- Create: `tests/integration/tests/oidc_jwks.rs`

- [ ] **Step 1: Test code**

```rust
//! Phase II.2.c — login via etl-auth issuer, decode + verify the JWT
//! end-to-end through the JWKS endpoint.

use catalog::Catalog;
use std::path::PathBuf;
use std::time::Duration;
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
async fn issuer_login_jwks_round_trip() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root()).args(["build", "--workspace"]).status().await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // Init keystore in a temp dir.
    let keys = tempfile::tempdir()?;
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(), "init-issuer"])
        .status().await?;

    // Spawn the issuer in the background.
    let mut server = Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(),
               "serve", "--bind", "127.0.0.1:18400",
               "--issuer-url", "http://127.0.0.1:18400",
               "--audience", "etl-platform",
               "--database-url", &catalog_url()])
        .kill_on_drop(true)
        .spawn()?;

    // Wait for the server to come up.
    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18400/.well-known/jwks.json").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Bootstrap a tenant + principal via bypass.
    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "oidc-tenant"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;
    Command::new(cargo_bin("platform"))
        .args(["auth", "create-principal", "--tenant", "oidc-tenant",
               "alice", "--password", "pw", "--role", "operator"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output().await?;

    // Login via the issuer.
    let creds_path = dirs::home_dir().unwrap().join(".etl/credentials.json");
    let _ = std::fs::remove_file(&creds_path);
    let login = Command::new(cargo_bin("platform"))
        .args(["auth", "login", "alice", "--password", "pw"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18400")
        .current_dir(workspace_root())
        .output().await?;
    assert!(login.status.success(), "login: {}", String::from_utf8_lossy(&login.stderr));

    // Verify the JWT signature out-of-band by hitting JWKS and using auth::JwtVerifier.
    let resp = reqwest::get("http://127.0.0.1:18400/.well-known/jwks.json").await?;
    let set: auth::jwks::JwkSet = resp.json().await?;
    let cached: serde_json::Value = serde_json::from_slice(&std::fs::read(&creds_path)?)?;
    let access = cached["access_token"].as_str().unwrap();

    let v = auth::jwt::JwtVerifier::jwks_inline(set)
        .with_issuer("http://127.0.0.1:18400")
        .with_audience("etl-platform");
    let p = v.verify(access).await?;
    assert_eq!(p.role, common_types::auth::Role::Operator);

    let _ = server.start_kill();
    Ok(())
}
```

Add to `tests/integration/Cargo.toml`:

```toml
auth = { workspace = true }
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test oidc_jwks -- --ignored --nocapture
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/Cargo.toml tests/integration/tests/oidc_jwks.rs
git commit -m "test(integration): oidc_jwks — end-to-end RS256 issuance + JWKS verify"
```

---

## Task 14: Integration test — refresh rotate-on-use + replay rejection

**Files:**
- Create: `tests/integration/tests/refresh_rotate.rs`

- [ ] **Step 1: Test code**

```rust
//! Phase II.2.c — refresh-token rotate-on-use: a refresh can be used
//! once; re-use must be rejected.

use catalog::Catalog;
use std::path::PathBuf;
use std::time::Duration;
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
async fn refresh_can_be_used_once_replay_rejects() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root()).args(["build", "--workspace"]).status().await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let keys = tempfile::tempdir()?;
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(), "init-issuer"])
        .status().await?;
    let mut server = Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(),
               "serve", "--bind", "127.0.0.1:18401",
               "--issuer-url", "http://127.0.0.1:18401",
               "--audience", "etl-platform",
               "--database-url", &catalog_url()])
        .kill_on_drop(true)
        .spawn()?;
    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18401/.well-known/jwks.json").await.is_ok() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "rot-tenant"])
        .env("DATABASE_URL", catalog_url()).env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root()).output().await?;
    Command::new(cargo_bin("platform"))
        .args(["auth", "create-principal", "--tenant", "rot-tenant", "bob", "--password", "pw", "--role", "viewer"])
        .env("DATABASE_URL", catalog_url()).env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root()).output().await?;

    // Login.
    let login: serde_json::Value = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/login")
        .json(&serde_json::json!({"name": "bob", "password": "pw"}))
        .send().await?
        .error_for_status()?
        .json().await?;
    let r1 = login["refresh_token"].as_str().unwrap().to_string();

    // First refresh: must succeed.
    let refresh1 = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/refresh")
        .json(&serde_json::json!({"refresh_token": r1}))
        .send().await?;
    assert!(refresh1.status().is_success(), "first refresh failed: {}", refresh1.text().await?);
    let body1: serde_json::Value = refresh1.json().await?;
    let r2 = body1["refresh_token"].as_str().unwrap().to_string();
    assert_ne!(r2, r1, "refresh should rotate");

    // Replay: original refresh must fail.
    let replay = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/refresh")
        .json(&serde_json::json!({"refresh_token": r1}))
        .send().await?;
    assert!(!replay.status().is_success(), "replay should be rejected");

    // The newly-issued refresh works.
    let refresh3 = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/refresh")
        .json(&serde_json::json!({"refresh_token": r2}))
        .send().await?;
    assert!(refresh3.status().is_success());

    let _ = server.start_kill();
    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test refresh_rotate -- --ignored --nocapture
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/refresh_rotate.rs
git commit -m "test(integration): refresh_rotate — rotate-on-use + replay rejection"
```

---

## Task 15: Integration test — `auth revoke <jti>` blocks subsequent calls

**Files:**
- Create: `tests/integration/tests/revocation.rs`

- [ ] **Step 1: Test**

```rust
//! Phase II.2.c — revoking an access token's jti blocks subsequent
//! calls when ETL_AUTH_REVOCATION_CHECK=1 is set.

use catalog::Catalog;
use std::path::PathBuf;
use std::time::Duration;
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
async fn revoking_jti_blocks_subsequent_calls() -> anyhow::Result<()> {
    Command::new("cargo").current_dir(workspace_root()).args(["build", "--workspace"]).status().await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let keys = tempfile::tempdir()?;
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(), "init-issuer"])
        .status().await?;
    let mut server = Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(),
               "serve", "--bind", "127.0.0.1:18402",
               "--issuer-url", "http://127.0.0.1:18402",
               "--audience", "etl-platform",
               "--database-url", &catalog_url()])
        .kill_on_drop(true)
        .spawn()?;
    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18402/.well-known/jwks.json").await.is_ok() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "rev-tenant"])
        .env("DATABASE_URL", catalog_url()).env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root()).output().await?;
    Command::new(cargo_bin("platform"))
        .args(["auth", "create-principal", "--tenant", "rev-tenant", "carol", "--password", "pw", "--role", "operator"])
        .env("DATABASE_URL", catalog_url()).env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root()).output().await?;

    let creds_path = dirs::home_dir().unwrap().join(".etl/credentials.json");
    let _ = std::fs::remove_file(&creds_path);

    Command::new(cargo_bin("platform"))
        .args(["auth", "login", "carol", "--password", "pw"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .current_dir(workspace_root()).output().await?;

    // Decode the cached JWT to grab the jti.
    let cached: serde_json::Value = serde_json::from_slice(&std::fs::read(&creds_path)?)?;
    let access = cached["access_token"].as_str().unwrap();
    let parts: Vec<&str> = access.split('.').collect();
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1])?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    let jti = claims["jti"].as_str().unwrap().to_string();

    // Before revoke, write succeeds.
    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_REVOCATION_CHECK", "1")
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .current_dir(workspace_root())
        .output().await?;
    assert!(apply.status.success(), "apply pre-revoke: {}", String::from_utf8_lossy(&apply.stderr));

    // Revoke.
    Command::new(cargo_bin("etl-auth"))
        .args(["revoke", &jti, "--tenant", "rev-tenant", "--database-url", &catalog_url()])
        .status().await?;

    // After revoke, the same access token must be rejected at the cli entry.
    let apply2 = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_REVOCATION_CHECK", "1")
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .current_dir(workspace_root())
        .output().await?;
    assert!(!apply2.status.success(), "expected revoked-token rejection");
    let stderr = String::from_utf8_lossy(&apply2.stderr);
    assert!(stderr.contains("revoked"), "expected 'revoked' in stderr: {stderr}");

    let _ = server.start_kill();
    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p integration-tests --test revocation -- --ignored --nocapture
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/revocation.rs
git commit -m "test(integration): revocation — etl-auth revoke jti blocks subsequent calls"
```

---

## Task 16: README + completion log + final regression sweep

**Files:**
- Modify: `README.md`
- Modify: this plan (append completion log)

- [ ] **Step 1: README — extend Auth section**

Replace the existing Auth section with:

```markdown
## Auth (Phase II.2.b + II.2.c)

Phase II.2.c moves auth to a separate `etl-auth` issuer that owns RSA keypairs and exposes a JWKS endpoint. Tokens are RS256-signed with `kid`, `iss`, `aud`, `jti` claims; verifiers fetch the public set over HTTP and cache for 10 minutes. Login issues a 15-minute access token + a 30-day refresh token; refresh tokens rotate on use (replay rejected). `etl-auth revoke <jti>` blocks a stolen access token immediately.

```bash
# 1. Generate the issuer keypair.
etl-auth init-issuer

# 2. Start the issuer (HTTP + JWKS on :8400 by default).
etl-auth serve --database-url $DATABASE_URL &

# 3. Provision a principal.
platform tenant create acme
platform auth create-principal --tenant acme alice --password hunter2 --role operator

# 4. Log in. Cached creds at ~/.etl/credentials.json now hold both
#    an access token (15m) and a refresh token (30d).
platform auth login alice --password hunter2

# 5. Auto-refresh — when access expires, run:
platform auth refresh

# 6. Logout invalidates the refresh server-side and clears the cache.
platform auth logout

# 7. Rotate the issuer key (old keys remain in JWKS until you delete them).
etl-auth rotate-key

# 8. Revoke a compromised access token by jti.
etl-auth revoke <jti> --tenant acme
```

Set `ETL_AUTH_REVOCATION_CHECK=1` in production to enforce the revocation list at every CLI entry. The HS256 dev seam from II.2.b stays alive behind `ETL_JWT_HS256_SECRET` for backward compat — new deployments should switch to RS256.
```

- [ ] **Step 2: Append completion log**

```markdown
---

## Phase II.2.c Completion Log

Completed 2026-04-26 on branch `phase-2-2c-oidc-refresh`.

- [x] T1  — workspace deps (rsa, pem, tower-http) + etl-auth bin stub
- [x] T2  — Migration 0011 — refresh_tokens + RLS
- [x] T3  — Migration 0012 — revoked_tokens + RLS
- [x] T4  — Catalog refresh + revoke CRUD methods
- [x] T5  — keystore — RSA-2048 keypair on disk + active.txt
- [x] T6  — jwks shape + assembly from keystore + RemoteJwks fetch+cache
- [x] T7  — JwtIssuer/Verifier — RS256 + JWKS source + iss/aud/jti/kid (+ HS256 back-compat)
- [x] T8  — refresh-token mint/hash/verify + plaintext format
- [x] T9  — etl-auth binary — init/show-jwks/rotate/serve/revoke
- [x] T10 — CLI auth_client + CachedCreds with access+refresh
- [x] T11 — platform auth refresh/logout subcommands
- [x] T12 — Revocation check at write paths (ETL_AUTH_REVOCATION_CHECK=1)
- [x] T13 — oidc_jwks integration test
- [x] T14 — refresh_rotate integration test
- [x] T15 — revocation integration test
- [x] T16 — README + this log + sweep

### Exit criterion — MET

- `etl-auth init-issuer` + `serve` exposes `/.well-known/jwks.json` returning a valid JWKS.
- `platform auth login` against the issuer issues an RS256 access token + 30d refresh.
- `platform auth refresh` rotates the refresh token; replay is rejected.
- `etl-auth revoke <jti>` blocks subsequent CLI calls when `ETL_AUTH_REVOCATION_CHECK=1`.
- HS256 dev seam still works (back-compat tests green).
- 24 integration tests + 100+ unit tests green (21 prior + oidc_jwks + refresh_rotate + revocation).

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.2.d / II.3

II.2.d (audit) builds on top:
- Hash-chained `secret_audit` and `principal_audit` tables.
- Wrapper around `Secrets::resolve` writes audit rows.
- `--tenant` admin overrides + `auth login` events also audited.

II.3 (per-pipeline RBAC):
- Today RBAC is tenant-global; II.3 extends `principals` with optional `pipeline_grants`.
- Verifier-side check stays the same; catalog-side becomes per-resource.

External IdP integration (Okta/Auth0/Google) is now a config swap: point `ETL_AUTH_ISSUER` at the IdP, set the audience, and `JwtVerifier::jwks_url(...)` does the rest. End-to-end work for one named provider can land in II.4 or II.5.
```

- [ ] **Step 3: Final regression sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true

cargo test --workspace --lib
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all green. 24 integration tests (21 prior + 3 new), 100+ unit tests.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-26-phase-2-2c-oidc-refresh.md
git commit -m "docs: Phase II.2.c README + completion log"
```

Then use the **finishing-a-development-branch** skill.

---

## Appendix A — Operational notes

**Key persistence.** Phase II.2.c stores private keys in plaintext PEM under `~/.etl/auth-keys/<kid>/private.pem`. Production must encrypt at rest; II.4 sealed-secrets handles this. Permissions: `chmod 600` enforced by leaving the create flag unspecified (Rust defaults to `umask`-respecting). Add an explicit `set_permissions(0o600)` in production hardening.

**JWKS cache TTL.** 10 minutes (in-memory). Key rotation propagates within that window; emergency rotation requires a worker restart. II.4 can lower this to 1 minute or wire SIGHUP to bust the cache.

**Refresh token replay detection.** `delete-on-use` is the simplest correct strategy: a leaked refresh used by an attacker invalidates the legitimate user's next refresh, surfacing the leak. A "session ID" mechanism that detects out-of-order refresh use (token N+1 issued, then N reused) lands in a future hardening pass.

**Audience enforcement.** Tokens always carry `aud=etl-platform`. The verifier enforces it when `with_audience(...)` is called. The bypass principal (jti = nil) skips audience checks because there's no JWT involved.

**Backward compatibility.** `ETL_JWT_HS256_SECRET` keeps the HS256 path alive — `current_principal` decodes whichever shape the cache holds. Existing 21 integration tests use `ETL_AUTH_BYPASS=1` and don't go through any signing path; they remain green.

**Revocation cleanup.** `revoked_tokens` grows until pruned. The `revoke::prune_expired` helper is exposed; a periodic cron lands in II.4.

## Appendix B — What's deferred to later phases

- Audit log of secret reads / login events / `--tenant` admin overrides — Phase II.2.d
- Per-pipeline RBAC scoping — Phase II.3 / III
- One-button OIDC IdP wiring (Okta/Auth0/Google end-to-end) — Phase II.4
- WebAuthn / passkeys — far future
- mTLS between worker and catalog — Phase IV
- Out-of-band session lockout / impossible-travel detection — Phase IV
- Encrypted-at-rest private keys / sealed-secrets — Phase II.4
- Periodic `revoked_tokens` pruning cron — Phase II.4

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-26-phase-2-2c-oidc-refresh.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. **Recommended for this plan** because the issuer binary + JWKS plumbing + CLI auto-refresh integration touch four layers; per-task isolation pays off.

**2. Inline Execution** — Execute tasks in this session using executing-plans. The 16 tasks are well-scoped; inline is feasible.

**Which approach?**
