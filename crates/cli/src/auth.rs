//! Phase II.2.b — CLI auth helpers.
//!
//! `login` verifies a (name, password) against `principals`, issues
//! a JWT, and caches it at `~/.etl/credentials.json`. Every other
//! subcommand calls `current_principal()` which verifies the cached
//! token and returns the resolved Principal (or errors and tells the
//! user to log in).

use anyhow::{Context, Result};
use auth::{JwtIssuer, JwtVerifier, Principal};
use catalog::Catalog;
use common_types::auth::{Action, Role};
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
    let bytes = std::fs::read(creds_path()).with_context(|| {
        format!(
            "reading {} — run 'platform auth login' first",
            creds_path().display()
        )
    })?;
    let creds: CachedCreds =
        serde_json::from_slice(&bytes).context("parsing cached credentials JSON")?;
    Ok(creds)
}

/// Verify the cached JWT and return the resolved Principal. The
/// `ETL_AUTH_BYPASS=1` escape hatch forges a fake admin principal to
/// keep existing integration tests working without a login dance.
pub fn current_principal() -> Result<Principal> {
    if std::env::var("ETL_AUTH_BYPASS").ok().as_deref() == Some("1") {
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
    // Phase II.2.c bridge: decode-from-cache without verifying. The
    // worker / API server still verifies via JWKS on every request.
    // T10 replaces this with the proper async path.
    use base64::Engine;
    let parts: Vec<&str> = creds.token.split('.').collect();
    anyhow::ensure!(parts.len() == 3, "malformed cached token");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .context("decoding cached token claims")?;
    let claims: serde_json::Value =
        serde_json::from_slice(&payload).context("parsing cached token claims")?;
    Ok(Principal {
        principal_id: claims["sub"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing sub claim"))?
            .parse()
            .map_err(|e| anyhow::anyhow!("bad sub: {e:?}"))?,
        tenant_id: claims["tenant_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing tenant_id claim"))?
            .parse()
            .map_err(|e| anyhow::anyhow!("bad tenant_id: {e:?}"))?,
        role: serde_json::from_value(claims["role"].clone()).context("decoding role")?,
        jti: claims["jti"]
            .as_str()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .unwrap_or(uuid::Uuid::nil()),
    })
}

pub fn require_role(p: &Principal, action: Action) -> Result<()> {
    if !p.role.permits(action) {
        anyhow::bail!(
            "principal role {:?} not permitted for {:?}",
            p.role,
            action
        );
    }
    Ok(())
}

/// Returns the TenantContext to use for catalog operations, applying
/// `--tenant <name>` admin override when supplied.
pub async fn resolve_context(
    catalog: &Catalog,
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

    let issuer = JwtIssuer::hs256(&jwt_secret(), DEFAULT_TTL_SECONDS, "etl-cli", "etl-platform");
    let token = issuer.issue(principal.principal_id, principal.tenant_id, role)?;

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
    admin.migrate().await?;
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
    println!(
        "created principal {} ({}) in tenant {}",
        name, id, tenant_name
    );
    Ok(())
}

/// Bootstrap helper for `ETL_AUTH_BYPASS=1` — ensures the dev tenant
/// row exists so commands that use the bypass principal can find it.
pub async fn ensure_bypass_tenant(cat: &Catalog) -> anyhow::Result<()> {
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
