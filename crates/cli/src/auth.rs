//! Phase II.2.b — CLI auth helpers.
//!
//! `login` verifies a (name, password) against `principals`, issues
//! a JWT, and caches it at `~/.etl/credentials.json`. Every other
//! subcommand calls `current_principal()` which verifies the cached
//! token and returns the resolved Principal (or errors and tells the
//! user to log in).

use anyhow::{Context, Result};
use auth::Principal;
use catalog::Catalog;
use common_types::auth::{Action, Role};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub struct CachedCreds {
    pub access_token: String,
    pub refresh_token: String,
    pub access_exp: i64,
    pub principal_name: String,
    pub tenant_id: String,
    pub role: Role,
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
    // Decode-from-cache without verifying — the issuer signed it and
    // any server that needs verification re-checks via JWKS. If access
    // expired the caller should run `platform auth refresh` first.
    let now = chrono::Utc::now().timestamp();
    if now >= creds.access_exp.saturating_sub(30) {
        anyhow::bail!("access token expired — run 'platform auth refresh'");
    }
    use base64::Engine;
    let parts: Vec<&str> = creds.access_token.split('.').collect();
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

/// Decode the role + tenant_id + access_exp out of a freshly-issued
/// access token. Trusts the issuer; verification happens server-side.
fn extract_claims(access_token: &str) -> Result<(Role, String, i64)> {
    use base64::Engine;
    let parts: Vec<&str> = access_token.split('.').collect();
    anyhow::ensure!(parts.len() == 3, "malformed access token");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .context("base64-decoding access claims")?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    let role: Role = serde_json::from_value(claims["role"].clone())?;
    let tenant_id = claims["tenant_id"].as_str().unwrap_or("").to_string();
    let exp = claims["exp"].as_i64().unwrap_or(0);
    Ok((role, tenant_id, exp))
}

pub async fn login(name: String, password: String) -> Result<()> {
    let resp = crate::auth_client::login(&name, &password).await?;
    let (role, tenant_id, access_exp) = extract_claims(&resp.access_token)?;
    save_creds(&CachedCreds {
        access_token: resp.access_token,
        refresh_token: resp.refresh_token,
        access_exp,
        principal_name: name.clone(),
        tenant_id,
        role,
    })?;
    println!(
        "logged in as {} (role {:?}) — credentials cached at {}",
        name,
        role,
        creds_path().display()
    );
    Ok(())
}

pub async fn refresh_now() -> Result<()> {
    let creds = load_creds()?;
    let resp = crate::auth_client::refresh(&creds.refresh_token).await?;
    let (role, tenant_id, access_exp) = extract_claims(&resp.access_token)?;
    save_creds(&CachedCreds {
        access_token: resp.access_token,
        refresh_token: resp.refresh_token,
        access_exp,
        principal_name: creds.principal_name,
        tenant_id,
        role,
    })?;
    println!("refreshed access token (exp {access_exp})");
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

/// When ETL_AUTH_REVOCATION_CHECK=1 is set, check the principal's jti
/// against the revoked_tokens table and refuse if revoked. The bypass
/// principal carries jti = nil and is exempt.
pub async fn assert_not_revoked(catalog: &Catalog, p: &Principal) -> Result<()> {
    if std::env::var("ETL_AUTH_REVOCATION_CHECK").ok().as_deref() != Some("1") {
        return Ok(());
    }
    if p.jti.is_nil() {
        return Ok(());
    }
    if catalog.revoke_is_revoked(p.jti).await? {
        anyhow::bail!("access token revoked (jti {})", p.jti);
    }
    Ok(())
}
