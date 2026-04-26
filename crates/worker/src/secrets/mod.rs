//! Secrets resolution backends. The `Secrets` trait is the seam every
//! activity calls; concrete impls (env-var, file, eventually Vault)
//! plug behind it.

pub mod auditing;
pub mod env;
pub mod file;
pub mod vault;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use common_types::connection_config::ConnectionConfig;
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

/// Resolve the URL out of a ConnectionConfig, preferring `url_secret`
/// when set. Returns a fresh `ConnectionConfig` with `url` populated and
/// `url_secret` cleared — safe to hand to a connector.
///
/// The plaintext lives in the returned config's String for the lifetime
/// of the activity. Drop semantics are NOT zeroizing for that copy; callers
/// who need stricter handling should keep using `PlaintextSecret`.
pub async fn resolve_connection(
    secrets: &dyn Secrets,
    conn: &ConnectionConfig,
) -> Result<ConnectionConfig> {
    if let Some(r) = conn.url_secret.as_ref() {
        let plaintext = secrets.resolve(r).await?;
        return Ok(ConnectionConfig::from_url(plaintext.expose().to_owned()));
    }
    if let Some(u) = conn.url.as_deref() {
        return Ok(ConnectionConfig::from_url(u.to_owned()));
    }
    Err(anyhow!(
        "ConnectionConfig has neither `url` nor `url_secret` populated"
    ))
}

/// Like `resolve_connection`, but emits a SECRET_READ audit row when
/// the connection holds a SecretRef. Activities use this path so the
/// resolve happens under a Principal-bearing context.
pub async fn resolve_connection_audited(
    secrets: &auditing::AuditingSecrets,
    conn: &ConnectionConfig,
    ctx: auditing::ResolveContext,
) -> Result<ConnectionConfig> {
    if let Some(r) = conn.url_secret.as_ref() {
        let plaintext = secrets.resolve_with_audit(r, ctx).await?;
        return Ok(ConnectionConfig::from_url(plaintext.expose().to_owned()));
    }
    if let Some(u) = conn.url.as_deref() {
        return Ok(ConnectionConfig::from_url(u.to_owned()));
    }
    Err(anyhow!(
        "ConnectionConfig has neither `url` nor `url_secret` populated"
    ))
}

