use anyhow::{Context, Result};
use async_trait::async_trait;
use common_types::secrets::{PlaintextSecret, SecretRef};
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use vaultrs::kv2;

use super::Secrets;

/// VaultSecrets reads from KV v2 at `<mount>/<path>`. `SecretRef.key`
/// may include a leading `<mount>/<path>`; the resolver splits at the
/// first `/` — anything before is the mount, the rest is the path.
/// Plaintexts are stored under the field name `value` by convention.
pub struct VaultSecrets {
    client: VaultClient,
    default_mount: String,
}

impl VaultSecrets {
    /// Build from `VAULT_ADDR` + `VAULT_TOKEN` + optional `VAULT_KV_MOUNT`.
    /// Returns `None` (caller's choice) when `VAULT_ADDR` is unset.
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
            .ok_or_else(|| {
                anyhow::anyhow!("vault entry at {mount}/{path} missing 'value' field")
            })?;
        Ok(PlaintextSecret::new(v.to_string()))
    }
}
