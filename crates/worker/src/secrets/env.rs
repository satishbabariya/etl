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
