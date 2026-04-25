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

