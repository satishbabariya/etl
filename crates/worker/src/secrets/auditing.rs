//! Wraps a Secrets impl. Before delegating, writes a SECRET_READ audit
//! row (tenant_id, principal_id, jti, secret_id, secret_name, backend).
//! Plaintext is NEVER touched.

use anyhow::Result;
use async_trait::async_trait;
use catalog::Catalog;
use common_types::ids::{PrincipalId, TenantId};
use common_types::secrets::{PlaintextSecret, SecretRef};
use std::sync::Arc;
use uuid::Uuid;

use super::Secrets;

pub struct AuditingSecrets {
    inner: Arc<dyn Secrets>,
    catalog: Arc<Catalog>,
}

impl AuditingSecrets {
    pub fn new(inner: Arc<dyn Secrets>, catalog: Arc<Catalog>) -> Self {
        Self { inner, catalog }
    }
}

/// Per-resolve context: who's resolving and on whose behalf. Threaded
/// from the activity input.
#[derive(Clone, Copy, Debug)]
pub struct ResolveContext {
    pub tenant_id: TenantId,
    pub principal_id: Option<PrincipalId>,
    pub jti: Option<Uuid>,
}

impl AuditingSecrets {
    pub async fn resolve_with_audit(
        &self,
        r: &SecretRef,
        ctx: ResolveContext,
    ) -> Result<PlaintextSecret> {
        let backend = format!("{:?}", r.backend).to_lowercase();
        let row = audit::AuditRow {
            tenant_id: Some(ctx.tenant_id),
            principal_id: ctx.principal_id,
            jti: ctx.jti,
            event: audit::AuditEvent::SecretRead,
            target: Some(r.name.clone()),
            occurred_at: chrono::Utc::now(),
            payload: serde_json::json!({
                "secret_id": r.secret_id.to_string(),
                "backend": backend,
                "key": r.key,
            }),
        };
        if let Err(e) = self.catalog.audit_write(&row).await {
            tracing::warn!(error = %e, "audit_write SECRET_READ failed");
        }
        self.inner.resolve(r).await
    }
}

#[async_trait]
impl Secrets for AuditingSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        // Fall-through: no audit context — system call (e.g. slot-lag poller).
        self.inner.resolve(r).await
    }
}
