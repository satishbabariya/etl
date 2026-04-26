//! Thin wrapper around catalog::Catalog::audit_write so every CLI write
//! site can emit a row with one line. Failures are logged but do NOT
//! abort the action — audit is observability, not a gate.

use audit::{AuditEvent, AuditRow};
use catalog::Catalog;
use chrono::Utc;
use common_types::ids::{PrincipalId, TenantId};
use serde_json::Value;
use uuid::Uuid;

pub async fn record(
    catalog: &Catalog,
    tenant_id: Option<TenantId>,
    principal_id: Option<PrincipalId>,
    jti: Option<Uuid>,
    event: AuditEvent,
    target: Option<String>,
    payload: Value,
) {
    let row = AuditRow {
        tenant_id,
        principal_id,
        jti,
        event,
        target,
        occurred_at: Utc::now(),
        payload,
    };
    if let Err(e) = catalog.audit_write(&row).await {
        tracing::warn!(error = %e, action = %event.as_action_str(), "audit_write failed");
    }
}

pub fn principal_into(p: &auth::Principal) -> (Option<PrincipalId>, Option<Uuid>) {
    if p.jti.is_nil() {
        // Bypass principal — record the synthetic id but no jti.
        (Some(p.principal_id), None)
    } else {
        (Some(p.principal_id), Some(p.jti))
    }
}
