//! Background tasks spawned inside `etl-auth serve`:
//!
//!  * audit_retention_loop  — prunes rows older than `retention_days`
//!                            within the verified prefix (1h cadence)
//!  * audit_verify_loop     — walks each tenant's chain, records a
//!                            checkpoint, emits AUDIT_CHAIN_BREAK on
//!                            mismatch (6h cadence)
//!  * revoke_cleanup_loop   — prunes expired revoked_tokens (1h cadence)

use anyhow::Result;
use catalog::Catalog;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;

pub fn spawn_all(
    catalog: Arc<Catalog>,
    retention_days: i64,
    shutdown: watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    handles.push(tokio::spawn(audit_retention_loop(
        catalog.clone(),
        retention_days,
        shutdown.clone(),
    )));
    handles.push(tokio::spawn(audit_verify_loop(
        catalog.clone(),
        shutdown.clone(),
    )));
    handles.push(tokio::spawn(revoke_cleanup_loop(catalog, shutdown)));
    handles
}

async fn audit_retention_loop(
    catalog: Arc<Catalog>,
    retention_days: i64,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut tick = interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = audit_retention_once(&catalog, retention_days).await {
                    tracing::warn!(error = %e, "audit_retention_once failed");
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}

async fn audit_retention_once(catalog: &Catalog, retention_days: i64) -> Result<()> {
    let tenants = catalog.list_tenants().await?;
    let cutoff = Utc::now() - chrono::Duration::days(retention_days);
    for t in tenants {
        if let Err(e) = catalog.audit_verify_and_checkpoint(Some(t.tenant_id)).await {
            tracing::warn!(tenant = %t.name, error = %e, "verify_and_checkpoint failed");
            continue;
        }
        let cp = match catalog.audit_get_checkpoint(Some(t.tenant_id)).await? {
            Some(c) => c,
            None => continue,
        };
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT audit_id FROM audit_log \
             WHERE tenant_id = $1 AND occurred_at < $2 AND audit_id <= $3 \
             ORDER BY audit_id DESC LIMIT 1",
        )
        .bind(t.tenant_id.as_uuid())
        .bind(cutoff)
        .bind(cp.last_verified_audit_id)
        .fetch_optional(catalog.pool())
        .await?;
        if let Some((id,)) = row {
            let n = catalog.audit_prune_before(Some(t.tenant_id), id + 1).await?;
            if n > 0 {
                tracing::info!(tenant = %t.name, pruned = n, "audit retention pruned");
            }
        }
    }
    Ok(())
}

pub async fn audit_retention_once_pub(catalog: &Catalog, retention_days: i64) -> Result<()> {
    audit_retention_once(catalog, retention_days).await
}

async fn audit_verify_loop(catalog: Arc<Catalog>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = interval(Duration::from_secs(6 * 3600));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = audit_verify_once(&catalog).await {
                    tracing::warn!(error = %e, "audit_verify_once failed");
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}

pub async fn audit_verify_once(catalog: &Catalog) -> Result<()> {
    let tenants = catalog.list_tenants().await?;
    for t in tenants {
        match catalog.audit_verify_and_checkpoint(Some(t.tenant_id)).await {
            Ok(audit::verify::VerifyResult::Ok { .. }) => {}
            Ok(audit::verify::VerifyResult::Mismatch { audit_id }) => {
                tracing::error!(
                    tenant = %t.name,
                    audit_id,
                    "AUDIT_CHAIN_BREAK: chain integrity failure"
                );
                let row = audit::AuditRow {
                    tenant_id: None,
                    principal_id: None,
                    jti: None,
                    event: audit::AuditEvent::AuditChainBreak,
                    target: Some(t.name.clone()),
                    occurred_at: Utc::now(),
                    payload: serde_json::json!({
                        "tenant_id": t.tenant_id.to_string(),
                        "audit_id": audit_id,
                    }),
                };
                if let Err(e) = catalog.audit_write(&row).await {
                    tracing::error!(error = %e, "failed to record AUDIT_CHAIN_BREAK");
                }
            }
            Err(e) => tracing::warn!(tenant = %t.name, error = %e, "verify failed"),
        }
    }
    Ok(())
}

async fn revoke_cleanup_loop(catalog: Arc<Catalog>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let mut conn = match catalog.pool().acquire().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, "revoke cleanup acquire");
                        continue;
                    }
                };
                match catalog::revoke::prune_expired(&mut conn).await {
                    Ok(n) if n > 0 => tracing::info!(pruned = n, "revoked_tokens cleanup"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "revoke prune"),
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}
