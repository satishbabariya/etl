use crate::chain::{next_hash, ChainError, GENESIS_PREV_HASH};
use crate::event::{AuditEvent, AuditRow};
use chrono::{DateTime, Utc};
use common_types::ids::{PrincipalId, TenantId};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    Ok { rows_checked: u64 },
    Mismatch { audit_id: i64 },
}

/// Walk a tenant's chain (or the system chain when `tenant_id` is None)
/// and re-hash each row. Returns the first mismatch or Ok with the
/// total row count.
pub async fn verify_chain(
    pool: &PgPool,
    tenant_id: Option<TenantId>,
) -> Result<VerifyResult, ChainError> {
    let cp = crate::chain::get_checkpoint(pool, tenant_id).await?;
    let (mut prev, mut last_id) = match cp {
        Some(c) => (c.last_verified_hash, c.last_verified_audit_id),
        None => (GENESIS_PREV_HASH, 0),
    };
    let mut count: u64 = 0;
    loop {
        let rows: Vec<(
            i64,
            Option<Uuid>,
            Option<Uuid>,
            Option<Uuid>,
            String,
            Option<String>,
            Value,
            DateTime<Utc>,
            Vec<u8>,
            Vec<u8>,
        )> = match tenant_id {
            Some(tid) => sqlx::query_as(
                "SELECT audit_id, tenant_id, principal_id, jti, action, target, payload, \
                        occurred_at, prev_hash, hash \
                 FROM audit_log \
                 WHERE tenant_id = $1 AND audit_id > $2 \
                 ORDER BY audit_id ASC \
                 LIMIT 1000",
            )
            .bind(tid.as_uuid())
            .bind(last_id)
            .fetch_all(pool)
            .await?,
            None => sqlx::query_as(
                "SELECT audit_id, tenant_id, principal_id, jti, action, target, payload, \
                        occurred_at, prev_hash, hash \
                 FROM audit_log \
                 WHERE tenant_id IS NULL AND audit_id > $1 \
                 ORDER BY audit_id ASC \
                 LIMIT 1000",
            )
            .bind(last_id)
            .fetch_all(pool)
            .await?,
        };
        if rows.is_empty() {
            return Ok(VerifyResult::Ok { rows_checked: count });
        }
        for (id, tid, pid, jti, action, target, payload, ts, db_prev, db_hash) in rows {
            if db_prev != prev.as_slice() {
                return Ok(VerifyResult::Mismatch { audit_id: id });
            }
            let event = parse_action(&action).ok_or(ChainError::HashMismatch(id))?;
            let row = AuditRow {
                tenant_id: tid.map(TenantId::from_uuid_unchecked),
                principal_id: pid.map(PrincipalId::from_uuid_unchecked),
                jti,
                event,
                target,
                occurred_at: ts,
                payload,
            };
            let computed = next_hash(&prev, &row);
            if db_hash != computed.as_slice() {
                return Ok(VerifyResult::Mismatch { audit_id: id });
            }
            prev = computed;
            last_id = id;
            count += 1;
        }
    }
}

/// Verify the chain AND record a checkpoint at the highest audit_id
/// successfully verified. Used by the periodic verify job.
pub async fn verify_and_checkpoint(
    pool: &PgPool,
    tenant_id: Option<TenantId>,
) -> Result<VerifyResult, ChainError> {
    let result = verify_chain(pool, tenant_id).await?;
    if let VerifyResult::Ok { rows_checked } = &result {
        if *rows_checked > 0 {
            let last: Option<(i64, Vec<u8>)> = match tenant_id {
                Some(tid) => sqlx::query_as(
                    "SELECT audit_id, hash FROM audit_log \
                     WHERE tenant_id = $1 ORDER BY audit_id DESC LIMIT 1",
                )
                .bind(tid.as_uuid())
                .fetch_optional(pool)
                .await?,
                None => sqlx::query_as(
                    "SELECT audit_id, hash FROM audit_log \
                     WHERE tenant_id IS NULL ORDER BY audit_id DESC LIMIT 1",
                )
                .fetch_optional(pool)
                .await?,
            };
            if let Some((id, h)) = last {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&h);
                crate::chain::record_checkpoint(
                    pool,
                    tenant_id,
                    crate::chain::Checkpoint {
                        last_verified_audit_id: id,
                        last_verified_hash: hash,
                    },
                )
                .await?;
            }
        }
    }
    Ok(result)
}

fn parse_action(s: &str) -> Option<AuditEvent> {
    Some(match s {
        "TENANT_CREATE" => AuditEvent::TenantCreate,
        "TENANT_SUSPEND" => AuditEvent::TenantSuspend,
        "TENANT_RESUME" => AuditEvent::TenantResume,
        "TENANT_TERMINATE" => AuditEvent::TenantTerminate,
        "PRINCIPAL_CREATE" => AuditEvent::PrincipalCreate,
        "SECRET_CREATE" => AuditEvent::SecretCreate,
        "SECRET_DELETE" => AuditEvent::SecretDelete,
        "SECRET_READ" => AuditEvent::SecretRead,
        "CONNECTION_APPLY" => AuditEvent::ConnectionApply,
        "PIPELINE_APPLY" => AuditEvent::PipelineApply,
        "AUTH_LOGIN" => AuditEvent::AuthLogin,
        "AUTH_LOGIN_FAILED" => AuditEvent::AuthLoginFailed,
        "AUTH_REFRESH" => AuditEvent::AuthRefresh,
        "AUTH_LOGOUT" => AuditEvent::AuthLogout,
        "TOKEN_REVOKE" => AuditEvent::TokenRevoke,
        "TENANT_OVERRIDE" => AuditEvent::TenantOverride,
        _ => return None,
    })
}
