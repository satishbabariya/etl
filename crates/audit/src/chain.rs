//! Per-tenant hash chain. Each row's hash = SHA256(prev_hash || row.canonical_bytes()).
//! Genesis prev_hash = 32 bytes of 0x00. Writes are linearized with
//! `pg_advisory_xact_lock(...)` keyed by the tenant_id (or 0 for the
//! system chain) so concurrent inserts within a tenant serialize while
//! cross-tenant inserts don't.

use crate::event::AuditRow;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

pub const GENESIS_PREV_HASH: [u8; 32] = [0u8; 32];

#[derive(thiserror::Error, Debug)]
pub enum ChainError {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error("hash mismatch at row id={0}")]
    HashMismatch(i64),
}

pub struct AuditWriter;

pub fn next_hash(prev: &[u8; 32], row: &AuditRow) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(prev);
    h.update(row.canonical_bytes());
    h.finalize().into()
}

/// Stable lock key for a tenant chain. NULL tenant_id (system chain)
/// uses 0; tenant chains hash the UUID into an i64.
fn lock_key(tenant_id: Option<common_types::ids::TenantId>) -> i64 {
    match tenant_id {
        None => 0,
        Some(t) => {
            let uuid = t.as_uuid();
            let bytes = uuid.as_bytes();
            let mut h: [u8; 8] = [0; 8];
            h.copy_from_slice(&bytes[..8]);
            i64::from_be_bytes(h)
        }
    }
}

/// Insert one audit row using a fresh transaction. Linearizes per
/// tenant via `pg_advisory_xact_lock(<derived key>)` so concurrent
/// writes for the same tenant serialize.
pub async fn write_event(pool: &PgPool, row: &AuditRow) -> Result<i64, ChainError> {
    let mut tx = pool.begin().await?;
    let key = lock_key(row.tenant_id);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *tx)
        .await?;

    // Find the latest row in this chain (per-tenant or system).
    let prev: Option<(Vec<u8>,)> = match row.tenant_id {
        Some(tid) => {
            sqlx::query_as(
                "SELECT hash FROM audit_log \
                 WHERE tenant_id = $1 \
                 ORDER BY audit_id DESC LIMIT 1",
            )
            .bind(tid.as_uuid())
            .fetch_optional(&mut *tx)
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT hash FROM audit_log \
                 WHERE tenant_id IS NULL \
                 ORDER BY audit_id DESC LIMIT 1",
            )
            .fetch_optional(&mut *tx)
            .await?
        }
    };
    let prev_arr: [u8; 32] = match prev {
        None => GENESIS_PREV_HASH,
        Some((b,)) => {
            let mut a = [0u8; 32];
            a.copy_from_slice(&b);
            a
        }
    };
    let hash = next_hash(&prev_arr, row);
    let id_row: (i64,) = sqlx::query_as(
        "INSERT INTO audit_log \
           (tenant_id, principal_id, jti, action, target, payload, occurred_at, prev_hash, hash) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         RETURNING audit_id",
    )
    .bind(row.tenant_id.map(|t| t.as_uuid()))
    .bind(row.principal_id.map(|p| p.as_uuid()))
    .bind(row.jti)
    .bind(row.event.as_action_str())
    .bind(row.target.as_deref())
    .bind(&row.payload)
    .bind(row.occurred_at)
    .bind(prev_arr.as_slice())
    .bind(hash.as_slice())
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id_row.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{AuditEvent, AuditRow};
    use chrono::DateTime;
    use common_types::ids::TenantId;
    use serde_json::json;
    use uuid::Uuid;

    fn fake_row(tag: &str) -> AuditRow {
        AuditRow {
            tenant_id: Some(TenantId::from_uuid_unchecked(
                Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            )),
            principal_id: None,
            jti: None,
            event: AuditEvent::SecretCreate,
            target: Some(tag.to_string()),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            payload: json!({"tag": tag}),
        }
    }

    #[test]
    fn next_hash_is_stable() {
        let row = fake_row("a");
        let h1 = next_hash(&GENESIS_PREV_HASH, &row);
        let h2 = next_hash(&GENESIS_PREV_HASH, &row);
        assert_eq!(h1, h2);
    }

    #[test]
    fn next_hash_differs_for_different_rows() {
        let a = next_hash(&GENESIS_PREV_HASH, &fake_row("a"));
        let b = next_hash(&GENESIS_PREV_HASH, &fake_row("b"));
        assert_ne!(a, b);
    }

    #[test]
    fn next_hash_differs_when_prev_changes() {
        let row = fake_row("a");
        let h1 = next_hash(&GENESIS_PREV_HASH, &row);
        let prev2 = [1u8; 32];
        let h2 = next_hash(&prev2, &row);
        assert_ne!(h1, h2);
    }
}
