use chrono::{DateTime, Utc};
use common_types::ids::{PrincipalId, TenantId};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RefreshTokenRow {
    pub token_id: Uuid,
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub hash: String,
    pub expires_at: DateTime<Utc>,
}

pub struct NewRefreshToken {
    pub tenant_id: TenantId,
    pub principal_id: PrincipalId,
    pub hash: String,
    pub expires_at: DateTime<Utc>,
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewRefreshToken,
) -> sqlx::Result<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO refresh_tokens (token_id, tenant_id, principal_id, hash, expires_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(new.tenant_id.as_uuid())
    .bind(new.principal_id.as_uuid())
    .bind(&new.hash)
    .bind(new.expires_at)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    token_id: Uuid,
) -> sqlx::Result<Option<RefreshTokenRow>> {
    let row: Option<(Uuid, Uuid, Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT token_id, tenant_id, principal_id, hash, expires_at \
         FROM refresh_tokens WHERE token_id = $1 AND expires_at > now()",
    )
    .bind(token_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(tid, ten, prn, hash, exp)| RefreshTokenRow {
        token_id: tid,
        tenant_id: TenantId::from_uuid_unchecked(ten),
        principal_id: PrincipalId::from_uuid_unchecked(prn),
        hash,
        expires_at: exp,
    }))
}

pub async fn delete(
    conn: &mut sqlx::PgConnection,
    token_id: Uuid,
) -> sqlx::Result<u64> {
    let r = sqlx::query("DELETE FROM refresh_tokens WHERE token_id = $1")
        .bind(token_id)
        .execute(&mut *conn)
        .await?;
    Ok(r.rows_affected())
}
