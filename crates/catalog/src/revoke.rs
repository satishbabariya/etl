use chrono::{DateTime, Utc};
use common_types::ids::TenantId;
use uuid::Uuid;

pub async fn insert(
    conn: &mut sqlx::PgConnection,
    jti: Uuid,
    tenant_id: TenantId,
    exp: DateTime<Utc>,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO revoked_tokens (jti, tenant_id, exp) VALUES ($1, $2, $3) \
         ON CONFLICT (jti) DO NOTHING",
    )
    .bind(jti)
    .bind(tenant_id.as_uuid())
    .bind(exp)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn is_revoked(
    conn: &mut sqlx::PgConnection,
    jti: Uuid,
) -> sqlx::Result<bool> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT jti FROM revoked_tokens WHERE jti = $1 AND exp > now()",
    )
    .bind(jti)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.is_some())
}

pub async fn prune_expired(conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
    let r = sqlx::query("DELETE FROM revoked_tokens WHERE exp <= now()")
        .execute(&mut *conn)
        .await?;
    Ok(r.rows_affected())
}
