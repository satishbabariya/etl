use chrono::{DateTime, Utc};
use common_types::ids::TenantId;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Tenant {
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

pub async fn create(pool: &PgPool, name: &str) -> sqlx::Result<TenantId> {
    let id = TenantId::new();
    sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
        .bind(id.as_uuid())
        .bind(name)
        .execute(pool)
        .await?;
    Ok(id)
}

pub async fn get(pool: &PgPool, id: TenantId) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(uuid::Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, created_at FROM tenants WHERE tenant_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(u, name, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(u),
        name,
        created_at,
    }))
}
