use chrono::{DateTime, Utc};
use common_types::ids::TenantId;
use sqlx::Postgres;

#[derive(Debug, Clone)]
pub struct Tenant {
    pub tenant_id: TenantId,
    pub name: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<TenantId> {
    let id = TenantId::new();
    sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
        .bind(id.as_uuid())
        .bind(name)
        .execute(&mut *conn)
        .await?;
    Ok(id)
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    id: TenantId,
) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(uuid::Uuid, String, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, status, created_at FROM tenants WHERE tenant_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(u, name, status, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(u),
        name,
        status,
        created_at,
    }))
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<Option<Tenant>> {
    let row: Option<(uuid::Uuid, String, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, status, created_at FROM tenants WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(u, name, status, created_at)| Tenant {
        tenant_id: TenantId::from_uuid_unchecked(u),
        name,
        status,
        created_at,
    }))
}

pub async fn list(conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Tenant>> {
    let rows: Vec<(uuid::Uuid, String, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT tenant_id, name, status, created_at FROM tenants ORDER BY created_at",
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(u, name, status, created_at)| Tenant {
            tenant_id: TenantId::from_uuid_unchecked(u),
            name,
            status,
            created_at,
        })
        .collect())
}

pub async fn delete(
    conn: &mut sqlx::PgConnection,
    id: TenantId,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[allow(dead_code)]
type _PostgresMarker = Postgres;
