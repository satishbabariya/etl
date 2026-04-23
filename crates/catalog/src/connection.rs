use chrono::{DateTime, Utc};
use common_types::ids::{ConnectionId, TenantId};
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Connection {
    pub connection_id: ConnectionId,
    pub tenant_id: TenantId,
    pub name: String,
    pub connector_ref: String,
    pub config: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewConnection {
    pub tenant_id: TenantId,
    pub name: String,
    pub connector_ref: String,
    pub config: Value,
}

pub async fn create(pool: &PgPool, new: NewConnection) -> sqlx::Result<ConnectionId> {
    // Auto-resolve the tenant's default workspace for denormalization.
    let workspace_id = crate::workspace::ensure_default(pool, new.tenant_id).await?;
    let id = ConnectionId::new();
    sqlx::query(
        "INSERT INTO connections (connection_id, tenant_id, workspace_id, name, connector_ref, config) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(workspace_id.as_uuid())
    .bind(&new.name)
    .bind(&new.connector_ref)
    .bind(&new.config)
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn get(pool: &PgPool, id: ConnectionId) -> sqlx::Result<Option<Connection>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        Value,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT connection_id, tenant_id, name, connector_ref, config, created_at, updated_at \
         FROM connections WHERE connection_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(cid, tid, name, connector_ref, config, c, u)| Connection {
        connection_id: ConnectionId::from_uuid_unchecked(cid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        connector_ref,
        config,
        created_at: c,
        updated_at: u,
    }))
}
