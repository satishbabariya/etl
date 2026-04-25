use chrono::{DateTime, Utc};
use common_types::ids::{ConnectionId, PipelineId, TenantId};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub pipeline_id: PipelineId,
    pub tenant_id: TenantId,
    pub name: String,
    pub source_conn_id: ConnectionId,
    pub dest_conn_id: Option<ConnectionId>,
    pub spec: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewPipeline {
    pub tenant_id: TenantId,
    pub name: String,
    pub source_conn_id: ConnectionId,
    pub dest_conn_id: Option<ConnectionId>,
    pub spec: Value,
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewPipeline,
) -> sqlx::Result<PipelineId> {
    let workspace_id = crate::workspace::ensure_default(conn, new.tenant_id).await?;
    let id = PipelineId::new();
    sqlx::query(
        "INSERT INTO pipelines (pipeline_id, tenant_id, workspace_id, name, source_conn_id, dest_conn_id, spec) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(workspace_id.as_uuid())
    .bind(&new.name)
    .bind(new.source_conn_id.as_uuid())
    .bind(new.dest_conn_id.map(|d| d.as_uuid()))
    .bind(&new.spec)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    id: PipelineId,
) -> sqlx::Result<Option<Pipeline>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        String,
        uuid::Uuid,
        Option<uuid::Uuid>,
        Value,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT pipeline_id, tenant_id, name, source_conn_id, dest_conn_id, spec, created_at, updated_at \
         FROM pipelines WHERE pipeline_id = $1",
    )
    .bind(id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(pid, tid, name, src, dst, spec, c, u)| Pipeline {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        source_conn_id: ConnectionId::from_uuid_unchecked(src),
        dest_conn_id: dst.map(ConnectionId::from_uuid_unchecked),
        spec,
        created_at: c,
        updated_at: u,
    }))
}
