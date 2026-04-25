use chrono::{DateTime, Utc};
use common_types::ids::{PipelineId, SchemaId, StreamId, TenantId};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Stream {
    pub stream_id: StreamId,
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub name: String,
    pub sync_mode: String,
    pub cursor_config: Value,
    pub pk_config: Value,
    pub destination_table: Option<String>,
    pub current_schema_id: Option<SchemaId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewStream {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub name: String,
    pub sync_mode: String,
    pub cursor_config: Value,
    pub pk_config: Value,
    pub destination_table: Option<String>,
}

/// Idempotent: if a stream with (pipeline_id, name) exists, returns its id.
pub async fn ensure(conn: &mut sqlx::PgConnection, new: NewStream) -> sqlx::Result<StreamId> {
    if let Some(existing) = get_by_name(conn, new.pipeline_id, &new.name).await? {
        return Ok(existing.stream_id);
    }
    let id = StreamId::new();
    sqlx::query(
        "INSERT INTO streams \
           (stream_id, tenant_id, pipeline_id, name, sync_mode, cursor_config, pk_config, destination_table) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (pipeline_id, name) DO NOTHING",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.pipeline_id.as_uuid())
    .bind(&new.name)
    .bind(&new.sync_mode)
    .bind(&new.cursor_config)
    .bind(&new.pk_config)
    .bind(&new.destination_table)
    .execute(&mut *conn)
    .await?;
    Ok(get_by_name(conn, new.pipeline_id, &new.name)
        .await?
        .expect("inserted or conflicted row must exist")
        .stream_id)
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
    name: &str,
) -> sqlx::Result<Option<Stream>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        uuid::Uuid,
        String,
        String,
        Value,
        Value,
        Option<String>,
        Option<uuid::Uuid>,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT stream_id, tenant_id, pipeline_id, name, sync_mode, cursor_config, \
                pk_config, destination_table, current_schema_id, created_at, updated_at \
         FROM streams WHERE pipeline_id = $1 AND name = $2",
    )
    .bind(pipeline_id.as_uuid())
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(sid, tid, pid, name, mode, cur, pk, dest, cs, c, u)| Stream {
        stream_id: StreamId::from_uuid_unchecked(sid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        name,
        sync_mode: mode,
        cursor_config: cur,
        pk_config: pk,
        destination_table: dest,
        current_schema_id: cs.map(SchemaId::from_uuid_unchecked),
        created_at: c,
        updated_at: u,
    }))
}

pub async fn set_current_schema(
    conn: &mut sqlx::PgConnection,
    stream_id: StreamId,
    schema_id: SchemaId,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE streams SET current_schema_id = $1, updated_at = NOW() WHERE stream_id = $2",
    )
    .bind(schema_id.as_uuid())
    .bind(stream_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}
