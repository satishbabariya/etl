use common_types::ids::{PipelineId, TenantId};

#[derive(Debug, Clone)]
pub struct CdcSnapshotState {
    pub pipeline_id: PipelineId,
    pub tenant_id: TenantId,
    pub last_pk: Option<i64>,
    pub completed: bool,
    pub captured_position: String,
}

/// Insert or update snapshot state for a pipeline. Used after each
/// snapshot chunk to checkpoint progress and once at completion.
pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    state: &CdcSnapshotState,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO cdc_snapshots(pipeline_id, tenant_id, last_pk, completed, captured_position, updated_at) \
         VALUES ($1,$2,$3,$4,$5, now()) \
         ON CONFLICT (pipeline_id) DO UPDATE SET \
           last_pk = EXCLUDED.last_pk, \
           completed = EXCLUDED.completed, \
           captured_position = EXCLUDED.captured_position, \
           updated_at = now()",
    )
    .bind(state.pipeline_id.as_uuid())
    .bind(state.tenant_id.as_uuid())
    .bind(state.last_pk)
    .bind(state.completed)
    .bind(&state.captured_position)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Fetch snapshot state for a pipeline. Returns None if no snapshot
/// has been started for this pipeline (typical for first run).
pub async fn get(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
) -> sqlx::Result<Option<CdcSnapshotState>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, Option<i64>, bool, String)> = sqlx::query_as(
        "SELECT pipeline_id, tenant_id, last_pk, completed, captured_position \
         FROM cdc_snapshots WHERE pipeline_id = $1",
    )
    .bind(pipeline_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(pid, tid, last_pk, completed, cp)| CdcSnapshotState {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        last_pk,
        completed,
        captured_position: cp,
    }))
}

/// Mark snapshot complete; idempotent.
pub async fn mark_completed(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE cdc_snapshots SET completed = true, updated_at = now() \
         WHERE pipeline_id = $1",
    )
    .bind(pipeline_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}
