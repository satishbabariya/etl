use chrono::{DateTime, Utc};
use common_types::cursor::{CursorKind, CursorValue};
use common_types::ids::{PipelineId, RunId};

#[derive(Debug, Clone)]
pub struct StreamState {
    pub pipeline_id: PipelineId,
    pub stream_name: String,
    pub cursor: Option<CursorValue>,
    pub last_run_id: Option<RunId>,
    pub updated_at: DateTime<Utc>,
}

fn kind_str(k: CursorKind) -> &'static str {
    match k {
        CursorKind::Int64 => "int64",
        CursorKind::TimestampTz => "timestamptz",
        CursorKind::Lsn => "lsn",
        CursorKind::Gtid => "gtid",
        CursorKind::SnapshotPk => "snapshot_pk",
    }
}

fn parse_kind(s: &str) -> CursorKind {
    match s {
        "int64" => CursorKind::Int64,
        "timestamptz" => CursorKind::TimestampTz,
        "lsn" => CursorKind::Lsn,
        "gtid" => CursorKind::Gtid,
        "snapshot_pk" => CursorKind::SnapshotPk,
        other => panic!("unknown cursor_kind in DB: {other}"),
    }
}

pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    tenant_id: common_types::ids::TenantId,
    pipeline_id: PipelineId,
    stream_name: &str,
    cursor: Option<CursorValue>,
    last_run_id: Option<RunId>,
) -> sqlx::Result<()> {
    let (kind, value) = match cursor {
        Some(c) => (kind_str(c.kind).to_string(), Some(c.value)),
        None => ("int64".to_string(), None),
    };
    sqlx::query(
        "INSERT INTO stream_state (pipeline_id, tenant_id, stream_name, cursor_kind, cursor_value, last_run_id, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
         ON CONFLICT (pipeline_id, stream_name) DO UPDATE SET \
           cursor_kind = EXCLUDED.cursor_kind, \
           cursor_value = EXCLUDED.cursor_value, \
           last_run_id = COALESCE(EXCLUDED.last_run_id, stream_state.last_run_id), \
           updated_at = NOW()",
    )
    .bind(pipeline_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(stream_name)
    .bind(kind)
    .bind(value)
    .bind(last_run_id.map(|r| r.as_uuid()))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn get(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
    stream_name: &str,
) -> sqlx::Result<Option<StreamState>> {
    let row: Option<(
        uuid::Uuid,
        String,
        String,
        Option<String>,
        Option<uuid::Uuid>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT pipeline_id, stream_name, cursor_kind, cursor_value, last_run_id, updated_at \
         FROM stream_state WHERE pipeline_id = $1 AND stream_name = $2",
    )
    .bind(pipeline_id.as_uuid())
    .bind(stream_name)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(pid, name, kind, val, lrid, ts)| StreamState {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        stream_name: name,
        cursor: val.map(|value| CursorValue {
            kind: parse_kind(&kind),
            value,
        }),
        last_run_id: lrid.map(RunId::from_uuid_unchecked),
        updated_at: ts,
    }))
}
