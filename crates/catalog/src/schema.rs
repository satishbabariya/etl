use chrono::{DateTime, Utc};
use common_types::evolution::ChangeKind;
use common_types::ids::{SchemaId, StreamId, TenantId};
use common_types::schema_fingerprint::SchemaFingerprint;
use serde_json::Value;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct Schema {
    pub schema_id: SchemaId,
    pub tenant_id: TenantId,
    pub stream_id: StreamId,
    pub version: i32,
    pub parent_schema_id: Option<SchemaId>,
    pub fingerprint: SchemaFingerprint,
    pub arrow_schema_json: Value,
    pub change_summary: Vec<ChangeKind>,
    pub detected_at: DateTime<Utc>,
    pub applied_to_destination_at: Option<DateTime<Utc>>,
}

pub struct NewSchema {
    pub tenant_id: TenantId,
    pub stream_id: StreamId,
    pub parent_schema_id: Option<SchemaId>,
    pub fingerprint: SchemaFingerprint,
    pub arrow_schema_json: Value,
    pub change_summary: Vec<ChangeKind>,
}

pub async fn insert(pool: &PgPool, new: NewSchema) -> sqlx::Result<SchemaId> {
    let mut tx = pool.begin().await?;
    let next_version: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM schemas WHERE stream_id = $1",
    )
    .bind(new.stream_id.as_uuid())
    .fetch_one(&mut *tx)
    .await?;

    let id = SchemaId::new();
    let change_summary_json =
        serde_json::to_value(&new.change_summary).expect("serialize Vec<ChangeKind>");
    sqlx::query(
        "INSERT INTO schemas \
           (schema_id, tenant_id, stream_id, version, parent_schema_id, fingerprint, arrow_schema_json, change_summary) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(new.stream_id.as_uuid())
    .bind(next_version)
    .bind(new.parent_schema_id.map(|p| p.as_uuid()))
    .bind(new.fingerprint.as_hex())
    .bind(&new.arrow_schema_json)
    .bind(&change_summary_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id)
}

pub async fn get_latest(pool: &PgPool, stream_id: StreamId) -> sqlx::Result<Option<Schema>> {
    let row: Option<(
        uuid::Uuid,
        uuid::Uuid,
        uuid::Uuid,
        i32,
        Option<uuid::Uuid>,
        String,
        Value,
        Value,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    )> = sqlx::query_as(
        "SELECT schema_id, tenant_id, stream_id, version, parent_schema_id, fingerprint, \
                arrow_schema_json, change_summary, detected_at, applied_to_destination_at \
         FROM schemas WHERE stream_id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(stream_id.as_uuid())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(sid, tid, stid, v, parent, fp, j, chg, d, app)| Schema {
        schema_id: SchemaId::from_uuid_unchecked(sid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        stream_id: StreamId::from_uuid_unchecked(stid),
        version: v,
        parent_schema_id: parent.map(SchemaId::from_uuid_unchecked),
        fingerprint: SchemaFingerprint::from_hex(fp),
        arrow_schema_json: j,
        change_summary: serde_json::from_value(chg).unwrap_or_default(),
        detected_at: d,
        applied_to_destination_at: app,
    }))
}
