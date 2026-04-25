use common_types::ids::{PipelineId, TenantId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotState {
    Active,
    Paused,
    Released,
}

#[derive(Debug, Clone)]
pub struct CdcSlot {
    pub pipeline_id: PipelineId,
    pub tenant_id: TenantId,
    pub slot_name: String,
    pub publication_name: String,
    pub consistent_point: String,
    pub confirmed_flush: Option<String>,
    pub state: SlotState,
}

pub async fn upsert(conn: &mut sqlx::PgConnection, slot: &CdcSlot) -> sqlx::Result<()> {
    let state_s = match slot.state {
        SlotState::Active => "active",
        SlotState::Paused => "paused",
        SlotState::Released => "released",
    };
    sqlx::query(
        "INSERT INTO cdc_slots(pipeline_id, tenant_id, slot_name, publication_name, \
          consistent_point, confirmed_flush, state, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7, now()) \
         ON CONFLICT (pipeline_id) DO UPDATE SET \
           slot_name=EXCLUDED.slot_name, \
           publication_name=EXCLUDED.publication_name, \
           consistent_point=EXCLUDED.consistent_point, \
           confirmed_flush=COALESCE(EXCLUDED.confirmed_flush, cdc_slots.confirmed_flush), \
           state=EXCLUDED.state, \
           updated_at=now()",
    )
    .bind(slot.pipeline_id.as_uuid())
    .bind(slot.tenant_id.as_uuid())
    .bind(&slot.slot_name)
    .bind(&slot.publication_name)
    .bind(&slot.consistent_point)
    .bind(&slot.confirmed_flush)
    .bind(state_s)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn get(conn: &mut sqlx::PgConnection, pipeline_id: PipelineId) -> sqlx::Result<Option<CdcSlot>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT pipeline_id, tenant_id, slot_name, publication_name, consistent_point, confirmed_flush, state \
         FROM cdc_slots WHERE pipeline_id = $1",
    )
    .bind(pipeline_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(pid, tid, sn, pn, cp, cf, st)| CdcSlot {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        slot_name: sn,
        publication_name: pn,
        consistent_point: cp,
        confirmed_flush: cf,
        state: match st.as_str() {
            "active" => SlotState::Active,
            "paused" => SlotState::Paused,
            "released" => SlotState::Released,
            other => panic!("unknown slot state {other}"),
        },
    }))
}

pub async fn update_confirmed_flush(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
    lsn: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE cdc_slots SET confirmed_flush=$1, updated_at=now() WHERE pipeline_id=$2",
    )
    .bind(lsn)
    .bind(pipeline_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}
