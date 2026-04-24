use anyhow::Context;
use catalog::Catalog;
use common_types::ids::PipelineId;
use serde::Serialize;

#[derive(Serialize)]
pub struct PipelineStatusRow {
    pub pipeline_id: String,
    pub latest_run_id: Option<String>,
    pub latest_run_status: Option<String>,
    pub latest_cursor: Option<serde_json::Value>,
    pub slot_name: Option<String>,
    pub slot_state: Option<String>,
    pub confirmed_flush: Option<String>,
}

pub async fn run(id_str: String) -> anyhow::Result<()> {
    let pid: PipelineId = id_str.parse().with_context(|| format!("parsing {id_str}"))?;
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;

    let latest: Option<(uuid::Uuid, String)> = sqlx::query_as(
        "SELECT run_id, status FROM runs WHERE pipeline_id = $1 \
         ORDER BY started_at DESC LIMIT 1",
    )
    .bind(pid.as_uuid())
    .fetch_optional(catalog.pool())
    .await?;

    let latest_cursor_raw: Option<(String, String)> = sqlx::query_as(
        "SELECT cursor_kind, cursor_value FROM stream_state WHERE pipeline_id = $1 \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(pid.as_uuid())
    .fetch_optional(catalog.pool())
    .await?;

    let latest_cursor = latest_cursor_raw.map(|(kind, value)| {
        serde_json::json!({ "kind": kind, "value": value })
    });

    let slot: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT slot_name, state, confirmed_flush FROM cdc_slots WHERE pipeline_id = $1",
    )
    .bind(pid.as_uuid())
    .fetch_optional(catalog.pool())
    .await?;

    let row = PipelineStatusRow {
        pipeline_id: pid.to_string(),
        latest_run_id: latest.as_ref().map(|(r, _)| r.to_string()),
        latest_run_status: latest.as_ref().map(|(_, s)| s.clone()),
        latest_cursor,
        slot_name: slot.as_ref().map(|(n, _, _)| n.clone()),
        slot_state: slot.as_ref().map(|(_, s, _)| s.clone()),
        confirmed_flush: slot.as_ref().and_then(|(_, _, f)| f.clone()),
    };
    println!("{}", serde_json::to_string_pretty(&row)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_row_serializes_without_nulls_only_when_populated() {
        let r = PipelineStatusRow {
            pipeline_id: "pipe-deadbeef-0000-0000-0000-000000000000".into(),
            latest_run_id: Some("run-abcdef".into()),
            latest_run_status: Some("completed".into()),
            latest_cursor: Some(serde_json::json!({"kind":"int64","value":"42"})),
            slot_name: None,
            slot_state: None,
            confirmed_flush: None,
        };
        let j = serde_json::to_value(&r).unwrap();
        assert_eq!(j["latest_run_status"], "completed");
        assert_eq!(j["slot_name"], serde_json::Value::Null);
    }
}
