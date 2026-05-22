use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::DestinationSpec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnsureSlotInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnsureSlotOutput {
    pub slot_name: String,
    pub publication_name: String,
    pub consistent_point: String,
    pub created: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotChunkInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub run_id: Uuid,
    pub batch_seq: u32,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
    pub pk_col: String,
    pub last_pk: Option<i64>,
    pub batch_size: usize,
    pub consistent_point: String,
    pub destination: DestinationSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotChunkOutput {
    pub rows: usize,
    pub is_final: bool,
    pub last_pk: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadWindowInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub run_id: Uuid,
    pub batch_seq: u32,
    pub source_conn: ConnectionConfig,
    pub slot_name: String,
    pub publication_name: String,
    pub start_lsn: Option<String>,
    pub max_events: usize,
    pub table_rel_name: String,
    pub destination: DestinationSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadWindowOutput {
    pub rows: usize,
    pub new_lsn: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReleaseSlotInput {
    pub pipeline_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    pub slot_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSnapshotStateGetInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSnapshotStateGetOutput {
    pub last_pk: Option<i64>,
    pub completed: bool,
    pub captured_position: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSnapshotMarkCompletedInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvanceSlotInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    /// Logical replication slot name to advance. Callers derive this as
    /// `format!("etl_{}", pipeline_id.as_simple())` — same formula as
    /// `ensure_slot`.
    pub slot_name: String,
    /// Target LSN in Postgres text format, e.g. `"0/1A2B3C4"`. The destination
    /// has durably persisted everything up to and including this position.
    pub target_lsn: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvanceSlotOutput {
    /// The slot's `confirmed_flush_lsn` after the advance, as reported by
    /// `pg_replication_slots`.
    pub confirmed_flush_lsn: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_slot_input_roundtrips_serde() {
        let input = AdvanceSlotInput {
            pipeline_id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            principal_id: Uuid::nil(),
            jti: Uuid::nil(),
            source_conn: ConnectionConfig::from_url("postgres://localhost/test".to_string()),
            slot_name: "etl_abc".into(),
            target_lsn: "0/1A2B3C4".into(),
        };
        let j = serde_json::to_string(&input).unwrap();
        let back: AdvanceSlotInput = serde_json::from_str(&j).unwrap();
        assert_eq!(back.slot_name, "etl_abc");
        assert_eq!(back.target_lsn, "0/1A2B3C4");
    }

    #[test]
    fn advance_slot_output_roundtrips_serde() {
        let out = AdvanceSlotOutput { confirmed_flush_lsn: "0/1A2B3C4".into() };
        let j = serde_json::to_string(&out).unwrap();
        let back: AdvanceSlotOutput = serde_json::from_str(&j).unwrap();
        assert_eq!(back.confirmed_flush_lsn, "0/1A2B3C4");
    }
}
