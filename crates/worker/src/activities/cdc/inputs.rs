use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::DestinationSpec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnsureSlotInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
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
    pub source_conn: ConnectionConfig,
    pub slot_name: String,
}
