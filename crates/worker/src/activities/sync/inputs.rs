use common_types::cursor::CursorValue;
use common_types::pipeline_spec::{DestinationSpec, SourceSpec};
use common_types::transform::TransformSpec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub connector_ref: String,
    pub tenant_id: Uuid,
    pub stream_name: String,
    pub pipeline_id: Uuid,
    pub cursor_column: String,
    pub cursor_kind: common_types::cursor::CursorKind,
    pub pk_columns: Vec<String>,
    pub evolution_policy: common_types::evolution::EvolutionPolicy,
    #[serde(default)]
    pub transform: Option<TransformSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverOutput {
    pub columns: Vec<String>,
    pub schema_id: Uuid,
    pub created_new_version: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub cursor: Option<CursorValue>,
    pub batch_size: usize,
    pub connector_ref: String,
    #[serde(default)]
    pub transform: Option<TransformSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchOutput {
    /// Base64-encoded Arrow IPC stream. Empty when rows == 0.
    pub batch_ipc_b64: String,
    pub rows: usize,
    pub new_cursor: Option<CursorValue>,
    pub is_final: bool,
    #[serde(default)]
    pub rejected_ipc_b64: Option<String>,
    #[serde(default)]
    pub rows_rejected: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchInput {
    pub destination: DestinationSpec,
    pub batch_ipc_b64: String,
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub batch_seq: u32,
    #[serde(default)]
    pub rejected_ipc_b64: Option<String>,
    #[serde(default)]
    pub dead_letter_threshold: f64,
    #[serde(default)]
    pub rows_rejected_so_far: usize,
    #[serde(default)]
    pub rows_total_so_far: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchOutput {
    pub rows_loaded: usize,
    pub bytes_written: u64,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitCursorInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    pub run_id: Uuid,
    pub stream_name: String,
    pub cursor: Option<CursorValue>,
}
