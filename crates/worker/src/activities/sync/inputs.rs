use common_types::connection_config::ConnectionConfig;
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::{DestinationSpec, SourceSpec};
use common_types::transform::TransformSpec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_conn: ConnectionConfig,
    pub connector_ref: String,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
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
    pub source_conn: ConnectionConfig,
    pub cursor: Option<CursorValue>,
    pub batch_size: usize,
    pub connector_ref: String,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
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
    /// Per-batch stream override from the connector (multi-table CDC).
    /// None = use the pipeline-level stream_name in LoadBatchInput.
    #[serde(default)]
    pub stream_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchInput {
    pub destination: DestinationSpec,
    pub batch_ipc_b64: String,
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
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
    /// Per-batch stream override that the loader uses to build a
    /// per-stream sub-path. Empty string = no subdir. The workflow
    /// fills this from `read_out.stream_name` (multi-table CDC) or
    /// the pipeline-level stream_name (everything else).
    #[serde(default)]
    pub stream_name: String,
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
