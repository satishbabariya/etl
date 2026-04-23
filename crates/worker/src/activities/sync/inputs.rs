use common_types::cursor::CursorValue;
use common_types::pipeline_spec::{DestinationSpec, SourceSpec};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub connector_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverOutput {
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub cursor: Option<CursorValue>,
    pub batch_size: usize,
    pub connector_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchOutput {
    /// Base64-encoded Arrow IPC stream. Empty when rows == 0.
    pub batch_ipc_b64: String,
    pub rows: usize,
    pub new_cursor: Option<CursorValue>,
    pub is_final: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchInput {
    pub destination: DestinationSpec,
    pub batch_ipc_b64: String,
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub batch_seq: u32,
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
    pub run_id: Uuid,
    pub stream_name: String,
    pub cursor: Option<CursorValue>,
}
