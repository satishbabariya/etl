//! Loader SDK — Rust-native trait for destination loaders (RFC-9).
//!
//! Phase I.2: Direct Append pattern only. Phase II.3 adds MERGE-on-commit,
//! Apply Change Stream, and Append-Only Event Log variants.

use arrow::record_batch::RecordBatch;
use common_types::ids::{PipelineId, RunId, TenantId};
use common_types::pipeline_spec::DestinationSpec;
use serde::{Deserialize, Serialize};

#[async_trait::async_trait]
pub trait DestinationLoader: Send + Sync {
    /// Cheap sanity-check (paths exist, credentials valid, etc.).
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()>;

    /// Idempotent write. Same `load_id` twice MUST produce the same durable
    /// state (overwrite, or no-op if already landed). Retries are safe.
    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult>;
}

/// Deterministic identifier for a single loaded batch.
/// Same `(tenant_id, pipeline_id, run_id, batch_seq)` tuple ⇒ same artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadId {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub run_id: RunId,
    pub batch_seq: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadResult {
    pub rows_loaded: usize,
    pub bytes_written: u64,
    /// Destination-specific path/URI (for logs and manual inspection).
    pub path: String,
}
