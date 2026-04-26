//! Connector SDK — traits for source connectors (RFC-6).
//!
//! Phase I.2 shape: in-process Rust implementations only. Phase I.3 adds
//! WASM Component Model packaging on top of these traits.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common_types::connection_config::ConnectionConfig;
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::SourceSpec;

/// A source connector: given a connection + source config, emit Arrow batches
/// from a cursor position.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    /// Introspect the source and return its Arrow schema.
    async fn discover(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
    ) -> anyhow::Result<SchemaRef>;

    /// Read up to `batch_size` rows strictly after `cursor`.
    async fn read_batch(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
        cursor: Option<CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome>;
}

/// Result of a single `read_batch` call.
pub struct ReadOutcome {
    /// Always valid — empty batches are encoded by `rows == 0`, not a null batch.
    pub batch: RecordBatch,
    /// Cursor value of the *last* row in the batch. None if the batch is empty.
    pub new_cursor: Option<CursorValue>,
    /// True if fewer than `batch_size` rows were returned — indicates the source
    /// has no more data at this moment.
    pub is_final: bool,
}

/// Canonical Component Model definition for source connectors. Host-side
/// `bindgen!` and guest-side `wit_bindgen::generate!` both consume this file.
pub const WIT_PATH: &str = "crates/connector-sdk/wit/source-connector.wit";

pub mod templates;
