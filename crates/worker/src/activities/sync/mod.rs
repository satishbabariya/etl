//! Phase I.2 sync activities, extended in Phase I.3 with WASM dispatch.

pub mod inputs;

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use catalog::Catalog;
use common_types::connection_config::ConnectionConfig;
use common_types::ids::{PipelineId, RunId};
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::dispatch::build_source_connector;
use crate::loaders::parquet_local::LocalParquetLoader;
use crate::wasm_runtime::WasmSourceRuntime;
use inputs::*;

pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
}

fn to_retryable(e: anyhow::Error) -> ActivityError {
    e.into()
}

fn encode_batch(batch: &RecordBatch) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    let schema = batch.schema();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema.as_ref())
            .context("StreamWriter::try_new")?;
        if batch.num_rows() > 0 {
            w.write(batch).context("StreamWriter::write")?;
        }
        w.finish().context("StreamWriter::finish")?;
    }
    Ok(BASE64.encode(&buf))
}

fn decode_batch(b64: &str) -> anyhow::Result<RecordBatch> {
    let bytes = BASE64.decode(b64).context("base64 decode")?;
    let mut reader = StreamReader::try_new(&*bytes, None).context("StreamReader::try_new")?;
    let batch = reader
        .next()
        .context("stream had no batches")?
        .context("decoding batch")?;
    Ok(batch)
}

#[activities]
impl SyncActivities {
    #[activity]
    pub async fn discover_stream(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverInput,
    ) -> Result<DiscoverOutput, ActivityError> {
        let connector =
            build_source_connector(&input.connector_ref, Some(self.wasm_runtime.clone()))
                .map_err(to_retryable)?;
        let schema = connector
            .discover(
                &ConnectionConfig { url: input.source_url.clone() },
                &input.source,
            )
            .await
            .map_err(to_retryable)?;

        let tenant_id = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
        let pipeline_id = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        let cursor_config = serde_json::json!({
            "column": input.cursor_column,
            "kind": input.cursor_kind,
        });
        let pk_config = serde_json::to_value(&input.pk_columns).unwrap_or(serde_json::json!([]));
        let stream_id = self
            .catalog
            .ensure_stream(catalog::stream::NewStream {
                tenant_id,
                pipeline_id,
                name: input.stream_name.clone(),
                sync_mode: "incremental".into(),
                cursor_config,
                pk_config,
                destination_table: None,
            })
            .await
            .map_err(|e| to_retryable(anyhow::anyhow!("ensure_stream: {e}")))?;

        let resolved = crate::schema_evolution::record_and_resolve(
            &self.catalog,
            tenant_id,
            stream_id,
            input.evolution_policy,
            schema.clone(),
        )
        .await
        .map_err(to_retryable)?;

        let columns = resolved.schema.fields().iter().map(|f| f.name().clone()).collect();
        Ok(DiscoverOutput {
            columns,
            schema_id: resolved.schema_id.as_uuid(),
            created_new_version: resolved.created_new_version,
        })
    }

    #[activity]
    pub async fn read_batch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadBatchInput,
    ) -> Result<ReadBatchOutput, ActivityError> {
        let connector =
            build_source_connector(&input.connector_ref, Some(self.wasm_runtime.clone()))
                .map_err(to_retryable)?;
        let outcome = connector
            .read_batch(
                &ConnectionConfig { url: input.source_url },
                &input.source,
                input.cursor,
                input.batch_size,
            )
            .await
            .map_err(to_retryable)?;

        let rows = outcome.batch.num_rows();
        let b64 = encode_batch(&outcome.batch).map_err(to_retryable)?;

        Ok(ReadBatchOutput {
            batch_ipc_b64: b64,
            rows,
            new_cursor: outcome.new_cursor,
            is_final: outcome.is_final,
        })
    }

    #[activity]
    pub async fn load_batch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: LoadBatchInput,
    ) -> Result<LoadBatchOutput, ActivityError> {
        let batch = decode_batch(&input.batch_ipc_b64).map_err(to_retryable)?;
        let load_id = LoadId {
            pipeline_id: PipelineId::from_uuid_unchecked(input.pipeline_id),
            run_id: RunId::from_uuid_unchecked(input.run_id),
            batch_seq: input.batch_seq,
        };
        let res = LocalParquetLoader
            .load(&input.destination, load_id, batch)
            .await
            .map_err(to_retryable)?;
        Ok(LoadBatchOutput {
            rows_loaded: res.rows_loaded,
            bytes_written: res.bytes_written,
            path: res.path,
        })
    }

    #[activity]
    pub async fn commit_cursor(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CommitCursorInput,
    ) -> Result<(), ActivityError> {
        let pid = PipelineId::from_uuid_unchecked(input.pipeline_id);
        let rid = Some(RunId::from_uuid_unchecked(input.run_id));
        self.catalog
            .upsert_stream_state(pid, &input.stream_name, input.cursor, rid)
            .await
            .map_err(|e| to_retryable(anyhow::anyhow!("upsert cursor: {e}")))?;
        Ok(())
    }
}
