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
use metrics;
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::dispatch::build_source_connector;
use crate::loaders::parquet_local::LocalParquetLoader;
use crate::loaders::postgres::PostgresLoader;

fn loader_name(dest: &common_types::pipeline_spec::DestinationSpec) -> &'static str {
    match dest {
        common_types::pipeline_spec::DestinationSpec::LocalParquet(_) => "local_parquet",
        common_types::pipeline_spec::DestinationSpec::Postgres(_) => "postgres",
    }
}
use crate::wasm_runtime::{WasmScalarRuntime, WasmSourceRuntime};
use inputs::*;

#[derive(Clone)]
pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
    pub scalar_runtime: Arc<WasmScalarRuntime>,
    pub secrets: Arc<crate::secrets::auditing::AuditingSecrets>,
    /// Best-effort metering event emitter. Errors are logged and ignored;
    /// activities never fail due to a metering write failure.
    pub metering: Arc<dyn metering::MeteringSink>,
}

fn to_retryable(e: anyhow::Error) -> ActivityError {
    // Surface the inner error to logs before it's wrapped by Temporal's
    // "Activity task failed" envelope. Without this, CI runs that fail
    // inside read_batch / discover_stream have no way to learn why.
    tracing::error!(error = %e, error_chain = ?e, "activity returning retryable error");
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
        let resolve_ctx = crate::secrets::auditing::ResolveContext {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            principal_id: (!input.principal_id.is_nil())
                .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
            jti: (!input.jti.is_nil()).then_some(input.jti),
        };
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            resolve_ctx,
        )
        .await
        .map_err(to_retryable)?;
        let discovered_schema = connector
            .discover(&resolved, &input.source)
            .await
            .map_err(to_retryable)?;

        // Phase I.5: if a transform is configured, record the DERIVED schema.
        let final_schema: arrow::datatypes::SchemaRef = match &input.transform {
            Some(spec) if !spec.operators.is_empty() => {
                let derived = crate::transform::derive_schema(
                    discovered_schema.as_ref(),
                    &spec.operators,
                )
                .map_err(to_retryable)?;
                std::sync::Arc::new(derived)
            }
            _ => discovered_schema,
        };

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
            final_schema.clone(),
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
        let resolve_ctx = crate::secrets::auditing::ResolveContext {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            principal_id: (!input.principal_id.is_nil())
                .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
            jti: (!input.jti.is_nil()).then_some(input.jti),
        };
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            resolve_ctx,
        )
        .await
        .map_err(to_retryable)?;
        let outcome = connector
            .read_batch(
                &resolved,
                &input.source,
                input.cursor,
                input.batch_size,
            )
            .await
            .map_err(to_retryable)?;

        let (kept_batch, rejected_batch) = match &input.transform {
            Some(spec) if !spec.operators.is_empty() => {
                let tx = crate::transform::apply(
                    outcome.batch,
                    &spec.operators,
                    &self.scalar_runtime,
                )
                .await
                .map_err(to_retryable)?;
                tracing::info!(
                    per_operator = ?tx.per_operator,
                    rows_kept = tx.kept.num_rows(),
                    rows_rejected = tx.rejected.as_ref().map(|b| b.num_rows()).unwrap_or(0),
                    "transform complete"
                );
                (tx.kept, tx.rejected)
            }
            _ => (outcome.batch, None),
        };

        let rows = kept_batch.num_rows();
        let rows_rejected = rejected_batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        let b64 = encode_batch(&kept_batch).map_err(to_retryable)?;
        let rejected_b64 = rejected_batch
            .as_ref()
            .map(encode_batch)
            .transpose()
            .map_err(to_retryable)?;

        metrics::counter!(
            crate::metrics::ROWS_READ,
            "tenant_id" => input.tenant_id.to_string(),
        )
        .increment(rows as u64);
        if rows_rejected > 0 {
            metrics::counter!(
                crate::metrics::ROWS_REJECTED,
                "tenant_id" => input.tenant_id.to_string(),
            )
            .increment(rows_rejected as u64);
        }

        // Metering: emit RowsRead + BytesRead (best-effort).
        // ReadBatchInput doesn't carry pipeline_id today; we emit None for it.
        {
            let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
            let rows_ev = metering::MeteringEvent::new(
                tid.clone(),
                None,
                None,
                metering::BillableMetric::RowsRead,
                rows as i64,
                metering::MeteringSource::Read,
            );
            let bytes_ev = metering::MeteringEvent::new(
                tid,
                None,
                None,
                metering::BillableMetric::BytesRead,
                b64.len() as i64,
                metering::MeteringSource::Read,
            );
            for ev in [&rows_ev, &bytes_ev] {
                if let Err(e) = self.metering.emit(ev).await {
                    tracing::warn!(
                        metric = ?ev.metric,
                        error = %e,
                        "metering emit failed in read_batch (best-effort, ignored)"
                    );
                }
            }
        }

        Ok(ReadBatchOutput {
            batch_ipc_b64: b64,
            rows,
            new_cursor: outcome.new_cursor,
            is_final: outcome.is_final,
            rejected_ipc_b64: rejected_b64,
            rows_rejected,
            stream_name: outcome.stream_name,
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
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            pipeline_id: PipelineId::from_uuid_unchecked(input.pipeline_id),
            run_id: RunId::from_uuid_unchecked(input.run_id),
            batch_seq: input.batch_seq,
            stream_name: input.stream_name.clone(),
        };
        let res = match &input.destination {
            common_types::pipeline_spec::DestinationSpec::LocalParquet(_) => {
                LocalParquetLoader
                    .load(&input.destination, load_id.clone(), batch)
                    .await
            }
            common_types::pipeline_spec::DestinationSpec::Postgres(_) => {
                PostgresLoader
                    .load(&input.destination, load_id.clone(), batch)
                    .await
            }
        }
        .map_err(to_retryable)?;
        metrics::counter!(
            crate::metrics::ROWS_LOADED,
            "tenant_id" => input.tenant_id.to_string(),
        )
        .increment(res.rows_loaded as u64);

        // Dead-letter routing (LocalParquet only — Postgres dead-letter
        // is a follow-up; rejected rows are logged and dropped).
        let dl_supported = matches!(
            &input.destination,
            common_types::pipeline_spec::DestinationSpec::LocalParquet(_)
        );
        if !dl_supported {
            if let Some(rej_b64) = input.rejected_ipc_b64.as_deref() {
                let rej = decode_batch(rej_b64).map_err(to_retryable)?;
                if rej.num_rows() > 0 {
                    tracing::warn!(
                        target: "loader.dead_letter",
                        loader = loader_name(&input.destination),
                        rows = rej.num_rows(),
                        "dead-letter routing not implemented for this destination; rejected rows dropped"
                    );
                }
            }
        }
        if dl_supported {
        if let Some(rej_b64) = input.rejected_ipc_b64.as_deref() {
            let rej = decode_batch(rej_b64).map_err(to_retryable)?;
            if rej.num_rows() > 0 {
                let dest_path = match &input.destination {
                    common_types::pipeline_spec::DestinationSpec::LocalParquet(s) => {
                        let mut p = std::path::PathBuf::from(&s.base_path);
                        p.push(load_id.tenant_id.as_uuid().to_string());
                        p.push(load_id.pipeline_id.as_uuid().to_string());
                        p.push("dead-letter");
                        p.push(load_id.run_id.as_uuid().to_string());
                        std::fs::create_dir_all(&p)
                            .map_err(|e| to_retryable(anyhow::anyhow!("create dir: {e}")))?;
                        p.push(format!("batch-{:05}.parquet", input.batch_seq));
                        p
                    }
                    common_types::pipeline_spec::DestinationSpec::Postgres(_) => {
                        unreachable!("guarded by dl_supported");
                    }
                };
                let file = std::fs::File::create(&dest_path)
                    .map_err(|e| to_retryable(anyhow::anyhow!("dead-letter create: {e}")))?;
                let props = parquet::file::properties::WriterProperties::builder().build();
                let mut writer =
                    parquet::arrow::ArrowWriter::try_new(file, rej.schema(), Some(props))
                        .map_err(|e| to_retryable(anyhow::anyhow!("ArrowWriter: {e}")))?;
                writer
                    .write(&rej)
                    .map_err(|e| to_retryable(anyhow::anyhow!("dead-letter write: {e}")))?;
                writer
                    .close()
                    .map_err(|e| to_retryable(anyhow::anyhow!("dead-letter close: {e}")))?;
                tracing::info!(
                    path = %dest_path.display(),
                    rows = rej.num_rows(),
                    "dead-letter batch written"
                );
            }
        }
        }

        // Threshold check (cumulative).
        if input.dead_letter_threshold > 0.0 && input.rows_total_so_far > 0 {
            let frac = input.rows_rejected_so_far as f64 / input.rows_total_so_far as f64;
            if frac > input.dead_letter_threshold {
                return Err(ActivityError::NonRetryable(
                    anyhow::anyhow!(
                        "dead-letter threshold exceeded: {:.4} > {:.4} (rejected {}/{} rows)",
                        frac,
                        input.dead_letter_threshold,
                        input.rows_rejected_so_far,
                        input.rows_total_so_far
                    )
                    .into(),
                ));
            }
        }

        // Metering: emit RowsWritten + BytesWritten (best-effort).
        {
            let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
            let pid = Some(common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id));
            let rid = Some(common_types::ids::RunId::from_uuid_unchecked(input.run_id));
            let rows_ev = metering::MeteringEvent::new(
                tid.clone(),
                pid.clone(),
                rid.clone(),
                metering::BillableMetric::RowsWritten,
                res.rows_loaded as i64,
                metering::MeteringSource::Load,
            );
            let bytes_ev = metering::MeteringEvent::new(
                tid,
                pid,
                rid,
                metering::BillableMetric::BytesWritten,
                res.bytes_written as i64,
                metering::MeteringSource::Load,
            );
            for ev in [&rows_ev, &bytes_ev] {
                if let Err(e) = self.metering.emit(ev).await {
                    tracing::warn!(
                        metric = ?ev.metric,
                        error = %e,
                        "metering emit failed in load_batch (best-effort, ignored)"
                    );
                }
            }
        }

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
        let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
        let ctx = common_types::ids::TenantContext::new(tid);
        let rid = Some(RunId::from_uuid_unchecked(input.run_id));
        self.catalog
            .upsert_stream_state(ctx, pid, &input.stream_name, input.cursor, rid)
            .await
            .map_err(|e| to_retryable(anyhow::anyhow!("upsert cursor: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};

    #[test]
    fn pick_loader_returns_postgres_for_postgres_spec() {
        let spec = DestinationSpec::Postgres(PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "t".into(),
            pk_columns: vec![],
        });
        assert_eq!(loader_name(&spec), "postgres");
    }

    #[test]
    fn pick_loader_returns_local_parquet_for_parquet_spec() {
        let spec = DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() },
        );
        assert_eq!(loader_name(&spec), "local_parquet");
    }
}

