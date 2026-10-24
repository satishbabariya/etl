//! In-process loader implementations: local Parquet, Postgres, Snowflake.
pub mod cdc_parquet;
pub mod parquet_local;
pub mod postgres;
pub mod snowflake;

use arrow::record_batch::RecordBatch;
use common_types::ids::{PipelineId, RunId, TenantId};
use common_types::pipeline_spec::DestinationSpec;
use loader_sdk::{DestinationLoader, LoadId};
use uuid::Uuid;

/// Write a native-CDC batch (a snapshot chunk or a streaming window) to
/// the pipeline's destination. LocalParquet keeps the change-log layout
/// (`cdc/<run_id>/batch-NNNNN.parquet`); Postgres and Snowflake apply it
/// by key through their CDC paths. Both are idempotent on
/// (run_id, batch_seq), which the at-least-once CDC reads rely on: a
/// retried window re-writes the same file or no-ops as already loaded.
pub async fn write_cdc_batch(
    dest: &DestinationSpec,
    tenant_id: Uuid,
    pipeline_id: Uuid,
    run_id: Uuid,
    batch_seq: u32,
    batch: &RecordBatch,
) -> anyhow::Result<()> {
    // Native CDC pipelines are single-table: the batch lands in spec.table.
    let load_id = || LoadId {
        tenant_id: TenantId::from_uuid_unchecked(tenant_id),
        pipeline_id: PipelineId::from_uuid_unchecked(pipeline_id),
        run_id: RunId::from_uuid_unchecked(run_id),
        batch_seq,
        stream_name: String::new(),
    };
    match dest {
        DestinationSpec::LocalParquet(_) => {
            cdc_parquet::CdcParquetLoader
                .write(dest, tenant_id, pipeline_id, run_id, batch_seq, batch)
                .await?;
        }
        DestinationSpec::Postgres(_) => {
            postgres::PostgresLoader
                .load(dest, load_id(), batch.clone())
                .await?;
        }
        DestinationSpec::Snowflake(_) => {
            snowflake::SnowflakeLoader
                .load(dest, load_id(), batch.clone())
                .await?;
        }
    }
    Ok(())
}
