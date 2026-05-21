use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::DestinationSpec;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use uuid::Uuid;

pub struct CdcParquetLoader;

impl CdcParquetLoader {
    pub async fn write(
        &self,
        dest: &DestinationSpec,
        tenant_id: Uuid,
        pipeline_id: Uuid,
        run_id: Uuid,
        batch_seq: u32,
        batch: &RecordBatch,
    ) -> Result<PathBuf> {
        let base = match dest {
            DestinationSpec::LocalParquet(s) => s.base_path.clone(),
            other => anyhow::bail!("CdcParquetLoader expects LocalParquet, got {other:?}"),
        };
        let mut path = PathBuf::from(&base);
        path.push(tenant_id.to_string());
        path.push(pipeline_id.to_string());
        path.push("cdc");
        path.push(run_id.to_string());
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create dir {}", path.display()))?;
        path.push(format!("batch-{:05}.parquet", batch_seq));
        let file = std::fs::File::create(&path)
            .with_context(|| format!("create {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        w.write(batch)?;
        w.close()?;
        Ok(path)
    }
}
