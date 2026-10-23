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
            other => anyhow::bail!(
                "CdcParquetLoader expects LocalParquet, got {}",
                other.kind()
            ),
        };
        let mut path = PathBuf::from(&base);
        path.push(tenant_id.to_string());
        path.push(pipeline_id.to_string());
        path.push("cdc");
        path.push(run_id.to_string());
        std::fs::create_dir_all(&path).with_context(|| format!("create dir {}", path.display()))?;
        path.push(format!("batch-{:05}.parquet", batch_seq));
        let file =
            std::fs::File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        w.write(batch)?;
        w.close()?;
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use common_types::pipeline_spec::PostgresDestinationSpec;
    use std::sync::Arc;

    #[tokio::test]
    async fn wrong_variant_error_does_not_leak_spec() {
        let dest = DestinationSpec::Postgres(PostgresDestinationSpec {
            connection_url: "postgres://etl:hunter2@db.internal/etl".into(),
            schema: "public".into(),
            table: "t".into(),
            pk_columns: vec![],
        });
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let id = Uuid::nil();
        let err = CdcParquetLoader
            .write(&dest, id, id, id, 0, &batch)
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(!msg.contains("hunter2"), "got {msg}");
        assert!(msg.contains("postgres"), "got {msg}");
    }
}
