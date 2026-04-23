use anyhow::Context;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::pipeline_spec::{DestinationSpec, LocalParquetSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::PathBuf;

pub struct LocalParquetLoader;

#[async_trait]
impl DestinationLoader for LocalParquetLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        match dest {
            DestinationSpec::LocalParquet(p) => {
                fs::create_dir_all(&p.base_path)
                    .with_context(|| format!("creating {}", p.base_path))?;
                Ok(())
            }
        }
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let spec = match dest {
            DestinationSpec::LocalParquet(s) => s,
        };
        let path = target_path(spec, &load_id);
        fs::create_dir_all(path.parent().unwrap())
            .with_context(|| format!("creating dir for {}", path.display()))?;

        let file = File::create(&path)
            .with_context(|| format!("creating {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .context("constructing ArrowWriter")?;
        if batch.num_rows() > 0 {
            writer.write(&batch).context("writing batch")?;
        }
        writer.close().context("closing ArrowWriter")?;

        let bytes = fs::metadata(&path)?.len();
        Ok(LoadResult {
            rows_loaded: batch.num_rows(),
            bytes_written: bytes,
            path: path.to_string_lossy().into_owned(),
        })
    }
}

fn target_path(spec: &LocalParquetSpec, load_id: &LoadId) -> PathBuf {
    let mut p = PathBuf::from(&spec.base_path);
    p.push(load_id.pipeline_id.as_uuid().to_string());
    p.push(load_id.run_id.as_uuid().to_string());
    p.push(format!("batch-{:05}.parquet", load_id.batch_seq));
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use common_types::ids::{PipelineId, RunId};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn tiny_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn load_writes_parquet_file_readable_back() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = DestinationSpec::LocalParquet(LocalParquetSpec {
            base_path: tmp.path().to_string_lossy().into_owned(),
        });
        let loader = LocalParquetLoader;
        loader.validate(&spec).await.unwrap();

        let load_id = LoadId {
            pipeline_id: PipelineId::new(),
            run_id: RunId::new(),
            batch_seq: 0,
        };
        let res = loader.load(&spec, load_id.clone(), tiny_batch()).await.unwrap();
        assert_eq!(res.rows_loaded, 3);
        assert!(res.bytes_written > 0);

        let f = File::open(&res.path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn load_idempotent_for_same_load_id() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = DestinationSpec::LocalParquet(LocalParquetSpec {
            base_path: tmp.path().to_string_lossy().into_owned(),
        });
        let loader = LocalParquetLoader;
        loader.validate(&spec).await.unwrap();
        let load_id = LoadId {
            pipeline_id: PipelineId::new(),
            run_id: RunId::new(),
            batch_seq: 5,
        };
        let r1 = loader.load(&spec, load_id.clone(), tiny_batch()).await.unwrap();
        let r2 = loader.load(&spec, load_id.clone(), tiny_batch()).await.unwrap();
        assert_eq!(r1.path, r2.path);
        assert_eq!(r1.bytes_written, r2.bytes_written);
    }
}
