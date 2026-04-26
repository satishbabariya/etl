//! Author-side helper. Drives a `SourceConnector` impl through a
//! single discover → read_batch round trip and validates basic
//! invariants. Connectors call this from their integration tests; the
//! platform CLI also wires it via `platform connector test`.

use crate::SourceConnector;
use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::SourceSpec;

#[derive(Debug)]
pub struct SmokeReport {
    pub schema_columns: Vec<String>,
    pub batch_rows: usize,
    pub is_final: bool,
}

pub async fn run_smoke<C: SourceConnector>(
    connector: &C,
    conn: &ConnectionConfig,
    source: &SourceSpec,
    batch_size: usize,
) -> anyhow::Result<SmokeReport> {
    let schema = connector.discover(conn, source).await?;
    if schema.fields().is_empty() {
        anyhow::bail!("discover() returned empty schema");
    }
    let outcome = connector.read_batch(conn, source, None, batch_size).await?;
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let batch_columns: Vec<String> = outcome
        .batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if batch_columns != columns {
        anyhow::bail!(
            "read_batch schema {:?} disagrees with discover schema {:?}",
            batch_columns,
            columns
        );
    }
    if outcome.batch.num_rows() > batch_size {
        anyhow::bail!(
            "read_batch returned {} rows but batch_size was {}",
            outcome.batch.num_rows(),
            batch_size
        );
    }
    Ok(SmokeReport {
        schema_columns: columns,
        batch_rows: outcome.batch.num_rows(),
        is_final: outcome.is_final,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReadOutcome;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    struct FakeOk {
        schema: SchemaRef,
    }

    #[async_trait::async_trait]
    impl SourceConnector for FakeOk {
        async fn discover(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
        ) -> anyhow::Result<SchemaRef> {
            Ok(self.schema.clone())
        }
        async fn read_batch(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
            _cursor: Option<common_types::cursor::CursorValue>,
            _batch_size: usize,
        ) -> anyhow::Result<ReadOutcome> {
            let arr = Int64Array::from(vec![1, 2, 3]);
            let batch =
                RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)]).unwrap();
            Ok(ReadOutcome {
                batch,
                new_cursor: None,
                is_final: true,
            })
        }
    }

    fn pg_source() -> SourceSpec {
        SourceSpec::Postgres(common_types::pipeline_spec::PostgresSourceSpec {
            schema: "public".into(),
            table: "t".into(),
            cursor_column: "id".into(),
            cursor_kind: common_types::cursor::CursorKind::Int64,
            pk_columns: vec!["id".into()],
            sync_mode: Default::default(),
        })
    }

    #[tokio::test]
    async fn smoke_passes_when_schema_matches() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let c = FakeOk { schema };
        let conn = ConnectionConfig::from_url("none://");
        let report = run_smoke(&c, &conn, &pg_source(), 10).await.unwrap();
        assert_eq!(report.schema_columns, vec!["id".to_string()]);
        assert_eq!(report.batch_rows, 3);
        assert!(report.is_final);
    }

    struct FakeBadSchema;
    #[async_trait::async_trait]
    impl SourceConnector for FakeBadSchema {
        async fn discover(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
        ) -> anyhow::Result<SchemaRef> {
            Ok(Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )])))
        }
        async fn read_batch(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
            _cursor: Option<common_types::cursor::CursorValue>,
            _batch_size: usize,
        ) -> anyhow::Result<ReadOutcome> {
            let other_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "name",
                DataType::Utf8,
                false,
            )]));
            let arr = StringArray::from(vec!["x"]);
            let batch =
                RecordBatch::try_new(other_schema.clone(), vec![Arc::new(arr)]).unwrap();
            Ok(ReadOutcome {
                batch,
                new_cursor: None,
                is_final: true,
            })
        }
    }

    #[tokio::test]
    async fn smoke_fails_on_schema_disagreement() {
        let c = FakeBadSchema;
        let conn = ConnectionConfig::from_url("none://");
        let err = run_smoke(&c, &conn, &pg_source(), 10).await.unwrap_err();
        assert!(format!("{err}").contains("disagrees"));
    }
}
