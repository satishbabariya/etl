//! Postgres destination loader (RFC-9). MVP: insert-only or
//! `ON CONFLICT DO UPDATE`, per-call transaction, idempotency log.
//!
//! Scope cuts (see plan): no CDC op-aware DELETE, no mid-run schema
//! evolution, no soft delete, no dead-letter routing.

use anyhow::{Context, bail};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};

pub(crate) fn pg_column_type(t: &DataType) -> anyhow::Result<&'static str> {
    Ok(match t {
        DataType::Int64 => "BIGINT",
        DataType::Int32 => "INTEGER",
        DataType::Int16 => "SMALLINT",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Boolean => "BOOLEAN",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Float32 => "REAL",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMPTZ",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP",
        DataType::Date32 => "DATE",
        DataType::Binary | DataType::LargeBinary => "BYTEA",
        DataType::Time64(_) | DataType::Time32(_) => "TIME",
        other => bail!("unsupported Arrow type for Postgres loader: {other:?}"),
    })
}

pub(crate) fn create_table_ddl(
    schema: &str,
    table: &str,
    arrow_schema: &Schema,
    pk_columns: &[String],
) -> anyhow::Result<String> {
    for pk in pk_columns {
        if arrow_schema.field_with_name(pk).is_err() {
            bail!("pk column {pk:?} missing from batch schema");
        }
    }
    let mut cols = Vec::with_capacity(arrow_schema.fields().len());
    for f in arrow_schema.fields() {
        let ty = pg_column_type(f.data_type())?;
        let null = if f.is_nullable() { "" } else { " NOT NULL" };
        cols.push(format!("\"{}\" {}{}", f.name(), ty, null));
    }
    let pk_clause = if pk_columns.is_empty() {
        String::new()
    } else {
        let quoted: Vec<String> = pk_columns.iter().map(|c| format!("\"{c}\"")).collect();
        format!(", PRIMARY KEY ({})", quoted.join(", "))
    };
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}\" ({}{})",
        cols.join(", "),
        pk_clause,
    ))
}

pub struct PostgresLoader;

#[async_trait]
impl DestinationLoader for PostgresLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let _spec = postgres_spec(dest)?;
        // Connectivity check arrives in Task 4.
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        _load_id: LoadId,
        _batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let _spec = postgres_spec(dest)?;
        bail!("PostgresLoader::load not yet implemented");
    }
}

fn postgres_spec(dest: &DestinationSpec) -> anyhow::Result<&PostgresDestinationSpec> {
    match dest {
        DestinationSpec::Postgres(s) => Ok(s),
        other => bail!("PostgresLoader received non-postgres destination: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};

    #[tokio::test]
    async fn validate_rejects_non_postgres_spec() {
        let loader = PostgresLoader;
        let spec = DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() },
        );
        let err = loader.validate(&spec).await.unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("postgres"));
    }

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn fields(items: &[(&str, DataType, bool)]) -> Vec<Field> {
        items
            .iter()
            .map(|(n, t, nullable)| Field::new(*n, t.clone(), *nullable))
            .collect()
    }

    #[test]
    fn pg_column_type_covers_cdc_source_types() {
        assert_eq!(pg_column_type(&DataType::Int64).unwrap(), "BIGINT");
        assert_eq!(pg_column_type(&DataType::Int32).unwrap(), "INTEGER");
        assert_eq!(pg_column_type(&DataType::Utf8).unwrap(), "TEXT");
        assert_eq!(pg_column_type(&DataType::Boolean).unwrap(), "BOOLEAN");
        assert_eq!(pg_column_type(&DataType::Float64).unwrap(), "DOUBLE PRECISION");
        assert_eq!(
            pg_column_type(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))).unwrap(),
            "TIMESTAMPTZ"
        );
        assert_eq!(pg_column_type(&DataType::Date32).unwrap(), "DATE");
        assert_eq!(pg_column_type(&DataType::Binary).unwrap(), "BYTEA");
        assert_eq!(
            pg_column_type(&DataType::Time64(TimeUnit::Microsecond)).unwrap(),
            "TIME"
        );
    }

    #[test]
    fn pg_column_type_rejects_unsupported() {
        let err =
            pg_column_type(&DataType::List(Arc::new(Field::new("x", DataType::Int8, true))))
                .unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("unsupported"));
    }

    #[test]
    fn create_table_ddl_quotes_identifiers_and_emits_columns() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ])));
        let ddl =
            create_table_ddl("public", "customers", &schema, &["id".to_string()]).unwrap();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"public\".\"customers\""));
        assert!(ddl.contains("\"id\" BIGINT NOT NULL"));
        assert!(ddl.contains("\"name\" TEXT"));
        assert!(ddl.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn create_table_ddl_omits_pk_when_pk_columns_empty() {
        let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
        let ddl = create_table_ddl("public", "events", &schema, &[]).unwrap();
        assert!(!ddl.contains("PRIMARY KEY"));
    }

    #[test]
    fn create_table_ddl_errors_when_pk_column_missing_from_schema() {
        let schema = Arc::new(Schema::new(fields(&[("name", DataType::Utf8, true)])));
        let err = create_table_ddl("public", "t", &schema, &["id".to_string()]).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("pk column"));
    }
}
