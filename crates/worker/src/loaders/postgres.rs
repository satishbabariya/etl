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
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Postgres, Transaction};

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

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

#[derive(Debug, Clone)]
pub(crate) enum BoundValue {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Bool(bool),
    Text(Option<String>),
    Bytea(Option<Vec<u8>>),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(DateTime<Utc>),
}

pub(crate) fn extract_row(batch: &RecordBatch, row: usize) -> anyhow::Result<Vec<BoundValue>> {
    let mut out = Vec::with_capacity(batch.num_columns());
    for (idx, col) in batch.columns().iter().enumerate() {
        let field = batch.schema().field(idx).clone();
        let is_null = col.is_null(row);
        let v = match field.data_type() {
            DataType::Int64 => BoundValue::Int64(
                col.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
            ),
            DataType::Int32 => BoundValue::Int32(
                col.as_any().downcast_ref::<Int32Array>().unwrap().value(row),
            ),
            DataType::Int16 => BoundValue::Int16(
                col.as_any().downcast_ref::<Int16Array>().unwrap().value(row),
            ),
            DataType::Boolean => BoundValue::Bool(
                col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
            ),
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                BoundValue::Float64(if is_null { None } else { Some(arr.value(row)) })
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                BoundValue::Float32(if is_null { None } else { Some(arr.value(row)) })
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                BoundValue::Text(if is_null { None } else { Some(arr.value(row).to_string()) })
            }
            DataType::Binary => {
                let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                BoundValue::Bytea(if is_null { None } else { Some(arr.value(row).to_vec()) })
            }
            DataType::Date32 => {
                let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(row);
                let date = NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .context("date32 out of range")?;
                BoundValue::Date(date)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let arr = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
                let micros = arr.value(row);
                let secs = (micros / 1_000_000) as u32;
                let nanos = ((micros % 1_000_000) * 1_000) as u32;
                let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .context("time64 out of range")?;
                BoundValue::Time(t)
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let micros = arr.value(row);
                let dt = DateTime::<Utc>::from_timestamp_micros(micros)
                    .context("timestamp_us out of range")?;
                BoundValue::TimestampTz(dt)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let micros = arr.value(row);
                let dt = DateTime::<Utc>::from_timestamp_micros(micros)
                    .context("timestamp_us out of range")?;
                BoundValue::Timestamp(dt.naive_utc())
            }
            other => bail!("extract_row: unsupported Arrow type {other:?}"),
        };
        out.push(v);
    }
    Ok(out)
}

pub(crate) fn ensure_log_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".\"_etl_loaded_batches\" (\
            tenant_id UUID NOT NULL, \
            pipeline_id UUID NOT NULL, \
            run_id UUID NOT NULL, \
            stream_name TEXT NOT NULL, \
            batch_seq BIGINT NOT NULL, \
            rows_loaded BIGINT NOT NULL, \
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
            PRIMARY KEY (tenant_id, pipeline_id, run_id, stream_name, batch_seq)\
        )"
    )
}

pub(crate) fn insert_sql(
    schema: &str,
    table: &str,
    arrow_schema: &Schema,
    pk_columns: &[String],
) -> String {
    let field_names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let col_list = field_names
        .iter()
        .map(|n| format!("\"{n}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (1..=field_names.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let base =
        format!("INSERT INTO \"{schema}\".\"{table}\" ({col_list}) VALUES ({placeholders})");
    if pk_columns.is_empty() {
        return base;
    }
    let pk_list = pk_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let non_pk: Vec<String> = field_names
        .iter()
        .filter(|n| !pk_columns.iter().any(|p| p == *n))
        .map(|n| format!("\"{n}\" = EXCLUDED.\"{n}\""))
        .collect();
    if non_pk.is_empty() {
        format!("{base} ON CONFLICT ({pk_list}) DO NOTHING")
    } else {
        format!(
            "{base} ON CONFLICT ({pk_list}) DO UPDATE SET {}",
            non_pk.join(", ")
        )
    }
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

impl PostgresLoader {
    async fn connect(spec: &PostgresDestinationSpec) -> anyhow::Result<sqlx::PgPool> {
        PgPoolOptions::new()
            .max_connections(4)
            .connect(&spec.connection_url)
            .await
            .with_context(|| format!("connect to {}", spec.connection_url))
    }
}

#[async_trait]
impl DestinationLoader for PostgresLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let spec = postgres_spec(dest)?;
        let pool = Self::connect(spec).await?;
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .context("SELECT 1 health check")?;
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let spec = postgres_spec(dest)?;
        let pool = Self::connect(spec).await?;
        let mut tx: Transaction<'_, Postgres> = pool.begin().await.context("begin tx")?;

        // 1. Ensure log table.
        tx.execute(sqlx::query(&ensure_log_table_ddl(&spec.schema)))
            .await
            .context("ensure log table")?;

        // 2. Idempotency check — if this load_id is already logged, no-op.
        let existing = sqlx::query(&format!(
            "SELECT rows_loaded FROM \"{}\".\"_etl_loaded_batches\" \
             WHERE tenant_id=$1 AND pipeline_id=$2 AND run_id=$3 \
             AND stream_name=$4 AND batch_seq=$5",
            spec.schema
        ))
        .bind(load_id.tenant_id.as_uuid())
        .bind(load_id.pipeline_id.as_uuid())
        .bind(load_id.run_id.as_uuid())
        .bind(&load_id.stream_name)
        .bind(load_id.batch_seq as i64)
        .fetch_optional(&mut *tx)
        .await
        .context("query log")?;

        if existing.is_some() {
            tx.commit().await.ok();
            return Ok(LoadResult {
                rows_loaded: 0,
                bytes_written: 0,
                path: format!("{}.{} (already loaded)", spec.schema, spec.table),
            });
        }

        // 3. Ensure target table on first non-empty batch.
        if batch.num_rows() > 0 {
            let ddl = create_table_ddl(
                &spec.schema,
                &spec.table,
                batch.schema().as_ref(),
                &spec.pk_columns,
            )?;
            tx.execute(sqlx::query(&ddl))
                .await
                .context("create target table")?;
        }

        // 4. Insert rows.
        let sql = insert_sql(
            &spec.schema,
            &spec.table,
            batch.schema().as_ref(),
            &spec.pk_columns,
        );
        let mut rows_loaded = 0usize;
        for r in 0..batch.num_rows() {
            let values = extract_row(&batch, r)?;
            let mut q = sqlx::query(&sql);
            for v in &values {
                q = bind_one(q, v);
            }
            q.execute(&mut *tx)
                .await
                .with_context(|| format!("INSERT row {r}"))?;
            rows_loaded += 1;
        }

        // 5. Record in log.
        sqlx::query(&format!(
            "INSERT INTO \"{}\".\"_etl_loaded_batches\" \
             (tenant_id, pipeline_id, run_id, stream_name, batch_seq, rows_loaded) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            spec.schema
        ))
        .bind(load_id.tenant_id.as_uuid())
        .bind(load_id.pipeline_id.as_uuid())
        .bind(load_id.run_id.as_uuid())
        .bind(&load_id.stream_name)
        .bind(load_id.batch_seq as i64)
        .bind(rows_loaded as i64)
        .execute(&mut *tx)
        .await
        .context("insert log row")?;

        tx.commit().await.context("commit tx")?;
        Ok(LoadResult {
            rows_loaded,
            bytes_written: 0,
            path: format!("{}.{}", spec.schema, spec.table),
        })
    }
}

fn bind_one<'a>(
    q: sqlx::query::Query<'a, Postgres, sqlx::postgres::PgArguments>,
    v: &'a BoundValue,
) -> sqlx::query::Query<'a, Postgres, sqlx::postgres::PgArguments> {
    match v {
        BoundValue::Int16(x) => q.bind(*x),
        BoundValue::Int32(x) => q.bind(*x),
        BoundValue::Int64(x) => q.bind(*x),
        BoundValue::Float32(x) => q.bind(*x),
        BoundValue::Float64(x) => q.bind(*x),
        BoundValue::Bool(x) => q.bind(*x),
        BoundValue::Text(x) => q.bind(x.clone()),
        BoundValue::Bytea(x) => q.bind(x.clone()),
        BoundValue::Date(x) => q.bind(*x),
        BoundValue::Time(x) => q.bind(*x),
        BoundValue::Timestamp(x) => q.bind(*x),
        BoundValue::TimestampTz(x) => q.bind(*x),
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

    #[test]
    fn insert_sql_append_form_no_pk() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ])));
        let sql = insert_sql("public", "events", &schema, &[]);
        assert_eq!(
            sql,
            r#"INSERT INTO "public"."events" ("id", "name") VALUES ($1, $2)"#
        );
    }

    #[test]
    fn insert_sql_upsert_form_with_pk() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("amount", DataType::Float64, true),
        ])));
        let sql = insert_sql("public", "customers", &schema, &["id".to_string()]);
        assert_eq!(
            sql,
            r#"INSERT INTO "public"."customers" ("id", "name", "amount") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "amount" = EXCLUDED."amount""#
        );
    }

    #[test]
    fn insert_sql_upsert_excludes_pk_columns_from_update_set() {
        let schema = Arc::new(Schema::new(fields(&[
            ("tenant", DataType::Utf8, false),
            ("id", DataType::Int64, false),
            ("val", DataType::Int64, true),
        ])));
        let sql = insert_sql("public", "t", &schema, &["tenant".into(), "id".into()]);
        assert!(sql.contains("ON CONFLICT (\"tenant\", \"id\")"));
        assert!(sql.contains("SET \"val\" = EXCLUDED.\"val\""));
        assert!(!sql.contains("SET \"tenant\""));
        assert!(!sql.contains("SET \"id\""));
    }

    #[test]
    fn insert_sql_pk_only_uses_do_nothing() {
        let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
        let sql = insert_sql("public", "t", &schema, &["id".into()]);
        assert!(sql.contains("ON CONFLICT (\"id\") DO NOTHING"));
    }

    use arrow::array::{
        BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };

    #[test]
    fn extract_row_handles_int64_text_bool_float() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("active", DataType::Boolean, false),
            ("score", DataType::Float64, true),
        ])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
            ],
        )
        .unwrap();

        let row0 = extract_row(&batch, 0).unwrap();
        assert!(matches!(row0[0], BoundValue::Int64(10)));
        assert!(matches!(row0[1], BoundValue::Text(Some(ref s)) if s == "a"));
        assert!(matches!(row0[2], BoundValue::Bool(true)));
        assert!(matches!(row0[3], BoundValue::Float64(Some(v)) if (v - 1.5).abs() < 1e-9));

        let row1 = extract_row(&batch, 1).unwrap();
        assert!(matches!(row1[1], BoundValue::Text(None)));
        assert!(matches!(row1[3], BoundValue::Float64(None)));
    }

    #[test]
    fn ensure_log_table_ddl_is_idempotent_and_keyed_by_load_id() {
        let ddl = ensure_log_table_ddl("public");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"public\".\"_etl_loaded_batches\""));
        assert!(ddl.contains("tenant_id UUID"));
        assert!(ddl.contains("pipeline_id UUID"));
        assert!(ddl.contains("run_id UUID"));
        assert!(ddl.contains("batch_seq BIGINT"));
        assert!(ddl.contains("stream_name TEXT"));
        assert!(ddl.contains(
            "PRIMARY KEY (tenant_id, pipeline_id, run_id, stream_name, batch_seq)"
        ));
    }

    #[test]
    fn extract_row_handles_timestamptz_utc() {
        let schema = Arc::new(Schema::new(fields(&[(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )])));
        let micros: i64 = 1_779_667_200_000_000;
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"),
            )],
        )
        .unwrap();
        let row = extract_row(&batch, 0).unwrap();
        if let BoundValue::TimestampTz(dt) = &row[0] {
            assert_eq!(dt.timestamp_micros(), micros);
        } else {
            panic!("expected TimestampTz, got {:?}", row[0]);
        }
    }
}
