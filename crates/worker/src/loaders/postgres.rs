//! Postgres destination loader (RFC-9).
//!
//! ## Delivery patterns
//! - Append: `pk_columns` empty ⇒ plain `INSERT`.
//! - Upsert: `pk_columns` non-empty ⇒ `INSERT ... ON CONFLICT (pk) DO UPDATE`
//!   (or `DO NOTHING` when every column is part of the PK).
//!
//! ## Idempotency
//! Per-call sqlx transaction:
//!   1. Ensure `<schema>._etl_loaded_batches` exists.
//!   2. If the (tenant, pipeline, run, stream, batch_seq) row is already there,
//!      short-circuit (rows_loaded = 0).
//!   3. Ensure target table on first non-empty batch.
//!   4. Bind and INSERT each row.
//!   5. Record the load_id in `_etl_loaded_batches`.
//!   6. Commit.
//!
//! Retrying the same `LoadId` re-runs steps 1–2 and stops at step 2.
//!
//! ## CDC mode
//! Auto-detected when the incoming batch carries `_cdc.op`. Requires
//! `pk_columns`. Routes per row:
//!   - `i` / `u` / `s` ⇒ upsert into target (same UPSERT SQL as plain mode)
//!   - `d` ⇒ DELETE keyed on the configured PKs
//!   - `c` ⇒ schema evolution applied before the row loop: additive changes
//!            (ADD COLUMN, widened types) are applied via ALTER TABLE; destructive
//!            changes (DROP, narrow, incompatible) return a non-retriable error per
//!            RFC-9 §"Mid-run schema change" + RFC-10 propagate_additive.
//!   - `t` ⇒ skipped (destructive ops are not auto-applied)
//! `_cdc.*` columns are stripped from the destination table schema.
//!
//! ## Multi-table routing
//! `LoadId.stream_name` selects the target table per batch:
//!   - `stream_name = ""` ⇒ `spec.table` (single-table pipeline).
//!   - `stream_name = "<n>"` ⇒ table `<n>` inside `spec.schema`. Connectors
//!     today emit `"<src_schema>.<src_table>"`, which lands as a literal
//!     `<src_schema>.<src_table>` table name inside `spec.schema`.
//! Validation forbids `"`, NUL, and control chars in the resolved name
//! to prevent quoted-identifier escapes.
//!
//! ## Deferred
//! - Catalog wiring for schema evolution (loader applies DDL inline; no
//!   applied_to_destination_at updates, no catalog policy evaluation).
//! - Column rename via ALTER TABLE ... RENAME COLUMN (treated as drop+add → pauses).
//! - PK-type-change guard (WidenType on a PK column is applied; should pause).
//! - Backfilling new columns with a non-null default value.
//! - Per-stream `pk_columns` override — every stream uses `spec.pk_columns`.
//! - Soft delete / tombstone columns.
//! - Dead-letter routing (rejected rows are logged + dropped by the activity).
//! - `COPY FROM STDIN` fast path (perf optimization).
//! - RFC-11 secret-ref connection URLs (MVP takes an inline `postgres://`).
//! - Audit-log destination mode (keep `_cdc.*` columns at the destination).
//! - PK-change updates that omit a delete of the old key.
//! - Destination-schema split: `stream_name = "<src_schema>.<table>"` ⇒
//!   destination `<src_schema>."<table>"` instead of one literal-dot table.

use anyhow::{Context, bail};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Postgres, Transaction};

pub(crate) fn resolve_target_table<'a>(
    spec: &'a PostgresDestinationSpec,
    stream_name: &'a str,
) -> anyhow::Result<&'a str> {
    let candidate = if stream_name.is_empty() {
        spec.table.as_str()
    } else {
        stream_name
    };
    if candidate.is_empty() {
        bail!(
            "postgres loader: target table is empty (both stream_name and spec.table are empty)"
        );
    }
    for ch in candidate.chars() {
        if ch == '"' || ch == '\0' || ch.is_control() {
            bail!(
                "postgres loader: illegal character {ch:?} in target table name {candidate:?}"
            );
        }
    }
    Ok(candidate)
}

pub(crate) fn is_cdc_batch(schema: &Schema) -> bool {
    schema.field_with_name(common_types::cdc::COL_OP).is_ok()
}

fn is_cdc_metadata_col(name: &str) -> bool {
    name == common_types::cdc::COL_OP
        || name == common_types::cdc::COL_LSN
        || name == common_types::cdc::COL_COMMIT_TS
        || name == common_types::cdc::COL_TXID
}

pub(crate) fn cdc_data_schema(schema: &Schema) -> Schema {
    let kept: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .filter(|f| !is_cdc_metadata_col(f.name()))
        .map(|f| f.as_ref().clone())
        .collect();
    Schema::new(kept)
}

pub(crate) fn cdc_data_field_indices(schema: &Schema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| (!is_cdc_metadata_col(f.name())).then_some(i))
        .collect()
}

pub(crate) fn cdc_op_at<'a>(batch: &'a RecordBatch, row: usize) -> anyhow::Result<&'a str> {
    let idx = batch
        .schema()
        .index_of(common_types::cdc::COL_OP)
        .map_err(|_| anyhow::anyhow!("batch is missing _cdc.op column"))?;
    let col = batch.column(idx);
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("_cdc.op column is not Utf8"))?;
    Ok(arr.value(row))
}

pub(crate) fn delete_sql(schema: &str, table: &str, pk_columns: &[String]) -> String {
    let where_clause = pk_columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("\"{c}\" = ${}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("DELETE FROM \"{schema}\".\"{table}\" WHERE {where_clause}")
}

pub(crate) fn extract_data_row(
    batch: &RecordBatch,
    row: usize,
) -> anyhow::Result<Vec<BoundValue>> {
    let schema = batch.schema();
    let keep = cdc_data_field_indices(schema.as_ref());
    let full = extract_row(batch, row)?;
    Ok(keep.into_iter().map(|i| full[i].clone()).collect())
}

pub(crate) fn extract_pk_values(
    batch: &RecordBatch,
    row: usize,
    pk_columns: &[String],
) -> anyhow::Result<Vec<BoundValue>> {
    let schema = batch.schema();
    let full = extract_row(batch, row)?;
    let mut out = Vec::with_capacity(pk_columns.len());
    for pk in pk_columns {
        let idx = schema
            .index_of(pk)
            .map_err(|_| anyhow::anyhow!("pk column {pk:?} missing from batch schema"))?;
        out.push(full[idx].clone());
    }
    Ok(out)
}

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

/// One column as reported by `information_schema.columns`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DestCol {
    pub name: String,
    /// Lower-cased PG type name as returned by `information_schema.columns.data_type`,
    /// e.g. `"bigint"`, `"text"`, `"timestamp with time zone"`.
    pub pg_type: String,
    pub nullable: bool,
}

/// Query the live column list for `"<schema>"."<table>"` from
/// `information_schema.columns`, ordered by `ordinal_position`.
/// Returns an empty `Vec` if the table does not yet exist.
pub(crate) async fn query_destination_columns(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<DestCol>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(&mut **tx)
    .await
    .context("query information_schema.columns")?;

    let cols = rows
        .into_iter()
        .map(|r| {
            let nullable_str: String = r.get(2);
            DestCol {
                name: r.get(0),
                pg_type: r.get::<String, _>(1).to_lowercase(),
                nullable: nullable_str.eq_ignore_ascii_case("YES"),
            }
        })
        .collect();
    Ok(cols)
}

/// A change between the batch's data schema and the destination table's columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaDelta {
    AddColumn { name: String, pg_type: String, nullable: bool },
    DropColumn { name: String },
    WidenType { name: String, new_pg_type: String },
    NarrowType { name: String },
    IncompatibleType { name: String, dest_pg_type: String, batch_pg_type: String },
}

impl SchemaDelta {
    pub(crate) fn is_destructive(&self) -> bool {
        matches!(
            self,
            SchemaDelta::DropColumn { .. }
                | SchemaDelta::NarrowType { .. }
                | SchemaDelta::IncompatibleType { .. }
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TypeRelation {
    Same,
    Widening,
    Narrowing,
    Incompatible,
}

fn pg_type_relation(dest: &str, batch: &str) -> TypeRelation {
    let d = dest.to_lowercase();
    let b = batch.to_lowercase();
    if d == b {
        return TypeRelation::Same;
    }
    const WIDENINGS: &[(&str, &str)] = &[
        ("smallint", "integer"),
        ("smallint", "bigint"),
        ("integer", "bigint"),
        ("real", "double precision"),
        ("character varying", "text"),
        ("varchar", "text"),
        ("character", "text"),
    ];
    const NARROWINGS: &[(&str, &str)] = &[
        ("integer", "smallint"),
        ("bigint", "smallint"),
        ("bigint", "integer"),
        ("double precision", "real"),
    ];
    if WIDENINGS.iter().any(|(f, t)| *f == d.as_str() && *t == b.as_str()) {
        TypeRelation::Widening
    } else if NARROWINGS.iter().any(|(f, t)| *f == d.as_str() && *t == b.as_str()) {
        TypeRelation::Narrowing
    } else {
        TypeRelation::Incompatible
    }
}

pub(crate) fn diff_schema(
    batch_data_schema: &Schema,
    dest_cols: &[DestCol],
) -> anyhow::Result<Vec<SchemaDelta>> {
    let mut deltas = Vec::new();

    for field in batch_data_schema.fields() {
        let batch_pg = pg_column_type(field.data_type())?;
        match dest_cols.iter().find(|c| c.name == *field.name()) {
            None => {
                deltas.push(SchemaDelta::AddColumn {
                    name: field.name().clone(),
                    pg_type: batch_pg.to_string(),
                    nullable: field.is_nullable(),
                });
            }
            Some(dest_col) => match pg_type_relation(dest_col.pg_type.as_str(), batch_pg) {
                TypeRelation::Same => {}
                TypeRelation::Widening => deltas.push(SchemaDelta::WidenType {
                    name: field.name().clone(),
                    new_pg_type: batch_pg.to_string(),
                }),
                TypeRelation::Narrowing => deltas.push(SchemaDelta::NarrowType {
                    name: field.name().clone(),
                }),
                TypeRelation::Incompatible => deltas.push(SchemaDelta::IncompatibleType {
                    name: field.name().clone(),
                    dest_pg_type: dest_col.pg_type.clone(),
                    batch_pg_type: batch_pg.to_string(),
                }),
            },
        }
    }

    for dest_col in dest_cols {
        if batch_data_schema.field_with_name(&dest_col.name).is_err() {
            deltas.push(SchemaDelta::DropColumn { name: dest_col.name.clone() });
        }
    }

    Ok(deltas)
}

/// `ALTER TABLE "<schema>"."<table>" ADD COLUMN IF NOT EXISTS "<name>" <pg_type>`.
/// Always omits `NOT NULL` (existing rows survive without DEFAULT).
pub(crate) fn add_column_ddl(
    schema: &str,
    table: &str,
    col_name: &str,
    pg_type: &str,
    _nullable: bool,
) -> String {
    format!(
        "ALTER TABLE \"{schema}\".\"{table}\" ADD COLUMN IF NOT EXISTS \"{col_name}\" {pg_type}"
    )
}

/// `ALTER TABLE "<schema>"."<table>" ALTER COLUMN "<name>" TYPE <new_type> USING "<name>"::<new_type>`
pub(crate) fn alter_column_type_ddl(
    schema: &str,
    table: &str,
    col_name: &str,
    new_pg_type: &str,
) -> String {
    format!(
        "ALTER TABLE \"{schema}\".\"{table}\" ALTER COLUMN \"{col_name}\" \
         TYPE {new_pg_type} USING \"{col_name}\"::{new_pg_type}"
    )
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

        // Fail-fast config validation BEFORE opening a connection — CDC + empty
        // pk_columns is a misconfiguration; no point hitting the DB. Also
        // makes this code path unit-testable without a live PG.
        if batch.num_rows() > 0
            && is_cdc_batch(batch.schema().as_ref())
            && spec.pk_columns.is_empty()
        {
            bail!(
                "CDC batch arrived at postgres loader but pk_columns is empty; \
                 CDC ops require a primary key for upsert/delete routing"
            );
        }
        // Same for stream_name — validate early so a bad name surfaces without
        // a wasted connection attempt.
        let _ = resolve_target_table(spec, &load_id.stream_name)?;

        let pool = Self::connect(spec).await?;
        let mut tx: Transaction<'_, Postgres> = pool.begin().await.context("begin tx")?;

        // 1. Ensure log table.
        tx.execute(sqlx::query(&ensure_log_table_ddl(&spec.schema)))
            .await
            .context("ensure log table")?;

        // 2. Resolve target table (validates stream_name even for retried loads).
        let target_table = resolve_target_table(spec, &load_id.stream_name)?;

        // 3. Idempotency check — if this load_id is already logged, no-op.
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
                path: format!("{}.{} (already loaded)", spec.schema, target_table),
            });
        }

        // 4. CDC vs plain. CDC mode is data-driven: any batch carrying
        //    `_cdc.op` is routed through the CDC path; otherwise the
        //    original append/upsert path applies.
        let mut rows_loaded = 0usize;

        if batch.num_rows() > 0 && is_cdc_batch(batch.schema().as_ref()) {
            // pk_columns emptiness already validated at the top of load().
            rows_loaded = cdc_apply(&mut tx, spec, target_table, &batch).await?;
        } else if batch.num_rows() > 0 {
            rows_loaded = plain_apply(&mut tx, spec, target_table, &batch).await?;
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
            path: format!("{}.{}", spec.schema, target_table),
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

async fn plain_apply(
    tx: &mut Transaction<'_, Postgres>,
    spec: &PostgresDestinationSpec,
    target_table: &str,
    batch: &RecordBatch,
) -> anyhow::Result<usize> {
    let ddl = create_table_ddl(
        &spec.schema,
        target_table,
        batch.schema().as_ref(),
        &spec.pk_columns,
    )?;
    tx.execute(sqlx::query(&ddl))
        .await
        .context("create target table")?;

    let sql = insert_sql(
        &spec.schema,
        target_table,
        batch.schema().as_ref(),
        &spec.pk_columns,
    );
    let mut rows_loaded = 0usize;
    for r in 0..batch.num_rows() {
        let values = extract_row(batch, r)?;
        let mut q = sqlx::query(&sql);
        for v in &values {
            q = bind_one(q, v);
        }
        q.execute(&mut **tx)
            .await
            .with_context(|| format!("INSERT row {r}"))?;
        rows_loaded += 1;
    }
    Ok(rows_loaded)
}

/// Query destination columns, diff against `batch_data_schema`, apply additive
/// changes, and return `Err` on any destructive change.
async fn apply_schema_evolution(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    target_table: &str,
    batch_data_schema: &Schema,
) -> anyhow::Result<()> {
    let dest_cols = query_destination_columns(tx, schema, target_table).await?;
    if dest_cols.is_empty() {
        return Ok(()); // table doesn't exist yet; CREATE TABLE will run after this.
    }

    let deltas = diff_schema(batch_data_schema, &dest_cols)?;

    let destructive: Vec<&SchemaDelta> = deltas.iter().filter(|d| d.is_destructive()).collect();
    if !destructive.is_empty() {
        let descriptions: Vec<String> = destructive
            .iter()
            .map(|d| match d {
                SchemaDelta::DropColumn { name } => {
                    format!("column dropped from source: {name:?}")
                }
                SchemaDelta::NarrowType { name } => {
                    format!("type narrowed (data loss risk): {name:?}")
                }
                SchemaDelta::IncompatibleType { name, dest_pg_type, batch_pg_type } => {
                    format!(
                        "incompatible type for {name:?}: \
                         destination is {dest_pg_type}, batch expects {batch_pg_type}"
                    )
                }
                _ => unreachable!(),
            })
            .collect();
        bail!(
            "destructive schema change detected for table {target_table:?} — \
             operator action required before pipeline can resume:\n  {}",
            descriptions.join("\n  ")
        );
    }

    for delta in &deltas {
        let ddl = match delta {
            SchemaDelta::AddColumn { name, pg_type, nullable } => {
                add_column_ddl(schema, target_table, name, pg_type, *nullable)
            }
            SchemaDelta::WidenType { name, new_pg_type } => {
                alter_column_type_ddl(schema, target_table, name, new_pg_type)
            }
            _ => continue,
        };
        tracing::info!(
            target: "loader.postgres.schema_evolution",
            %target_table,
            %ddl,
            "applying additive schema change"
        );
        tx.execute(sqlx::query(&ddl))
            .await
            .with_context(|| format!("schema evolution DDL failed: {ddl}"))?;
    }

    Ok(())
}

async fn cdc_apply(
    tx: &mut Transaction<'_, Postgres>,
    spec: &PostgresDestinationSpec,
    target_table: &str,
    batch: &RecordBatch,
) -> anyhow::Result<usize> {
    let data_schema = cdc_data_schema(batch.schema().as_ref());

    // Pre-loop: if the batch carries a "c" sentinel, evolve the destination
    // schema before any row is processed. The Arrow batch already has the
    // widened schema; the "c" row is just the signal.
    let has_schema_change = (0..batch.num_rows())
        .any(|r| cdc_op_at(batch, r).map(|op| op == "c").unwrap_or(false));
    if has_schema_change {
        apply_schema_evolution(&mut *tx, &spec.schema, target_table, &data_schema).await?;
    }

    let ddl = create_table_ddl(&spec.schema, target_table, &data_schema, &spec.pk_columns)?;
    tx.execute(sqlx::query(&ddl))
        .await
        .context("create target table (cdc)")?;

    let upsert_sql = insert_sql(&spec.schema, target_table, &data_schema, &spec.pk_columns);
    let del_sql = delete_sql(&spec.schema, target_table, &spec.pk_columns);

    let mut applied = 0usize;
    for r in 0..batch.num_rows() {
        let op = cdc_op_at(batch, r)?;
        match op {
            "i" | "u" | "s" => {
                let values = extract_data_row(batch, r)?;
                let mut q = sqlx::query(&upsert_sql);
                for v in &values {
                    q = bind_one(q, v);
                }
                q.execute(&mut **tx)
                    .await
                    .with_context(|| format!("CDC upsert row {r}"))?;
                applied += 1;
            }
            "d" => {
                let pks = extract_pk_values(batch, r, &spec.pk_columns)?;
                let mut q = sqlx::query(&del_sql);
                for v in &pks {
                    q = bind_one(q, v);
                }
                q.execute(&mut **tx)
                    .await
                    .with_context(|| format!("CDC delete row {r}"))?;
                applied += 1;
            }
            "c" => {
                // Schema evolution was applied pre-loop; the "c" row itself
                // carries no data values — skip data binding.
                tracing::debug!(
                    target: "loader.postgres.cdc",
                    row = r,
                    "schema-change event: DDL already applied pre-loop, skipping row data"
                );
            }
            "t" => {
                tracing::warn!(
                    target: "loader.postgres.cdc",
                    "truncate CDC event skipped (destructive ops not auto-applied)"
                );
            }
            other => {
                bail!("unknown CDC op {other:?} at row {r}");
            }
        }
    }
    Ok(applied)
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

    // ───── phase-2-4b: CDC op-aware writes ─────

    use arrow::array::StringArray as ArrStr;

    #[test]
    fn is_cdc_batch_true_when_op_column_present() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
        ])));
        assert!(is_cdc_batch(&schema));
    }

    #[test]
    fn is_cdc_batch_false_for_plain_data_schema() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ])));
        assert!(!is_cdc_batch(&schema));
    }

    #[test]
    fn cdc_data_schema_drops_metadata_columns() {
        let schema = Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
            (common_types::cdc::COL_LSN, DataType::Utf8, false),
            (
                common_types::cdc::COL_COMMIT_TS,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let stripped = cdc_data_schema(&schema);
        let names: Vec<&str> = stripped.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn cdc_data_schema_is_identity_on_non_cdc_schema() {
        let schema = Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]));
        let stripped = cdc_data_schema(&schema);
        let names: Vec<&str> = stripped.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn cdc_data_field_indices_lists_non_cdc_columns_in_order() {
        let schema = Schema::new(fields(&[
            ("id", DataType::Int64, false),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
            ("name", DataType::Utf8, true),
            (common_types::cdc::COL_LSN, DataType::Utf8, false),
        ]));
        assert_eq!(cdc_data_field_indices(&schema), vec![0usize, 2]);
    }

    #[test]
    fn cdc_op_at_returns_op_string_per_row() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
        ])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(ArrStr::from(vec!["i", "u", "d"])),
            ],
        )
        .unwrap();
        assert_eq!(cdc_op_at(&batch, 0).unwrap(), "i");
        assert_eq!(cdc_op_at(&batch, 1).unwrap(), "u");
        assert_eq!(cdc_op_at(&batch, 2).unwrap(), "d");
    }

    #[test]
    fn cdc_op_at_errors_when_column_missing() {
        let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let err = cdc_op_at(&batch, 0).unwrap_err();
        assert!(format!("{err}").contains("_cdc.op"));
    }

    #[test]
    fn delete_sql_single_pk() {
        let sql = delete_sql("public", "customers", &["id".to_string()]);
        assert_eq!(sql, r#"DELETE FROM "public"."customers" WHERE "id" = $1"#);
    }

    #[test]
    fn delete_sql_composite_pk() {
        let sql = delete_sql("public", "t", &["tenant".to_string(), "id".to_string()]);
        assert_eq!(
            sql,
            r#"DELETE FROM "public"."t" WHERE "tenant" = $1 AND "id" = $2"#
        );
    }

    #[test]
    fn extract_data_row_skips_cdc_columns() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
            ("name", DataType::Utf8, true),
            (common_types::cdc::COL_LSN, DataType::Utf8, false),
        ])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(ArrStr::from(vec!["i"])),
                Arc::new(ArrStr::from(vec![Some("hello")])),
                Arc::new(ArrStr::from(vec!["lsn-1"])),
            ],
        )
        .unwrap();
        let row = extract_data_row(&batch, 0).unwrap();
        assert_eq!(row.len(), 2);
        assert!(matches!(row[0], BoundValue::Int64(42)));
        assert!(matches!(row[1], BoundValue::Text(Some(ref s)) if s == "hello"));
    }

    #[test]
    fn extract_pk_values_picks_pk_columns_in_order() {
        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("region", DataType::Utf8, false),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
        ])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(ArrStr::from(vec!["eu"])),
                Arc::new(ArrStr::from(vec!["d"])),
            ],
        )
        .unwrap();
        let pks = extract_pk_values(&batch, 0, &["region".into(), "id".into()]).unwrap();
        assert_eq!(pks.len(), 2);
        assert!(matches!(pks[0], BoundValue::Text(Some(ref s)) if s == "eu"));
        assert!(matches!(pks[1], BoundValue::Int64(7)));
    }

    #[test]
    fn resolve_target_table_uses_stream_name_when_present() {
        let spec = PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "fallback".into(),
            pk_columns: vec![],
        };
        assert_eq!(
            resolve_target_table(&spec, "public.users").unwrap(),
            "public.users"
        );
    }

    #[test]
    fn resolve_target_table_falls_back_to_spec_table_when_stream_empty() {
        let spec = PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "fallback".into(),
            pk_columns: vec![],
        };
        assert_eq!(resolve_target_table(&spec, "").unwrap(), "fallback");
    }

    #[test]
    fn resolve_target_table_rejects_double_quote() {
        let spec = PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "t".into(),
            pk_columns: vec![],
        };
        let err = resolve_target_table(&spec, "evil\"name").unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("illegal"));
    }

    #[test]
    fn resolve_target_table_rejects_control_chars() {
        let spec = PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "t".into(),
            pk_columns: vec![],
        };
        for bad in &["with\0nul", "with\nnewline", "with\rcr"] {
            let err = resolve_target_table(&spec, bad).unwrap_err();
            assert!(
                format!("{err}").to_lowercase().contains("illegal"),
                "expected illegal-char rejection for {bad:?}"
            );
        }
    }

    #[test]
    fn resolve_target_table_errors_when_both_empty() {
        let spec = PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "".into(),
            pk_columns: vec![],
        };
        let err = resolve_target_table(&spec, "").unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(msg.contains("table") && msg.contains("empty"), "got: {msg}");
    }

    #[tokio::test]
    async fn load_cdc_batch_errors_when_pk_columns_empty() {
        use common_types::ids::{PipelineId, RunId, TenantId};
        use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
        use loader_sdk::LoadId;

        let schema = Arc::new(Schema::new(fields(&[
            ("id", DataType::Int64, false),
            (common_types::cdc::COL_OP, DataType::Utf8, false),
        ])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(ArrStr::from(vec!["i"])),
            ],
        )
        .unwrap();
        let spec = DestinationSpec::Postgres(PostgresDestinationSpec {
            connection_url: std::env::var("ETL_INTEGRATION_PG_URL")
                .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into()),
            schema: "public".into(),
            table: "t".into(),
            pk_columns: vec![],
        });
        let load_id = LoadId {
            tenant_id: TenantId::new(),
            pipeline_id: PipelineId::new(),
            run_id: RunId::new(),
            batch_seq: 0,
            stream_name: String::new(),
        };
        let err = PostgresLoader.load(&spec, load_id, batch).await.unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(msg.contains("cdc") && msg.contains("pk"), "got: {msg}");
    }

    #[test]
    fn extract_pk_values_errors_on_missing_pk_column() {
        let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let err = extract_pk_values(&batch, 0, &["missing".into()]).unwrap_err();
        assert!(format!("{err}").contains("missing"));
    }

    // ───── phase-2-4d: schema evolution helpers ─────

    #[test]
    fn dest_col_equality() {
        let a = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false };
        let b = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false };
        assert_eq!(a, b);
    }

    #[test]
    fn dest_col_nullable_differs() {
        let a = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false };
        let b = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: true };
        assert_ne!(a, b);
    }

    #[test]
    fn diff_schema_detects_new_column() {
        let dest = vec![DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false }];
        let batch_schema = Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]));
        let deltas = diff_schema(&batch_schema, &dest).unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(
            &deltas[0],
            SchemaDelta::AddColumn { name, pg_type, nullable: true }
                if name == "name" && pg_type == "TEXT"
        ));
    }

    #[test]
    fn diff_schema_detects_widen_int32_to_int64() {
        let dest = vec![DestCol { name: "id".into(), pg_type: "integer".into(), nullable: false }];
        let batch_schema = Schema::new(fields(&[("id", DataType::Int64, false)]));
        let deltas = diff_schema(&batch_schema, &dest).unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(
            &deltas[0],
            SchemaDelta::WidenType { name, new_pg_type }
                if name == "id" && new_pg_type == "BIGINT"
        ));
    }

    #[test]
    fn diff_schema_detects_dropped_column() {
        let dest = vec![
            DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false },
            DestCol { name: "name".into(), pg_type: "text".into(), nullable: true },
        ];
        let batch_schema = Schema::new(fields(&[("id", DataType::Int64, false)]));
        let deltas = diff_schema(&batch_schema, &dest).unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(&deltas[0], SchemaDelta::DropColumn { name } if name == "name"));
    }

    #[test]
    fn diff_schema_no_delta_when_schemas_match() {
        let dest = vec![
            DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false },
            DestCol { name: "name".into(), pg_type: "text".into(), nullable: true },
        ];
        let batch_schema = Schema::new(fields(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]));
        let deltas = diff_schema(&batch_schema, &dest).unwrap();
        assert!(deltas.is_empty());
    }

    #[test]
    fn diff_schema_narrowing_int64_to_int32_is_destructive() {
        let dest = vec![DestCol { name: "val".into(), pg_type: "bigint".into(), nullable: true }];
        let batch_schema = Schema::new(fields(&[("val", DataType::Int32, true)]));
        let deltas = diff_schema(&batch_schema, &dest).unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(matches!(&deltas[0], SchemaDelta::NarrowType { name } if name == "val"));
    }

    #[test]
    fn add_column_ddl_builds_nullable_column() {
        let sql = add_column_ddl("myschema", "mytable", "score", "DOUBLE PRECISION", true);
        assert_eq!(
            sql,
            r#"ALTER TABLE "myschema"."mytable" ADD COLUMN IF NOT EXISTS "score" DOUBLE PRECISION"#
        );
    }

    #[test]
    fn add_column_ddl_always_omits_not_null() {
        let sql = add_column_ddl("s", "t", "id2", "BIGINT", false);
        assert!(!sql.contains("NOT NULL"), "ADD COLUMN must never emit NOT NULL; got: {sql}");
    }

    #[test]
    fn alter_column_type_ddl_emits_using_cast() {
        let sql = alter_column_type_ddl("public", "orders", "amount", "BIGINT");
        assert_eq!(
            sql,
            r#"ALTER TABLE "public"."orders" ALTER COLUMN "amount" TYPE BIGINT USING "amount"::BIGINT"#
        );
    }

    #[test]
    fn schema_delta_is_destructive_classification() {
        assert!(!SchemaDelta::AddColumn {
            name: "x".into(), pg_type: "TEXT".into(), nullable: true,
        }.is_destructive());
        assert!(!SchemaDelta::WidenType {
            name: "x".into(), new_pg_type: "BIGINT".into(),
        }.is_destructive());
        assert!(SchemaDelta::DropColumn { name: "x".into() }.is_destructive());
        assert!(SchemaDelta::NarrowType { name: "x".into() }.is_destructive());
        assert!(SchemaDelta::IncompatibleType {
            name: "x".into(),
            dest_pg_type: "text".into(),
            batch_pg_type: "boolean".into(),
        }.is_destructive());
    }
}
