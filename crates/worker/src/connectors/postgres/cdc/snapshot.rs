use anyhow::Context;
use arrow::array::{ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use sqlx::{Connection, PgConnection, Row};
use std::sync::Arc;

pub struct SnapshotChunk {
    pub batch: RecordBatch,
    pub is_final: bool,
    pub last_pk: Option<i64>,
}

/// Reads one chunk of rows where pk > last_pk, ordered by pk, limit batch_size.
/// The read runs in a fresh REPEATABLE READ READ ONLY transaction — for MVP
/// we rely on the slot's consistent_point being close in time.
pub async fn read_chunk(
    conn_url: &str,
    schema: &str,
    table: &str,
    pk_col: &str,
    last_pk: Option<i64>,
    batch_size: usize,
    consistent_point: &str,
    cdc_schema: SchemaRef,
) -> anyhow::Result<SnapshotChunk> {
    let mut c = PgConnection::connect(conn_url).await?;
    sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut c)
        .await?;
    // Build a `SELECT col1::text AS col1, col2::text AS col2, ..."
    // projection so every value lands as a Postgres text-format
    // string. parse_pg_text in rows_to_cdc_batch then produces
    // typed Arrow values per the schema's declared DataType.
    let data_field_names: Vec<&str> = cdc_schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with("_cdc"))
        .map(|f| f.name().as_str())
        .collect();
    let projection = data_field_names
        .iter()
        .map(|n| format!("\"{n}\"::text AS \"{n}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = match last_pk {
        Some(_) => format!(" WHERE \"{pk_col}\" > $1"),
        None => String::new(),
    };
    let stmt = format!(
        "SELECT {projection} FROM \"{schema}\".\"{table}\"{where_clause} ORDER BY \"{pk_col}\" LIMIT {batch_size}"
    );
    let mut q = sqlx::query(&stmt);
    if let Some(pk) = last_pk {
        q = q.bind(pk);
    }
    let rows = q.fetch_all(&mut c).await?;
    sqlx::query("COMMIT").execute(&mut c).await?;

    let (batch, last) = rows_to_cdc_batch(rows, pk_col, cdc_schema, consistent_point)
        .context("rows → cdc batch")?;
    let is_final = batch.num_rows() < batch_size;
    Ok(SnapshotChunk { batch, is_final, last_pk: last })
}

fn rows_to_cdc_batch(
    rows: Vec<sqlx::postgres::PgRow>,
    pk_col: &str,
    schema: SchemaRef,
    consistent_point: &str,
) -> anyhow::Result<(RecordBatch, Option<i64>)> {
    use arrow::array::ArrayBuilder;
    use crate::connectors::postgres::cdc::types::{
        append_pg_scalar, make_pg_builder, parse_pg_text,
    };

    let mut last_pk: Option<i64> = None;
    let n_data = schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with("_cdc"))
        .count();

    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data)
        .map(|i| make_pg_builder(schema.field(i).data_type()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    let mut tx_b = Int64Builder::new();

    for r in &rows {
        for i in 0..n_data {
            let f = schema.field(i);
            let dt = f.data_type();
            // Every column was selected as ::text — extract as
            // Option<String> and let parse_pg_text do the typed conversion.
            let raw: Option<String> = r
                .try_get::<Option<String>, _>(f.name().as_str())
                .with_context(|| format!("try_get text for column {}", f.name()))?;
            let parsed = match raw.as_deref() {
                Some(s) => parse_pg_text(s, dt).with_context(|| {
                    format!("parse_pg_text col={} dt={:?} raw={:?}", f.name(), dt, s)
                })?,
                None => None,
            };
            append_pg_scalar(&mut *col_builders[i], parsed.as_ref(), dt)?;
        }
        op_b.append_value("s");
        lsn_b.append_value(consistent_point);
        ts_b.append_null();
        tx_b.append_null();
        // pk extraction: the column was selected as ::text, so try_get
        // as String and re-parse to i64. Snapshot only supports i64 PKs
        // for now (matches the cursor type elsewhere in CDC).
        if let Ok(Some(s)) = r.try_get::<Option<String>, _>(pk_col) {
            if let Ok(n) = s.parse::<i64>() {
                last_pk = Some(n);
            }
        }
    }

    let mut cols: Vec<ArrayRef> = col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish()));
    cols.push(Arc::new(tx_b.finish()));
    let batch = RecordBatch::try_new(schema, cols)?;
    Ok((batch, last_pk))
}

pub fn cdc_schema_for(data_cols: &[(&str, DataType)]) -> SchemaRef {
    let mut fields: Vec<Field> = data_cols
        .iter()
        .map(|(n, t)| Field::new(*n, t.clone(), true))
        .collect();
    fields.push(Field::new(common_types::cdc::COL_OP, DataType::Utf8, false));
    fields.push(Field::new(common_types::cdc::COL_LSN, DataType::Utf8, false));
    fields.push(Field::new(
        common_types::cdc::COL_COMMIT_TS,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    ));
    fields.push(Field::new(common_types::cdc::COL_TXID, DataType::Int64, true));
    Arc::new(Schema::new(fields))
}
