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
    let where_clause = match last_pk {
        Some(_) => format!(" WHERE \"{pk_col}\" > $1"),
        None => String::new(),
    };
    let stmt = format!(
        "SELECT * FROM \"{schema}\".\"{table}\"{where_clause} ORDER BY \"{pk_col}\" LIMIT {batch_size}"
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
    let mut last_pk: Option<i64> = None;
    // Data columns (non-_cdc) rendered as String for MVP — CDC lands TEXT;
    // downstream Phase-II consumers can type-narrow.
    let data_fields: Vec<Field> = schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with("_cdc"))
        .map(|f| Field::clone(f))
        .collect();
    let mut col_builders: Vec<StringBuilder> =
        data_fields.iter().map(|_| StringBuilder::new()).collect();
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    let mut tx_b = Int64Builder::new();
    for r in &rows {
        for (i, f) in data_fields.iter().enumerate() {
            // Try string first; fall back to cast-to-text of any kind via ::text cast would
            // require a second pass. For MVP the snapshot query is `SELECT *` and we cast
            // values via try_get::<Option<String>>, which works for text/varchar columns
            // and returns an Err for non-text (we then write NULL).
            let v: Option<String> = r
                .try_get::<Option<String>, _>(f.name().as_str())
                .ok()
                .flatten()
                .or_else(|| {
                    // Numeric types: try i64/f64 and to_string.
                    r.try_get::<Option<i64>, _>(f.name().as_str())
                        .ok()
                        .flatten()
                        .map(|v| v.to_string())
                        .or_else(|| {
                            r.try_get::<Option<f64>, _>(f.name().as_str())
                                .ok()
                                .flatten()
                                .map(|v| v.to_string())
                        })
                });
            col_builders[i].append_option(v);
        }
        op_b.append_value("s");
        lsn_b.append_value(consistent_point);
        ts_b.append_null();
        tx_b.append_null();
        if let Ok(pk) = r.try_get::<i64, _>(pk_col) {
            last_pk = Some(pk);
        }
    }
    let mut cols: Vec<ArrayRef> = col_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
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
