//! Snapshot reader for MySQL CDC.
//!
//! Per-chunk `START TRANSACTION WITH CONSISTENT SNAPSHOT` over a
//! PK-monotonic SELECT. Columns are text-cast (`CAST(col AS CHAR)`)
//! and parsed via `parse_mysql_text` to typed `ScalarValue`s. Builds
//! a typed Arrow `RecordBatch` with `_cdc.op = "s"` per row.

use anyhow::{anyhow, Context, Result};
use arrow::array::{ArrayBuilder, ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::decode::{parse_mysql_text, ScalarValue};

pub struct SnapshotChunk {
    pub batch: Option<RecordBatch>,
    pub rows: usize,
    pub last_pk: Option<i64>,
    pub is_final: bool,
}

/// Compose the SELECT statement for one snapshot chunk. Public for
/// unit-testing the SQL shape; the real query is executed by `read_chunk`.
/// Per-column projection: `HEX(\`col\`)` for Binary columns,
/// `CAST(\`col\` AS CHAR)` for everything else.
pub fn build_chunk_sql(
    schema: &str,
    table: &str,
    pk_col: &str,
    data_columns: &[(&str, &arrow::datatypes::DataType)],
    has_last_pk: bool,
    batch_size: usize,
) -> String {
    let projection = data_columns
        .iter()
        .map(|(name, dt)| match dt {
            arrow::datatypes::DataType::Binary => {
                format!("HEX(`{name}`) AS `{name}`")
            }
            _ => format!("CAST(`{name}` AS CHAR) AS `{name}`"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = if has_last_pk {
        format!(" WHERE `{pk_col}` > ?")
    } else {
        String::new()
    };
    format!(
        "SELECT {projection} FROM `{schema}`.`{table}`{where_clause} ORDER BY `{pk_col}` LIMIT {batch_size}"
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn read_chunk(
    conn_url: &str,
    schema_name: &str,
    table_name: &str,
    pk_column: &str,
    last_pk: Option<i64>,
    batch_size: usize,
    arrow_schema: SchemaRef,
    captured_gtid: &str,
) -> Result<SnapshotChunk> {
    use mysql_async::prelude::*;

    let pool = mysql_async::Pool::new(conn_url);
    let mut conn = pool.get_conn().await.context("mysql connect")?;

    // Single statement: sets isolation + takes the consistent point.
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .context("BEGIN consistent snapshot")?;

    let n_data = arrow_schema
        .fields()
        .len()
        .checked_sub(3)
        .ok_or_else(|| anyhow!("schema must have at least 3 _cdc.* metadata columns"))?;
    let data_columns: Vec<(&str, &arrow::datatypes::DataType)> = arrow_schema
        .fields()
        .iter()
        .take(n_data)
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();

    let stmt = build_chunk_sql(
        schema_name,
        table_name,
        pk_column,
        &data_columns,
        last_pk.is_some(),
        batch_size,
    );

    let rows: Vec<mysql_async::Row> = match last_pk {
        Some(pk) => conn.exec(&stmt, (pk,)).await.context("snapshot SELECT")?,
        None => conn.query(&stmt).await.context("snapshot SELECT")?,
    };

    conn.query_drop("COMMIT")
        .await
        .context("COMMIT snapshot tx")?;
    drop(conn);
    pool.disconnect().await.ok();

    if rows.is_empty() {
        return Ok(SnapshotChunk {
            batch: None,
            rows: 0,
            last_pk,
            is_final: true,
        });
    }

    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data)
        .map(|i| make_snapshot_builder(arrow_schema.field(i).data_type()))
        .collect::<Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();

    let mut last_pk_seen: Option<i64> = last_pk;

    for row in &rows {
        for i in 0..n_data {
            let f = arrow_schema.field(i);
            let dt = f.data_type();
            // CAST(col AS CHAR) returns String or NULL; we type-extract
            // as Option<String>.
            let raw: Option<String> = row
                .get_opt::<Option<String>, _>(f.name().as_str())
                .ok_or_else(|| anyhow!("column {} not found in row", f.name()))?
                .map_err(|e| anyhow!("column {} extract: {}", f.name(), e))?;
            let parsed = match raw.as_deref() {
                Some(s) => parse_mysql_text(s, dt)?,
                None => None,
            };
            append_snapshot_scalar(&mut *col_builders[i], parsed.as_ref(), dt)?;
        }
        op_b.append_value("s");
        lsn_b.append_value(captured_gtid);
        ts_b.append_null();
        // PK is one of the data columns — re-extract as i64.
        let pk_raw: Option<String> = row
            .get_opt::<Option<String>, _>(pk_column)
            .ok_or_else(|| anyhow!("pk column {} not found", pk_column))?
            .map_err(|e| anyhow!("pk extract: {}", e))?;
        if let Some(pk_s) = pk_raw {
            if let Ok(n) = pk_s.parse::<i64>() {
                last_pk_seen = Some(n);
            }
        }
    }

    let mut cols: Vec<ArrayRef> =
        col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    // _cdc.commit_ts has UTC timezone in the schema; finish-with-timezone
    // matches the schema-declared type.
    cols.push(Arc::new(ts_b.finish().with_timezone("UTC")));

    let batch = RecordBatch::try_new(arrow_schema, cols).context("build snapshot RecordBatch")?;
    let row_count = rows.len();
    Ok(SnapshotChunk {
        batch: Some(batch),
        rows: row_count,
        last_pk: last_pk_seen,
        is_final: row_count < batch_size,
    })
}

fn make_snapshot_builder(
    dt: &arrow::datatypes::DataType,
) -> Result<Box<dyn ArrayBuilder>> {
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, Time64MicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match dt {
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::new();
            if let Some(tz) = tz.as_ref() {
                b = b.with_timezone(Arc::clone(tz));
            }
            Box::new(b)
        }
        other => return Err(anyhow!("no snapshot builder for DataType {:?}", other)),
    })
}

fn append_snapshot_scalar(
    builder: &mut dyn ArrayBuilder,
    scalar: Option<&ScalarValue>,
    dt: &arrow::datatypes::DataType,
) -> Result<()> {
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, Time64MicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_null(),
        (Some(ScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_null(),
        (Some(ScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_null(),
        (Some(ScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_null(),
        (Some(ScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_null(),
        (Some(ScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_null(),
        (Some(ScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_null(),
        (Some(ScalarValue::Binary(b)), DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_value(b.as_slice()),
        (None, DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_null(),
        (Some(ScalarValue::Time64Micros(t)), DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_value(*t),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_null(),
        (Some(ScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(ScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
                .append_value(*t)
        }
        (Some(other_v), other_dt) => {
            return Err(anyhow!(
                "scalar/builder mismatch: {:?} into {:?}",
                other_v,
                other_dt
            ))
        }
        (None, other_dt) => {
            return Err(anyhow!(
                "no null-append path for builder type {:?}",
                other_dt
            ))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_sql_with_last_pk() {
        use arrow::datatypes::DataType;
        let cols: Vec<(&str, &DataType)> = vec![
            ("id", &DataType::Int64),
            ("customer", &DataType::Utf8),
            ("amount", &DataType::Utf8),
        ];
        let s = build_chunk_sql("shop", "orders", "id", &cols, true, 500);
        assert!(s.contains("`shop`.`orders`"));
        assert!(s.contains("WHERE `id` > ?"));
        assert!(s.contains("ORDER BY `id`"));
        assert!(s.contains("LIMIT 500"));
        assert!(s.contains("CAST(`id` AS CHAR) AS `id`"));
        assert!(s.contains("CAST(`customer` AS CHAR) AS `customer`"));
    }

    #[test]
    fn build_sql_without_last_pk() {
        use arrow::datatypes::DataType;
        let cols: Vec<(&str, &DataType)> = vec![
            ("id", &DataType::Int64),
            ("amount", &DataType::Utf8),
        ];
        let s = build_chunk_sql("shop", "orders", "id", &cols, false, 100);
        assert!(!s.contains("WHERE"));
        assert!(s.contains("ORDER BY `id`"));
        assert!(s.contains("LIMIT 100"));
    }

    #[test]
    fn build_sql_uses_hex_for_binary() {
        use arrow::datatypes::DataType;
        let cols: Vec<(&str, &DataType)> = vec![
            ("id", &DataType::Int64),
            ("payload", &DataType::Binary),
            ("name", &DataType::Utf8),
        ];
        let s = build_chunk_sql("shop", "blobs", "id", &cols, false, 100);
        assert!(
            s.contains("HEX(`payload`) AS `payload`"),
            "expected HEX projection for binary column; got: {s}"
        );
        assert!(s.contains("CAST(`id` AS CHAR) AS `id`"));
        assert!(s.contains("CAST(`name` AS CHAR) AS `name`"));
    }
}
