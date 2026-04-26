use anyhow::{Context, bail};
use arrow::array::{
    ArrayBuilder, ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use common_types::connection_config::ConnectionConfig;
use common_types::cursor::{CursorKind, CursorValue};
use common_types::pipeline_spec::PostgresSourceSpec;
use connector_sdk::ReadOutcome;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

pub async fn run(
    conn: &ConnectionConfig,
    spec: &PostgresSourceSpec,
    cursor: Option<CursorValue>,
    batch_size: usize,
) -> anyhow::Result<ReadOutcome> {
    let schema = super::discover::run(conn, spec).await?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(conn.expect_url())
        .await
        .context("connecting to source for read_batch")?;

    let column_list = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = match cursor.as_ref() {
        None => format!(
            "SELECT {} FROM \"{}\".\"{}\" ORDER BY \"{}\" ASC LIMIT {}",
            column_list, spec.schema, spec.table, spec.cursor_column, batch_size as i64,
        ),
        Some(_) => format!(
            "SELECT {} FROM \"{}\".\"{}\" WHERE \"{}\" > $1 ORDER BY \"{}\" ASC LIMIT {}",
            column_list,
            spec.schema,
            spec.table,
            spec.cursor_column,
            spec.cursor_column,
            batch_size as i64,
        ),
    };

    let mut query = sqlx::query(&sql);
    if let Some(c) = cursor.as_ref() {
        query = bind_cursor(query, c)?;
    }

    let rows = query.fetch_all(&pool).await.context("executing read_batch")?;
    let row_count = rows.len();

    let batch = rows_to_recordbatch(&schema, &rows)?;

    let new_cursor = if row_count == 0 {
        None
    } else {
        let last = &rows[row_count - 1];
        Some(extract_cursor(last, &spec.cursor_column, spec.cursor_kind)?)
    };

    Ok(ReadOutcome {
        batch,
        new_cursor,
        is_final: row_count < batch_size,
    })
}

fn bind_cursor<'a>(
    q: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    cursor: &CursorValue,
) -> anyhow::Result<sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    Ok(match cursor.kind {
        CursorKind::Int64 => {
            let v: i64 = cursor.value.parse().context("cursor value is not i64")?;
            q.bind(v)
        }
        CursorKind::TimestampTz => {
            let v: DateTime<Utc> = cursor
                .value
                .parse()
                .context("cursor value is not RFC-3339 timestamptz")?;
            q.bind(v)
        }
        CursorKind::Lsn => anyhow::bail!("LSN cursors only valid in CDC mode"),
    })
}

fn extract_cursor(
    row: &sqlx::postgres::PgRow,
    column: &str,
    kind: CursorKind,
) -> anyhow::Result<CursorValue> {
    Ok(match kind {
        CursorKind::Int64 => CursorValue {
            kind,
            value: row.try_get::<i64, _>(column)?.to_string(),
        },
        CursorKind::TimestampTz => {
            let ts: DateTime<Utc> = row.try_get(column)?;
            CursorValue {
                kind,
                value: ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            }
        }
        CursorKind::Lsn => anyhow::bail!("LSN cursors only valid in CDC mode"),
    })
}

fn rows_to_recordbatch(
    schema: &SchemaRef,
    rows: &[sqlx::postgres::PgRow],
) -> anyhow::Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut builders: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type(), rows.len()))
        .collect::<anyhow::Result<Vec<_>>>()?;

    for row in rows {
        for (col_idx, field) in schema.fields().iter().enumerate() {
            append_cell(&mut builders[col_idx], field, row, field.name())?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

fn make_builder(dtype: &DataType, capacity: usize) -> anyhow::Result<Box<dyn ArrayBuilder>> {
    Ok(match dtype {
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 16)),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => Box::new(
            TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_timezone("+00:00"),
        ),
        other => bail!("no Arrow builder wired for {other:?} in Phase I.2"),
    })
}

fn append_cell(
    builder: &mut Box<dyn ArrayBuilder>,
    field: &arrow::datatypes::Field,
    row: &sqlx::postgres::PgRow,
    col_name: &str,
) -> anyhow::Result<()> {
    match field.data_type() {
        DataType::Int64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("builder type mismatch");
            let v: Option<i64> = row.try_get(col_name)?;
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("builder type mismatch");
            let v: Option<String> = row.try_get(col_name)?;
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .expect("builder type mismatch");
            let v: Option<DateTime<Utc>> = row.try_get(col_name)?;
            match v {
                Some(x) => b.append_value(x.timestamp_micros()),
                None => b.append_null(),
            }
        }
        other => bail!("no cell appender wired for {other:?} in Phase I.2"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_url() -> String {
        std::env::var("SOURCE_URL")
            .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
    }

    fn spec() -> PostgresSourceSpec {
        PostgresSourceSpec {
            schema: "public".into(),
            table: "customers".into(),
            cursor_column: "updated_at".into(),
            cursor_kind: CursorKind::TimestampTz,
            pk_columns: vec!["id".into()],
            sync_mode: Default::default(),
        }
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn fresh_read_returns_first_batch_sorted() {
        let conn = ConnectionConfig::from_url(test_url());
        let out = run(&conn, &spec(), None, 3).await.unwrap();
        assert_eq!(out.batch.num_rows(), 3);
        assert!(!out.is_final);
        assert!(out.new_cursor.unwrap().value.starts_with("2026-04-20T12:00"));
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn cursor_advance_reads_subsequent_rows() {
        let conn = ConnectionConfig::from_url(test_url());
        let first = run(&conn, &spec(), None, 3).await.unwrap();
        let second = run(&conn, &spec(), first.new_cursor.clone(), 3).await.unwrap();
        assert_eq!(second.batch.num_rows(), 3);
        let id_col = second
            .batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[4, 5, 6]);
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn is_final_when_batch_smaller_than_requested() {
        let conn = ConnectionConfig::from_url(test_url());
        let out = run(&conn, &spec(), None, 100).await.unwrap();
        assert_eq!(out.batch.num_rows(), 10);
        assert!(out.is_final);
    }
}
