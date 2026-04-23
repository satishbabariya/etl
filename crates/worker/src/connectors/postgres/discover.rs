use anyhow::{Context, bail};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::PostgresSourceSpec;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

/// Introspect a Postgres table via `information_schema` and produce an
/// Arrow schema. Phase I.2 supported types: bigint, text, timestamptz.
pub async fn run(
    conn: &ConnectionConfig,
    spec: &PostgresSourceSpec,
) -> anyhow::Result<SchemaRef> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&conn.url)
        .await
        .with_context(|| format!("connecting to source for discover: {}", spec.table))?;

    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT column_name, udt_name, is_nullable = 'YES' \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(&spec.schema)
    .bind(&spec.table)
    .fetch_all(&pool)
    .await
    .with_context(|| format!("introspecting {}.{}", spec.schema, spec.table))?;

    if rows.is_empty() {
        bail!("table {}.{} not found or has no columns", spec.schema, spec.table);
    }

    let mut fields = Vec::with_capacity(rows.len());
    for (col_name, udt, nullable) in rows {
        let dtype = pg_udt_to_arrow(&udt)
            .with_context(|| format!("unsupported column {}: {}", col_name, udt))?;
        fields.push(Field::new(&col_name, dtype, nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn pg_udt_to_arrow(udt: &str) -> anyhow::Result<DataType> {
    Ok(match udt {
        "int8" => DataType::Int64,
        "int4" => DataType::Int32,
        "int2" => DataType::Int16,
        "text" | "varchar" | "bpchar" => DataType::Utf8,
        "bool" => DataType::Boolean,
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "date" => DataType::Date32,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        other => bail!("Phase I.2 does not support Postgres type '{other}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::cursor::CursorKind;

    fn test_url() -> String {
        std::env::var("SOURCE_URL")
            .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn discover_customers() {
        let schema = run(
            &ConnectionConfig { url: test_url() },
            &PostgresSourceSpec {
                schema: "public".into(),
                table: "customers".into(),
                cursor_column: "updated_at".into(),
                cursor_kind: CursorKind::TimestampTz,
                pk_columns: vec!["id".into()],
                sync_mode: Default::default(),
            },
        )
        .await
        .unwrap();

        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "email", "created_at", "updated_at"]);

        assert_eq!(schema.field_with_name("id").unwrap().data_type(), &DataType::Int64);
        assert_eq!(schema.field_with_name("name").unwrap().data_type(), &DataType::Utf8);
        assert!(!schema.field_with_name("name").unwrap().is_nullable());
        assert!(schema.field_with_name("email").unwrap().is_nullable());
    }
}
