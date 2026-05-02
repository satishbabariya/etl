//! MySQL → Arrow schema discovery.
//!
//! v1 supports a fixed type subset; anything else fails the workflow at
//! discovery time. We add more types as connectors need them — we don't
//! speculate.

use anyhow::{bail, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InfoSchemaColumn {
    pub column_name: String,
    /// `DATA_TYPE` from `information_schema.columns` (e.g. "int", "varchar").
    pub data_type: String,
    pub is_nullable: bool,
    pub ordinal_position: u32,
}

pub fn map_mysql_type(mysql_type: &str) -> Result<DataType> {
    let lower = mysql_type.to_ascii_lowercase();
    let dt = match lower.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "decimal" | "numeric" => DataType::Float64,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => DataType::Utf8,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "boolean" | "bool" | "bit" => DataType::Boolean,
        "json" => DataType::Utf8,
        "tinyblob" | "blob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
            DataType::Binary
        }
        other => bail!("unsupported MySQL type '{other}'"),
    };
    Ok(dt)
}

/// Build the Arrow schema for the streaming RecordBatch. Data columns
/// carry the typed `DataType` from `map_mysql_type`; the trailing three
/// `_cdc.*` metadata columns are fixed (Utf8 for op/lsn, Timestamp for
/// commit_ts) per RFC-0008.
pub fn schema_from_columns(cols: &[InfoSchemaColumn]) -> Result<Schema> {
    let mut sorted: Vec<_> = cols.iter().collect();
    sorted.sort_by_key(|c| c.ordinal_position);
    let mut fields: Vec<Field> = Vec::with_capacity(sorted.len() + 3);
    for c in sorted {
        let dt = map_mysql_type(&c.data_type)?;
        fields.push(Field::new(&c.column_name, dt, c.is_nullable));
    }
    fields.push(Field::new("_cdc.op", DataType::Utf8, false));
    fields.push(Field::new("_cdc.lsn", DataType::Utf8, false));
    fields.push(Field::new(
        "_cdc.commit_ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    Ok(Schema::new(fields))
}

/// Live `information_schema.columns` query. Returns the raw column
/// records so callers can serialize them for replay through Temporal.
/// Use `schema_from_columns` to derive the Arrow `Schema`.
pub async fn discover_columns(
    pool: &mysql_async::Pool,
    schema: &str,
    table: &str,
) -> Result<Vec<InfoSchemaColumn>> {
    use mysql_async::prelude::*;
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(String, String, String, u32)> = conn
        .exec(
            "SELECT column_name, data_type, is_nullable, ordinal_position
             FROM information_schema.columns
             WHERE table_schema = ? AND table_name = ?
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await?;
    if rows.is_empty() {
        bail!("table {schema}.{table} not found in information_schema");
    }
    Ok(rows
        .into_iter()
        .map(|(column_name, data_type, is_nullable, ordinal_position)| InfoSchemaColumn {
            column_name,
            data_type,
            is_nullable: is_nullable.eq_ignore_ascii_case("YES"),
            ordinal_position,
        })
        .collect())
}

/// Convenience: discover and convert in one call.
pub async fn discover_schema(
    pool: &mysql_async::Pool,
    schema: &str,
    table: &str,
) -> Result<Schema> {
    let cols = discover_columns(pool, schema, table).await?;
    schema_from_columns(&cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, n: u32, nullable: bool) -> InfoSchemaColumn {
        InfoSchemaColumn {
            column_name: name.into(),
            data_type: ty.into(),
            is_nullable: nullable,
            ordinal_position: n,
        }
    }

    #[test]
    fn maps_int_family() {
        assert_eq!(map_mysql_type("int").unwrap(), DataType::Int32);
        assert_eq!(map_mysql_type("INT").unwrap(), DataType::Int32);
        assert_eq!(map_mysql_type("smallint").unwrap(), DataType::Int32);
        assert_eq!(map_mysql_type("bigint").unwrap(), DataType::Int64);
    }

    #[test]
    fn maps_varchar_to_utf8() {
        assert_eq!(map_mysql_type("varchar").unwrap(), DataType::Utf8);
        assert_eq!(map_mysql_type("text").unwrap(), DataType::Utf8);
        assert_eq!(map_mysql_type("longtext").unwrap(), DataType::Utf8);
    }

    #[test]
    fn maps_datetime_to_timestamp_micros() {
        let got = map_mysql_type("datetime").unwrap();
        match got {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert_eq!(tz.as_ref(), "UTC")
            }
            other => panic!("expected Timestamp(Micro, UTC), got {other:?}"),
        }
    }

    #[test]
    fn unsupported_type_returns_error() {
        let err = map_mysql_type("geometry").unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn schema_emits_typed_data_columns() {
        let cols = vec![
            col("id", "bigint", 1, false),
            col("email", "varchar", 2, true),
            col("created", "timestamp", 3, false),
        ];
        let s = schema_from_columns(&cols).unwrap();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "email", "created", "_cdc.op", "_cdc.lsn", "_cdc.commit_ts"]
        );
        // v2: typed data columns.
        assert_eq!(s.field(0).data_type(), &DataType::Int64);
        assert_eq!(s.field(1).data_type(), &DataType::Utf8);
        assert!(s.field(1).is_nullable());
        match s.field(2).data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert_eq!(tz.as_ref(), "UTC")
            }
            other => panic!("expected Timestamp(Micro, UTC) for created, got {other:?}"),
        }
        // _cdc.op stays Utf8.
        assert_eq!(s.field(3).data_type(), &DataType::Utf8);
        assert!(!s.field(3).is_nullable());
        // _cdc.commit_ts is the timestamp metadata column.
        assert!(matches!(
            s.field(5).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
    }

    #[test]
    fn maps_blob_family_to_binary() {
        assert_eq!(map_mysql_type("blob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("tinyblob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("mediumblob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("longblob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("binary").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("varbinary").unwrap(), DataType::Binary);
    }

    #[test]
    fn maps_time_to_time64_micros() {
        assert_eq!(
            map_mysql_type("time").unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }
}
