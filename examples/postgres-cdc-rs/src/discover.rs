//! Schema discovery for Postgres: information_schema.columns +
//! information_schema.key_column_usage for primary key. Maps Postgres
//! data_type to Arrow DataType, falling back to Utf8 for unsupported
//! types.

use arrow_schema::{DataType, Field, TimeUnit};

use crate::platform::connector::db;
use crate::snapshot::db_err_to_connector_err;
use crate::ConnectorError;

#[derive(Clone, Debug)]
pub struct DiscoveredColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub fn query_columns(
    h: db::DbHandle,
    schema: &str,
    table: &str,
) -> Result<Vec<DiscoveredColumn>, ConnectorError> {
    let sql = "SELECT column_name, data_type, is_nullable \
               FROM information_schema.columns \
               WHERE table_schema = $1 AND table_name = $2 \
               ORDER BY ordinal_position";
    let rows = db::query(h, sql, &[schema.to_string(), table.to_string()])
        .map_err(db_err_to_connector_err)?;
    if rows.is_empty() {
        return Err(ConnectorError::InvalidConfig(format!(
            "table {schema}.{table} not found in information_schema.columns"
        )));
    }
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let name: &str = r.first().and_then(|c| c.as_deref()).unwrap_or("");
        let ty: &str = r.get(1).and_then(|c| c.as_deref()).unwrap_or("");
        let nullable: bool = r
            .get(2)
            .and_then(|c| c.as_deref())
            .map(|s| s.eq_ignore_ascii_case("YES"))
            .unwrap_or(true);
        if name.is_empty() {
            continue;
        }
        out.push(DiscoveredColumn {
            name: name.to_string(),
            data_type: map_pg_type(ty),
            nullable,
        });
    }
    Ok(out)
}

pub fn query_pk_column(
    h: db::DbHandle,
    schema: &str,
    table: &str,
) -> Result<String, ConnectorError> {
    let sql = "SELECT kcu.column_name \
               FROM information_schema.table_constraints tc \
               JOIN information_schema.key_column_usage kcu \
                 ON tc.constraint_name = kcu.constraint_name \
                AND tc.table_schema = kcu.table_schema \
               WHERE tc.constraint_type = 'PRIMARY KEY' \
                 AND tc.table_schema = $1 \
                 AND tc.table_name = $2 \
               ORDER BY kcu.ordinal_position LIMIT 1";
    let rows = db::query(h, sql, &[schema.to_string(), table.to_string()])
        .map_err(db_err_to_connector_err)?;
    rows.into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .flatten()
        .ok_or_else(|| {
            ConnectorError::InvalidConfig(format!(
                "table {schema}.{table} has no primary key; required for snapshot ordering"
            ))
        })
}

pub fn map_pg_type(t: &str) -> DataType {
    let t = t.to_ascii_lowercase();
    let t = t.trim();
    match t {
        "bigint" | "int8" => DataType::Int64,
        "integer" | "int4" => DataType::Int32,
        "smallint" | "int2" => DataType::Int16,
        "text" | "character varying" | "varchar" | "name" | "character" | "char" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "real" | "float4" => DataType::Float32,
        "double precision" | "float8" => DataType::Float64,
        "timestamp without time zone" | "timestamp" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "timestamp with time zone" | "timestamptz" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "date" => DataType::Date32,
        _ => DataType::Utf8,
    }
}

pub fn columns_to_fields(cols: &[DiscoveredColumn]) -> Vec<Field> {
    cols.iter()
        .map(|c| Field::new(c.name.as_str(), c.data_type.clone(), c.nullable))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_pg_type_handles_common_scalars() {
        assert_eq!(map_pg_type("bigint"), DataType::Int64);
        assert_eq!(map_pg_type("text"), DataType::Utf8);
        assert_eq!(map_pg_type("Boolean"), DataType::Boolean);
        assert_eq!(
            map_pg_type("timestamp without time zone"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn map_pg_type_falls_back_to_utf8() {
        assert_eq!(map_pg_type("numeric"), DataType::Utf8);
        assert_eq!(map_pg_type("uuid"), DataType::Utf8);
    }

    #[test]
    fn columns_to_fields_preserves_order() {
        let cols = vec![
            DiscoveredColumn {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            DiscoveredColumn {
                name: "name".into(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ];
        let fields = columns_to_fields(&cols);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
    }
}
