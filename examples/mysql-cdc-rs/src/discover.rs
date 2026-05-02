//! Schema discovery: query information_schema for column metadata,
//! map MySQL types to Arrow DataTypes, find the table's primary key.

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
               WHERE table_schema = ? AND table_name = ? \
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
            data_type: map_mysql_type(ty),
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
    let sql = "SELECT column_name FROM information_schema.key_column_usage \
               WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY' \
               ORDER BY ordinal_position LIMIT 1";
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

pub fn map_mysql_type(t: &str) -> DataType {
    let t = t.to_ascii_lowercase();
    match t.as_str() {
        "bigint" => DataType::Int64,
        "int" | "mediumint" => DataType::Int32,
        "smallint" => DataType::Int16,
        "tinyint" => DataType::Int8,
        "varchar" | "text" | "char" | "mediumtext" | "longtext" | "tinytext" => DataType::Utf8,
        "bit" => DataType::Boolean,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
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
    fn map_mysql_type_handles_common_scalars() {
        assert_eq!(map_mysql_type("bigint"), DataType::Int64);
        assert_eq!(map_mysql_type("VARCHAR"), DataType::Utf8);
        assert_eq!(map_mysql_type("DateTime"), DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(map_mysql_type("date"), DataType::Date32);
    }

    #[test]
    fn map_mysql_type_falls_back_to_utf8() {
        assert_eq!(map_mysql_type("numeric"), DataType::Utf8);
        assert_eq!(map_mysql_type("json"), DataType::Utf8);
    }

    #[test]
    fn columns_to_fields_preserves_nullability_and_order() {
        let cols = vec![
            DiscoveredColumn { name: "id".into(), data_type: DataType::Int64, nullable: false },
            DiscoveredColumn { name: "name".into(), data_type: DataType::Utf8, nullable: true },
        ];
        let fields = columns_to_fields(&cols);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "id");
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(fields[1].is_nullable());
    }
}
