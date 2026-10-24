//! Destination schema evolution for the Snowflake loader (phase 2-5d,
//! mirroring the PG loader's 2-4d), run when a CDC batch carries a `c`
//! (schema change) event. The batch already has the new schema; the `c`
//! row is the signal.
//!
//! Additive changes apply: a batch column missing at the destination is
//! added (`ADD COLUMN IF NOT EXISTS`, nullable, safe on retry). Anything
//! else — a destination column the batch dropped, or a changed type —
//! pauses the pipeline with an error before any DDL runs. Snowflake has
//! no integer or float widening to apply: BIGINT/INTEGER/SMALLINT are all
//! NUMBER(38,0) and DOUBLE/FLOAT are both FLOAT, so base types either
//! match or the change is incompatible.
//!
//! Snowflake DDL commits implicitly, so the ALTERs run as their own
//! requests before the BEGIN/COMMIT data transaction.

use anyhow::bail;
use arrow::datatypes::Schema;

use super::sql::{escape_str, qualified, quote_ident, snowflake_type_for};

/// Columns of the target table: (COLUMN_NAME, DATA_TYPE).
pub fn dest_columns_sql(db: &str, schema: &str, table: &str) -> anyhow::Result<String> {
    Ok(format!(
        "SELECT COLUMN_NAME, DATA_TYPE FROM {}.INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
        quote_ident(db)?,
        escape_str(schema),
        escape_str(table),
    ))
}

pub fn parse_dest_columns(body: &serde_json::Value) -> anyhow::Result<Vec<(String, String)>> {
    let rows = body
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("INFORMATION_SCHEMA.COLUMNS response has no data"))?;
    rows.iter()
        .map(|r| {
            match (
                r.get(0).and_then(|v| v.as_str()),
                r.get(1).and_then(|v| v.as_str()),
            ) {
                (Some(name), Some(ty)) => Ok((name.to_string(), ty.to_string())),
                _ => bail!("unexpected INFORMATION_SCHEMA.COLUMNS row: {r}"),
            }
        })
        .collect()
}

/// The INFORMATION_SCHEMA.COLUMNS.DATA_TYPE a column created with
/// `ddl_type` reports.
fn base_type(ddl_type: &str) -> &str {
    match ddl_type {
        "BIGINT" | "INTEGER" | "SMALLINT" => "NUMBER",
        "DOUBLE" => "FLOAT",
        other => other,
    }
}

/// ALTERs that bring the destination up to `batch_schema` (data columns
/// only), or an error if the change is destructive.
pub fn evolution_ddl(
    db: &str,
    schema: &str,
    table: &str,
    dest: &[(String, String)],
    batch_schema: &Schema,
) -> anyhow::Result<Vec<String>> {
    for (name, _) in dest {
        if batch_schema.field_with_name(name).is_err() {
            bail!(
                "destructive schema change detected for {table:?}: column {name:?} was dropped \
                 at the source — operator action required"
            );
        }
    }
    let mut ddl = Vec::new();
    for field in batch_schema.fields() {
        let want = snowflake_type_for(field.data_type())?;
        match dest.iter().find(|(n, _)| n == field.name()) {
            Some((_, have)) if have.eq_ignore_ascii_case(base_type(want)) => {}
            Some((_, have)) => bail!(
                "destructive schema change detected for {table:?}: column {:?} changed type \
                 {have} → {want} — operator action required",
                field.name()
            ),
            None => ddl.push(format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {want}",
                qualified(db, schema, table)?,
                quote_ident(field.name())?,
            )),
        }
    }
    Ok(ddl)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn dest(cols: &[(&str, &str)]) -> Vec<(String, String)> {
        cols.iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    fn schema(cols: &[(&str, DataType)]) -> Schema {
        Schema::new(
            cols.iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn new_column_is_added_nullable() {
        let ddl = evolution_ddl(
            "DB",
            "RAW",
            "t",
            &dest(&[("id", "NUMBER"), ("name", "TEXT")]),
            &schema(&[
                ("id", DataType::Int64),
                ("name", DataType::Utf8),
                ("email", DataType::Utf8),
            ]),
        )
        .unwrap();
        assert_eq!(
            ddl,
            vec!["ALTER TABLE \"DB\".\"RAW\".\"t\" ADD COLUMN IF NOT EXISTS \"email\" TEXT"]
        );
    }

    #[test]
    fn matching_schema_needs_nothing() {
        // Every integer width reports NUMBER; DOUBLE and FLOAT report FLOAT.
        let ddl = evolution_ddl(
            "DB",
            "RAW",
            "t",
            &dest(&[
                ("a", "NUMBER"),
                ("b", "NUMBER"),
                ("c", "FLOAT"),
                ("d", "TIMESTAMP_TZ"),
            ]),
            &schema(&[
                ("a", DataType::Int32),
                ("b", DataType::Int16),
                ("c", DataType::Float64),
                (
                    "d",
                    DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ),
                ),
            ]),
        )
        .unwrap();
        assert!(ddl.is_empty(), "got {ddl:?}");
    }

    #[test]
    fn dropped_column_pauses() {
        let err = evolution_ddl(
            "DB",
            "RAW",
            "t",
            &dest(&[("id", "NUMBER"), ("old", "TEXT")]),
            &schema(&[("id", DataType::Int64)]),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("destructive"), "got {err}");
        assert!(format!("{err}").contains("\"old\""), "got {err}");
    }

    #[test]
    fn changed_type_pauses_before_any_add() {
        let err = evolution_ddl(
            "DB",
            "RAW",
            "t",
            &dest(&[("id", "NUMBER")]),
            &schema(&[("id", DataType::Utf8), ("new", DataType::Utf8)]),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("destructive"), "got {err}");
    }

    #[test]
    fn dest_columns_query_and_parse() {
        let sql = dest_columns_sql("DB", "RAW", "it's").unwrap();
        assert!(
            sql.contains("\"DB\".INFORMATION_SCHEMA.COLUMNS"),
            "got {sql}"
        );
        assert!(sql.contains("TABLE_NAME = 'it''s'"), "got {sql}");
        let cols =
            parse_dest_columns(&serde_json::json!({"data": [["id", "NUMBER"], ["n", "TEXT"]]}))
                .unwrap();
        assert_eq!(cols, dest(&[("id", "NUMBER"), ("n", "TEXT")]));
        assert!(parse_dest_columns(&serde_json::json!({"data": [["id"]]})).is_err());
    }
}
