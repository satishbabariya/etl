//! Pure SQL text builders for the Snowflake loader. No I/O — everything
//! here is unit-testable. Literals are rendered (no bind params) because
//! the SQL API's binding model doesn't do multi-row inserts; batches are
//! dogfood-sized (see the 2-5a design memo §5).

use anyhow::{bail, Context};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};

pub fn quote_ident(name: &str) -> anyhow::Result<String> {
    if name.is_empty() {
        bail!("empty identifier");
    }
    if name
        .chars()
        .any(|c| c == '"' || c == '\0' || c.is_control())
    {
        bail!("invalid character in identifier {name:?}");
    }
    Ok(format!("\"{name}\""))
}

pub fn snowflake_type_for(dt: &DataType) -> anyhow::Result<&'static str> {
    Ok(match dt {
        DataType::Int64 => "BIGINT",
        DataType::Int32 => "INTEGER",
        DataType::Int16 => "SMALLINT",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Boolean => "BOOLEAN",
        DataType::Float64 => "DOUBLE",
        DataType::Float32 => "FLOAT",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMP_TZ",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP_NTZ",
        DataType::Date32 => "DATE",
        DataType::Binary | DataType::LargeBinary => "BINARY",
        DataType::Time64(TimeUnit::Microsecond) => "TIME",
        other => bail!("unsupported Arrow type for Snowflake: {other:?}"),
    })
}

// Snowflake treats backslash as an escape character inside single-quoted
// literals, so it must be doubled (before quotes) or a value ending in `\`
// would escape the closing quote.
fn escape_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn sql_literal(col: &dyn Array, row: usize) -> anyhow::Result<String> {
    if col.is_null(row) {
        return Ok("NULL".into());
    }
    let any = col.as_any();
    Ok(match col.data_type() {
        DataType::Int64 => any
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int32 => any
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int16 => any
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Boolean => {
            if any.downcast_ref::<BooleanArray>().unwrap().value(row) {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        DataType::Float64 => {
            let v = any.downcast_ref::<Float64Array>().unwrap().value(row);
            if v.is_finite() {
                v.to_string()
            } else {
                format!("'{v}'::DOUBLE")
            }
        }
        DataType::Float32 => {
            let v = any.downcast_ref::<Float32Array>().unwrap().value(row);
            if v.is_finite() {
                v.to_string()
            } else {
                format!("'{v}'::FLOAT")
            }
        }
        DataType::Utf8 => escape_str(any.downcast_ref::<StringArray>().unwrap().value(row)),
        DataType::LargeUtf8 => {
            escape_str(any.downcast_ref::<LargeStringArray>().unwrap().value(row))
        }
        DataType::Binary => {
            let v = any.downcast_ref::<BinaryArray>().unwrap().value(row);
            format!("TO_BINARY('{}', 'HEX')", hex_encode(v))
        }
        DataType::LargeBinary => {
            let v = any.downcast_ref::<LargeBinaryArray>().unwrap().value(row);
            format!("TO_BINARY('{}', 'HEX')", hex_encode(v))
        }
        DataType::Date32 => {
            let days = any.downcast_ref::<Date32Array>().unwrap().value(row);
            let d = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days as i64);
            format!("'{d}'::DATE")
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let us = any
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap()
                .value(row);
            let t = NaiveTime::from_num_seconds_from_midnight_opt(
                (us / 1_000_000) as u32,
                ((us % 1_000_000) * 1_000) as u32,
            )
            .context("time out of range")?;
            format!("'{}'::TIME", t.format("%H:%M:%S%.6f"))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let us = any
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(row);
            let dt: DateTime<Utc> =
                DateTime::from_timestamp_micros(us).context("timestamp out of range")?;
            let iso = dt.format("%Y-%m-%dT%H:%M:%S%.6fZ");
            if tz.is_some() {
                format!("'{iso}'::TIMESTAMP_TZ")
            } else {
                format!("'{iso}'::TIMESTAMP_NTZ")
            }
        }
        other => bail!("unsupported Arrow type for Snowflake literal: {other:?}"),
    })
}

pub const LOG_TABLE: &str = "_ETL_LOADED_BATCHES";

pub fn qualified(db: &str, schema: &str, table: &str) -> anyhow::Result<String> {
    Ok(format!(
        "{}.{}.{}",
        quote_ident(db)?,
        quote_ident(schema)?,
        quote_ident(table)?
    ))
}

pub fn ensure_log_table_ddl(db: &str, schema: &str) -> anyhow::Result<String> {
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         TENANT_ID TEXT NOT NULL, PIPELINE_ID TEXT NOT NULL, \
         RUN_ID TEXT NOT NULL, STREAM_NAME TEXT NOT NULL, \
         BATCH_SEQ BIGINT NOT NULL, ROWS_LOADED BIGINT NOT NULL, \
         LOADED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), \
         PRIMARY KEY (TENANT_ID, PIPELINE_ID, RUN_ID, STREAM_NAME, BATCH_SEQ))",
        qualified(db, schema, LOG_TABLE)?
    ))
}

pub fn ensure_target_table_ddl(
    db: &str,
    schema: &str,
    table: &str,
    arrow_schema: &arrow::datatypes::Schema,
) -> anyhow::Result<String> {
    let cols: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            Ok(format!(
                "{} {}",
                quote_ident(f.name())?,
                snowflake_type_for(f.data_type())?
            ))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified(db, schema, table)?,
        cols.join(", ")
    ))
}

pub fn check_loaded_sql(db: &str, schema: &str, id: &loader_sdk::LoadId) -> anyhow::Result<String> {
    Ok(format!(
        "SELECT ROWS_LOADED FROM {} WHERE TENANT_ID = '{}' AND PIPELINE_ID = '{}' \
         AND RUN_ID = '{}' AND STREAM_NAME = {} AND BATCH_SEQ = {}",
        qualified(db, schema, LOG_TABLE)?,
        id.tenant_id.as_uuid(),
        id.pipeline_id.as_uuid(),
        id.run_id.as_uuid(),
        escape_str(&id.stream_name),
        id.batch_seq,
    ))
}

pub fn log_insert_sql(
    db: &str,
    schema: &str,
    id: &loader_sdk::LoadId,
    rows: usize,
) -> anyhow::Result<String> {
    Ok(format!(
        "INSERT INTO {} (TENANT_ID, PIPELINE_ID, RUN_ID, STREAM_NAME, BATCH_SEQ, ROWS_LOADED) \
         VALUES ('{}', '{}', '{}', {}, {}, {})",
        qualified(db, schema, LOG_TABLE)?,
        id.tenant_id.as_uuid(),
        id.pipeline_id.as_uuid(),
        id.run_id.as_uuid(),
        escape_str(&id.stream_name),
        id.batch_seq,
        rows,
    ))
}

fn rendered_rows(batch: &arrow::record_batch::RecordBatch) -> anyhow::Result<Vec<String>> {
    (0..batch.num_rows())
        .map(|row| {
            let vals: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| sql_literal(c.as_ref(), row))
                .collect::<anyhow::Result<_>>()?;
            Ok(format!("({})", vals.join(", ")))
        })
        .collect()
}

fn quoted_columns(batch: &arrow::record_batch::RecordBatch) -> anyhow::Result<Vec<String>> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect()
}

pub fn append_insert_sql(
    db: &str,
    schema: &str,
    table: &str,
    batch: &arrow::record_batch::RecordBatch,
) -> anyhow::Result<String> {
    let cols = quoted_columns(batch)?;
    let rows = rendered_rows(batch)?;
    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        qualified(db, schema, table)?,
        cols.join(", "),
        rows.join(", ")
    ))
}

/// `DELETE` of every key in `batch` (one `(pk = .. AND ..)` group per row,
/// OR-ed). Used by CDC apply for keys whose last change is a delete.
pub fn delete_keys_sql(
    db: &str,
    schema: &str,
    table: &str,
    batch: &arrow::record_batch::RecordBatch,
    pk_columns: &[String],
) -> anyhow::Result<String> {
    anyhow::ensure!(batch.num_rows() > 0, "delete_keys_sql on an empty batch");
    let batch_schema = batch.schema();
    let pks: Vec<(String, usize)> = pk_columns
        .iter()
        .map(|pk| {
            let idx = batch_schema
                .index_of(pk)
                .map_err(|_| anyhow::anyhow!("pk column {pk:?} not in batch"))?;
            Ok((quote_ident(pk)?, idx))
        })
        .collect::<anyhow::Result<_>>()?;
    let groups: Vec<String> = (0..batch.num_rows())
        .map(|row| {
            let terms: Vec<String> = pks
                .iter()
                .map(|(q, idx)| {
                    Ok(format!(
                        "{q} = {}",
                        sql_literal(batch.column(*idx).as_ref(), row)?
                    ))
                })
                .collect::<anyhow::Result<_>>()?;
            Ok(format!("({})", terms.join(" AND ")))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(format!(
        "DELETE FROM {} WHERE {}",
        qualified(db, schema, table)?,
        groups.join(" OR ")
    ))
}

pub fn merge_upsert_sql(
    db: &str,
    schema: &str,
    table: &str,
    batch: &arrow::record_batch::RecordBatch,
    pk_columns: &[String],
) -> anyhow::Result<String> {
    let schema_ref = batch.schema();
    let names: Vec<&str> = schema_ref
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    for pk in pk_columns {
        anyhow::ensure!(
            names.contains(&pk.as_str()),
            "pk column {pk:?} not in batch"
        );
    }
    let cols = quoted_columns(batch)?;
    let rows = rendered_rows(batch)?;
    let on: Vec<String> = pk_columns
        .iter()
        .map(|pk| Ok(format!("t.{q} = s.{q}", q = quote_ident(pk)?)))
        .collect::<anyhow::Result<_>>()?;
    let non_pk: Vec<String> = schema_ref
        .fields()
        .iter()
        .filter(|f| !pk_columns.iter().any(|pk| pk == f.name()))
        .map(|f| Ok(format!("t.{q} = s.{q}", q = quote_ident(f.name())?)))
        .collect::<anyhow::Result<_>>()?;
    let insert_vals: Vec<String> = cols.iter().map(|c| format!("s.{c}")).collect();
    let matched = if non_pk.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {}", non_pk.join(", "))
    };
    Ok(format!(
        "MERGE INTO {} AS t USING (SELECT * FROM VALUES {} AS v({})) AS s ON {}{} \
         WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        qualified(db, schema, table)?,
        rows.join(", "),
        cols.join(", "),
        on.join(" AND "),
        matched,
        cols.join(", "),
        insert_vals.join(", "),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use std::sync::Arc;

    fn load_id() -> loader_sdk::LoadId {
        loader_sdk::LoadId {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::nil()),
            pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(uuid::Uuid::nil()),
            run_id: common_types::ids::RunId::from_uuid_unchecked(uuid::Uuid::nil()),
            batch_seq: 7,
            stream_name: "public.items".into(),
        }
    }

    #[test]
    fn log_table_ddl_shape() {
        let ddl = ensure_log_table_ddl("DB", "RAW").unwrap();
        assert!(
            ddl.starts_with("CREATE TABLE IF NOT EXISTS \"DB\".\"RAW\".\"_ETL_LOADED_BATCHES\""),
            "got {ddl}"
        );
        assert!(ddl.contains("BATCH_SEQ"));
    }

    #[test]
    fn target_ddl_from_arrow_schema() {
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
        ]);
        let ddl = ensure_target_table_ddl("DB", "RAW", "customers", &schema).unwrap();
        assert_eq!(
            ddl,
            "CREATE TABLE IF NOT EXISTS \"DB\".\"RAW\".\"customers\" (\"id\" BIGINT, \"name\" TEXT)"
        );
    }

    #[test]
    fn check_and_log_sql_carry_full_load_id() {
        let id = load_id();
        let check = check_loaded_sql("DB", "RAW", &id).unwrap();
        assert!(check.contains("BATCH_SEQ = 7"), "got {check}");
        assert!(
            check.contains("STREAM_NAME = 'public.items'"),
            "got {check}"
        );
        let ins = log_insert_sql("DB", "RAW", &id, 42).unwrap();
        assert!(ins.contains("42"), "got {ins}");
        assert!(ins.contains(&uuid::Uuid::nil().to_string()), "got {ins}");
    }

    fn two_row_batch() -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn append_insert_renders_all_rows() {
        let sql = append_insert_sql("DB", "RAW", "t", &two_row_batch()).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"DB\".\"RAW\".\"t\" (\"id\", \"name\") VALUES (1, 'a'), (2, NULL)"
        );
    }

    #[test]
    fn delete_keys_shape_and_escaping() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("org", DataType::Utf8, false),
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
        ]));
        let b = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a'co", "b"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
            ],
        )
        .unwrap();
        let pk = vec!["org".to_string(), "id".to_string()];
        let sql = delete_keys_sql("DB", "RAW", "t", &b, &pk).unwrap();
        assert_eq!(
            sql,
            "DELETE FROM \"DB\".\"RAW\".\"t\" WHERE \
             (\"org\" = 'a''co' AND \"id\" = 1) OR (\"org\" = 'b' AND \"id\" = 2)"
        );
        assert!(delete_keys_sql("DB", "RAW", "t", &b, &["nope".into()]).is_err());
        assert!(delete_keys_sql("DB", "RAW", "t", &b.slice(0, 0), &pk).is_err());
    }

    #[test]
    fn merge_upsert_shape() {
        let sql = merge_upsert_sql("DB", "RAW", "t", &two_row_batch(), &["id".into()]).unwrap();
        assert!(
            sql.starts_with(
                "MERGE INTO \"DB\".\"RAW\".\"t\" AS t USING (SELECT * FROM VALUES (1, 'a'), (2, NULL) AS v(\"id\", \"name\")) AS s ON t.\"id\" = s.\"id\""
            ),
            "got: {sql}"
        );
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET t.\"name\" = s.\"name\""),
            "got {sql}"
        );
        assert!(
            sql.contains(
                "WHEN NOT MATCHED THEN INSERT (\"id\", \"name\") VALUES (s.\"id\", s.\"name\")"
            ),
            "got {sql}"
        );
    }

    #[test]
    fn merge_all_pk_columns_skips_update_clause() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let sql = merge_upsert_sql("DB", "RAW", "t", &batch, &["id".into()]).unwrap();
        assert!(!sql.contains("WHEN MATCHED THEN UPDATE"), "got {sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "got {sql}");
    }

    #[test]
    fn merge_missing_pk_errors() {
        assert!(merge_upsert_sql("DB", "RAW", "t", &two_row_batch(), &["nope".into()]).is_err());
    }

    #[test]
    fn idents_quoted_and_validated() {
        assert_eq!(quote_ident("customers").unwrap(), "\"customers\"");
        assert_eq!(quote_ident("UPPER_case").unwrap(), "\"UPPER_case\"");
        assert!(quote_ident("bad\"quote").is_err());
        assert!(quote_ident("bad\nnewline").is_err());
        assert!(quote_ident("").is_err());
    }

    #[test]
    fn type_mapping_matches_design_table() {
        assert_eq!(snowflake_type_for(&DataType::Int64).unwrap(), "BIGINT");
        assert_eq!(snowflake_type_for(&DataType::Utf8).unwrap(), "TEXT");
        assert_eq!(snowflake_type_for(&DataType::Boolean).unwrap(), "BOOLEAN");
        assert_eq!(snowflake_type_for(&DataType::Float64).unwrap(), "DOUBLE");
        assert_eq!(
            snowflake_type_for(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap(),
            "TIMESTAMP_TZ"
        );
        assert_eq!(
            snowflake_type_for(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "TIMESTAMP_NTZ"
        );
        assert_eq!(snowflake_type_for(&DataType::Date32).unwrap(), "DATE");
        assert_eq!(snowflake_type_for(&DataType::Binary).unwrap(), "BINARY");
        assert!(snowflake_type_for(&DataType::Duration(TimeUnit::Second)).is_err());
    }

    #[test]
    fn literals_render_and_escape() {
        let strs: ArrayRef = Arc::new(StringArray::from(vec![Some("O'Brien"), None]));
        assert_eq!(sql_literal(strs.as_ref(), 0).unwrap(), "'O''Brien'");
        assert_eq!(sql_literal(strs.as_ref(), 1).unwrap(), "NULL");

        let backslashes: ArrayRef = Arc::new(StringArray::from(vec![
            r"C:\path\",
            r"\'); DROP TABLE t; --",
        ]));
        assert_eq!(
            sql_literal(backslashes.as_ref(), 0).unwrap(),
            r"'C:\\path\\'"
        );
        assert_eq!(
            sql_literal(backslashes.as_ref(), 1).unwrap(),
            r"'\\''); DROP TABLE t; --'"
        );

        let ints: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        assert_eq!(sql_literal(ints.as_ref(), 0).unwrap(), "42");

        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        assert_eq!(sql_literal(bools.as_ref(), 0).unwrap(), "TRUE");

        let bins: ArrayRef = Arc::new(BinaryArray::from(vec![&[0xDEu8, 0xAD][..]]));
        assert_eq!(
            sql_literal(bins.as_ref(), 0).unwrap(),
            "TO_BINARY('dead', 'HEX')"
        );

        let ts: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![1_650_000_000_000_000i64]).with_timezone("UTC"),
        );
        let lit = sql_literal(ts.as_ref(), 0).unwrap();
        assert!(lit.ends_with("::TIMESTAMP_TZ"), "got {lit}");
        assert!(lit.starts_with("'2022-04-15T"), "got {lit}");
    }
}
