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
    if name.chars().any(|c| c == '"' || c == '\0' || c.is_control()) {
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

fn escape_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use std::sync::Arc;

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
            TimestampMicrosecondArray::from(vec![1_650_000_000_000_000i64])
                .with_timezone("UTC"),
        );
        let lit = sql_literal(ts.as_ref(), 0).unwrap();
        assert!(lit.ends_with("::TIMESTAMP_TZ"), "got {lit}");
        assert!(lit.starts_with("'2022-04-15T"), "got {lit}");
    }
}
