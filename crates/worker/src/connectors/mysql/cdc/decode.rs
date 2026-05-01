//! Binlog row-event value rendering.
//!
//! `mysql_common::binlog::events::Event::read_data()` does the heavy
//! lifting (typed `EventData` enum). This module provides the small
//! surface for converting a `BinlogValue` and `BinlogRow` into the
//! `Option<String>` representation the v1 schema uses for all data
//! columns. The stream loop in `stream.rs` consumes these helpers.

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;

/// Internal scalar mapping to the Arrow types we support in v2 CDC
/// columns. One variant per supported Arrow `DataType`. Producers
/// (decode) emit these; consumers (the batch builder) append them to
/// the matching Arrow `ArrayBuilder`.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    /// Microseconds since the unix epoch, UTC.
    TimestampMicros(i64),
    /// Days since 1970-01-01.
    Date32(i32),
}

/// One row event in our internal representation.
#[derive(Clone, Debug, PartialEq)]
pub enum RowOp {
    Insert {
        table_id: u64,
        after: Vec<Option<ScalarValue>>,
    },
    Update {
        table_id: u64,
        before: Option<Vec<Option<ScalarValue>>>,
        after: Vec<Option<ScalarValue>>,
    },
    Delete {
        table_id: u64,
        before: Vec<Option<ScalarValue>>,
    },
}

/// Convert a binlog `BinlogValue` to our internal `ScalarValue`,
/// targeting `expected` as the Arrow column type. Returns `None` for
/// SQL NULL.
pub fn binlog_value_to_scalar(
    v: &BinlogValue<'_>,
    expected: &DataType,
) -> Result<Option<ScalarValue>> {
    match v {
        BinlogValue::Value(inner) => value_to_scalar(inner, expected),
        BinlogValue::Jsonb(json) => {
            let parsed: serde_json::Value = json
                .clone()
                .parse()
                .context("parsing JSONB to JSON")?
                .into();
            Ok(Some(ScalarValue::Utf8(parsed.to_string())))
        }
        BinlogValue::JsonDiff(_) => Ok(Some(ScalarValue::Utf8(
            "__partial_json_diff__".into(),
        ))),
    }
}

fn value_to_scalar(v: &Value, expected: &DataType) -> Result<Option<ScalarValue>> {
    match (v, expected) {
        (Value::NULL, _) => Ok(None),

        (Value::Int(i), DataType::Int32) => {
            let v: i32 = (*i).try_into().map_err(|_| {
                anyhow!("Int32 column overflow: source value {} doesn't fit in i32", i)
            })?;
            Ok(Some(ScalarValue::Int32(v)))
        }
        (Value::Int(i), DataType::Int64) => Ok(Some(ScalarValue::Int64(*i))),
        (Value::UInt(u), DataType::Int32) => {
            let v: i32 = (*u).try_into().map_err(|_| {
                anyhow!("Int32 column overflow: source value {} doesn't fit in i32", u)
            })?;
            Ok(Some(ScalarValue::Int32(v)))
        }
        (Value::UInt(u), DataType::Int64) => {
            let v: i64 = (*u).try_into().map_err(|_| {
                anyhow!("Int64 column overflow: source value {} doesn't fit in i64", u)
            })?;
            Ok(Some(ScalarValue::Int64(v)))
        }

        (Value::Float(f), DataType::Float32) => Ok(Some(ScalarValue::Float32(*f))),
        (Value::Double(d), DataType::Float64) => Ok(Some(ScalarValue::Float64(*d))),
        // DECIMAL columns map to Float64 in our schema, but the binlog
        // often returns DECIMAL as Bytes (string). Parse it.
        (Value::Bytes(b), DataType::Float64) => {
            let s = std::str::from_utf8(b).context("decimal bytes not UTF-8")?;
            let f: f64 = s.parse().with_context(|| format!("parse decimal '{}'", s))?;
            Ok(Some(ScalarValue::Float64(f)))
        }

        (Value::Bytes(b), DataType::Utf8) => {
            Ok(Some(ScalarValue::Utf8(String::from_utf8_lossy(b).into_owned())))
        }

        (Value::Int(i), DataType::Boolean) => Ok(Some(ScalarValue::Boolean(*i != 0))),
        (Value::Bytes(b), DataType::Boolean) => {
            // BIT(1) lands as a single byte: 0 = false, anything else = true.
            Ok(Some(ScalarValue::Boolean(b.iter().any(|&x| x != 0))))
        }

        (Value::Date(y, m, d, h, mi, s, us), DataType::Date32) => {
            let _ = (h, mi, s, us);
            let date = NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                .with_context(|| format!("invalid date {}-{}-{}", y, m, d))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = date.signed_duration_since(epoch).num_days();
            let days_i32: i32 = days
                .try_into()
                .map_err(|_| anyhow!("date out of i32 range: {} days", days))?;
            Ok(Some(ScalarValue::Date32(days_i32)))
        }

        (Value::Date(y, m, d, h, mi, s, us), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let dt = NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                .with_context(|| format!("invalid date {}-{}-{}", y, m, d))?
                .and_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us)
                .with_context(|| {
                    format!("invalid time {}:{}:{}.{}", h, mi, s, us)
                })?;
            let micros = dt.and_utc().timestamp_micros();
            Ok(Some(ScalarValue::TimestampMicros(micros)))
        }

        (other_v, other_dt) => Err(anyhow!(
            "unsupported BinlogValue→ScalarValue conversion: {:?} → {:?}",
            other_v,
            other_dt
        )),
    }
}

/// Convert a binlog row to a vector of typed scalars, one per column,
/// in column index order. `target_types` must have one entry per data
/// column — mismatched lengths produce an error. Columns missing from
/// the row image (binlog_row_image != FULL on update before-images)
/// render as `None`.
pub fn binlog_row_to_scalars(
    row: &BinlogRow,
    target_types: &[DataType],
) -> Result<Vec<Option<ScalarValue>>> {
    if row.len() != target_types.len() {
        return Err(anyhow!(
            "row has {} columns but schema declares {}",
            row.len(),
            target_types.len()
        ));
    }
    let mut out = Vec::with_capacity(row.len());
    for i in 0..row.len() {
        match row.as_ref(i) {
            Some(v) => out.push(binlog_value_to_scalar(v, &target_types[i])?),
            None => out.push(None),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_int32_from_int_value() {
        let v = BinlogValue::Value(Value::Int(42));
        let s = binlog_value_to_scalar(&v, &DataType::Int32).unwrap();
        assert_eq!(s, Some(ScalarValue::Int32(42)));
    }

    #[test]
    fn scalar_int64_from_int_value() {
        let v = BinlogValue::Value(Value::Int(42));
        let s = binlog_value_to_scalar(&v, &DataType::Int64).unwrap();
        assert_eq!(s, Some(ScalarValue::Int64(42)));
    }

    #[test]
    fn scalar_int32_overflow_errors() {
        let v = BinlogValue::Value(Value::Int(i64::MAX));
        let err = binlog_value_to_scalar(&v, &DataType::Int32).unwrap_err();
        assert!(err.to_string().contains("overflow"), "got: {err}");
    }

    #[test]
    fn scalar_utf8_from_bytes() {
        let v = BinlogValue::Value(Value::Bytes(b"alice@x.com".to_vec()));
        let s = binlog_value_to_scalar(&v, &DataType::Utf8).unwrap();
        assert_eq!(s, Some(ScalarValue::Utf8("alice@x.com".into())));
    }

    #[test]
    fn scalar_float64_from_double() {
        let v = BinlogValue::Value(Value::Double(3.14));
        let s = binlog_value_to_scalar(&v, &DataType::Float64).unwrap();
        match s.unwrap() {
            ScalarValue::Float64(f) => assert!((f - 3.14).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn scalar_timestamp_from_datetime() {
        // 2026-01-01 00:00:00 UTC = 1767225600 unix seconds.
        let v = BinlogValue::Value(Value::Date(2026, 1, 1, 0, 0, 0, 0));
        let s = binlog_value_to_scalar(
            &v,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )
        .unwrap();
        assert_eq!(
            s,
            Some(ScalarValue::TimestampMicros(1767225600 * 1_000_000))
        );
    }

    #[test]
    fn scalar_date32_from_date_only() {
        // 2026-01-01 = 20454 days since 1970-01-01.
        let v = BinlogValue::Value(Value::Date(2026, 1, 1, 0, 0, 0, 0));
        let s = binlog_value_to_scalar(&v, &DataType::Date32).unwrap();
        assert_eq!(s, Some(ScalarValue::Date32(20454)));
    }

    #[test]
    fn scalar_boolean_from_tinyint_zero_one() {
        let true_v = BinlogValue::Value(Value::Int(1));
        let false_v = BinlogValue::Value(Value::Int(0));
        assert_eq!(
            binlog_value_to_scalar(&true_v, &DataType::Boolean).unwrap(),
            Some(ScalarValue::Boolean(true))
        );
        assert_eq!(
            binlog_value_to_scalar(&false_v, &DataType::Boolean).unwrap(),
            Some(ScalarValue::Boolean(false))
        );
    }

    #[test]
    fn scalar_null_passes_through() {
        let v = BinlogValue::Value(Value::NULL);
        let s = binlog_value_to_scalar(&v, &DataType::Int64).unwrap();
        assert_eq!(s, None);
    }
}
