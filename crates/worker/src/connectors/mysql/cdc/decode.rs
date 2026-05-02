//! Binlog row-event value rendering.
//!
//! `mysql_common::binlog::events::Event::read_data()` does the heavy
//! lifting (typed `EventData` enum). This module provides the small
//! surface for converting a `BinlogValue` and `BinlogRow` into the
//! `Option<String>` representation the v1 schema uses for all data
//! columns. The stream loop in `stream.rs` consumes these helpers.

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
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
    /// Raw bytes (BLOB / BINARY / VARBINARY).
    Binary(Vec<u8>),
    /// Microseconds since midnight (no date, no timezone).
    Time64Micros(i64),
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
        // MySQL TIMESTAMP columns sometimes land as ASCII-encoded unix
        // seconds in the binlog row image (mysql_async parses them as
        // Bytes when no fractional precision and no per-column metadata).
        // Parse the ASCII integer back to seconds and scale to micros.
        (Value::Bytes(b), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let s = std::str::from_utf8(b).context("timestamp bytes not UTF-8")?;
            let secs: i64 = s
                .parse()
                .with_context(|| format!("parse timestamp seconds '{}'", s))?;
            Ok(Some(ScalarValue::TimestampMicros(secs * 1_000_000)))
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

/// Parse MySQL's textual value (from `SELECT CAST(col AS CHAR) AS col`)
/// into a typed `ScalarValue` for the given Arrow `DataType`. NULL
/// signalling is upstream (callers pass `None` for SQL NULL); a
/// non-empty `s` always parses to `Some(...)`.
pub fn parse_mysql_text(s: &str, target: &DataType) -> Result<Option<ScalarValue>> {
    let v = match target {
        DataType::Int32 => {
            let n: i32 = s.parse().with_context(|| format!("parse i32 '{s}'"))?;
            ScalarValue::Int32(n)
        }
        DataType::Int64 => {
            let n: i64 = s.parse().with_context(|| format!("parse i64 '{s}'"))?;
            ScalarValue::Int64(n)
        }
        DataType::Float32 => {
            let f: f32 = s.parse().with_context(|| format!("parse f32 '{s}'"))?;
            ScalarValue::Float32(f)
        }
        DataType::Float64 => {
            let f: f64 = s.parse().with_context(|| format!("parse f64 '{s}'"))?;
            ScalarValue::Float64(f)
        }
        DataType::Utf8 => ScalarValue::Utf8(s.to_owned()),
        DataType::Boolean => match s {
            "1" | "true" | "TRUE" | "t" | "T" => ScalarValue::Boolean(true),
            "0" | "false" | "FALSE" | "f" | "F" => ScalarValue::Boolean(false),
            other => return Err(anyhow!("unrecognised boolean text '{}'", other)),
        },
        DataType::Date32 => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .with_context(|| format!("parse date '{s}'"))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = date.signed_duration_since(epoch).num_days();
            let days_i32: i32 = days
                .try_into()
                .map_err(|_| anyhow!("date out of i32 range: {days} days"))?;
            ScalarValue::Date32(days_i32)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            // MySQL's CAST(timestamp AS CHAR) emits "YYYY-MM-DD HH:MM:SS"
            // (no fractional unless the column has DATETIME(N) precision)
            // and no timezone offset (MySQL's TIMESTAMP is stored UTC,
            // session-converted; we treat the text as UTC for our v1).
            let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                .with_context(|| format!("parse mysql timestamp '{s}'"))?;
            let micros = Utc.from_utc_datetime(&naive).timestamp_micros();
            ScalarValue::TimestampMicros(micros)
        }
        DataType::Binary => {
            // Snapshot path uses HEX(col) projection for binary columns,
            // so the text we receive is a hex string with no prefix
            // (unlike Postgres BYTEA which prefixes with `\x`).
            if s.len() % 2 != 0 {
                return Err(anyhow!("BINARY hex string has odd length: {}", s.len()));
            }
            let mut bytes = Vec::with_capacity(s.len() / 2);
            for chunk in s.as_bytes().chunks(2) {
                let byte = u8::from_str_radix(
                    std::str::from_utf8(chunk).context("BINARY hex utf8")?,
                    16,
                )
                .with_context(|| {
                    format!("parse BINARY hex byte '{}'", String::from_utf8_lossy(chunk))
                })?;
                bytes.push(byte);
            }
            ScalarValue::Binary(bytes)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let nt = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                .with_context(|| format!("parse mysql time '{s}'"))?;
            use chrono::Timelike;
            let secs = nt.num_seconds_from_midnight() as i64;
            // chrono's NaiveTime stores fractional component in nanoseconds.
            let micros = secs * 1_000_000 + (nt.nanosecond() as i64) / 1_000;
            ScalarValue::Time64Micros(micros)
        }
        other => {
            return Err(anyhow!(
                "unsupported target DataType for mysql text parse: {:?}",
                other
            ))
        }
    };
    Ok(Some(v))
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

    #[test]
    fn parse_text_int32_decimal() {
        let v = parse_mysql_text("42", &DataType::Int32).unwrap();
        assert_eq!(v, Some(ScalarValue::Int32(42)));
    }

    #[test]
    fn parse_text_int64_negative() {
        let v = parse_mysql_text("-1234567890123", &DataType::Int64).unwrap();
        assert_eq!(v, Some(ScalarValue::Int64(-1234567890123)));
    }

    #[test]
    fn parse_text_int32_overflow_errors() {
        let err = parse_mysql_text("9999999999", &DataType::Int32).unwrap_err();
        assert!(err.to_string().contains("parse i32"), "got: {err}");
    }

    #[test]
    fn parse_text_float64_decimal() {
        let v = parse_mysql_text("3.14", &DataType::Float64).unwrap();
        match v.unwrap() {
            ScalarValue::Float64(f) => assert!((f - 3.14).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn parse_text_utf8() {
        let v = parse_mysql_text("alice@x.com", &DataType::Utf8).unwrap();
        assert_eq!(v, Some(ScalarValue::Utf8("alice@x.com".into())));
    }

    #[test]
    fn parse_text_boolean_zero_one() {
        assert_eq!(
            parse_mysql_text("1", &DataType::Boolean).unwrap(),
            Some(ScalarValue::Boolean(true))
        );
        assert_eq!(
            parse_mysql_text("0", &DataType::Boolean).unwrap(),
            Some(ScalarValue::Boolean(false))
        );
    }

    #[test]
    fn parse_text_date_iso() {
        // 2026-01-01 = 20454 days since 1970-01-01.
        let v = parse_mysql_text("2026-01-01", &DataType::Date32).unwrap();
        assert_eq!(v, Some(ScalarValue::Date32(20454)));
    }

    #[test]
    fn parse_text_timestamp_with_microseconds() {
        // 2026-01-01 00:00:00 UTC = 1_767_225_600_000_000 micros.
        let v = parse_mysql_text(
            "2026-01-01 00:00:00",
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )
        .unwrap();
        assert_eq!(v, Some(ScalarValue::TimestampMicros(1_767_225_600_000_000)));
    }

    #[test]
    fn parse_text_binary_hex() {
        let v = parse_mysql_text("DEADBEEF", &DataType::Binary).unwrap();
        assert_eq!(v, Some(ScalarValue::Binary(vec![0xde, 0xad, 0xbe, 0xef])));
    }

    #[test]
    fn parse_text_binary_empty() {
        let v = parse_mysql_text("", &DataType::Binary).unwrap();
        assert_eq!(v, Some(ScalarValue::Binary(vec![])));
    }

    #[test]
    fn parse_text_binary_lowercase_hex() {
        let v = parse_mysql_text("deadbeef", &DataType::Binary).unwrap();
        assert_eq!(v, Some(ScalarValue::Binary(vec![0xde, 0xad, 0xbe, 0xef])));
    }

    #[test]
    fn parse_text_binary_rejects_odd_length() {
        let err = parse_mysql_text("ABC", &DataType::Binary).unwrap_err();
        assert!(err.to_string().contains("odd length"), "got: {err}");
    }

    #[test]
    fn parse_text_time_with_micros() {
        // 12:30:45.123456 = (12*3600 + 30*60 + 45) * 1_000_000 + 123_456
        //                 = 45_045_123_456
        let v = parse_mysql_text(
            "12:30:45.123456",
            &DataType::Time64(TimeUnit::Microsecond),
        )
        .unwrap();
        assert_eq!(v, Some(ScalarValue::Time64Micros(45_045_123_456)));
    }

    #[test]
    fn parse_text_time_without_fraction() {
        let v = parse_mysql_text(
            "00:00:01",
            &DataType::Time64(TimeUnit::Microsecond),
        )
        .unwrap();
        assert_eq!(v, Some(ScalarValue::Time64Micros(1_000_000)));
    }
}
