//! Postgres pgoutput type adapters: OID → Arrow `DataType`, and a
//! text-value parser that yields typed `PgScalarValue`s for the
//! type-aware streaming RecordBatch.

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};

/// Internal scalar mapping to the Arrow types we support in v2 CDC
/// streaming columns. Producers (`parse_pg_text`) emit these; consumers
/// (the batch builder) append them to the matching Arrow `ArrayBuilder`.
#[derive(Clone, Debug, PartialEq)]
pub enum PgScalarValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    /// Microseconds since the unix epoch.
    TimestampMicros(i64),
    /// Days since 1970-01-01.
    Date32(i32),
}

/// Map a Postgres type OID to the Arrow `DataType` we use in v2 CDC
/// streaming columns. Unknown OIDs fall back to `Utf8` — pgoutput
/// always provides a textual representation, so this is safe; the
/// downstream loader sees the raw text. Add specific OIDs as
/// connectors actually exercise them.
pub fn pg_oid_to_arrow_type(oid: u32) -> DataType {
    match oid {
        16 => DataType::Boolean,                                              // bool
        20 => DataType::Int64,                                                // int8
        21 | 23 => DataType::Int32,                                           // int2 / int4
        25 | 1042 | 1043 => DataType::Utf8,                                   // text / bpchar / varchar
        700 => DataType::Float32,                                             // float4
        701 | 1700 => DataType::Float64,                                      // float8 / numeric
        1082 => DataType::Date32,                                             // date
        1114 => DataType::Timestamp(TimeUnit::Microsecond, None),             // timestamp
        1184 => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), // timestamptz
        114 | 3802 => DataType::Utf8,                                         // json / jsonb
        2950 => DataType::Utf8,                                               // uuid
        _ => DataType::Utf8,
    }
}

/// Parse pgoutput's textual value representation into a typed
/// `PgScalarValue`, given the column's target Arrow `DataType`.
/// pgoutput's NULL sentinel is signalled upstream via `Option::None`;
/// here, a non-empty `s` always parses to `Some(...)`.
pub fn parse_pg_text(s: &str, target: &DataType) -> Result<Option<PgScalarValue>> {
    let v = match target {
        DataType::Int32 => {
            let n: i32 = s.parse().with_context(|| format!("parse i32 '{s}'"))?;
            PgScalarValue::Int32(n)
        }
        DataType::Int64 => {
            let n: i64 = s.parse().with_context(|| format!("parse i64 '{s}'"))?;
            PgScalarValue::Int64(n)
        }
        DataType::Float32 => {
            let f: f32 = s.parse().with_context(|| format!("parse f32 '{s}'"))?;
            PgScalarValue::Float32(f)
        }
        DataType::Float64 => {
            let f: f64 = s.parse().with_context(|| format!("parse f64 '{s}'"))?;
            PgScalarValue::Float64(f)
        }
        DataType::Utf8 => PgScalarValue::Utf8(s.to_owned()),
        DataType::Boolean => match s {
            "t" | "true" | "T" | "TRUE" | "1" => PgScalarValue::Boolean(true),
            "f" | "false" | "F" | "FALSE" | "0" => PgScalarValue::Boolean(false),
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
            PgScalarValue::Date32(days_i32)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let micros = parse_pg_timestamp_to_micros(s, tz.is_some())?;
            PgScalarValue::TimestampMicros(micros)
        }
        other => {
            return Err(anyhow!(
                "unsupported target DataType for pg text parse: {:?}",
                other
            ))
        }
    };
    Ok(Some(v))
}

fn parse_pg_timestamp_to_micros(s: &str, has_tz: bool) -> Result<i64> {
    if has_tz {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z") {
            return Ok(dt.with_timezone(&Utc).timestamp_micros());
        }
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
            return Ok(dt.with_timezone(&Utc).timestamp_micros());
        }
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(Utc.from_utc_datetime(&naive).timestamp_micros());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&naive).timestamp_micros());
    }
    Err(anyhow!("unrecognised pg timestamp text '{s}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_int_family() {
        assert_eq!(pg_oid_to_arrow_type(21), DataType::Int32);
        assert_eq!(pg_oid_to_arrow_type(23), DataType::Int32);
        assert_eq!(pg_oid_to_arrow_type(20), DataType::Int64);
    }

    #[test]
    fn maps_text_family_to_utf8() {
        assert_eq!(pg_oid_to_arrow_type(25), DataType::Utf8);
        assert_eq!(pg_oid_to_arrow_type(1042), DataType::Utf8);
        assert_eq!(pg_oid_to_arrow_type(1043), DataType::Utf8);
    }

    #[test]
    fn maps_timestamp_oids() {
        assert_eq!(
            pg_oid_to_arrow_type(1114),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        match pg_oid_to_arrow_type(1184) {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert_eq!(tz.as_ref(), "UTC")
            }
            other => panic!("expected timestamptz, got {other:?}"),
        }
    }

    #[test]
    fn maps_unknown_oid_to_utf8_fallback() {
        assert_eq!(pg_oid_to_arrow_type(999_999), DataType::Utf8);
    }

    #[test]
    fn maps_jsonb_to_utf8() {
        assert_eq!(pg_oid_to_arrow_type(3802), DataType::Utf8);
    }

    #[test]
    fn maps_bool_to_boolean() {
        assert_eq!(pg_oid_to_arrow_type(16), DataType::Boolean);
    }

    #[test]
    fn parse_int32_decimal_text() {
        let v = parse_pg_text("42", &DataType::Int32).unwrap();
        assert_eq!(v, Some(PgScalarValue::Int32(42)));
    }

    #[test]
    fn parse_int64_negative_text() {
        let v = parse_pg_text("-100", &DataType::Int64).unwrap();
        assert_eq!(v, Some(PgScalarValue::Int64(-100)));
    }

    #[test]
    fn parse_utf8_text() {
        let v = parse_pg_text("alice@x.com", &DataType::Utf8).unwrap();
        assert_eq!(v, Some(PgScalarValue::Utf8("alice@x.com".into())));
    }

    #[test]
    fn parse_boolean_t_f() {
        assert_eq!(
            parse_pg_text("t", &DataType::Boolean).unwrap(),
            Some(PgScalarValue::Boolean(true))
        );
        assert_eq!(
            parse_pg_text("f", &DataType::Boolean).unwrap(),
            Some(PgScalarValue::Boolean(false))
        );
    }

    #[test]
    fn parse_float64_decimal() {
        let v = parse_pg_text("3.14", &DataType::Float64).unwrap();
        match v.unwrap() {
            PgScalarValue::Float64(f) => assert!((f - 3.14).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn parse_date_iso() {
        let v = parse_pg_text("2026-01-01", &DataType::Date32).unwrap();
        assert_eq!(v, Some(PgScalarValue::Date32(20454)));
    }

    #[test]
    fn parse_timestamp_with_microseconds() {
        let v = parse_pg_text(
            "2026-01-01 00:00:00",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(v, Some(PgScalarValue::TimestampMicros(1_767_225_600_000_000)));
    }

    #[test]
    fn parse_timestamptz_with_offset() {
        let v = parse_pg_text(
            "2026-01-01 00:00:00+00",
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )
        .unwrap();
        assert_eq!(v, Some(PgScalarValue::TimestampMicros(1_767_225_600_000_000)));
    }
}
