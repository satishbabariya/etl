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

/// One column's identity from a Postgres table's catalog row.
#[derive(Clone, Debug, PartialEq)]
pub struct PgColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub is_nullable: bool,
    pub ordinal_position: u32,
}

/// Live `information_schema.columns` query keyed by the column's
/// Postgres OID via `pg_attribute`. Returns columns in
/// `ordinal_position` order. Fails if the table has zero columns
/// visible to the connecting role.
pub async fn discover_pg_table_oids(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    table: &str,
) -> Result<Vec<PgColumnInfo>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT a.attname AS name, \
                a.atttypid::int8 AS type_oid, \
                NOT a.attnotnull AS is_nullable, \
                a.attnum::int4 AS ordinal_position \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 \
           AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(conn)
    .await
    .context("query pg_attribute")?;

    if rows.is_empty() {
        return Err(anyhow!(
            "table {schema}.{table} not found (or no visible columns)"
        ));
    }

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let name: String = r.try_get("name").context("name")?;
        let type_oid: i64 = r.try_get("type_oid").context("type_oid")?;
        let is_nullable: bool = r.try_get("is_nullable").context("is_nullable")?;
        let ordinal_position: i32 =
            r.try_get("ordinal_position").context("ordinal_position")?;
        out.push(PgColumnInfo {
            name,
            type_oid: type_oid as u32,
            is_nullable,
            ordinal_position: ordinal_position as u32,
        });
    }
    Ok(out)
}

/// Build an Arrow `ArrayBuilder` for a v2 CDC data column type.
/// Snapshot and stream both call this when constructing per-column
/// builders for a typed `RecordBatch`.
pub fn make_pg_builder(
    dt: &DataType,
) -> Result<Box<dyn arrow::array::ArrayBuilder>> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use std::sync::Arc;
    Ok(match dt {
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::new();
            if let Some(tz) = tz.as_ref() {
                b = b.with_timezone(Arc::clone(tz));
            }
            Box::new(b)
        }
        other => anyhow::bail!("no pg builder for DataType {:?}", other),
    })
}

/// Append one `PgScalarValue` (or null) to the matching Arrow
/// builder, dispatching on the column's `DataType`.
pub fn append_pg_scalar(
    builder: &mut dyn arrow::array::ArrayBuilder,
    scalar: Option<&PgScalarValue>,
    dt: &DataType,
) -> Result<()> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(PgScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
                .append_value(*t)
        }
        (Some(other_v), other_dt) => {
            anyhow::bail!("scalar/builder mismatch: {:?} into {:?}", other_v, other_dt)
        }
        (None, other_dt) => {
            anyhow::bail!("no null-append path for builder type {:?}", other_dt)
        }
    }
    Ok(())
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
