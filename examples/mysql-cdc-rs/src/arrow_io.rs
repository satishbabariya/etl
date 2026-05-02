//! Arrow IPC + a dynamic, schema-driven batch builder.
//!
//! `DynamicBatchBuilder` holds one builder per column matching the
//! discovered Arrow schema, plus the static _cdc.op + _cdc.position
//! metadata builders. `append_row` accepts a positional slice of
//! `Option<&str>` cells (one per data column) plus the op/position
//! metadata, and dispatches to the right typed builder.
//!
//! Supported builders cover the types from `discover::map_mysql_type`:
//! Int8/16/32/64, Float32/64, Boolean, Utf8, Date32, Timestamp(Micros).

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};

pub fn build_full_schema(data_fields: &[Field]) -> Arc<Schema> {
    let mut all = data_fields.to_vec();
    all.push(Field::new("_cdc.op", DataType::Utf8, false));
    all.push(Field::new("_cdc.position", DataType::Utf8, false));
    Arc::new(Schema::new(all))
}

pub fn schema_ipc_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema.as_ref()).map_err(|e| e.to_string())?;
        w.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}

enum Builder {
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    TsMicro(TimestampMicrosecondBuilder),
}

impl Builder {
    fn for_type(t: &DataType) -> Self {
        match t {
            DataType::Int8 => Builder::Int8(Int8Builder::new()),
            DataType::Int16 => Builder::Int16(Int16Builder::new()),
            DataType::Int32 => Builder::Int32(Int32Builder::new()),
            DataType::Int64 => Builder::Int64(Int64Builder::new()),
            DataType::Float32 => Builder::Float32(Float32Builder::new()),
            DataType::Float64 => Builder::Float64(Float64Builder::new()),
            DataType::Boolean => Builder::Boolean(BooleanBuilder::new()),
            DataType::Date32 => Builder::Date32(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Builder::TsMicro(TimestampMicrosecondBuilder::new())
            }
            // discover already collapses unsupported types to Utf8;
            // this arm catches anything else passed through directly.
            _ => Builder::Utf8(StringBuilder::new()),
        }
    }

    fn append_text(&mut self, cell: Option<&str>) {
        match self {
            Builder::Int8(b) => match cell.and_then(|s| s.parse::<i8>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::Int16(b) => match cell.and_then(|s| s.parse::<i16>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::Int32(b) => match cell.and_then(|s| s.parse::<i32>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::Int64(b) => match cell.and_then(|s| s.parse::<i64>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::Float32(b) => match cell.and_then(|s| s.parse::<f32>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::Float64(b) => match cell.and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::Boolean(b) => match cell {
                Some(s)
                    if s == "1"
                        || s.eq_ignore_ascii_case("true")
                        || s.eq_ignore_ascii_case("t") =>
                {
                    b.append_value(true)
                }
                Some(s)
                    if s == "0"
                        || s.eq_ignore_ascii_case("false")
                        || s.eq_ignore_ascii_case("f") =>
                {
                    b.append_value(false)
                }
                Some(_) => b.append_null(),
                None => b.append_null(),
            },
            Builder::Utf8(b) => match cell {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            },
            Builder::Date32(b) => match cell.and_then(parse_date32) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            Builder::TsMicro(b) => match cell.and_then(parse_ts_micros) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Int8(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Int16(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Int32(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Float32(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::Date32(b) => Arc::new(b.finish()) as ArrayRef,
            Builder::TsMicro(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

pub struct DynamicBatchBuilder {
    schema: Arc<Schema>,
    data_builders: Vec<Builder>,
    op_builder: StringBuilder,
    pos_builder: StringBuilder,
}

impl DynamicBatchBuilder {
    pub fn new(schema: Arc<Schema>) -> Self {
        // Last 2 fields are _cdc.op + _cdc.position — exclude them
        // from data_builders.
        let n_data = schema.fields().len().saturating_sub(2);
        let data_builders = schema
            .fields()
            .iter()
            .take(n_data)
            .map(|f| Builder::for_type(f.data_type()))
            .collect();
        Self {
            schema,
            data_builders,
            op_builder: StringBuilder::new(),
            pos_builder: StringBuilder::new(),
        }
    }

    /// Append one row. `cells` is a positional slice of length
    /// `schema.fields().len() - 2` (data columns only).
    pub fn append_row(&mut self, cells: &[Option<&str>], op: char, position: &str) {
        for (i, b) in self.data_builders.iter_mut().enumerate() {
            let cell = cells.get(i).copied().flatten();
            b.append_text(cell);
        }
        self.op_builder.append_value(op.to_string());
        self.pos_builder.append_value(position);
    }

    pub fn rows(&self) -> usize {
        self.op_builder.len()
    }

    pub fn finish_to_ipc(mut self) -> Result<Vec<u8>, String> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for b in self.data_builders.iter_mut() {
            arrays.push(b.finish());
        }
        arrays.push(Arc::new(self.op_builder.finish()) as ArrayRef);
        arrays.push(Arc::new(self.pos_builder.finish()) as ArrayRef);
        let batch =
            RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, self.schema.as_ref())
                .map_err(|e| e.to_string())?;
            w.write(&batch).map_err(|e| e.to_string())?;
            w.finish().map_err(|e| e.to_string())?;
        }
        Ok(buf)
    }
}

fn parse_date32(s: &str) -> Option<i32> {
    // tolerate "YYYY-MM-DD HH:MM:SS"
    let s = s.split(' ').next().unwrap_or(s);
    let mut parts = s.split('-');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    days_since_epoch(y, m, d)
}

fn parse_ts_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let core = strip_tz(s);
    let (date_str, time_str) = split_date_time(core)?;
    let mut date_parts = date_str.split('-');
    let y: i32 = date_parts.next()?.parse().ok()?;
    let mo: u32 = date_parts.next()?.parse().ok()?;
    let d: u32 = date_parts.next()?.parse().ok()?;
    let days = days_since_epoch(y, mo, d)?;
    let (h, mi, s_int, micros) = parse_hms_micros(time_str)?;
    let secs =
        (days as i64) * 86_400 + (h as i64) * 3600 + (mi as i64) * 60 + s_int as i64;
    Some(secs * 1_000_000 + micros as i64)
}

fn strip_tz(s: &str) -> &str {
    // Find a '+', 'Z', or post-time '-' marker. The date hyphens come
    // before the first ':'; a TZ '-' would come after.
    let mut chars = s.char_indices();
    let mut seen_colon = false;
    while let Some((i, c)) = chars.next() {
        match c {
            ':' => seen_colon = true,
            '+' | 'Z' if i > 10 => return &s[..i],
            '-' if seen_colon => return &s[..i],
            _ => {}
        }
    }
    s
}

fn split_date_time(s: &str) -> Option<(&str, &str)> {
    if let Some(idx) = s.find('T') {
        return Some((&s[..idx], &s[idx + 1..]));
    }
    if let Some(idx) = s.find(' ') {
        return Some((&s[..idx], &s[idx + 1..]));
    }
    None
}

fn parse_hms_micros(s: &str) -> Option<(u32, u32, u32, u32)> {
    let mut parts = s.split(':');
    let h: u32 = parts.next()?.parse().ok()?;
    let mi: u32 = parts.next()?.parse().ok()?;
    let s_part = parts.next()?;
    let (s_int, micros) = if let Some((si, fi)) = s_part.split_once('.') {
        let si: u32 = si.parse().ok()?;
        let fi = format!("{:0<6}", fi).chars().take(6).collect::<String>();
        let micros: u32 = fi.parse().ok()?;
        (si, micros)
    } else {
        (s_part.parse().ok()?, 0u32)
    };
    Some((h, mi, s_int, micros))
}

/// Days since 1970-01-01 (proleptic Gregorian), Howard Hinnant style.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = month;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe as i32 - 719_468)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date32_basic() {
        assert_eq!(parse_date32("1970-01-01"), Some(0));
        assert_eq!(parse_date32("1970-01-02"), Some(1));
        // 2026-05-02 = 56 years (14 leap) * 365 + 14 + 121 days into 2026 = 20575.
        assert_eq!(parse_date32("2026-05-02"), Some(20_575));
    }

    #[test]
    fn parse_ts_micros_basic() {
        let ts = parse_ts_micros("2026-05-02 13:14:15.999999").unwrap();
        assert!(ts > 1_700_000_000_000_000);
        assert_eq!(ts % 1_000_000, 999_999);
    }

    #[test]
    fn parse_ts_micros_handles_trailing_tz() {
        let a = parse_ts_micros("2026-05-02 13:14:15.000000").unwrap();
        let b = parse_ts_micros("2026-05-02 13:14:15+00").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn dynamic_builder_round_trip_int_string() {
        let s = build_full_schema(&[
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let mut bb = DynamicBatchBuilder::new(s.clone());
        bb.append_row(&[Some("1"), Some("alice")], 's', "p1");
        bb.append_row(&[Some("2"), None], 's', "p2");
        assert_eq!(bb.rows(), 2);
        let bytes = bb.finish_to_ipc().unwrap();
        assert!(!bytes.is_empty());
    }
}
