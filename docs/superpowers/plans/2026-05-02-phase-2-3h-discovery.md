# Phase II.3.h — WASM Connector Schema Discovery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded `id BIGINT, name TEXT` schema in the two example WASM CDC connectors with real `information_schema`-driven discovery, so they handle arbitrary tables with the supported scalar types.

**Architecture:** Each example connector gains a `discover.rs` module that queries column metadata + primary key on every `discover` and `read_batch` call (handles don't survive activations; re-discovery is cheap). A new `arrow_io::DynamicBatchBuilder` dispatches per-column appends through an enum of typed `arrow_array::builder::*`. Snapshot SQL becomes dynamic projection over the discovered column list; streaming JSON decode iterates `(field, cell)` pairs and parses by Arrow type.

**Tech Stack:** wasm32-wasip2, wit-bindgen 0.37, arrow-array 53 (dynamic builders), `information_schema.columns` + `pg_index`/`information_schema.key_column_usage` for PK lookup.

---

## File structure

| Path | Action |
|---|---|
| `examples/mysql-cdc-rs/src/discover.rs` | **New** |
| `examples/mysql-cdc-rs/src/arrow_io.rs` | Rewrite (DynamicBatchBuilder) |
| `examples/mysql-cdc-rs/src/lib.rs` | Modify (discover wiring) |
| `examples/mysql-cdc-rs/src/snapshot.rs` | Modify (dynamic SELECT + decode) |
| `examples/mysql-cdc-rs/src/streaming.rs` | Modify (decode by field type) |
| `examples/postgres-cdc-rs/src/discover.rs` | **New** |
| `examples/postgres-cdc-rs/src/arrow_io.rs` | Rewrite (mirror) |
| `examples/postgres-cdc-rs/src/{lib,snapshot,streaming}.rs` | Modify (mirror) |
| `tests/integration/tests/mysql_cdc_wasm_e2e.rs` | Modify (4-column table) |
| `tests/integration/tests/postgres_cdc_wasm_e2e.rs` | Modify (4-column table) |
| `README.md` | Modify ("Currently:" line) |

---

## Task 1: mysql-cdc-rs discover module

**Files:**
- Create: `examples/mysql-cdc-rs/src/discover.rs`

- [ ] **Step 1: Write the discover module**

Create `examples/mysql-cdc-rs/src/discover.rs`:

```rust
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
```

- [ ] **Step 2: Register the module in lib.rs**

Modify `examples/mysql-cdc-rs/src/lib.rs` — add `mod discover;` near the other `mod` declarations.

- [ ] **Step 3: Build + test**

```bash
cd examples/mysql-cdc-rs && cargo build --release && cargo test
```

Expected: clean build, 5 tests pass (3 existing + 3 new module tests visible after the next task adds them; for now just the existing 3 stay). Actually 6 tests pass: 3 old + 3 new from `discover::tests`.

- [ ] **Step 4: Commit**

```bash
git add examples/mysql-cdc-rs/src/discover.rs examples/mysql-cdc-rs/src/lib.rs && \
git commit -m "phase-2-3h-1: mysql-cdc-rs discover module

query_columns + query_pk_column hit information_schema. map_mysql_type
covers bigint/int/mediumint/smallint/tinyint/varchar/text/char/bit/
float/double/datetime/timestamp/date with Utf8 fallback for unsupported
types (numeric, json, blob, etc.).

3 new unit tests; full suite at 6 passing."
```

---

## Task 2: mysql-cdc-rs DynamicBatchBuilder

**Files:**
- Modify: `examples/mysql-cdc-rs/src/arrow_io.rs` (rewrite)

- [ ] **Step 1: Replace arrow_io.rs entirely**

Replace `examples/mysql-cdc-rs/src/arrow_io.rs` with:

```rust
//! Arrow IPC + a dynamic, schema-driven batch builder.
//!
//! `DynamicBatchBuilder` holds one builder per column matching the
//! discovered Arrow schema, plus the static _cdc.op + _cdc.position
//! metadata builders. `append_row` accepts a positional slice of
//! Option<&str> cells (one per data column) plus the op/position
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
            // Anything else falls back to text — discover already
            // collapsed unsupported types to Utf8, but we still need
            // an arm for foreign Arrow types passed through.
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
                Some(s) if s == "1" || s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("t") => {
                    b.append_value(true)
                }
                Some(s) if s == "0" || s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("f") => {
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
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        {
            let mut w =
                StreamWriter::try_new(&mut buf, self.schema.as_ref()).map_err(|e| e.to_string())?;
            w.write(&batch).map_err(|e| e.to_string())?;
            w.finish().map_err(|e| e.to_string())?;
        }
        Ok(buf)
    }
}

/// Parse YYYY-MM-DD into days since the 1970 epoch.
fn parse_date32(s: &str) -> Option<i32> {
    let s = s.split(' ').next().unwrap_or(s); // tolerate "YYYY-MM-DD HH:MM:SS"
    let mut parts = s.split('-');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    days_since_epoch(y, m, d)
}

/// Parse "YYYY-MM-DD HH:MM:SS[.ffffff]" or with a "T" separator and
/// optional trailing timezone offset, into microseconds since epoch.
fn parse_ts_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    // Strip trailing timezone (+HH, +HHMM, +HH:MM, Z) if present.
    let core = strip_tz(s);
    let (date_str, time_str) = split_date_time(core)?;
    let mut date_parts = date_str.split('-');
    let y: i32 = date_parts.next()?.parse().ok()?;
    let mo: u32 = date_parts.next()?.parse().ok()?;
    let d: u32 = date_parts.next()?.parse().ok()?;
    let days = days_since_epoch(y, mo, d)?;
    let (h, mi, s_int, micros) = parse_hms_micros(time_str)?;
    let secs = (days as i64) * 86_400 + (h as i64) * 3600 + (mi as i64) * 60 + s_int as i64;
    Some(secs * 1_000_000 + micros as i64)
}

fn strip_tz(s: &str) -> &str {
    if let Some(idx) = s.rfind(|c: char| c == '+' || c == 'Z' || (c == '-' && {
        // disambiguate the date hyphens from a timezone hyphen — only treat
        // a hyphen as TZ if it appears AFTER the time portion (i.e. after a ':').
        let prefix = &s[..s.rfind(c).unwrap_or(0)];
        prefix.contains(':')
    })) {
        if idx > 10 {
            return &s[..idx];
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
        // Pad/truncate fractional to 6 digits.
        let fi = format!("{:0<6}", fi).chars().take(6).collect::<String>();
        let micros: u32 = fi.parse().ok()?;
        (si, micros)
    } else {
        (s_part.parse().ok()?, 0u32)
    };
    Some((h, mi, s_int, micros))
}

/// Days since 1970-01-01 for proleptic Gregorian (Y, M, D).
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }
    // Howard Hinnant's days_from_civil algorithm.
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
        assert_eq!(parse_date32("2026-05-02"), Some(20_576));
    }

    #[test]
    fn parse_ts_micros_basic() {
        let ts = parse_ts_micros("2026-05-02 13:14:15.999999").unwrap();
        // 2026-05-02 13:14:15.999999 UTC ≈ 1778073255999999
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
```

- [ ] **Step 2: Build + test**

```bash
cd examples/mysql-cdc-rs && cargo build --release && cargo test
```

Expected: clean build, 9 tests pass (existing 6 + 3 new in arrow_io::tests + 1 dynamic_builder round-trip).

- [ ] **Step 3: Commit**

```bash
git add examples/mysql-cdc-rs/src/arrow_io.rs && \
git commit -m "phase-2-3h-2: mysql-cdc-rs dynamic batch builder

Replaces the static Row struct + hardcoded schema with
DynamicBatchBuilder driven by a runtime Arrow Schema. Per-column
typed builders (Int8/16/32/64, Float32/64, Boolean, Utf8, Date32,
Timestamp(Micros)) dispatch text-shaped cells to the right append
path with type-aware parsing.

Date/timestamp parsing is hand-rolled (no chrono dep in the guest
to keep the .wasm small); covers MySQL DATETIME format (YYYY-MM-DD
HH:MM:SS[.ffffff]) and Postgres TIMESTAMPTZ format with optional
trailing offset.

4 new unit tests; full suite at 9."
```

---

## Task 3: mysql-cdc-rs snapshot dynamic projection

**Files:**
- Modify: `examples/mysql-cdc-rs/src/snapshot.rs`

- [ ] **Step 1: Replace snapshot.rs with the dynamic version**

Rewrite `examples/mysql-cdc-rs/src/snapshot.rs`:

```rust
//! Snapshot phase: discover schema + PK on each call, build dynamic
//! SELECT projection, decode rows through the DynamicBatchBuilder.

use std::sync::Arc;

use arrow_schema::Schema;

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns, query_pk_column, DiscoveredColumn};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    run_chunk(url, cfg, batch_size, 0, None)
}

pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (gtid, last_pk) = parse_snapshot_cursor(cursor_value)?;
    run_chunk(url, cfg, batch_size, last_pk, Some(gtid))
}

fn run_chunk(
    url: &str,
    cfg: &SourceCfg,
    batch_size: i64,
    last_pk: i64,
    pinned_gtid: Option<String>,
) -> Result<ReadOutcome, ConnectorError> {
    let h = open(url)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let pk = query_pk_column(h, &cfg.schema, &cfg.table)?;
    let gtid = match pinned_gtid {
        Some(g) => g,
        None => read_gtid_executed(h)?,
    };
    let chunk = chunk_after(h, cfg, &cols, &pk, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, &gtid, last_pk, batch_size)
}

struct Chunk {
    rows: Vec<Vec<Option<String>>>,
    last_pk_in_chunk: Option<i64>,
}

fn chunk_after(
    h: db::DbHandle,
    cfg: &SourceCfg,
    cols: &[DiscoveredColumn],
    pk: &str,
    last_pk: i64,
    batch_size: i64,
) -> Result<Chunk, ConnectorError> {
    let select_list = cols
        .iter()
        .map(|c| format!("`{}`", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select_list} FROM `{schema}`.`{table}` \
         WHERE `{pk}` > ? ORDER BY `{pk}` LIMIT {limit}",
        schema = cfg.schema,
        table = cfg.table,
        limit = batch_size,
    );
    let rows = db::query(h, &sql, &[last_pk.to_string()])
        .map_err(crate::snapshot::db_err_to_connector_err)?;
    let pk_idx = cols
        .iter()
        .position(|c| c.name == pk)
        .ok_or_else(|| ConnectorError::Other(format!("PK column {pk} missing from discovered columns")))?;
    let mut last_pk_in_chunk: Option<i64> = None;
    for r in &rows {
        if let Some(Some(s)) = r.get(pk_idx) {
            if let Ok(v) = s.parse::<i64>() {
                last_pk_in_chunk = Some(v);
            }
        }
    }
    Ok(Chunk {
        rows: rows.into_iter().collect(),
        last_pk_in_chunk,
    })
}

fn finalize(
    chunk: Chunk,
    cols: &[DiscoveredColumn],
    gtid: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let schema = build_full_schema(&columns_to_fields(cols));
    if chunk.rows.is_empty() {
        return Ok(ReadOutcome {
            batch_ipc: schema_only_bytes(&schema)?,
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Gtid,
                value: gtid.to_string(),
            }),
            is_final: true,
        });
    }
    let new_last_pk = chunk.last_pk_in_chunk.unwrap_or(last_pk_in);
    let position = format!("snapshot:{gtid}|{new_last_pk}");
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    for row in &chunk.rows {
        let cells: Vec<Option<&str>> = row
            .iter()
            .take(cols.len())
            .map(|c| c.as_deref())
            .collect();
        bb.append_row(&cells, 's', &position);
    }
    let rows_n = bb.rows() as u32;
    let bytes = bb
        .finish_to_ipc()
        .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?;
    let snapshot_done = (rows_n as i64) < batch_size;
    let (kind, value) = if snapshot_done {
        (CursorKind::Gtid, gtid.to_string())
    } else {
        (CursorKind::SnapshotPk, format!("{gtid}|{new_last_pk}"))
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue { kind, value }),
        is_final: snapshot_done,
    })
}

fn schema_only_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, ConnectorError> {
    crate::arrow_io::schema_ipc_bytes(schema)
        .map_err(|e| ConnectorError::Other(format!("schema_ipc_bytes: {e}")))
}

fn read_gtid_executed(h: db::DbHandle) -> Result<String, ConnectorError> {
    let rows = db::query(h, "SELECT @@gtid_executed", &[])
        .map_err(db_err_to_connector_err)?;
    let cell = rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .flatten()
        .unwrap_or_default();
    Ok(cell)
}

fn open(url: &str) -> Result<db::DbHandle, ConnectorError> {
    db::open(url).map_err(db_err_to_connector_err)
}

pub(crate) fn parse_snapshot_cursor(s: &str) -> Result<(String, i64), ConnectorError> {
    let (gtid, pk) = s.split_once('|').ok_or_else(|| {
        ConnectorError::InvalidConfig(format!("snapshot cursor missing '|': {s}"))
    })?;
    let pk: i64 = pk
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor pk not i64: {e}")))?;
    Ok((gtid.to_string(), pk))
}

pub(crate) fn db_err_to_connector_err(e: db::DbError) -> ConnectorError {
    match e {
        db::DbError::InvalidConfig(s) => ConnectorError::InvalidConfig(s),
        db::DbError::ConnectFailed(s) | db::DbError::PositionLost(s) => {
            ConnectorError::SourceUnavailable(s)
        }
        db::DbError::QueryFailed(s) => ConnectorError::Other(s),
        db::DbError::Unsupported(s) => ConnectorError::Other(format!("unsupported: {s}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_cursor_basic() {
        let (g, pk) = parse_snapshot_cursor("uuid:1-7|42").unwrap();
        assert_eq!(g, "uuid:1-7");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_bad() {
        assert!(parse_snapshot_cursor("nopipe").is_err());
        assert!(parse_snapshot_cursor("g|x").is_err());
    }
}
```

- [ ] **Step 2: Build**

```bash
cd examples/mysql-cdc-rs && cargo build --release
```

Expected: clean build (the `lib.rs` will reference `discover` via `mod discover;` from Task 1; the snapshot module + arrow_io are now wired together).

- [ ] **Step 3: Commit**

```bash
git add examples/mysql-cdc-rs/src/snapshot.rs && \
git commit -m "phase-2-3h-3: mysql-cdc-rs dynamic snapshot projection

snapshot::run_chunk discovers columns + PK on each call (handles
don't survive activations; re-discovery is cheap and avoids
serializing schema state in the cursor). Builds dynamic SELECT
with backtick-quoted column list; orders by the discovered PK
rather than hardcoded id.

Uses arrow_io::DynamicBatchBuilder to populate Arrow values from
text cells per discovered column type. Cursor format unchanged:
snapshot-pk -> gtid transition still works."
```

---

## Task 4: mysql-cdc-rs streaming dynamic decode

**Files:**
- Modify: `examples/mysql-cdc-rs/src/streaming.rs`
- Modify: `examples/mysql-cdc-rs/src/lib.rs` (Guest::discover uses dynamic schema)

- [ ] **Step 1: Replace streaming.rs**

Rewrite `examples/mysql-cdc-rs/src/streaming.rs`:

```rust
//! Streaming phase: discover schema once per read_batch, drain events
//! from db.subscribe-changes, decode JSON rows positionally per
//! discovered column type.

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::snapshot::db_err_to_connector_err;
use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn next_window(
    url: &str,
    cfg: &SourceCfg,
    start_gtid: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let h = db::open(url).map_err(db_err_to_connector_err)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let sub = db::subscribe_changes(h, start_gtid, &[]).map_err(db_err_to_connector_err)?;
    let qualified = format!("{}.{}", cfg.schema, cfg.table);
    let schema = build_full_schema(&columns_to_fields(&cols));
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    let mut latest_position = start_gtid.to_string();
    let mut rows_collected = 0i64;
    while rows_collected < batch_size {
        let evt = match db::next_event(sub).map_err(db_err_to_connector_err)? {
            Some(e) => e,
            None => break,
        };
        if !evt.position.is_empty() {
            latest_position = evt.position.clone();
        }
        if evt.table != qualified {
            continue;
        }
        if append_event(&mut bb, &evt, cols.len()) {
            rows_collected += 1;
        }
    }
    db::close_stream(sub);
    let rows_n = bb.rows() as u32;
    let bytes = if rows_n == 0 {
        Vec::new()
    } else {
        bb.finish_to_ipc()
            .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue {
            kind: CursorKind::Gtid,
            value: latest_position,
        }),
        is_final: false,
    })
}

fn append_event(bb: &mut DynamicBatchBuilder, evt: &db::ChangeEvent, n_cols: usize) -> bool {
    use serde_json::Value;
    let v: Value = match serde_json::from_str(&evt.row_json) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let arr = match evt.op {
        'd' => v.get("before").and_then(|x| x.as_array()),
        _ => v.get("after").and_then(|x| x.as_array()),
    };
    let arr = match arr {
        Some(a) => a,
        None => return false,
    };
    let mut owned: Vec<Option<String>> = Vec::with_capacity(n_cols);
    for i in 0..n_cols {
        owned.push(match arr.get(i) {
            Some(Value::Null) | None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            Some(other) => Some(other.to_string()),
        });
    }
    let cells: Vec<Option<&str>> = owned.iter().map(|c| c.as_deref()).collect();
    bb.append_row(&cells, evt.op, &evt.position);
    true
}
```

- [ ] **Step 2: Update lib.rs Guest::discover to use dynamic schema**

In `examples/mysql-cdc-rs/src/lib.rs`, replace the Guest::discover impl:

```rust
    fn discover(conn: ConnectionConfig, source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let cfg = parse_source_cfg(&source.json)?;
        let h = db::open(&conn.url).map_err(snapshot::db_err_to_connector_err)?;
        let cols = discover::query_columns(h, &cfg.schema, &cfg.table)?;
        db::close(h);
        let schema = arrow_io::build_full_schema(&discover::columns_to_fields(&cols));
        arrow_io::schema_ipc_bytes(&schema)
            .map_err(|e| ConnectorError::Other(format!("schema ipc: {e}")))
    }
```

This requires importing `db` at the top of `lib.rs` — adjust the existing `use platform::connector::host::{log, LogLevel};` line to:

```rust
use platform::connector::db;
use platform::connector::host::{log, LogLevel};
use platform::connector::types::CursorKind;
```

Also remove the now-unused `mod arrow_io;` import — wait no, `arrow_io` is still used. Keep `mod arrow_io;`. Add `mod discover;`.

- [ ] **Step 3: Build + run worker tests against the example to make sure nothing else regressed**

```bash
cd examples/mysql-cdc-rs && cargo build --release && \
cd /Users/satishbabariya/Desktop/etl && cargo build -p worker --lib && cargo test -p worker --lib
```

Expected: example clean, worker lib clean, 138 tests pass.

- [ ] **Step 4: Commit**

```bash
cd /Users/satishbabariya/Desktop/etl && \
git add examples/mysql-cdc-rs/src/streaming.rs examples/mysql-cdc-rs/src/lib.rs && \
git commit -m "phase-2-3h-4: mysql-cdc-rs dynamic streaming decode

Streaming next_window now builds DynamicBatchBuilder from the
discovered schema and decodes JSON cells per column type. JSON
shape from the host is positional, so column order matches the
information_schema.columns ordinal_position used by discover.

Guest::discover also lifts to dynamic — no more hardcoded
'id BIGINT, name TEXT' fields. arbitrary-table tables now
schema-discover at every entry point."
```

---

## Task 5: postgres-cdc-rs discover module

**Files:**
- Create: `examples/postgres-cdc-rs/src/discover.rs`

- [ ] **Step 1: Write the Postgres discover module**

Create `examples/postgres-cdc-rs/src/discover.rs`:

```rust
//! Schema discovery for Postgres: information_schema.columns +
//! pg_index for primary key. Maps Postgres data_type to Arrow
//! DataType, falling back to Utf8 for unsupported types.

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
    // Use information_schema for portability — same shape as MySQL.
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
        "timestamp without time zone" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
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
            DiscoveredColumn { name: "id".into(), data_type: DataType::Int64, nullable: false },
            DiscoveredColumn { name: "name".into(), data_type: DataType::Utf8, nullable: true },
        ];
        let fields = columns_to_fields(&cols);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
    }
}
```

- [ ] **Step 2: Build + test**

```bash
cd examples/postgres-cdc-rs && cargo build --release && cargo test
```

Expected: clean build (the module is unused so far; gives us a `dead_code` warning that disappears with Task 6). 8 tests pass: 5 existing + 3 new in discover::tests.

- [ ] **Step 3: Commit**

```bash
git add examples/postgres-cdc-rs/src/discover.rs && \
git commit -m "phase-2-3h-5: postgres-cdc-rs discover module

Mirror of mysql-cdc-rs discover: query_columns + query_pk_column
hit information_schema with $1/$2 placeholders. map_pg_type covers
bigint/integer/smallint/text/varchar/boolean/real/double/timestamp/
timestamptz/date with Utf8 fallback for unsupported types.

3 new unit tests; example test suite at 8 passing."
```

---

## Task 6: postgres-cdc-rs arrow_io + snapshot + streaming (bundled)

**Files:**
- Modify: `examples/postgres-cdc-rs/src/arrow_io.rs` (rewrite — copy of mysql-cdc-rs version)
- Modify: `examples/postgres-cdc-rs/src/lib.rs` (mod discover; dynamic discover)
- Modify: `examples/postgres-cdc-rs/src/snapshot.rs` (dynamic SELECT, slot/publication unchanged)
- Modify: `examples/postgres-cdc-rs/src/streaming.rs` (dynamic decode)

- [ ] **Step 1: Copy arrow_io.rs from mysql-cdc-rs**

Replace `examples/postgres-cdc-rs/src/arrow_io.rs` with the same content as `examples/mysql-cdc-rs/src/arrow_io.rs` (Task 2's file).

- [ ] **Step 2: Update lib.rs**

In `examples/postgres-cdc-rs/src/lib.rs`, add `mod discover;` near the other mods, and replace the `Guest::discover` impl with the dynamic version (mirroring Task 4 step 2 for mysql-cdc-rs):

```rust
    fn discover(conn: ConnectionConfig, source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let cfg = parse_source_cfg(&source.json)?;
        let h = db::open(&conn.url).map_err(snapshot::db_err_to_connector_err)?;
        let cols = discover::query_columns(h, &cfg.schema, &cfg.table)?;
        db::close(h);
        let schema = arrow_io::build_full_schema(&discover::columns_to_fields(&cols));
        arrow_io::schema_ipc_bytes(&schema)
            .map_err(|e| ConnectorError::Other(format!("schema ipc: {e}")))
    }
```

Add `use platform::connector::db;` to the top of `lib.rs` if not already present.

- [ ] **Step 3: Update snapshot.rs to dynamic SELECT**

Replace `examples/postgres-cdc-rs/src/snapshot.rs`. Mirror the structure of mysql-cdc-rs's snapshot from Task 3, but:
- SQL placeholder is `$1` (not `?`).
- LSN pinning via `SELECT pg_current_wal_lsn()::text` (not `@@gtid_executed`).
- Identifier quoting uses `"..."` (not backticks).
- Slot/publication setup (the existing `ensure_publication` and `ensure_slot` functions) stays.
- Cursor kinds are `Lsn` / `SnapshotPk` (not `Gtid`).

Full file:

```rust
//! Snapshot phase for Postgres: discover schema + PK on each call,
//! ensure publication + slot on the initial call, build dynamic
//! SELECT projection, decode rows through DynamicBatchBuilder.

use std::sync::Arc;

use arrow_schema::Schema;

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns, query_pk_column, DiscoveredColumn};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::{
    publication_name as pub_name_fn, slot_name as slot_name_fn, ConnectorError, ReadOutcome,
    SourceCfg,
};

pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    let h = open(url)?;
    ensure_publication(h, cfg)?;
    ensure_slot(h, cfg)?;
    let lsn = read_current_lsn(h)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let pk = query_pk_column(h, &cfg.schema, &cfg.table)?;
    let chunk = chunk_after(h, cfg, &cols, &pk, 0, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, &lsn, 0, batch_size)
}

pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (lsn, last_pk) = parse_snapshot_cursor(cursor_value)?;
    let h = open(url)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let pk = query_pk_column(h, &cfg.schema, &cfg.table)?;
    let chunk = chunk_after(h, cfg, &cols, &pk, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, &lsn, last_pk, batch_size)
}

struct Chunk {
    rows: Vec<Vec<Option<String>>>,
    last_pk_in_chunk: Option<i64>,
}

fn chunk_after(
    h: db::DbHandle,
    cfg: &SourceCfg,
    cols: &[DiscoveredColumn],
    pk: &str,
    last_pk: i64,
    batch_size: i64,
) -> Result<Chunk, ConnectorError> {
    let select_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select_list} FROM \"{schema}\".\"{table}\" \
         WHERE \"{pk}\" > $1 ORDER BY \"{pk}\" LIMIT {limit}",
        schema = cfg.schema,
        table = cfg.table,
        limit = batch_size,
    );
    let rows = db::query(h, &sql, &[last_pk.to_string()]).map_err(db_err_to_connector_err)?;
    let pk_idx = cols
        .iter()
        .position(|c| c.name == pk)
        .ok_or_else(|| ConnectorError::Other(format!("PK column {pk} missing from discovered columns")))?;
    let mut last_pk_in_chunk: Option<i64> = None;
    for r in &rows {
        if let Some(Some(s)) = r.get(pk_idx) {
            if let Ok(v) = s.parse::<i64>() {
                last_pk_in_chunk = Some(v);
            }
        }
    }
    Ok(Chunk {
        rows: rows.into_iter().collect(),
        last_pk_in_chunk,
    })
}

fn finalize(
    chunk: Chunk,
    cols: &[DiscoveredColumn],
    lsn: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let schema = build_full_schema(&columns_to_fields(cols));
    if chunk.rows.is_empty() {
        return Ok(ReadOutcome {
            batch_ipc: schema_only_bytes(&schema)?,
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Lsn,
                value: lsn.to_string(),
            }),
            is_final: true,
        });
    }
    let new_last_pk = chunk.last_pk_in_chunk.unwrap_or(last_pk_in);
    let position = format!("snapshot:{lsn}|{new_last_pk}");
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    for row in &chunk.rows {
        let cells: Vec<Option<&str>> = row
            .iter()
            .take(cols.len())
            .map(|c| c.as_deref())
            .collect();
        bb.append_row(&cells, 's', &position);
    }
    let rows_n = bb.rows() as u32;
    let bytes = bb
        .finish_to_ipc()
        .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?;
    let snapshot_done = (rows_n as i64) < batch_size;
    let (kind, value) = if snapshot_done {
        (CursorKind::Lsn, lsn.to_string())
    } else {
        (CursorKind::SnapshotPk, format!("{lsn}|{new_last_pk}"))
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue { kind, value }),
        is_final: snapshot_done,
    })
}

fn schema_only_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, ConnectorError> {
    crate::arrow_io::schema_ipc_bytes(schema)
        .map_err(|e| ConnectorError::Other(format!("schema_ipc_bytes: {e}")))
}

fn read_current_lsn(h: db::DbHandle) -> Result<String, ConnectorError> {
    let rows = db::query(h, "SELECT pg_current_wal_lsn()::text", &[])
        .map_err(db_err_to_connector_err)?;
    let cell = rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .flatten()
        .ok_or_else(|| ConnectorError::Other("pg_current_wal_lsn returned no rows".into()))?;
    Ok(cell)
}

fn ensure_publication(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let pub_name = pub_name_fn(&cfg.schema, &cfg.table);
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_publication WHERE pubname = $1",
        &[pub_name.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        let stmt = format!(
            "CREATE PUBLICATION \"{pub_name}\" \
             FOR TABLE \"{schema}\".\"{table}\"",
            schema = cfg.schema,
            table = cfg.table,
        );
        db::query(h, &stmt, &[]).map_err(db_err_to_connector_err)?;
    }
    Ok(())
}

fn ensure_slot(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let slot = slot_name_fn(&cfg.schema, &cfg.table);
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
        &[slot.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        db::query(
            h,
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[slot],
        )
        .map_err(db_err_to_connector_err)?;
    }
    Ok(())
}

fn open(url: &str) -> Result<db::DbHandle, ConnectorError> {
    db::open(url).map_err(db_err_to_connector_err)
}

pub(crate) fn parse_snapshot_cursor(s: &str) -> Result<(String, i64), ConnectorError> {
    let (lsn, pk) = s.split_once('|').ok_or_else(|| {
        ConnectorError::InvalidConfig(format!("snapshot cursor missing '|': {s}"))
    })?;
    let pk: i64 = pk
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor pk not i64: {e}")))?;
    Ok((lsn.to_string(), pk))
}

pub(crate) fn db_err_to_connector_err(e: db::DbError) -> ConnectorError {
    match e {
        db::DbError::InvalidConfig(s) => ConnectorError::InvalidConfig(s),
        db::DbError::ConnectFailed(s) | db::DbError::PositionLost(s) => {
            ConnectorError::SourceUnavailable(s)
        }
        db::DbError::QueryFailed(s) => ConnectorError::Other(s),
        db::DbError::Unsupported(s) => ConnectorError::Other(format!("unsupported: {s}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_cursor_basic() {
        let (lsn, pk) = parse_snapshot_cursor("0/16B3748|42").unwrap();
        assert_eq!(lsn, "0/16B3748");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_bad() {
        assert!(parse_snapshot_cursor("nopipe").is_err());
        assert!(parse_snapshot_cursor("lsn|bad").is_err());
    }
}
```

- [ ] **Step 4: Update streaming.rs**

Replace `examples/postgres-cdc-rs/src/streaming.rs`:

```rust
//! Streaming phase for Postgres: discover schema once per read_batch,
//! drain pgoutput-decoded events from db.subscribe-changes, decode
//! JSON rows positionally per discovered column type.

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::snapshot::db_err_to_connector_err;
use crate::{
    publication_name as pub_name_fn, slot_name as slot_name_fn, ConnectorError, ReadOutcome,
    SourceCfg,
};

pub fn next_window(
    url: &str,
    cfg: &SourceCfg,
    start_lsn: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let h = db::open(url).map_err(db_err_to_connector_err)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let slot = slot_name_fn(&cfg.schema, &cfg.table);
    let pub_name = pub_name_fn(&cfg.schema, &cfg.table);
    let opts: Vec<(String, String)> = vec![
        ("slot_name".to_string(), slot),
        ("publication_names".to_string(), pub_name),
    ];
    let sub = db::subscribe_changes(h, start_lsn, &opts).map_err(db_err_to_connector_err)?;
    let qualified = format!("{}.{}", cfg.schema, cfg.table);
    let schema = build_full_schema(&columns_to_fields(&cols));
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    let mut latest_position = start_lsn.to_string();
    let mut rows_collected = 0i64;
    while rows_collected < batch_size {
        let evt = match db::next_event(sub).map_err(db_err_to_connector_err)? {
            Some(e) => e,
            None => break,
        };
        if !evt.position.is_empty() {
            latest_position = evt.position.clone();
        }
        if evt.table != qualified {
            continue;
        }
        if append_event(&mut bb, &evt, cols.len()) {
            rows_collected += 1;
        }
    }
    db::close_stream(sub);
    let rows_n = bb.rows() as u32;
    let bytes = if rows_n == 0 {
        Vec::new()
    } else {
        bb.finish_to_ipc()
            .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue {
            kind: CursorKind::Lsn,
            value: latest_position,
        }),
        is_final: false,
    })
}

fn append_event(bb: &mut DynamicBatchBuilder, evt: &db::ChangeEvent, n_cols: usize) -> bool {
    use serde_json::Value;
    let v: Value = match serde_json::from_str(&evt.row_json) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let arr = match evt.op {
        'd' => v.get("before").and_then(|x| x.as_array()),
        _ => v.get("after").and_then(|x| x.as_array()),
    };
    let arr = match arr {
        Some(a) => a,
        None => return false,
    };
    let mut owned: Vec<Option<String>> = Vec::with_capacity(n_cols);
    for i in 0..n_cols {
        owned.push(match arr.get(i) {
            Some(Value::Null) | None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            Some(other) => Some(other.to_string()),
        });
    }
    let cells: Vec<Option<&str>> = owned.iter().map(|c| c.as_deref()).collect();
    bb.append_row(&cells, evt.op, &evt.position);
    true
}
```

- [ ] **Step 5: Build + test**

```bash
cd examples/postgres-cdc-rs && cargo build --release && cargo test
```

Expected: clean build + 8 tests pass.

- [ ] **Step 6: Commit**

```bash
cd /Users/satishbabariya/Desktop/etl && \
git add examples/postgres-cdc-rs/ && \
git commit -m "phase-2-3h-6: postgres-cdc-rs dynamic schema flow

arrow_io.rs cloned from mysql-cdc-rs (DynamicBatchBuilder + date/ts
parsing). lib.rs Guest::discover uses dynamic schema. snapshot.rs +
streaming.rs lift to discovered columns + dynamic SELECT/decode.

Slot+publication setup unchanged. Cursor kinds remain Lsn /
SnapshotPk. SQL uses \$1/\$2 placeholders and double-quoted
identifiers per Postgres syntax.

Example test suite at 8 passing."
```

---

## Task 7: e2e tests use 4-column tables

**Files:**
- Modify: `tests/integration/tests/mysql_cdc_wasm_e2e.rs`
- Modify: `tests/integration/tests/postgres_cdc_wasm_e2e.rs`

- [ ] **Step 1: Update MySQL e2e**

In `tests/integration/tests/mysql_cdc_wasm_e2e.rs`, replace the `seed_table_and_rows` body with:

```rust
async fn seed_table_and_rows(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            active TINYINT(1),
            created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
         )",
    )
    .await?;
    conn.query_drop(
        "INSERT INTO items (id, name, active, created) VALUES \
         (1, 'one',   1, '2026-01-01 00:00:00'), \
         (2, 'two',   0, '2026-01-01 00:00:01'), \
         (3, 'three', 1, '2026-01-01 00:00:02')",
    )
    .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}
```

And `perform_iud`:

```rust
async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "INSERT INTO items (id, name, active, created) \
         VALUES (4, 'four', 1, '2026-01-02 00:00:00')",
    )
    .await?;
    conn.query_drop("UPDATE items SET name='TWO' WHERE id=2")
        .await?;
    conn.query_drop("DELETE FROM items WHERE id=1").await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}
```

- [ ] **Step 2: Update Postgres e2e**

In `tests/integration/tests/postgres_cdc_wasm_e2e.rs`, replace `seed_table_and_rows`:

```rust
async fn seed_table_and_rows(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query(
        "CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            name TEXT,
            active BOOLEAN,
            created TIMESTAMP NOT NULL DEFAULT '2026-01-01 00:00:00'
         )",
    )
    .execute(&mut conn)
    .await?;
    sqlx::query(
        "INSERT INTO items (id, name, active, created) VALUES \
         (1, 'one', true, '2026-01-01 00:00:00'), \
         (2, 'two', false, '2026-01-01 00:00:01'), \
         (3, 'three', true, '2026-01-01 00:00:02')",
    )
    .execute(&mut conn)
    .await?;
    conn.close().await?;
    Ok(())
}
```

And `perform_iud`:

```rust
async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query("INSERT INTO items (id, name, active, created) VALUES (4, 'four', true, '2026-01-02 00:00:00')")
        .execute(&mut conn)
        .await?;
    sqlx::query("UPDATE items SET name='TWO' WHERE id=2")
        .execute(&mut conn)
        .await?;
    sqlx::query("DELETE FROM items WHERE id=1")
        .execute(&mut conn)
        .await?;
    conn.close().await?;
    Ok(())
}
```

- [ ] **Step 3: Verify integration-tests still build**

```bash
cargo build -p integration-tests --tests
```

Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/mysql_cdc_wasm_e2e.rs \
        tests/integration/tests/postgres_cdc_wasm_e2e.rs && \
git commit -m "phase-2-3h-7: e2e tests exercise 4-column dynamic schema

Both e2e tests now seed a table with id BIGINT, name TEXT,
active BOOLEAN, created TIMESTAMP — exercising Int64, Utf8,
Boolean, and Timestamp(Microsecond) Arrow type mappings via
schema discovery rather than the previous hardcoded shape.

Still #[ignore]; structural compile is the gate."
```

---

## Task 8: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, replace the existing "Currently:" line with:

```markdown
Currently: **Phase II.3.h — WASM connector schema discovery (complete)** on top of II.3.f. Both `mysql-cdc-rs` and `postgres-cdc-rs` now query `information_schema.columns` at every `discover` and `read_batch` call, building Arrow schemas dynamically from real column metadata. A new `arrow_io::DynamicBatchBuilder` (in each example) dispatches typed column appends through an enum of Int8/16/32/64, Float32/64, Boolean, Utf8, Date32, Timestamp(Microsecond) builders. SQL projection and streaming JSON decode follow the discovered column order. Tables with arbitrary scalar-type schemas now snapshot+stream end-to-end; unsupported types (numeric, json, uuid, etc.) fall back to Utf8. Multi-table CDC (II.3.g) requires workflow/catalog/loader changes and is the next big phase. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final test sweep**

```bash
cargo test -p worker -p common-types -p catalog -p connector-sdk -p loader-sdk -p audit --lib
```

Expected: all pass.

- [ ] **Step 3: Verify both example connectors build + tests pass**

```bash
cd examples/mysql-cdc-rs && cargo build --release && cargo test && \
cd ../postgres-cdc-rs && cargo build --release && cargo test
```

Expected: both clean. mysql-cdc-rs ≥9 tests; postgres-cdc-rs ≥8 tests.

- [ ] **Step 4: Verify worker+cli binaries**

```bash
cd /Users/satishbabariya/Desktop/etl && cargo build -p worker -p cli
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add README.md && \
git commit -m "phase-2-3h-8: README — Phase II.3.h schema discovery complete

Worker lib: 138 tests pass (no host changes; II.3.h is example-side
only).

Example connectors:
  - mysql-cdc-rs: 9 tests pass; ~700 KiB wasm
  - postgres-cdc-rs: 8 tests pass; ~720 KiB wasm

Both example crates now handle arbitrary tables with any combination
of Int8/16/32/64, Float32/64, Boolean, Utf8, Date32,
Timestamp(Microsecond) columns.

Untested at runtime: e2e tests exercise the new 4-column tables but
remain #[ignore]; needs docker + temporal."
```

---

## Self-review

### Spec coverage

| Spec section | Plan task |
|---|---|
| Architecture overview | Tasks 1-6 collectively |
| Postgres → Arrow mapping | Task 5 (`map_pg_type`) |
| MySQL → Arrow mapping | Task 1 (`map_mysql_type`) |
| Snapshot SELECT projection | Task 3 (mysql) + Task 6 (pg) |
| PK extraction | Task 1 + Task 5 (`query_pk_column`) |
| Row decode (snapshot) | Task 2 (`Builder::append_text`) + Tasks 3, 6 (`run_chunk` wiring) |
| Row decode (streaming) | Task 4 (mysql `append_event`) + Task 6 (pg `append_event`) |
| File structure | Tasks 1-6 |
| e2e schema test | Task 7 |
| README | Task 8 |

All spec sections have a corresponding task.

### Placeholder scan

No "TBD"/"TODO"/"implement later". The "Anything else falls back to text" arm in `Builder::for_type` is a real concrete arm, not a placeholder.

### Type consistency

- `DiscoveredColumn { name: String, data_type: DataType, nullable: bool }` — same shape in both `discover.rs` files.
- `query_columns(h, schema, table) -> Result<Vec<DiscoveredColumn>, ConnectorError>` — same signature.
- `query_pk_column(h, schema, table) -> Result<String, ConnectorError>` — same signature.
- `map_mysql_type(t: &str) -> DataType` / `map_pg_type(t: &str) -> DataType` — same shape, DB-specific bodies.
- `columns_to_fields(cols: &[DiscoveredColumn]) -> Vec<Field>` — identical helper.
- `DynamicBatchBuilder::new(Arc<Schema>)` / `.append_row(&[Option<&str>], char, &str)` / `.rows() -> usize` / `.finish_to_ipc() -> Result<Vec<u8>, String>` — identical surface in both arrow_io.rs files.
- `arrow_io::build_full_schema(data_fields: &[Field]) -> Arc<Schema>` and `schema_ipc_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, String>` — identical signatures across both connectors.

All types and method signatures cross-checked.
