# Phase II.3.d.7 — MySQL CDC OID Coverage: BLOB + TIME Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mirror Phase II.3.d.4 (Postgres BYTEA + TIME) for the MySQL CDC connector. Add typed support for the BLOB family (TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB/BINARY/VARBINARY) → Arrow `Binary`, and TIME → Arrow `Time64(Microsecond)`. Both currently raise `unsupported MySQL type` because `map_mysql_type` doesn't have a Utf8 fallback.

**Architecture:** Two new `ScalarValue` variants (`Binary`, `Time64Micros`), six new `map_mysql_type` arms, streaming-side `binlog_value_to_scalar` decoding `BinlogValue::Bytes` directly to `Binary`, snapshot-side per-column SQL projection (HEX for Binary, CAST AS CHAR for everything else), and `parse_mysql_text` branches for both new types (hex decode for Binary, `HH:MM:SS[.ffffff]` for Time64).

**Tech Stack:** Existing — `mysql_async` 0.36, `arrow::array::*Builder`, `chrono::NaiveTime` (already a workspace dep). No new deps.

**Predecessor:** Phase II.3.d.4 (PR #33) shipped the same coverage for Postgres CDC. This phase brings MySQL CDC to parity.

---

## File Map

- **`crates/worker/src/connectors/mysql/cdc/decode.rs`** — Add `Binary(Vec<u8>)` and `Time64Micros(i64)` variants to `ScalarValue`. Extend `binlog_value_to_scalar` so `BinlogValue::Bytes` → `Binary` (when target is Binary) and `BinlogValue::Value(Value::Time(...))` → `Time64Micros`. Extend `parse_mysql_text` with hex-decode and `HH:MM:SS` parsing branches.
- **`crates/worker/src/connectors/mysql/cdc/schema.rs`** — Six new `map_mysql_type` arms: `tinyblob | blob | mediumblob | longblob | binary | varbinary` → `DataType::Binary`; `time` → `DataType::Time64(TimeUnit::Microsecond)`.
- **`crates/worker/src/connectors/mysql/cdc/stream.rs`** — `make_builder` and `append_scalar` (or whatever the local helpers are called) gain Binary + Time64 arms.
- **`crates/worker/src/connectors/mysql/cdc/snapshot.rs`** — `build_chunk_sql` adds per-column projection: `HEX(\`col\`) AS \`col\`` for Binary columns, `CAST(\`col\` AS CHAR) AS \`col\`` for everything else. `make_snapshot_builder` + `append_snapshot_scalar` gain Binary + Time64 arms.

No new files. Both Postgres CDC e2e tests and MySQL CDC e2e tests are unchanged behaviorally; columns of these types in the table will land typed instead of erroring.

---

## Task 1: Add `ScalarValue::Binary` + `Time64Micros` variants, OID + parser entries

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/decode.rs`
- Modify: `crates/worker/src/connectors/mysql/cdc/schema.rs`

- [ ] **Step 1: Extend the `ScalarValue` enum**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, find the `ScalarValue` definition. Replace:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
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
```

with:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
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
    /// Raw bytes (BLOB / BINARY / VARBINARY).
    Binary(Vec<u8>),
    /// Microseconds since midnight (no date, no timezone).
    Time64Micros(i64),
}
```

- [ ] **Step 2: Extend `map_mysql_type` with the new types**

In `crates/worker/src/connectors/mysql/cdc/schema.rs`, find the existing `map_mysql_type` match. Replace:

```rust
pub fn map_mysql_type(mysql_type: &str) -> Result<DataType> {
    let lower = mysql_type.to_ascii_lowercase();
    let dt = match lower.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "decimal" | "numeric" => DataType::Float64,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => DataType::Utf8,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "date" => DataType::Date32,
        "boolean" | "bool" | "bit" => DataType::Boolean,
        "json" => DataType::Utf8,
        other => bail!("unsupported MySQL type '{other}'"),
    };
    Ok(dt)
}
```

with:

```rust
pub fn map_mysql_type(mysql_type: &str) -> Result<DataType> {
    let lower = mysql_type.to_ascii_lowercase();
    let dt = match lower.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "decimal" | "numeric" => DataType::Float64,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => DataType::Utf8,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "boolean" | "bool" | "bit" => DataType::Boolean,
        "json" => DataType::Utf8,
        "tinyblob" | "blob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
            DataType::Binary
        }
        other => bail!("unsupported MySQL type '{other}'"),
    };
    Ok(dt)
}
```

- [ ] **Step 3: Add `parse_mysql_text` branches for Binary + Time64**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, find the existing `parse_mysql_text` function. Insert two new match arms before the catch-all `other => Err(...)` arm:

```rust
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
```

(Insert just before the existing `other => return Err(anyhow!("unsupported target DataType for mysql text parse: {:?}", other))` arm.)

- [ ] **Step 4: Write the failing tests**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, append to the `#[cfg(test)] mod tests` block:

```rust
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
```

In `crates/worker/src/connectors/mysql/cdc/schema.rs`, append to the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn maps_blob_family_to_binary() {
        assert_eq!(map_mysql_type("blob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("tinyblob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("mediumblob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("longblob").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("binary").unwrap(), DataType::Binary);
        assert_eq!(map_mysql_type("varbinary").unwrap(), DataType::Binary);
    }

    #[test]
    fn maps_time_to_time64_micros() {
        assert_eq!(
            map_mysql_type("time").unwrap(),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }
```

- [ ] **Step 5: Run the new tests**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::parse_text_binary -- --nocapture`
Expected: 4 passes.

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::parse_text_time -- --nocapture`
Expected: 2 passes.

Run: `cargo test -p worker connectors::mysql::cdc::schema::tests::maps_blob -- --nocapture`
Expected: 1 pass.

Run: `cargo test -p worker connectors::mysql::cdc::schema::tests::maps_time -- --nocapture`
Expected: 1 pass.

The build will FAIL workspace-wide at this point because stream.rs and snapshot.rs don't yet handle the new ScalarValue variants in their builder/append logic. That's expected — Tasks 2 and 3 fix it.

- [ ] **Step 6: Verify the type map + parser are isolated; commit**

Run: `cargo test -p worker connectors::mysql::cdc::decode -- --nocapture`
Expected: all tests pass (the new variants don't break existing decode logic).

Run: `cargo test -p worker connectors::mysql::cdc::schema -- --nocapture`
Expected: all tests pass.

```bash
git add crates/worker/src/connectors/mysql/cdc/decode.rs crates/worker/src/connectors/mysql/cdc/schema.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-7-1: ScalarValue::Binary + Time64Micros, OID + parser entries

Adds Binary(Vec<u8>) and Time64Micros(i64) to ScalarValue. map_mysql_type
gains tinyblob/blob/mediumblob/longblob/binary/varbinary → DataType::Binary
and time → DataType::Time64(Microsecond). parse_mysql_text decodes
HEX-encoded text for Binary (snapshot uses HEX() projection) and
HH:MM:SS[.ffffff] for Time64.

Streaming + snapshot batch-builder paths still need updates (Tasks 2-3)
before the workspace builds clean.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Streaming side — `binlog_value_to_scalar` + builder/append for Binary + Time64

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/decode.rs` (the `binlog_value_to_scalar` function)
- Modify: `crates/worker/src/connectors/mysql/cdc/stream.rs` (per-column dispatch)

- [ ] **Step 1: Extend `binlog_value_to_scalar` for `Binary`**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, find the `value_to_scalar` function (called by `binlog_value_to_scalar`). MySQL's binlog row image for BLOB/VARBINARY columns delivers `Value::Bytes(Vec<u8>)`. Add a match arm for `(Value::Bytes(b), DataType::Binary)` that copies the bytes directly:

Find:

```rust
        (Value::Bytes(b), DataType::Boolean) => {
            // BIT(1) lands as a single byte: 0 = false, anything else = true.
            Ok(Some(ScalarValue::Boolean(b.iter().any(|&x| x != 0))))
        }
```

Insert just after that arm (before the Date32 / Timestamp arms):

```rust
        (Value::Bytes(b), DataType::Binary) => {
            Ok(Some(ScalarValue::Binary(b.clone())))
        }
```

- [ ] **Step 2: Extend `binlog_value_to_scalar` for `Time64`**

MySQL's binlog row image for TIME columns delivers `Value::Time(neg, days, h, m, s, us)`. The day component is non-zero only for INTERVAL-style values that exceed 24h; standard TIME columns clamp to <24h. Convert directly to micros since midnight, with `neg` meaning the value is negative.

In the same `value_to_scalar` function, find the existing `(Value::Date(...), DataType::Timestamp(...))` arm. Insert right after it:

```rust
        (Value::Time(neg, days, h, m, s, us), DataType::Time64(TimeUnit::Microsecond)) => {
            // MySQL TIME range is [-838:59:59, 838:59:59] (multi-day);
            // Arrow Time64 expects [00:00:00, 24:00:00). For values
            // outside that range, error rather than silently truncate.
            if *days != 0 {
                return Err(anyhow!(
                    "MySQL TIME with day component {} not representable in Arrow Time64",
                    days
                ));
            }
            let mut micros = (*h as i64) * 3_600_000_000
                + (*m as i64) * 60_000_000
                + (*s as i64) * 1_000_000
                + (*us as i64);
            if *neg {
                micros = -micros;
            }
            Ok(Some(ScalarValue::Time64Micros(micros)))
        }
```

- [ ] **Step 3: Find the streaming-side per-column builder/append helpers**

Run: `grep -n "fn make_builder\|fn append_scalar" /Users/satishbabariya/Desktop/etl/crates/worker/src/connectors/mysql/cdc/stream.rs`

Expected: two function definitions inside `stream.rs`. Note them — Step 4 extends both.

- [ ] **Step 4: Extend `make_builder` (streaming) for Binary + Time64**

In `crates/worker/src/connectors/mysql/cdc/stream.rs`, find `make_builder`. Find the existing `use arrow::array::{ ... }` line inside the function and add `BinaryBuilder` and `Time64MicrosecondBuilder`:

```rust
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    };
```

(The existing imports lack Binary + Time64; the line above is the new full list.)

Find the existing match arms — they're a series of `DataType::Int32 => Box::new(Int32Builder::new()),` lines. Insert two new arms (anywhere among them, but conventionally adjacent to similar types):

```rust
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
```

- [ ] **Step 5: Extend `append_scalar` (streaming) for Binary + Time64**

Same file, find `append_scalar` (or similarly-named — match on the function whose body is a giant `match (scalar, dt) { ... }` over `ScalarValue` variants).

Add `BinaryBuilder` and `Time64MicrosecondBuilder` to the function's `use arrow::array::{ ... }` line.

In the giant match, find the `(None, DataType::Date32)` arm (or similar — a "null append" arm for an existing typed column). Insert four new arms covering Binary and Time64 (Some/None × 2):

```rust
        (None, DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_null(),
        (Some(ScalarValue::Binary(b)), DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_value(b.as_slice()),
        (None, DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_null(),
        (Some(ScalarValue::Time64Micros(t)), DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_value(*t),
```

- [ ] **Step 6: Verify build is clean; lib tests pass**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green; worker test count up by ~9 (4 binary parser + 2 time parser + 1 blob OID + 1 time OID + the original tests). The streaming-side `binlog_value_to_scalar` additions don't have unit tests — those are exercised end-to-end in MySQL CDC's existing e2e (no new e2e needed for this task).

- [ ] **Step 7: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/decode.rs crates/worker/src/connectors/mysql/cdc/stream.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-7-2: streaming — binlog Bytes/Time → Binary/Time64 + builders

binlog_value_to_scalar gains a Bytes → Binary path (BLOB/VARBINARY
columns land as Vec<u8> directly) and Time → Time64Micros (with a
guard against multi-day MySQL TIME values that don't fit Arrow's
[00:00, 24:00) range).

stream.rs's make_builder + append_scalar extended with the Binary
and Time64Microsecond arms so the streaming RecordBatch carries
typed columns end-to-end.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Snapshot side — per-column SQL projection + builder/append

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/snapshot.rs`

The snapshot path currently uses `CAST(\`col\` AS CHAR)` for every column. That's wrong for Binary columns: MySQL tries to UTF-8 decode the bytes and either errors or produces garbage. We switch to per-column projection: `HEX(\`col\`)` for Binary, `CAST AS CHAR` for the rest.

- [ ] **Step 1: Update `build_chunk_sql` to take typed projection info**

Current signature:

```rust
pub fn build_chunk_sql(
    schema: &str,
    table: &str,
    pk_col: &str,
    data_field_names: &[&str],
    has_last_pk: bool,
    batch_size: usize,
) -> String
```

Replace with:

```rust
pub fn build_chunk_sql(
    schema: &str,
    table: &str,
    pk_col: &str,
    data_columns: &[(&str, &arrow::datatypes::DataType)],
    has_last_pk: bool,
    batch_size: usize,
) -> String {
    let projection = data_columns
        .iter()
        .map(|(name, dt)| match dt {
            arrow::datatypes::DataType::Binary => {
                format!("HEX(`{name}`) AS `{name}`")
            }
            _ => format!("CAST(`{name}` AS CHAR) AS `{name}`"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = if has_last_pk {
        format!(" WHERE `{pk_col}` > ?")
    } else {
        String::new()
    };
    format!(
        "SELECT {projection} FROM `{schema}`.`{table}`{where_clause} ORDER BY `{pk_col}` LIMIT {batch_size}"
    )
}
```

- [ ] **Step 2: Update the SQL-composition tests**

In the same file, find the existing `build_sql_with_last_pk` and `build_sql_without_last_pk` tests. They currently pass `&["id", "customer", "amount"]`. Update them to pass typed tuples and add a Binary-specific assertion:

```rust
    #[test]
    fn build_sql_with_last_pk() {
        use arrow::datatypes::DataType;
        let cols: Vec<(&str, &DataType)> = vec![
            ("id", &DataType::Int64),
            ("customer", &DataType::Utf8),
            ("amount", &DataType::Utf8),
        ];
        let s = build_chunk_sql("shop", "orders", "id", &cols, true, 500);
        assert!(s.contains("`shop`.`orders`"));
        assert!(s.contains("WHERE `id` > ?"));
        assert!(s.contains("ORDER BY `id`"));
        assert!(s.contains("LIMIT 500"));
        assert!(s.contains("CAST(`id` AS CHAR) AS `id`"));
        assert!(s.contains("CAST(`customer` AS CHAR) AS `customer`"));
    }

    #[test]
    fn build_sql_without_last_pk() {
        use arrow::datatypes::DataType;
        let cols: Vec<(&str, &DataType)> = vec![
            ("id", &DataType::Int64),
            ("amount", &DataType::Utf8),
        ];
        let s = build_chunk_sql("shop", "orders", "id", &cols, false, 100);
        assert!(!s.contains("WHERE"));
        assert!(s.contains("ORDER BY `id`"));
        assert!(s.contains("LIMIT 100"));
    }

    #[test]
    fn build_sql_uses_hex_for_binary() {
        use arrow::datatypes::DataType;
        let cols: Vec<(&str, &DataType)> = vec![
            ("id", &DataType::Int64),
            ("payload", &DataType::Binary),
            ("name", &DataType::Utf8),
        ];
        let s = build_chunk_sql("shop", "blobs", "id", &cols, false, 100);
        assert!(
            s.contains("HEX(`payload`) AS `payload`"),
            "expected HEX projection for binary column; got: {s}"
        );
        assert!(s.contains("CAST(`id` AS CHAR) AS `id`"));
        assert!(s.contains("CAST(`name` AS CHAR) AS `name`"));
    }
```

- [ ] **Step 3: Update `read_chunk` to pass typed columns**

In the same file, find the `read_chunk` function. The current call to `build_chunk_sql` passes a `Vec<&str>` of names. Update it to build the typed tuples from the schema:

Find:

```rust
    let data_field_names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .take(n_data)
        .map(|f| f.name().as_str())
        .collect();

    let stmt = build_chunk_sql(
        schema_name,
        table_name,
        pk_column,
        &data_field_names,
        last_pk.is_some(),
        batch_size,
    );
```

Replace with:

```rust
    let data_columns: Vec<(&str, &arrow::datatypes::DataType)> = arrow_schema
        .fields()
        .iter()
        .take(n_data)
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();

    let stmt = build_chunk_sql(
        schema_name,
        table_name,
        pk_column,
        &data_columns,
        last_pk.is_some(),
        batch_size,
    );
```

- [ ] **Step 4: Extend `make_snapshot_builder` and `append_snapshot_scalar`**

In the same file, find `make_snapshot_builder`. Add `BinaryBuilder` and `Time64MicrosecondBuilder` to the function's `use arrow::array::{ ... }` import line, then add two arms to the match:

```rust
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
```

Find `append_snapshot_scalar`. Add `BinaryBuilder` and `Time64MicrosecondBuilder` to the function's `use arrow::array::{ ... }` line.

In the giant match, insert four new arms (matching the streaming-side additions from Task 2 Step 5):

```rust
        (None, DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_null(),
        (Some(ScalarValue::Binary(b)), DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_value(b.as_slice()),
        (None, DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_null(),
        (Some(ScalarValue::Time64Micros(t)), DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_value(*t),
```

- [ ] **Step 5: Verify everything builds + tests pass**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test -p worker connectors::mysql::cdc::snapshot -- --nocapture`
Expected: 3 passes (the two existing SQL tests now pass tuples; the new `build_sql_uses_hex_for_binary` test passes).

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/snapshot.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-7-3: snapshot — per-column projection + builder/append

build_chunk_sql now takes (name, DataType) tuples and emits HEX(\`col\`)
for Binary columns, CAST(\`col\` AS CHAR) for everything else. read_chunk
threads the typed column list through. make_snapshot_builder +
append_snapshot_scalar gain Binary + Time64 arms.

The streaming side already lands BLOB/VARBINARY directly via
BinlogValue::Bytes (Task 2); snapshot's HEX projection feeds the
same parse_mysql_text → ScalarValue::Binary pipeline as Task 1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find:

```markdown
Currently: **Phase II.3.d.6 — CDC snapshot resume via catalog persistence (complete)** on top of II.3.d.5.
```

Replace the entire "Currently:" line with:

```markdown
Currently: **Phase II.3.d.7 — MySQL CDC OID coverage: BLOB + TIME (complete)** on top of II.3.d.6. MySQL CDC now lands BLOB/TINYBLOB/MEDIUMBLOB/LONGBLOB/BINARY/VARBINARY columns as Arrow `Binary` and TIME as `Time64(Microsecond)`. Both connectors are at type-coverage parity for binary and time-of-day. Snapshot uses `HEX()` projection for Binary columns; streaming reads `BinlogValue::Bytes` directly. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 3: (Optional, requires Docker) Existing MySQL CDC e2e regression check**

If the docker stack is up:

```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock \
  cargo test -p integration-tests --test mysql_cdc_e2e mysql_cdc_streaming_only_e2e -- --ignored --nocapture 2>&1 | tail -5
```

Expected: pass. The existing `customers` test table doesn't have BLOB or TIME columns, so this is a pure no-regression check on the existing types.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.7 — BLOB + TIME OID coverage

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
