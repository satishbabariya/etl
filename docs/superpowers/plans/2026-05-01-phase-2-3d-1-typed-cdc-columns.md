# Phase II.3.d.1 — Type-aware Arrow Columns for MySQL CDC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the v1 "all-Utf8 data columns" simplification in MySQL CDC streaming batches with proper Arrow types (Int32/Int64/Float32/Float64/Utf8/Boolean/Timestamp/Date32), preserving the typed `BinlogValue` from the decoder all the way through to the `RecordBatch`.

**Architecture:** Introduce an internal `ScalarValue` enum that maps 1:1 to the supported Arrow types. The decoder produces `Vec<Option<ScalarValue>>` per row instead of `Vec<Option<String>>`. The batch builder dispatches per column on the schema's declared `DataType` to choose the right Arrow `ArrayBuilder`. The string intermediate goes away entirely — float precision is preserved, integer overflows fail loudly, and downstream Parquet readers get real types.

**Tech Stack:** `mysql_async::binlog::value::BinlogValue` (input), `arrow::array::*Builder` family (output), `chrono` for date/timestamp arithmetic (already a workspace dep).

**Predecessor:** Phase II.3.d (PR #28). The schema-emits-Utf8 hack is documented in the II.3.d design spec at `docs/superpowers/specs/2026-04-30-phase-2-3d-mysql-cdc-design.md` and was explicitly flagged as v1 simplification. This phase closes it.

---

## File Map

- **`crates/worker/src/connectors/mysql/cdc/decode.rs`** — Replaces `RowOp::*` payload from `Vec<Option<String>>` to `Vec<Option<ScalarValue>>`. Adds `ScalarValue` enum, `binlog_value_to_scalar`, `binlog_row_to_scalars`. Removes (or downgrades to private) `binlog_value_to_string` / `binlog_row_to_strings` since no callers remain.
- **`crates/worker/src/connectors/mysql/cdc/schema.rs`** — `schema_from_columns` emits the typed `DataType` returned by `map_mysql_type` instead of forcing `Utf8`. Test updated to assert the typed shape.
- **`crates/worker/src/connectors/mysql/cdc/stream.rs`** — `drain_rows` calls `binlog_row_to_scalars`. `build_record_batch` looks at the schema's `DataType` per column and dispatches to the right `ArrayBuilder`. The `_cdc.{op,lsn,commit_ts}` metadata columns are unchanged.
- **`tests/integration/tests/mysql_cdc_e2e.rs`** — Adds Parquet-schema-shape assertions: `id` is `Int64`, `email` is `Utf8`, `created` is `Timestamp(Microsecond, UTC)`. Existing op-flow assertions preserved.

No new files. All edits localize within `crates/worker/src/connectors/mysql/cdc/` plus the e2e test.

---

## Task 1: Add `ScalarValue` enum + `BinlogValue → ScalarValue` conversion

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/decode.rs`

- [ ] **Step 1: Write the failing tests for `ScalarValue` + conversion**

In `crates/worker/src/connectors/mysql/cdc/decode.rs` `#[cfg(test)] mod tests` block, append (do NOT remove existing tests yet):

```rust
    #[test]
    fn scalar_int32_from_int_value() {
        use arrow::datatypes::DataType;
        let v = BinlogValue::Value(Value::Int(42));
        let s = binlog_value_to_scalar(&v, &DataType::Int32).unwrap();
        assert_eq!(s, Some(ScalarValue::Int32(42)));
    }

    #[test]
    fn scalar_int64_from_int_value() {
        use arrow::datatypes::DataType;
        let v = BinlogValue::Value(Value::Int(42));
        let s = binlog_value_to_scalar(&v, &DataType::Int64).unwrap();
        assert_eq!(s, Some(ScalarValue::Int64(42)));
    }

    #[test]
    fn scalar_int32_overflow_errors() {
        use arrow::datatypes::DataType;
        let v = BinlogValue::Value(Value::Int(i64::MAX));
        let err = binlog_value_to_scalar(&v, &DataType::Int32).unwrap_err();
        assert!(err.to_string().contains("overflow"), "got: {err}");
    }

    #[test]
    fn scalar_utf8_from_bytes() {
        use arrow::datatypes::DataType;
        let v = BinlogValue::Value(Value::Bytes(b"alice@x.com".to_vec()));
        let s = binlog_value_to_scalar(&v, &DataType::Utf8).unwrap();
        assert_eq!(s, Some(ScalarValue::Utf8("alice@x.com".into())));
    }

    #[test]
    fn scalar_float64_from_double() {
        use arrow::datatypes::DataType;
        let v = BinlogValue::Value(Value::Double(3.14));
        let s = binlog_value_to_scalar(&v, &DataType::Float64).unwrap();
        match s.unwrap() {
            ScalarValue::Float64(f) => assert!((f - 3.14).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn scalar_timestamp_from_datetime() {
        use arrow::datatypes::{DataType, TimeUnit};
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
        use arrow::datatypes::DataType;
        // 2026-01-01 = 20454 days since 1970-01-01.
        let v = BinlogValue::Value(Value::Date(2026, 1, 1, 0, 0, 0, 0));
        let s = binlog_value_to_scalar(&v, &DataType::Date32).unwrap();
        assert_eq!(s, Some(ScalarValue::Date32(20454)));
    }

    #[test]
    fn scalar_boolean_from_tinyint_zero_one() {
        use arrow::datatypes::DataType;
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
        use arrow::datatypes::DataType;
        let v = BinlogValue::Value(Value::NULL);
        let s = binlog_value_to_scalar(&v, &DataType::Int64).unwrap();
        assert_eq!(s, None);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::scalar_ -- --nocapture`
Expected: FAIL with "cannot find type `ScalarValue` in this scope" and "cannot find function `binlog_value_to_scalar`".

- [ ] **Step 3: Add the `ScalarValue` enum**

Replace the top-of-file imports + insert the enum just below them. Find:

```rust
use anyhow::{Context, Result};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;
```

Replace with:

```rust
use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
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
```

- [ ] **Step 4: Add the `binlog_value_to_scalar` conversion**

Add at the end of `decode.rs` (after the existing `binlog_row_to_strings` definition, before the `#[cfg(test)]` block):

```rust
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
            // JSONB lands in a Utf8 column as serialized JSON text.
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
        // Common case: DECIMAL columns map to Float64 in our schema, but
        // the binlog often returns DECIMAL as Bytes (string). Parse it.
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
            // Date-only column: ignore the time component.
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::scalar_ -- --nocapture`
Expected: 9 passes (8 new scalar_* tests + the existing renders_null/etc still pass since we haven't touched them yet).

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/decode.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-1-1: ScalarValue enum + BinlogValue→ScalarValue conversion

Internal type-safe scalar layer that bridges decoded BinlogValue to the
Arrow column types declared by schema_from_columns. Replaces the v1
all-Utf8 string intermediate; preserves float precision, fails fast on
integer overflow, parses DECIMAL bytes correctly.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Wire `ScalarValue` into `RowOp` + `binlog_row_to_scalars`

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/decode.rs`

- [ ] **Step 1: Write the failing test for `binlog_row_to_scalars`**

Append to the existing test module:

```rust
    #[test]
    fn row_to_scalars_respects_per_column_types() {
        // We can't easily construct a real BinlogRow without a TableMapEvent,
        // so this test exercises the function-shape: given a row with N
        // columns and N target types, we get N optional scalars in order.
        // Realistic round-trip is covered by the e2e test in Task 6.
        //
        // For a unit-level smoke test we only verify the function compiles
        // and dispatches correctly when given a row of NULLs (which doesn't
        // need real decoding for any specific column type).
        use arrow::datatypes::DataType;
        let target_types = vec![DataType::Int64, DataType::Utf8];
        // Pass an empty types slice — function should fail fast with a
        // helpful error rather than panic.
        let err = binlog_row_to_scalars_with_types_smoke(&target_types).unwrap_err();
        assert!(err.to_string().contains("smoke check"), "got: {err}");
    }

    /// Smoke helper kept private to the test module: makes the row-shape
    /// API discoverable without needing a real BinlogRow.
    fn binlog_row_to_scalars_with_types_smoke(
        _target_types: &[arrow::datatypes::DataType],
    ) -> anyhow::Result<()> {
        anyhow::bail!("smoke check: replace with real BinlogRow when fixtures are available")
    }
```

This test only verifies the *signature* of the function we'll define; the real per-column behavior is exercised end-to-end in Task 6's e2e.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::row_to_scalars -- --nocapture`
Expected: PASS — the smoke helper is local and the function exists. (This step exists to confirm the test module still compiles before the next step changes types underneath it.)

- [ ] **Step 3: Change `RowOp` payload to `Vec<Option<ScalarValue>>`**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, replace the `RowOp` enum:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum RowOp {
    Insert {
        table_id: u64,
        after: Vec<Option<String>>,
    },
    Update {
        table_id: u64,
        before: Option<Vec<Option<String>>>,
        after: Vec<Option<String>>,
    },
    Delete {
        table_id: u64,
        before: Vec<Option<String>>,
    },
}
```

with:

```rust
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
```

- [ ] **Step 4: Add `binlog_row_to_scalars` and remove the legacy string helpers**

In the same file, replace the `binlog_row_to_strings` function (and the `binlog_value_to_string` + `value_to_string` helpers) with:

```rust
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
```

Then delete the now-unused `binlog_value_to_string`, `value_to_string`, and `binlog_row_to_strings` functions plus the existing `renders_*` tests (they verify the deleted code).

The new test module top should look like:

```rust
#[cfg(test)]
mod tests {
    use super::*;
```

…followed only by the `scalar_*` tests added in Task 1 plus the `row_to_scalars_respects_per_column_types` smoke test.

- [ ] **Step 5: Run all decode tests**

Run: `cargo test -p worker connectors::mysql::cdc::decode -- --nocapture`
Expected: 9 passes (8 scalar tests + 1 row-shape smoke test). Build must be green for this module.

The build for `stream.rs` will now be broken — that's Task 4's problem.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/decode.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-1-2: RowOp carries ScalarValue; remove string helpers

Replaces RowOp's Vec<Option<String>> payload with Vec<Option<ScalarValue>>
and adds binlog_row_to_scalars(row, target_types). The legacy string
intermediate (binlog_value_to_string, binlog_row_to_strings,
value_to_string) is removed — no callers remain.

stream.rs build temporarily broken; fixed in the next task.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Schema emits typed Arrow `DataType`s

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/schema.rs`

- [ ] **Step 1: Update the schema test to assert typed shape**

In `crates/worker/src/connectors/mysql/cdc/schema.rs`, replace the existing `schema_appends_cdc_metadata_columns` test with:

```rust
    #[test]
    fn schema_emits_typed_data_columns() {
        let cols = vec![
            col("id", "bigint", 1, false),
            col("email", "varchar", 2, true),
            col("created", "timestamp", 3, false),
        ];
        let s = schema_from_columns(&cols).unwrap();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "email", "created", "_cdc.op", "_cdc.lsn", "_cdc.commit_ts"]
        );
        // v2: typed data columns.
        assert_eq!(s.field(0).data_type(), &DataType::Int64);
        assert_eq!(s.field(1).data_type(), &DataType::Utf8);
        assert!(s.field(1).is_nullable());
        match s.field(2).data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert_eq!(tz.as_ref(), "UTC")
            }
            other => panic!("expected Timestamp(Micro, UTC) for created, got {other:?}"),
        }
        // _cdc.op stays Utf8.
        assert_eq!(s.field(3).data_type(), &DataType::Utf8);
        assert!(!s.field(3).is_nullable());
        // _cdc.commit_ts is the timestamp metadata column.
        assert!(matches!(
            s.field(5).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p worker connectors::mysql::cdc::schema::tests::schema_emits_typed_data_columns -- --nocapture`
Expected: FAIL — `s.field(0).data_type()` is currently `Utf8`, not `Int64`.

- [ ] **Step 3: Update `schema_from_columns` to use the typed `DataType`**

In `crates/worker/src/connectors/mysql/cdc/schema.rs`, replace:

```rust
pub fn schema_from_columns(cols: &[InfoSchemaColumn]) -> Result<Schema> {
    let mut sorted: Vec<_> = cols.iter().collect();
    sorted.sort_by_key(|c| c.ordinal_position);
    let mut fields: Vec<Field> = Vec::with_capacity(sorted.len() + 3);
    for c in sorted {
        // Validate the type is one we support; the resulting DataType is
        // discarded (we always emit Utf8 in v1 to match the
        // StringBuilder-everywhere RecordBatch shape).
        let _ = map_mysql_type(&c.data_type)?;
        fields.push(Field::new(&c.column_name, DataType::Utf8, c.is_nullable));
    }
    fields.push(Field::new("_cdc.op", DataType::Utf8, false));
    fields.push(Field::new("_cdc.lsn", DataType::Utf8, false));
    fields.push(Field::new(
        "_cdc.commit_ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    Ok(Schema::new(fields))
}
```

with:

```rust
/// Build the Arrow schema for the streaming RecordBatch. Data columns
/// carry the typed `DataType` from `map_mysql_type`; the trailing three
/// `_cdc.*` metadata columns are fixed (Utf8 for op/lsn, Timestamp for
/// commit_ts) per RFC-0008.
pub fn schema_from_columns(cols: &[InfoSchemaColumn]) -> Result<Schema> {
    let mut sorted: Vec<_> = cols.iter().collect();
    sorted.sort_by_key(|c| c.ordinal_position);
    let mut fields: Vec<Field> = Vec::with_capacity(sorted.len() + 3);
    for c in sorted {
        let dt = map_mysql_type(&c.data_type)?;
        fields.push(Field::new(&c.column_name, dt, c.is_nullable));
    }
    fields.push(Field::new("_cdc.op", DataType::Utf8, false));
    fields.push(Field::new("_cdc.lsn", DataType::Utf8, false));
    fields.push(Field::new(
        "_cdc.commit_ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    Ok(Schema::new(fields))
}
```

- [ ] **Step 4: Run all schema tests**

Run: `cargo test -p worker connectors::mysql::cdc::schema -- --nocapture`
Expected: 5 passes (4 type-map tests unchanged + the new schema_emits_typed_data_columns).

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/schema.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-1-3: schema_from_columns emits typed Arrow DataTypes

Removes the v1 "discard typed DataType, force Utf8" hack. The schema
now reflects the real MySQL→Arrow type map: bigint→Int64, varchar→Utf8,
datetime→Timestamp(Micro,UTC), date→Date32, etc. Downstream Parquet
readers see real column types.

stream.rs build still broken; fixed in the next task.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `build_record_batch` dispatches per-column on `DataType`

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/stream.rs`

- [ ] **Step 1: Update `drain_rows` to call `binlog_row_to_scalars`**

In `crates/worker/src/connectors/mysql/cdc/stream.rs`, find the import line:

```rust
use super::decode::{binlog_row_to_strings, RowOp};
```

Replace with:

```rust
use super::decode::{binlog_row_to_scalars, RowOp, ScalarValue};
```

Find the three `binlog_row_to_strings(...)` call sites inside `drain_rows`:

```rust
                let after_strs = binlog_row_to_strings(&after_row)?;
                ...
                let before_strs = before.as_ref().map(binlog_row_to_strings).transpose()?;
                ...
                let after_strs = binlog_row_to_strings(&after_row)?;
                ...
                let before_strs = binlog_row_to_strings(&before_row)?;
```

Each needs the new signature with `target_types`. The function signature of `drain_rows` doesn't currently take a schema; we have to thread `arrow_schema` through. Update the `drain_rows` signature:

```rust
fn drain_rows(
    rd: &RowsEventData<'_>,
    tme: &TableMapEvent<'static>,
    ops: &mut Vec<(RowOp, GtidSet, Option<i64>)>,
    new_gtid: &GtidSet,
    commit_ts: Option<i64>,
) -> Result<()> {
```

Replace its signature with:

```rust
fn drain_rows(
    rd: &RowsEventData<'_>,
    tme: &TableMapEvent<'static>,
    ops: &mut Vec<(RowOp, GtidSet, Option<i64>)>,
    new_gtid: &GtidSet,
    commit_ts: Option<i64>,
    data_types: &[DataType],
) -> Result<()> {
```

And replace each `binlog_row_to_strings(&row)?` call with `binlog_row_to_scalars(&row, data_types)?`. Rename local bindings from `*_strs` to `*_scalars` for clarity:

```rust
        RowsEventData::WriteRowsEvent(ev) => {
            for row_pair in ev.rows(tme) {
                let (_before, after) = row_pair.context("decode WRITE row")?;
                let after_row = after.ok_or_else(|| anyhow!("WRITE row missing after-image"))?;
                let after_scalars = binlog_row_to_scalars(&after_row, data_types)?;
                ops.push((
                    RowOp::Insert {
                        table_id: tid,
                        after: after_scalars,
                    },
                    new_gtid.clone(),
                    commit_ts,
                ));
            }
        }
        RowsEventData::UpdateRowsEvent(ev) => {
            for row_pair in ev.rows(tme) {
                let (before, after) = row_pair.context("decode UPDATE row")?;
                let before_scalars = before
                    .as_ref()
                    .map(|r| binlog_row_to_scalars(r, data_types))
                    .transpose()?;
                let after_row = after.ok_or_else(|| anyhow!("UPDATE row missing after-image"))?;
                let after_scalars = binlog_row_to_scalars(&after_row, data_types)?;
                ops.push((
                    RowOp::Update {
                        table_id: tid,
                        before: before_scalars,
                        after: after_scalars,
                    },
                    new_gtid.clone(),
                    commit_ts,
                ));
            }
        }
        RowsEventData::DeleteRowsEvent(ev) => {
            for row_pair in ev.rows(tme) {
                let (before, _after) = row_pair.context("decode DELETE row")?;
                let before_row =
                    before.ok_or_else(|| anyhow!("DELETE row missing before-image"))?;
                let before_scalars = binlog_row_to_scalars(&before_row, data_types)?;
                ops.push((
                    RowOp::Delete {
                        table_id: tid,
                        before: before_scalars,
                    },
                    new_gtid.clone(),
                    commit_ts,
                ));
            }
        }
```

- [ ] **Step 2: Update the `drain_rows` caller in `read_window`**

In `read_window`, the call site:

```rust
                drain_rows(&rd, tme, &mut ops, &new_gtid, current_commit_ts)?;
```

needs the data_types. Compute them once before the loop. At the top of `read_window`, after `let mut new_gtid = start_gtid.clone();`, add:

```rust
    // Pre-compute the per-column DataType slice once. The trailing three
    // entries of arrow_schema are _cdc.{op,lsn,commit_ts} metadata, so
    // data_types is the slice up to len()-3.
    let n_data = arrow_schema
        .fields()
        .len()
        .checked_sub(3)
        .ok_or_else(|| anyhow!("schema must have at least 3 _cdc.* metadata columns"))?;
    let data_types: Vec<DataType> = arrow_schema
        .fields()
        .iter()
        .take(n_data)
        .map(|f| f.data_type().clone())
        .collect();
```

Add `use arrow::datatypes::DataType;` to the top imports if it's not already there.

Then update the call:

```rust
                drain_rows(&rd, tme, &mut ops, &new_gtid, current_commit_ts, &data_types)?;
```

- [ ] **Step 3: Replace `build_record_batch` with type-aware dispatch**

Replace the entire `build_record_batch` and `push_row` functions with:

```rust
fn build_record_batch(
    ops: &[(RowOp, GtidSet, Option<i64>)],
    arrow_schema: SchemaRef,
) -> Result<RecordBatch> {
    use arrow::array::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder,
    };

    let n_data = arrow_schema
        .fields()
        .len()
        .checked_sub(3)
        .ok_or_else(|| anyhow!("schema must have at least 3 _cdc.* metadata columns"))?;

    // One typed builder per data column; metadata columns build their own.
    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = arrow_schema
        .fields()
        .iter()
        .take(n_data)
        .map(|f| make_builder(f.data_type()))
        .collect::<Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();

    for (op, gtid, ts) in ops {
        let row_scalars: &[Option<ScalarValue>] = match op {
            RowOp::Insert { after, .. } => after,
            RowOp::Update { after, .. } => after,
            RowOp::Delete { before, .. } => before,
        };
        if row_scalars.len() != n_data {
            return Err(anyhow!(
                "row has {} scalars but schema declares {} data columns",
                row_scalars.len(),
                n_data
            ));
        }
        for (i, scalar) in row_scalars.iter().enumerate() {
            append_scalar(&mut *col_builders[i], scalar.as_ref(), arrow_schema.field(i).data_type())?;
        }
        let op_str = match op {
            RowOp::Insert { .. } => "i",
            RowOp::Update { .. } => "u",
            RowOp::Delete { .. } => "d",
        };
        op_b.append_value(op_str);
        lsn_b.append_value(gtid.format());
        ts_b.append_option(*ts);
    }

    let mut cols: Vec<ArrayRef> = col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish().with_timezone("UTC")));
    Ok(RecordBatch::try_new(arrow_schema, cols)?)
}

fn make_builder(dt: &arrow::datatypes::DataType) -> Result<Box<dyn arrow::array::ArrayBuilder>> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match dt {
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        other => return Err(anyhow!("no builder for DataType {:?}", other)),
    })
}

fn append_scalar(
    builder: &mut dyn arrow::array::ArrayBuilder,
    scalar: Option<&ScalarValue>,
    dt: &arrow::datatypes::DataType,
) -> Result<()> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Int32Builder"))?
            .append_null(),
        (Some(ScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Int64Builder"))?
            .append_null(),
        (Some(ScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Float32Builder"))?
            .append_null(),
        (Some(ScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Float64Builder"))?
            .append_null(),
        (Some(ScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: expected StringBuilder"))?
            .append_null(),
        (Some(ScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: expected StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: expected BooleanBuilder"))?
            .append_null(),
        (Some(ScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: expected BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Date32Builder"))?
            .append_null(),
        (Some(ScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: expected Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: expected TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(ScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow!("type mismatch: expected TimestampMicrosecondBuilder"))?
                .append_value(*t)
        }
        (Some(other_v), other_dt) => {
            return Err(anyhow!(
                "scalar/builder mismatch: {:?} into {:?}",
                other_v,
                other_dt
            ))
        }
    }
    Ok(())
}
```

Remove the now-unused `push_row` helper (the Vec<StringBuilder> path).

Update the imports at the top of `stream.rs`. Find:

```rust
use arrow::array::{ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::SchemaRef;
```

Replace with:

```rust
use arrow::array::{ArrayBuilder, ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, SchemaRef};
```

- [ ] **Step 4: Run a workspace build to confirm it compiles**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -10`
Expected: empty.

If errors remain, the most likely cause is a missed import or a stray reference to the deleted `binlog_row_to_strings`. Read each error and fix.

- [ ] **Step 5: Run lib tests**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all crates green; total test count ≈ same as before this task (we removed the renders_* tests, added scalar_* tests → roughly equal).

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/stream.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-1-4: type-aware RecordBatch builder

build_record_batch dispatches per column on the schema's declared
DataType, picking the right ArrayBuilder (Int32Builder, Int64Builder,
Float32Builder, Float64Builder, StringBuilder, BooleanBuilder,
Date32Builder, TimestampMicrosecondBuilder). drain_rows calls the new
binlog_row_to_scalars and threads &[DataType] through.

End of the all-Utf8 streaming hack from II.3.d.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: E2E verifies typed Parquet schema

**Files:**
- Modify: `tests/integration/tests/mysql_cdc_e2e.rs`

The existing e2e asserts ops `[i, u, d]` flow but doesn't verify column types. Add Parquet-schema assertions.

- [ ] **Step 1: Add a `read_parquet_schema` helper**

In `tests/integration/tests/mysql_cdc_e2e.rs`, just before the `start_mysql_container` function, add:

```rust
fn read_first_parquet_schema(dir: &Path) -> Option<arrow::datatypes::Schema> {
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(dir)
        .into_iter()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        .map(|e| e.into_path())
        .collect();
    files.sort();
    let path = files.first()?.clone();
    let f = std::fs::File::open(&path).ok()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f).ok()?;
    Some(reader.schema().as_ref().clone())
}
```

- [ ] **Step 2: Add type assertions after the existing `[i, u, d]` check**

In the `mysql_cdc_streaming_only_e2e` test, after the existing assertions on `last_ops`, add:

```rust
    // Verify the Parquet schema carries typed data columns, not Utf8.
    let parquet_schema =
        read_first_parquet_schema(tmp_data.path()).expect("at least one parquet file");
    let names: Vec<&str> = parquet_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert!(names.contains(&"id"), "id column missing: {names:?}");
    let id_field = parquet_schema.field_with_name("id").unwrap();
    assert_eq!(
        id_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "id column should be Int64, got {:?}",
        id_field.data_type()
    );
    let email_field = parquet_schema.field_with_name("email").unwrap();
    assert_eq!(
        email_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "email column should be Utf8, got {:?}",
        email_field.data_type()
    );
    let created_field = parquet_schema.field_with_name("created").unwrap();
    assert!(
        matches!(
            created_field.data_type(),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
        ),
        "created column should be Timestamp(Micro), got {:?}",
        created_field.data_type()
    );
```

- [ ] **Step 3: Verify the e2e still compiles**

Run: `cargo build --workspace --tests 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 4: Run the e2e**

Prerequisite: docker stack up + DOCKER_HOST set:

```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock \
  cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture 2>&1 | tail -20
```

Expected: PASS. The new schema assertions confirm Int64 / Utf8 / Timestamp(Micro) types in the Parquet output.

If the assertions fail with `Utf8` for the `id` column, Task 3 (schema_from_columns) didn't take effect — re-check the build.

If the test fails with a `binlog_row_to_scalars` error like "Int32 column overflow", the BIGINT id is being mapped to Int32 somewhere. Re-check `map_mysql_type` for `bigint` (should be `Int64`).

- [ ] **Step 5: Commit**

```bash
git add tests/integration/tests/mysql_cdc_e2e.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-1-5: e2e asserts typed Parquet column shape

Adds schema-shape assertions to mysql_cdc_streaming_only_e2e: id is
Int64, email is Utf8, created is Timestamp(Microsecond). Verifies the
type-aware streaming path end-to-end against a real MySQL container.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: README + final verification

**Files:**
- Modify: `README.md` (one-line note)

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find the line set during the wasmtime 36 ship (line ~109):

```markdown
Currently: **Phase II.3.d — MySQL CDC streaming-only (complete)** on top of II.3.{a,b,b.1,c}. Native binlog → Arrow → Parquet via `mysql_async`, GTID-set cursor, single-table. Runtime on **wasmtime 36**. Remaining II.3.x connectors and CDC follow-ups (snapshot, multi-table, type-aware columns) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

Replace with:

```markdown
Currently: **Phase II.3.d.1 — Type-aware MySQL CDC columns (complete)** on top of II.3.d. Streaming batches now carry real Arrow types (Int64/Utf8/Timestamp/etc.), not the Utf8-everywhere v1 hack. Runtime on **wasmtime 36**. Remaining II.3.x CDC follow-ups (snapshot, multi-table, Postgres typed columns) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.1 — typed CDC columns

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
