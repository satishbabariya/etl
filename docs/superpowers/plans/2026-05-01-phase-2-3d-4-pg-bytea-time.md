# Phase II.3.d.4 — Postgres CDC OID Coverage: BYTEA + TIME Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the Postgres CDC OID-to-Arrow type map with two common types currently falling through to Utf8: `BYTEA` (binary data, OID 17) → `Binary`, and `TIME` (OID 1083) → `Time64(Microsecond)`. Both already work via the Utf8 fallback, but downstream readers lose the type information.

**Architecture:** Pure type-map extension. Two new `PgScalarValue` variants (`Binary(Vec<u8>)`, `Time64Micros(i64)`), two new `pg_oid_to_arrow_type` arms, two new `parse_pg_text` branches, and matching `make_pg_builder` + `append_pg_scalar` arms. Mirrors the shape of every other type already supported. No new files.

**Tech Stack:** `arrow::array::{BinaryBuilder, Time64MicrosecondBuilder}`, `chrono::NaiveTime` (already a workspace dep) for parsing `HH:MM:SS.ffffff`, manual hex-decode for `\xDEADBEEF` BYTEA text format.

**Predecessor:** Phase II.3.d.3 (PR #32). The OID coverage list is documented in `pg_oid_to_arrow_type` in `types.rs`; this phase adds two entries to it.

---

## File Map

- **`crates/worker/src/connectors/postgres/cdc/types.rs`** — Add `Binary(Vec<u8>)` and `Time64Micros(i64)` variants to `PgScalarValue`. Add OID 17 (BYTEA) and 1083 (TIME) arms in `pg_oid_to_arrow_type`. Add `parse_pg_text` branches for `DataType::Binary` (parsing `\xHEX` and escape format) and `DataType::Time64(TimeUnit::Microsecond)` (parsing `HH:MM:SS.ffffff`). Extend `make_pg_builder` to return `BinaryBuilder` and `Time64MicrosecondBuilder`. Extend `append_pg_scalar` with the four new arms (Some/None × Binary/Time64).

No other files modified. Both Postgres CDC e2e tests are unchanged behaviorally; columns with these types in the snapshot/streaming output will now land typed instead of Utf8 text.

---

## Task 1: Add `ScalarValue::Binary` + BYTEA support

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/types.rs`

- [ ] **Step 1: Add variant + OID arm + tests for the type map**

In `crates/worker/src/connectors/postgres/cdc/types.rs`, add the `Binary` variant to `PgScalarValue`. Find:

```rust
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
```

Replace with:

```rust
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
    /// Raw bytes (BYTEA).
    Binary(Vec<u8>),
}
```

In `pg_oid_to_arrow_type`, find the existing match block. Insert a `17 => DataType::Binary,` arm. The full match becomes:

```rust
pub fn pg_oid_to_arrow_type(oid: u32) -> DataType {
    match oid {
        16 => DataType::Boolean,
        17 => DataType::Binary,                                               // bytea
        20 => DataType::Int64,
        21 | 23 => DataType::Int32,
        25 | 1042 | 1043 => DataType::Utf8,
        700 => DataType::Float32,
        701 | 1700 => DataType::Float64,
        1082 => DataType::Date32,
        1114 => DataType::Timestamp(TimeUnit::Microsecond, None),
        1184 => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        114 | 3802 => DataType::Utf8,
        2950 => DataType::Utf8,
        _ => DataType::Utf8,
    }
}
```

In the `#[cfg(test)] mod tests` block, append:

```rust
    #[test]
    fn maps_bytea_to_binary() {
        assert_eq!(pg_oid_to_arrow_type(17), DataType::Binary);
    }
```

- [ ] **Step 2: Add `parse_pg_text` branch for BYTEA**

Postgres BYTEA's text format is hex-prefixed: `\xDEADBEEF` (the default in modern Postgres) or escape format (`\\000\\001`...) on older settings. We support hex only — escape format is exotic and error-prone. Modern Postgres (`bytea_output=hex`, default since 9.0) always emits hex.

In `parse_pg_text`, after the existing `DataType::Date32` arm, add a `DataType::Binary` arm. Find:

```rust
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let micros = parse_pg_timestamp_to_micros(s, tz.is_some())?;
            PgScalarValue::TimestampMicros(micros)
        }
```

Insert just before that block:

```rust
        DataType::Binary => {
            // Postgres BYTEA hex format: '\xDEADBEEF'. Reject the
            // legacy escape format ('\\000\\001'...) since it's
            // exotic and ambiguous with embedded backslashes.
            let hex = s
                .strip_prefix("\\x")
                .ok_or_else(|| anyhow!(
                    "BYTEA values must use hex format (got {:?}); set bytea_output=hex on the source",
                    &s[..s.len().min(8)]
                ))?;
            if hex.len() % 2 != 0 {
                return Err(anyhow!("BYTEA hex string has odd length: {}", hex.len()));
            }
            let mut bytes = Vec::with_capacity(hex.len() / 2);
            for chunk in hex.as_bytes().chunks(2) {
                let byte = u8::from_str_radix(
                    std::str::from_utf8(chunk).context("BYTEA hex utf8")?,
                    16,
                )
                .with_context(|| format!("parse BYTEA hex byte '{}'", String::from_utf8_lossy(chunk)))?;
                bytes.push(byte);
            }
            PgScalarValue::Binary(bytes)
        }
```

In the test module, append:

```rust
    #[test]
    fn parse_bytea_hex_format() {
        let v = parse_pg_text("\\xDEADBEEF", &DataType::Binary).unwrap();
        assert_eq!(v, Some(PgScalarValue::Binary(vec![0xde, 0xad, 0xbe, 0xef])));
    }

    #[test]
    fn parse_bytea_empty_hex() {
        let v = parse_pg_text("\\x", &DataType::Binary).unwrap();
        assert_eq!(v, Some(PgScalarValue::Binary(vec![])));
    }

    #[test]
    fn parse_bytea_lowercase_hex() {
        let v = parse_pg_text("\\xdeadbeef", &DataType::Binary).unwrap();
        assert_eq!(v, Some(PgScalarValue::Binary(vec![0xde, 0xad, 0xbe, 0xef])));
    }

    #[test]
    fn parse_bytea_rejects_escape_format() {
        let err = parse_pg_text("\\000\\001", &DataType::Binary).unwrap_err();
        assert!(
            err.to_string().contains("hex format"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_bytea_rejects_odd_length() {
        let err = parse_pg_text("\\xABC", &DataType::Binary).unwrap_err();
        assert!(err.to_string().contains("odd length"), "got: {err}");
    }
```

- [ ] **Step 3: Run the new parser tests**

Run: `cargo test -p worker connectors::postgres::cdc::types::tests::parse_bytea -- --nocapture`
Expected: 5 passes.

Run: `cargo test -p worker connectors::postgres::cdc::types::tests::maps_bytea -- --nocapture`
Expected: 1 pass.

- [ ] **Step 4: Extend `make_pg_builder` for `DataType::Binary`**

In `make_pg_builder`, find the existing match block. Insert a `DataType::Binary => Box::new(BinaryBuilder::new()),` arm after `DataType::Boolean`:

```rust
pub fn make_pg_builder(
    dt: &DataType,
) -> Result<Box<dyn arrow::array::ArrayBuilder>> {
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use std::sync::Arc;
    Ok(match dt {
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
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
```

(The full block above replaces the existing function.)

- [ ] **Step 5: Extend `append_pg_scalar` with the two Binary arms**

In `append_pg_scalar`, find the existing imports inside the function body. Add `BinaryBuilder` to the import list:

```rust
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
```

Then in the `match (scalar, dt)` block, find the existing `(None, DataType::Date32)` arm. Insert two new arms just before it (so the Binary handling sits next to its sibling types):

```rust
        (None, DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Binary(b)), DataType::Binary) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BinaryBuilder"))?
            .append_value(b.as_slice()),
```

- [ ] **Step 6: Verify build is clean**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green; worker count up by 6 (1 OID + 5 parser).

- [ ] **Step 7: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/types.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-4-1: BYTEA support in Postgres CDC OID coverage

Adds OID 17 → DataType::Binary, PgScalarValue::Binary(Vec<u8>), and
parse_pg_text branch for the canonical hex format (\xDEADBEEF).
Rejects the legacy escape format with a clear error pointing at
bytea_output=hex.

Snapshot and streaming both pick up Binary builders via the existing
make_pg_builder + append_pg_scalar dispatch.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Add `ScalarValue::Time64Micros` + TIME support

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/types.rs`

- [ ] **Step 1: Add variant + OID arm + tests for the type map**

Append a `Time64Micros` variant to `PgScalarValue`. Replace the enum with:

```rust
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
    /// Raw bytes (BYTEA).
    Binary(Vec<u8>),
    /// Microseconds since midnight (no date, no timezone).
    Time64Micros(i64),
}
```

In `pg_oid_to_arrow_type`, add a `1083 => DataType::Time64(TimeUnit::Microsecond),` arm after the Date32 line:

```rust
        1082 => DataType::Date32,
        1083 => DataType::Time64(TimeUnit::Microsecond),                      // time
        1114 => DataType::Timestamp(TimeUnit::Microsecond, None),
```

(Postgres `TIMETZ` is OID 1266 — we're skipping it for v1; tz-aware time is unusual and Arrow's representation drops the offset anyway.)

In the test module, append:

```rust
    #[test]
    fn maps_time_to_time64_micros() {
        assert_eq!(
            pg_oid_to_arrow_type(1083),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }
```

- [ ] **Step 2: Add `parse_pg_text` branch for Time64**

Postgres TIME text format: `HH:MM:SS` or `HH:MM:SS.ffffff` (microsecond precision). Convert to micros since midnight as `i64`.

In `parse_pg_text`, find the existing `DataType::Date32` arm. Insert just after it:

```rust
        DataType::Time64(TimeUnit::Microsecond) => {
            let nt = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
                .with_context(|| format!("parse time '{s}'"))?;
            let secs = nt.num_seconds_from_midnight() as i64;
            // chrono's NaiveTime stores fractional component in nanoseconds.
            let micros = secs * 1_000_000 + (nt.nanosecond() as i64) / 1_000;
            PgScalarValue::Time64Micros(micros)
        }
```

Add the `chrono::Timelike` import at the top of `types.rs`:

```rust
use chrono::{NaiveDate, NaiveDateTime, Timelike, TimeZone, Utc};
```

(Replaces the existing `use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};` line.)

In the test module, append:

```rust
    #[test]
    fn parse_time_with_micros() {
        // 12:30:45.123456 = (12*3600 + 30*60 + 45) * 1_000_000 + 123_456
        //                 = 45045 * 1_000_000 + 123_456
        //                 = 45_045_123_456
        let v = parse_pg_text(
            "12:30:45.123456",
            &DataType::Time64(TimeUnit::Microsecond),
        )
        .unwrap();
        assert_eq!(v, Some(PgScalarValue::Time64Micros(45_045_123_456)));
    }

    #[test]
    fn parse_time_without_fraction() {
        // 00:00:01 = 1_000_000 micros.
        let v = parse_pg_text("00:00:01", &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        assert_eq!(v, Some(PgScalarValue::Time64Micros(1_000_000)));
    }

    #[test]
    fn parse_time_midnight() {
        let v = parse_pg_text("00:00:00", &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        assert_eq!(v, Some(PgScalarValue::Time64Micros(0)));
    }
```

- [ ] **Step 3: Run the new parser tests**

Run: `cargo test -p worker connectors::postgres::cdc::types::tests::parse_time -- --nocapture`
Expected: 3 passes.

Run: `cargo test -p worker connectors::postgres::cdc::types::tests::maps_time -- --nocapture`
Expected: 1 pass.

- [ ] **Step 4: Extend `make_pg_builder` for `Time64`**

In `make_pg_builder`, add `Time64MicrosecondBuilder` to the import list:

```rust
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    };
```

Insert a `DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),` arm after `DataType::Date32`:

```rust
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
```

- [ ] **Step 5: Extend `append_pg_scalar` with the two Time64 arms**

In `append_pg_scalar`, add `Time64MicrosecondBuilder` to the imports list:

```rust
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    };
```

Find the `(None, DataType::Timestamp(TimeUnit::Microsecond, _))` arm. Insert two new arms just before it:

```rust
        (None, DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Time64Micros(t)), DataType::Time64(TimeUnit::Microsecond)) => builder
            .as_any_mut()
            .downcast_mut::<Time64MicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: Time64MicrosecondBuilder"))?
            .append_value(*t),
```

- [ ] **Step 6: Verify build is clean**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green; worker count up by 4 from Task 1's count (1 OID + 3 parser).

- [ ] **Step 7: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/types.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-4-2: TIME support in Postgres CDC OID coverage

Adds OID 1083 → DataType::Time64(Microsecond), PgScalarValue::
Time64Micros(i64), and parse_pg_text branch for HH:MM:SS[.ffffff]
text format. Computes micros since midnight from chrono NaiveTime.

TIMETZ (OID 1266) is intentionally skipped — Arrow's Time64 doesn't
carry a timezone, so the offset would be silently dropped; v2 can
add a typed adapter if needed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find:

```markdown
Currently: **Phase II.3.d.3 — Typed Postgres CDC snapshot batches (complete)** on top of II.3.d.2. Snapshot now captures all data columns (not just PK) with native Arrow types matching the streaming schema. Postgres CDC is fully type-aware end-to-end. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (MySQL initial snapshot, multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

Replace with:

```markdown
Currently: **Phase II.3.d.4 — Postgres CDC OID coverage: BYTEA + TIME (complete)** on top of II.3.d.3. BYTEA columns now land as Arrow `Binary`; TIME as `Time64(Microsecond)`. Postgres CDC is fully type-aware end-to-end including binary blobs and time-of-day. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (MySQL initial snapshot, multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 3: (Optional, requires Docker) Postgres CDC e2e regression check**

If the docker stack is up, regression-test both Postgres CDC e2e tests to confirm BYTEA/TIME additions didn't break the existing types:

```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
cargo test -p integration-tests --test cdc_insert_update_delete -- --ignored --nocapture 2>&1 | tail -5
cargo test -p integration-tests --test cdc_snapshot_streaming_handoff -- --ignored --nocapture 2>&1 | tail -5
```

Expected: both pass. The `orders` test table doesn't have BYTEA or TIME columns, so this is purely a no-regression check on the existing types.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.4 — BYTEA + TIME OID coverage

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
