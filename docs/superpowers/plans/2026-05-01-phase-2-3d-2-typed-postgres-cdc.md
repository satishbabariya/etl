# Phase II.3.d.2 — Type-aware Arrow Columns for Postgres CDC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mirror Phase II.3.d.1 for the Postgres CDC streaming path: replace the all-Utf8 data columns in `events_to_batch` with proper Arrow types derived from the pgoutput Relation message's `type_oid`. Snapshot path is explicitly deferred.

**Architecture:** Add `pg_oid_to_arrow_type` (Postgres OID → Arrow `DataType`) mapping. Add `PgScalarValue` enum + `parse_pg_text` parser converting pgoutput's text-encoded values to typed scalars. Update `events_to_batch` to derive per-column types from the Relation's OIDs and dispatch to type-appropriate Arrow builders. The `read_window` activity wires the OID-typed schema through to the batch builder.

**Tech Stack:** `arrow::array::*Builder` family, `chrono` for date/timestamp parsing (already a workspace dep). No new deps.

**Predecessor:** Phase II.3.d.1 (PR #30). The MySQL CDC streaming path now emits typed columns; this phase brings the Postgres CDC streaming path to parity. Snapshot path (`snapshot_chunk` activity, `snapshot::read_chunk`) remains all-Utf8 — fixing it requires a separate change to the sqlx-driven row-fetch path and is deferred to its own follow-up phase.

---

## File Map

- **`crates/worker/src/connectors/postgres/cdc/types.rs`** *(new)* — Hosts `pg_oid_to_arrow_type`, `PgScalarValue`, and `parse_pg_text`. All pure logic, fully unit-testable, no I/O.
- **`crates/worker/src/connectors/postgres/cdc/mod.rs`** — Add `pub mod types;`.
- **`crates/worker/src/connectors/postgres/cdc/stream.rs`** — `events_to_batch` becomes type-aware: takes the Relation's `Vec<ColumnInfo>` (which already carries `type_oid`), derives Arrow types, dispatches per column.
- **`crates/worker/src/activities/cdc/mod.rs`** (`read_window` activity) — Replaces the hardcoded `(name, DataType::Utf8)` list with `(name, pg_oid_to_arrow_type(oid))`.

Snapshot path untouched. Decoder unchanged. `cdc_schema_for` signature unchanged.

---

## Task 1: `pg_oid_to_arrow_type` mapping

**Files:**
- Create: `crates/worker/src/connectors/postgres/cdc/types.rs`
- Modify: `crates/worker/src/connectors/postgres/cdc/mod.rs`

- [ ] **Step 1: Add the new module path**

In `crates/worker/src/connectors/postgres/cdc/mod.rs`, find the existing `pub mod` lines and append:

```rust
pub mod types;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/worker/src/connectors/postgres/cdc/types.rs`:

```rust
//! Postgres pgoutput type adapters: OID → Arrow `DataType`, and a
//! text-value parser that yields typed `PgScalarValue`s for the
//! type-aware streaming RecordBatch.

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};

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
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p worker connectors::postgres::cdc::types -- --nocapture`
Expected: 6 passes.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/mod.rs crates/worker/src/connectors/postgres/cdc/types.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-2-1: pg_oid_to_arrow_type mapping

Postgres OID → Arrow DataType lookup for the type-aware streaming
batch. Common OIDs (int2/int4/int8, text family, float4/float8,
timestamp/timestamptz, date, bool, jsonb, uuid) map to native types;
unknown OIDs fall back to Utf8 so unfamiliar columns still round-trip
through pgoutput's text format.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `PgScalarValue` enum + `parse_pg_text` parser

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/types.rs`

- [ ] **Step 1: Append failing tests**

Add to the existing `tests` module in `types.rs`:

```rust
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
        // 2026-01-01 = 20454 days since 1970-01-01.
        let v = parse_pg_text("2026-01-01", &DataType::Date32).unwrap();
        assert_eq!(v, Some(PgScalarValue::Date32(20454)));
    }

    #[test]
    fn parse_timestamp_with_microseconds() {
        // 2026-01-01 00:00:00 UTC = 1_767_225_600_000_000 micros.
        let v = parse_pg_text(
            "2026-01-01 00:00:00",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(v, Some(PgScalarValue::TimestampMicros(1_767_225_600_000_000)));
    }

    #[test]
    fn parse_timestamptz_with_offset() {
        // 2026-01-01 00:00:00+00 = same as above.
        let v = parse_pg_text(
            "2026-01-01 00:00:00+00",
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )
        .unwrap();
        assert_eq!(v, Some(PgScalarValue::TimestampMicros(1_767_225_600_000_000)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p worker connectors::postgres::cdc::types -- --nocapture`
Expected: FAIL with "cannot find type `PgScalarValue`" and "cannot find function `parse_pg_text`".

- [ ] **Step 3: Add `PgScalarValue` enum and the parser**

In `crates/worker/src/connectors/postgres/cdc/types.rs`, replace the imports + `pg_oid_to_arrow_type` block with:

```rust
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
        16 => DataType::Boolean,
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

/// Parse pgoutput's textual value representation into a typed
/// `PgScalarValue`, given the column's target Arrow `DataType`.
/// Returns `None` only when the caller passes the empty/null
/// sentinel — pgoutput's NULL sentinel is `None` upstream, so a
/// non-`None` `s` always parses to `Some(...)`.
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
    // Postgres timestamp text varies: "2026-01-01 00:00:00",
    // "2026-01-01 00:00:00.123456", "2026-01-01 00:00:00+00",
    // "2026-01-01 00:00:00.123456+00:00". Try the most-specific
    // format first, then fall back. We always end up in UTC micros.
    if has_tz {
        // chrono's DateTime parse handles a wide range of offset forms.
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z") {
            return Ok(dt.with_timezone(&Utc).timestamp_micros());
        }
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
            return Ok(dt.with_timezone(&Utc).timestamp_micros());
        }
    }
    // No-tz path (TIMESTAMP without time zone) — interpret as UTC.
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(Utc.from_utc_datetime(&naive).timestamp_micros());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&naive).timestamp_micros());
    }
    Err(anyhow!("unrecognised pg timestamp text '{s}'"))
}
```

(Remove the standalone `pg_oid_to_arrow_type` block from Task 1's first paste — the consolidated block above replaces it.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p worker connectors::postgres::cdc::types -- --nocapture`
Expected: 14 passes (6 OID + 8 parse_pg_text).

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/types.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-2-2: PgScalarValue + parse_pg_text

Typed scalar layer for the Postgres CDC streaming path. parse_pg_text
converts pgoutput's textual values to PgScalarValue per the column's
target Arrow DataType. Handles int{32,64}, float{32,64}, Utf8, Boolean
(t/f/true/false/0/1), Date32 (ISO yyyy-mm-dd), and Timestamp(Micro)
both with and without timezone offsets.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `read_window` activity uses OID-typed schema

**Files:**
- Modify: `crates/worker/src/activities/cdc/mod.rs`

- [ ] **Step 1: Inspect the current Utf8-forcing site**

Run: `grep -n 'cdc_schema_for\|DataType::Utf8' /Users/satishbabariya/Desktop/etl/crates/worker/src/activities/cdc/mod.rs`
Expected: a line like:

```rust
        let cols: Vec<(&str, DataType)> =
            rel.columns.iter().map(|c| (c.name.as_str(), DataType::Utf8)).collect();
```

This is inside `read_window`. The Relation message's `ColumnInfo` already carries `type_oid: u32` — we can map it directly.

- [ ] **Step 2: Replace the hardcoded Utf8 list with type-derived columns**

In `crates/worker/src/activities/cdc/mod.rs`, find:

```rust
        let cols: Vec<(&str, DataType)> =
            rel.columns.iter().map(|c| (c.name.as_str(), DataType::Utf8)).collect();
```

Replace with:

```rust
        let cols: Vec<(&str, DataType)> = rel
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    crate::connectors::postgres::cdc::types::pg_oid_to_arrow_type(
                        c.type_oid,
                    ),
                )
            })
            .collect();
```

The `cdc_schema_for` signature is unchanged; it just receives real types now.

- [ ] **Step 3: Verify the snapshot site is unchanged**

Run: `grep -n 'cdc_schema_for' /Users/satishbabariya/Desktop/etl/crates/worker/src/activities/cdc/mod.rs`
Expected: snapshot's `cdc_schema_for(&[(input.pk_col.as_str(), DataType::Utf8)])` is still Utf8 — that's intentional. Snapshot is deferred to a follow-up phase.

- [ ] **Step 4: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty. (The schema now declares Int64/Utf8/Timestamp/etc. for the typed columns, but `events_to_batch` still appends string values via `StringBuilder` — Task 4 fixes that. In the meantime the build is fine, but the runtime will fail with a builder-vs-schema type mismatch on a real CDC pipeline.)

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/cdc/mod.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-2-3: read_window builds typed schema from Relation OIDs

Replaces the hardcoded "all-Utf8" column list in read_window with
pg_oid_to_arrow_type(c.type_oid) per column. cdc_schema_for now
receives real types; events_to_batch is updated in Task 4 to match.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `events_to_batch` dispatches per-column on `DataType`

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/stream.rs`

- [ ] **Step 1: Read the current `events_to_batch`**

Run: `sed -n '70,150p' /Users/satishbabariya/Desktop/etl/crates/worker/src/connectors/postgres/cdc/stream.rs`
Expected: the function builds `Vec<StringBuilder>` for data columns and pushes string values from each `CdcEvent::{Insert,Update,Delete}` row.

- [ ] **Step 2: Replace the data-column builders with per-type dispatch**

In `crates/worker/src/connectors/postgres/cdc/stream.rs`, find the `events_to_batch` function and replace it. The shape:

```rust
/// Convert a flush of `read_window` into a `RecordBatch`. Begin/Commit/
/// Relation are folded into per-row `_cdc.lsn` / `_cdc.commit_ts` /
/// `_cdc.txid` metadata; only data rows (i/u/d) become Arrow rows.
pub fn events_to_batch(
    events: &[CdcEvent],
    relations: &RelationTable,
    rel_id_filter: u32,
    schema: arrow::datatypes::SchemaRef,
) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{
        ArrayBuilder, ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    let rel = relations
        .get(&rel_id_filter)
        .ok_or_else(|| anyhow::anyhow!("no Relation seen for rel_id {rel_id_filter}"))?;
    let n_data = rel.columns.len();

    // The trailing four metadata fields are: _cdc.op, _cdc.lsn,
    // _cdc.commit_ts, _cdc.txid. Data fields are the first n_data.
    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data)
        .map(|i| make_pg_builder(schema.field(i).data_type()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    let mut tx_b = Int64Builder::new();

    let mut current_txid: Option<u32> = None;
    let mut current_commit_ts: Option<i64> = None;
    let mut current_lsn: Option<u64> = None;

    for ev in events {
        match ev {
            CdcEvent::Begin { xid, commit_ts_micros, .. } => {
                current_txid = Some(*xid);
                current_commit_ts = Some(*commit_ts_micros);
            }
            CdcEvent::Commit { end_lsn, .. } => {
                current_lsn = Some(*end_lsn);
            }
            CdcEvent::Relation(_) => {}
            CdcEvent::Insert { rel_id, row } if *rel_id == rel_id_filter => {
                append_pg_row(&mut col_builders, &schema, row)?;
                op_b.append_value("i");
                lsn_b.append_value(common_types::cursor::lsn_to_string(
                    current_lsn.unwrap_or(0),
                ));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            CdcEvent::Update { rel_id, row } if *rel_id == rel_id_filter => {
                append_pg_row(&mut col_builders, &schema, row)?;
                op_b.append_value("u");
                lsn_b.append_value(common_types::cursor::lsn_to_string(
                    current_lsn.unwrap_or(0),
                ));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            CdcEvent::Delete { rel_id, key } if *rel_id == rel_id_filter => {
                // For DELETE the row image only contains key columns;
                // non-key columns are appended as nulls.
                append_pg_row_partial(&mut col_builders, &schema, key, &rel.columns)?;
                op_b.append_value("d");
                lsn_b.append_value(common_types::cursor::lsn_to_string(
                    current_lsn.unwrap_or(0),
                ));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            _ => {}
        }
    }

    let mut cols: Vec<ArrayRef> = col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish()));
    cols.push(Arc::new(tx_b.finish()));
    Ok(arrow::record_batch::RecordBatch::try_new(schema, cols)?)
}

fn make_pg_builder(
    dt: &arrow::datatypes::DataType,
) -> anyhow::Result<Box<dyn arrow::array::ArrayBuilder>> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
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

fn append_pg_row(
    builders: &mut [Box<dyn arrow::array::ArrayBuilder>],
    schema: &arrow::datatypes::SchemaRef,
    row: &[Option<String>],
) -> anyhow::Result<()> {
    if row.len() != builders.len() {
        anyhow::bail!(
            "row has {} columns but {} builders",
            row.len(),
            builders.len()
        );
    }
    for (i, value_opt) in row.iter().enumerate() {
        let dt = schema.field(i).data_type();
        let parsed = match value_opt.as_deref() {
            Some(s) => super::types::parse_pg_text(s, dt)?,
            None => None,
        };
        append_pg_scalar(&mut **builders.get_mut(i).unwrap(), parsed.as_ref(), dt)?;
    }
    Ok(())
}

fn append_pg_row_partial(
    builders: &mut [Box<dyn arrow::array::ArrayBuilder>],
    schema: &arrow::datatypes::SchemaRef,
    key: &[Option<String>],
    columns: &[super::decode::ColumnInfo],
) -> anyhow::Result<()> {
    // DELETE row image only carries key columns. We iterate by column
    // index and append from `key` if the column is a key, otherwise null.
    for (i, col) in columns.iter().enumerate() {
        let dt = schema.field(i).data_type();
        let parsed = if col.is_key {
            match key.get(i).and_then(|v| v.as_deref()) {
                Some(s) => super::types::parse_pg_text(s, dt)?,
                None => None,
            }
        } else {
            None
        };
        append_pg_scalar(&mut **builders.get_mut(i).unwrap(), parsed.as_ref(), dt)?;
    }
    Ok(())
}

fn append_pg_scalar(
    builder: &mut dyn arrow::array::ArrayBuilder,
    scalar: Option<&super::types::PgScalarValue>,
    dt: &arrow::datatypes::DataType,
) -> anyhow::Result<()> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use super::types::PgScalarValue;
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: StringBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: BooleanBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Date32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow::anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(PgScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow::anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
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
```

(Delete the old `events_to_batch` body that uses `Vec<StringBuilder>`. The new function above replaces it entirely.)

- [ ] **Step 3: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 4: Run lib tests**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -10`
Expected: all crates green; worker tally up by 14 from the new `types::tests`.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/stream.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-2-4: events_to_batch — type-aware ArrayBuilder dispatch

Replaces Vec<StringBuilder> in events_to_batch with per-column
Box<dyn ArrayBuilder> picked from the schema's DataType. Pgoutput
text values flow through parse_pg_text → PgScalarValue → typed
builder. DELETE row image (key columns only) appends nulls for
non-key columns.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: E2E re-verifies typed Parquet schema

**Files:**
- Modify: `tests/integration/tests/cdc_insert_update_delete.rs`

The existing Postgres CDC IUD e2e doesn't currently assert column types. Add a small Parquet-schema check.

- [ ] **Step 1: Identify the test's table shape and tmp Parquet dir**

Run: `grep -n 'CREATE TABLE\|tmp_data\|tempdir\|local_parquet' /Users/satishbabariya/Desktop/etl/tests/integration/tests/cdc_insert_update_delete.rs | head -10`
Expected: a `CREATE TABLE` with at least one int + text + timestamp column, and a tmp dir for the Parquet destination. Note the column shape exactly — the assertions in Step 3 must reference real column names from this test.

- [ ] **Step 2: Add a `read_first_parquet_schema` helper if missing**

If the test file doesn't already have a Parquet-schema-reading helper, append one (anywhere above the test function):

```rust
fn read_first_parquet_schema(dir: &std::path::Path) -> Option<arrow::datatypes::Schema> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut files: Vec<std::path::PathBuf> = walkdir::WalkDir::new(dir)
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

- [ ] **Step 3: Add type assertions after the existing op-flow assertions**

At the end of the test (just before `Ok(())`), add type assertions for at least one int column, one text column, and one timestamp column from the test's `CREATE TABLE`. Adjust the column names to match Step 1's findings. Example assuming a table with `id BIGINT, name TEXT, created_at TIMESTAMPTZ`:

```rust
    let parquet_schema =
        read_first_parquet_schema(tmp_data.path()).expect("at least one parquet file");
    let id_field = parquet_schema.field_with_name("id").unwrap();
    assert_eq!(
        id_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "id should be Int64, got {:?}",
        id_field.data_type()
    );
    let name_field = parquet_schema.field_with_name("name").unwrap();
    assert_eq!(
        name_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "name should be Utf8, got {:?}",
        name_field.data_type()
    );
    if let Ok(ts_field) = parquet_schema.field_with_name("created_at") {
        assert!(
            matches!(
                ts_field.data_type(),
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
            ),
            "created_at should be Timestamp(Micro), got {:?}",
            ts_field.data_type()
        );
    }
```

If the test's `CREATE TABLE` uses different column names or a non-bigint primary key, adapt the assertions accordingly. The intent is: prove at least one numeric, one text, and one timestamp column round-trip with their declared Arrow types.

- [ ] **Step 4: Verify the test still compiles**

Run: `cargo build --workspace --tests 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 5: Run the e2e (requires the docker stack)**

Prerequisite:

```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
```

Then:

```bash
cargo test -p integration-tests --test cdc_insert_update_delete -- --ignored --nocapture 2>&1 | tail -20
```

Expected: PASS. The Parquet schema assertions confirm typed columns.

If the test fails with a builder/schema type mismatch at runtime, the most likely cause is a Postgres OID we don't yet handle — pgoutput's text value passed through but `pg_oid_to_arrow_type` returned Utf8 while some other path expected a typed builder. Inspect the error message; add the OID to `pg_oid_to_arrow_type` if it should be a typed type.

If the test fails with `parse_pg_text` errors, the table has a column type whose text format isn't covered. Add the format to `parse_pg_text` or downgrade the OID to Utf8 fallback.

- [ ] **Step 6: Run the snapshot+streaming handoff e2e for completeness**

```bash
cargo test -p integration-tests --test cdc_snapshot_streaming_handoff -- --ignored --nocapture 2>&1 | tail -10
```

Expected: PASS. Snapshot path is still all-Utf8 (intentional — deferred); streaming half now emits typed columns. The test should still pass because it asserts row counts, not column types.

- [ ] **Step 7: Commit**

```bash
git add tests/integration/tests/cdc_insert_update_delete.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-2-5: e2e asserts typed Parquet shape for Postgres CDC

Adds Parquet schema-shape assertions to cdc_insert_update_delete:
numeric column round-trips as Int64, text as Utf8, timestamp as
Timestamp(Microsecond). Verifies the type-aware streaming path
end-to-end against the existing Postgres CDC docker harness.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find the line set during Phase II.3.d.1:

```markdown
Currently: **Phase II.3.d.1 — Type-aware MySQL CDC columns (complete)** on top of II.3.d. Streaming batches now carry real Arrow types (Int64/Utf8/Timestamp/etc.), not the Utf8-everywhere v1 hack. Runtime on **wasmtime 36**. Remaining II.3.x CDC follow-ups (snapshot, multi-table, Postgres typed columns) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

Replace with:

```markdown
Currently: **Phase II.3.d.2 — Type-aware Postgres CDC columns (complete)** on top of II.3.d.1. Both MySQL and Postgres CDC streaming paths now emit typed Arrow columns (Int32/Int64/Float64/Utf8/Boolean/Date32/Timestamp). Snapshot path for both connectors is still Utf8 (deferred). Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (typed snapshot, MySQL initial snapshot, multi-table) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -10`
Expected: all crates green.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.2 — typed Postgres CDC columns

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
