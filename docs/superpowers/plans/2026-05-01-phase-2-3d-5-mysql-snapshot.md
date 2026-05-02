# Phase II.3.d.5 — MySQL CDC Initial Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an initial snapshot phase to the MySQL CDC connector. Existing rows land in the destination as `_cdc.op = "s"` rows before streaming begins; the GTID is captured before snapshot starts so concurrent updates flow through streaming and reconcile via destination PK merge.

**Architecture:** Per-chunk `START TRANSACTION WITH CONSISTENT SNAPSHOT` with PK-monotonic chunking. Single workflow shape (mirrors Postgres CDC). New `initial_sync` field on `MysqlCdcSourceSpec`, default `SnapshotThenStreaming`. Snapshot reads via `SELECT CAST(col AS CHAR) AS col, …` and parses to typed `ScalarValue`s using a new `parse_mysql_text` helper.

**Tech Stack:** `mysql_async` 0.36 (already a workspace dep), `arrow::array::*Builder` family, `chrono::NaiveDate` / `NaiveDateTime` (already a workspace dep). No new deps.

**Spec:** `docs/superpowers/specs/2026-05-01-phase-2-3d-5-mysql-snapshot-design.md`.

---

## File Map

- **`crates/common-types/src/pipeline_spec.rs`** — Add `MysqlInitialSync` enum + `initial_sync` + `pk_column` fields to `MysqlCdcSourceSpec`.
- **`crates/worker/src/connectors/mysql/cdc/decode.rs`** — Add `parse_mysql_text(s, target_type) -> Result<Option<ScalarValue>>`. Reuses existing `ScalarValue` enum.
- **`crates/worker/src/connectors/mysql/cdc/snapshot.rs`** *(new)* — `read_chunk` opens a connection, runs `START TRANSACTION WITH CONSISTENT SNAPSHOT`, executes the chunked SELECT with `CAST AS CHAR` projection, builds a typed `RecordBatch` via per-column `ArrayBuilder` dispatch.
- **`crates/worker/src/connectors/mysql/cdc/mod.rs`** — Add `pub mod snapshot;`.
- **`crates/worker/src/activities/mysql_cdc/inputs.rs`** — Add `MysqlSnapshotChunkInput` / `MysqlSnapshotChunkOutput`.
- **`crates/worker/src/activities/mysql_cdc/mod.rs`** — Add `mysql_snapshot_chunk` activity.
- **`crates/worker/src/workflows/mysql_cdc_pipeline.rs`** — Conditional snapshot loop between `discover_mysql_schema` and the streaming loop.
- **`tests/integration/tests/mysql_cdc_e2e.rs`** — Modify existing test to opt into `streaming_only`; add new `mysql_cdc_snapshot_then_streaming_e2e` test.
- **`README.md`** — One-line "Currently:" refresh.

---

## Task 1: Pipeline spec extension — `MysqlInitialSync` + `initial_sync` + `pk_column`

**Files:**
- Modify: `crates/common-types/src/pipeline_spec.rs`

- [ ] **Step 1: Write the failing serde tests**

In `crates/common-types/src/pipeline_spec.rs`, append to the existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn mysql_cdc_initial_sync_defaults_to_snapshot_then_streaming() {
        let j = r#"{
            "type": "mysql_cdc", "schema": "shop", "table": "orders", "server_id": 4242
        }"#;
        let s: SourceSpec = serde_json::from_str(j).unwrap();
        if let SourceSpec::MysqlCdc(m) = s {
            assert_eq!(m.initial_sync, MysqlInitialSync::SnapshotThenStreaming);
            assert_eq!(m.pk_column, None);
        } else {
            panic!();
        }
    }

    #[test]
    fn mysql_cdc_streaming_only_parses() {
        let j = r#"{
            "type": "mysql_cdc", "schema": "shop", "table": "orders",
            "server_id": 4242, "initial_sync": "streaming_only"
        }"#;
        let s: SourceSpec = serde_json::from_str(j).unwrap();
        if let SourceSpec::MysqlCdc(m) = s {
            assert_eq!(m.initial_sync, MysqlInitialSync::StreamingOnly);
        } else {
            panic!();
        }
    }

    #[test]
    fn mysql_cdc_with_pk_column() {
        let j = r#"{
            "type": "mysql_cdc", "schema": "shop", "table": "orders",
            "server_id": 4242, "pk_column": "id"
        }"#;
        let s: SourceSpec = serde_json::from_str(j).unwrap();
        if let SourceSpec::MysqlCdc(m) = s {
            assert_eq!(m.pk_column.as_deref(), Some("id"));
        } else {
            panic!();
        }
    }

    #[test]
    fn mysql_cdc_full_roundtrips() {
        let s = SourceSpec::MysqlCdc(MysqlCdcSourceSpec {
            schema: "shop".into(),
            table: "orders".into(),
            server_id: 4242,
            heartbeat_secs: 30,
            initial_sync: MysqlInitialSync::SnapshotThenStreaming,
            pk_column: Some("id".into()),
        });
        let j = serde_json::to_string(&s).unwrap();
        let back: SourceSpec = serde_json::from_str(&j).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), j);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p common-types pipeline_spec::tests::mysql_cdc_initial_sync -- --nocapture`
Expected: FAIL with errors about `MysqlInitialSync` and `initial_sync` / `pk_column` fields not found.

- [ ] **Step 3: Add the enum + extend the struct**

Find the existing `MysqlCdcSourceSpec` definition and replace it. Find:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlCdcSourceSpec {
    /// MySQL "database" (== schema) name.
    pub schema: String,
    /// Single table this pipeline streams. Multi-table is a future phase.
    pub table: String,
    /// Unique server_id for this consumer; MySQL uses it as the binlog
    /// client identity. Pick a value not used by any other replica.
    pub server_id: u32,
    /// Server-side heartbeat interval. 0 leaves MySQL's default in place.
    #[serde(default)]
    pub heartbeat_secs: u32,
}
```

Replace with:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MysqlInitialSync {
    /// Snapshot existing rows first (op="s"), then stream from the
    /// captured GTID. Default — usually what you want.
    #[default]
    SnapshotThenStreaming,
    /// Skip snapshot; only emit changes from the captured GTID forward.
    /// Niche use case ("I only care about future changes").
    StreamingOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlCdcSourceSpec {
    /// MySQL "database" (== schema) name.
    pub schema: String,
    /// Single table this pipeline streams. Multi-table is a future phase.
    pub table: String,
    /// Unique server_id for this consumer; MySQL uses it as the binlog
    /// client identity. Pick a value not used by any other replica.
    pub server_id: u32,
    /// Server-side heartbeat interval. 0 leaves MySQL's default in place.
    #[serde(default)]
    pub heartbeat_secs: u32,
    /// Whether to snapshot existing rows before streaming. Default:
    /// SnapshotThenStreaming.
    #[serde(default)]
    pub initial_sync: MysqlInitialSync,
    /// PK column for snapshot chunking. Required when initial_sync ==
    /// SnapshotThenStreaming. v1 supports integer PKs only.
    #[serde(default)]
    pub pk_column: Option<String>,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p common-types pipeline_spec::tests::mysql_cdc -- --nocapture`
Expected: 4 new tests pass plus the 3 existing `mysql_cdc_*` tests still pass (7 total).

- [ ] **Step 5: Verify nothing downstream broke**

Run: `cargo build --workspace 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 6: Commit**

```bash
git add crates/common-types/src/pipeline_spec.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-5-1: MysqlInitialSync enum + spec fields

Adds MysqlInitialSync::{SnapshotThenStreaming, StreamingOnly} (default
SnapshotThenStreaming). Extends MysqlCdcSourceSpec with initial_sync
and pk_column fields. Both fields default-friendly via serde — existing
test fixtures without these fields parse to SnapshotThenStreaming
(v2 default) with no pk_column set.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `parse_mysql_text` parser in `decode.rs`

**Files:**
- Modify: `crates/worker/src/connectors/mysql/cdc/decode.rs`

- [ ] **Step 1: Write the failing tests**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, append to the existing `#[cfg(test)] mod tests` block:

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::parse_text -- --nocapture`
Expected: FAIL with "cannot find function `parse_mysql_text`".

- [ ] **Step 3: Add the parser**

In `crates/worker/src/connectors/mysql/cdc/decode.rs`, find the existing imports at the top:

```rust
use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;
```

Replace with:

```rust
use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;
```

Append at the end of the file, before the `#[cfg(test)]` block:

```rust
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
        other => {
            return Err(anyhow!(
                "unsupported target DataType for mysql text parse: {:?}",
                other
            ))
        }
    };
    Ok(Some(v))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::parse_text -- --nocapture`
Expected: 8 passes.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/decode.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-5-2: parse_mysql_text — text → ScalarValue parser

Mirrors parse_pg_text from the Postgres CDC types module. Converts
MySQL CAST(col AS CHAR) text values to typed ScalarValues per the
column's Arrow DataType. Reuses the existing ScalarValue enum from
II.3.d.1; no new variants needed.

Used by snapshot.rs (Task 3) to decode chunked SELECT results.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `snapshot.rs` — `read_chunk` with consistent-snapshot transaction

**Files:**
- Create: `crates/worker/src/connectors/mysql/cdc/snapshot.rs`
- Modify: `crates/worker/src/connectors/mysql/cdc/mod.rs`

- [ ] **Step 1: Wire the new module path**

In `crates/worker/src/connectors/mysql/cdc/mod.rs`, find the existing `pub mod` lines and append:

```rust
pub mod snapshot;
```

The full file should now contain:

```rust
pub mod decode;
pub mod position;
pub mod schema;
pub mod stream;
pub mod snapshot;
```

- [ ] **Step 2: Write the failing SQL-composition tests**

Create `crates/worker/src/connectors/mysql/cdc/snapshot.rs` with the test scaffolding only:

```rust
//! Snapshot reader for MySQL CDC.
//!
//! Per-chunk `START TRANSACTION WITH CONSISTENT SNAPSHOT` over a
//! PK-monotonic SELECT. Columns are text-cast (`CAST(col AS CHAR)`)
//! and parsed via `parse_mysql_text` to typed `ScalarValue`s. Builds
//! a typed Arrow `RecordBatch` with `_cdc.op = "s"` per row.

use anyhow::{anyhow, Context, Result};
use arrow::array::{ArrayBuilder, ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::decode::{parse_mysql_text, ScalarValue};

pub struct SnapshotChunk {
    pub batch: Option<RecordBatch>,
    pub rows: usize,
    pub last_pk: Option<i64>,
    pub is_final: bool,
}

/// Compose the SELECT statement for one snapshot chunk. Public for
/// unit-testing the SQL shape; the real query is executed by `read_chunk`.
pub fn build_chunk_sql(
    schema: &str,
    table: &str,
    pk_col: &str,
    data_field_names: &[&str],
    has_last_pk: bool,
    batch_size: usize,
) -> String {
    let projection = data_field_names
        .iter()
        .map(|n| format!("CAST(`{n}` AS CHAR) AS `{n}`"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_sql_with_last_pk() {
        let s = build_chunk_sql(
            "shop",
            "orders",
            "id",
            &["id", "customer", "amount"],
            true,
            500,
        );
        assert!(s.contains("`shop`.`orders`"));
        assert!(s.contains("WHERE `id` > ?"));
        assert!(s.contains("ORDER BY `id`"));
        assert!(s.contains("LIMIT 500"));
        assert!(s.contains("CAST(`id` AS CHAR) AS `id`"));
        assert!(s.contains("CAST(`customer` AS CHAR) AS `customer`"));
    }

    #[test]
    fn build_sql_without_last_pk() {
        let s = build_chunk_sql("shop", "orders", "id", &["id", "amount"], false, 100);
        assert!(!s.contains("WHERE"));
        assert!(s.contains("ORDER BY `id`"));
        assert!(s.contains("LIMIT 100"));
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p worker connectors::mysql::cdc::snapshot::tests -- --nocapture`
Expected: 2 passes (the `build_chunk_sql` function compiles and produces the expected SQL).

- [ ] **Step 4: Add `read_chunk` and the typed-batch builder**

Append to `crates/worker/src/connectors/mysql/cdc/snapshot.rs`, before the `#[cfg(test)]` block:

```rust
#[allow(clippy::too_many_arguments)]
pub async fn read_chunk(
    conn_url: &str,
    schema_name: &str,
    table_name: &str,
    pk_column: &str,
    last_pk: Option<i64>,
    batch_size: usize,
    arrow_schema: SchemaRef,
    captured_gtid: &str,
) -> Result<SnapshotChunk> {
    use mysql_async::prelude::*;

    let pool = mysql_async::Pool::new(conn_url);
    let mut conn = pool.get_conn().await.context("mysql connect")?;

    // Single statement: sets isolation + takes the consistent point.
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .context("BEGIN consistent snapshot")?;

    let n_data = arrow_schema
        .fields()
        .len()
        .checked_sub(3)
        .ok_or_else(|| anyhow!("schema must have at least 3 _cdc.* metadata columns"))?;
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

    let rows: Vec<mysql_async::Row> = match last_pk {
        Some(pk) => conn.exec(&stmt, (pk,)).await.context("snapshot SELECT")?,
        None => conn.query(&stmt).await.context("snapshot SELECT")?,
    };

    conn.query_drop("COMMIT")
        .await
        .context("COMMIT snapshot tx")?;
    drop(conn);
    pool.disconnect().await.ok();

    if rows.is_empty() {
        return Ok(SnapshotChunk {
            batch: None,
            rows: 0,
            last_pk,
            is_final: true,
        });
    }

    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data)
        .map(|i| make_snapshot_builder(arrow_schema.field(i).data_type()))
        .collect::<Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();

    let mut last_pk_seen: Option<i64> = last_pk;

    for row in &rows {
        for i in 0..n_data {
            let f = arrow_schema.field(i);
            let dt = f.data_type();
            // CAST(col AS CHAR) returns String or NULL; we type-extract
            // as Option<String>.
            let raw: Option<String> = row
                .get_opt::<Option<String>, _>(f.name().as_str())
                .ok_or_else(|| anyhow!("column {} not found in row", f.name()))?
                .map_err(|e| anyhow!("column {} extract: {}", f.name(), e))?;
            let parsed = match raw.as_deref() {
                Some(s) => parse_mysql_text(s, dt)?,
                None => None,
            };
            append_snapshot_scalar(&mut *col_builders[i], parsed.as_ref(), dt)?;
        }
        op_b.append_value("s");
        lsn_b.append_value(captured_gtid);
        ts_b.append_null();
        // PK is one of the data columns — re-extract as i64.
        let pk_raw: Option<String> = row
            .get_opt::<Option<String>, _>(pk_column)
            .ok_or_else(|| anyhow!("pk column {} not found", pk_column))?
            .map_err(|e| anyhow!("pk extract: {}", e))?;
        if let Some(pk_s) = pk_raw {
            if let Ok(n) = pk_s.parse::<i64>() {
                last_pk_seen = Some(n);
            }
        }
    }

    let mut cols: Vec<ArrayRef> =
        col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    // _cdc.commit_ts has UTC timezone in the schema; finish-with-timezone
    // matches the schema-declared type.
    cols.push(Arc::new(ts_b.finish().with_timezone("UTC")));

    let batch = RecordBatch::try_new(arrow_schema, cols).context("build snapshot RecordBatch")?;
    let row_count = rows.len();
    Ok(SnapshotChunk {
        batch: Some(batch),
        rows: row_count,
        last_pk: last_pk_seen,
        is_final: row_count < batch_size,
    })
}

fn make_snapshot_builder(
    dt: &arrow::datatypes::DataType,
) -> Result<Box<dyn ArrayBuilder>> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder,
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
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::new();
            if let Some(tz) = tz.as_ref() {
                b = b.with_timezone(Arc::clone(tz));
            }
            Box::new(b)
        }
        other => return Err(anyhow!("no snapshot builder for DataType {:?}", other)),
    })
}

fn append_snapshot_scalar(
    builder: &mut dyn ArrayBuilder,
    scalar: Option<&ScalarValue>,
    dt: &arrow::datatypes::DataType,
) -> Result<()> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_null(),
        (Some(ScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_null(),
        (Some(ScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_null(),
        (Some(ScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_null(),
        (Some(ScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_null(),
        (Some(ScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_null(),
        (Some(ScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_null(),
        (Some(ScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(ScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
                .append_value(*t)
        }
        (Some(other_v), other_dt) => {
            return Err(anyhow!(
                "scalar/builder mismatch: {:?} into {:?}",
                other_v,
                other_dt
            ))
        }
        (None, other_dt) => {
            return Err(anyhow!(
                "no null-append path for builder type {:?}",
                other_dt
            ))
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Verify the build is clean**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test -p worker connectors::mysql::cdc::snapshot -- --nocapture`
Expected: 2 SQL-composition tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/mod.rs crates/worker/src/connectors/mysql/cdc/snapshot.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-5-3: snapshot.rs — read_chunk with consistent snapshot tx

Per-chunk START TRANSACTION WITH CONSISTENT SNAPSHOT, then SELECT
CAST(col AS CHAR) ... WHERE pk > ? ORDER BY pk LIMIT N. Each row is
parsed via parse_mysql_text and appended to a typed Arrow batch with
_cdc.op = "s", _cdc.lsn = captured_gtid, _cdc.commit_ts = NULL.

build_chunk_sql is pub for unit-testing SQL shape; the actual query
runs inside read_chunk with the bind value for last_pk.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `mysql_snapshot_chunk` activity + workflow snapshot loop

**Files:**
- Modify: `crates/worker/src/activities/mysql_cdc/inputs.rs`
- Modify: `crates/worker/src/activities/mysql_cdc/mod.rs`
- Modify: `crates/worker/src/workflows/mysql_cdc_pipeline.rs`

- [ ] **Step 1: Add input/output structs**

In `crates/worker/src/activities/mysql_cdc/inputs.rs`, append after the existing `MysqlReadWindowOutput`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotChunkInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub batch_seq: u32,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
    pub pk_column: String,
    pub last_pk: Option<i64>,
    pub batch_size: u32,
    pub schema_json: String,
    pub captured_gtid: String,
    pub destination: DestinationSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotChunkOutput {
    pub rows: u32,
    pub last_pk: Option<i64>,
    pub is_final: bool,
}
```

- [ ] **Step 2: Add the activity**

In `crates/worker/src/activities/mysql_cdc/mod.rs`, add `use crate::connectors::mysql::cdc::snapshot;` near the existing imports. Then add a new activity inside the `#[activities] impl MysqlCdcActivities` block, right after `read_window`:

```rust
    #[activity]
    pub async fn mysql_snapshot_chunk(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlSnapshotChunkInput,
    ) -> Result<MysqlSnapshotChunkOutput, ActivityError> {
        tracing::info!(
            batch_seq = input.batch_seq,
            schema = %input.schema, table = %input.table,
            pk_column = %input.pk_column, last_pk = ?input.last_pk,
            "mysql_cdc: snapshot_chunk entering"
        );
        let url = resolve_url(
            &self.secrets,
            &input.source_conn,
            input.tenant_id,
            input.principal_id,
            input.jti,
        )
        .await
        .map_err(into_activity_err)?;
        let cols: Vec<InfoSchemaColumn> = serde_json::from_str(&input.schema_json)
            .context("parse schema_json")
            .map_err(into_activity_err)?;
        // Validate pk_column existence + integer type before opening tx.
        let pk_meta = cols
            .iter()
            .find(|c| c.column_name == input.pk_column)
            .ok_or_else(|| {
                into_activity_err(anyhow!(
                    "pk_column '{}' not found in table {}.{}",
                    input.pk_column,
                    input.schema,
                    input.table
                ))
            })?;
        let pk_dt = schema::map_mysql_type(&pk_meta.data_type)
            .map_err(into_activity_err)?;
        if !matches!(
            pk_dt,
            arrow::datatypes::DataType::Int32 | arrow::datatypes::DataType::Int64
        ) {
            return Err(into_activity_err(anyhow!(
                "snapshot only supports integer pk columns in v1; '{}' is {:?}",
                input.pk_column,
                pk_dt
            )));
        }
        let arrow_schema = schema::schema_from_columns(&cols).map_err(into_activity_err)?;
        let arrow_schema = std::sync::Arc::new(arrow_schema);
        let chunk = snapshot::read_chunk(
            &url,
            &input.schema,
            &input.table,
            &input.pk_column,
            input.last_pk,
            input.batch_size as usize,
            arrow_schema,
            &input.captured_gtid,
        )
        .await
        .map_err(into_activity_err)?;
        if let Some(batch) = chunk.batch.as_ref() {
            CdcParquetLoader
                .write(
                    &input.destination,
                    input.tenant_id,
                    input.pipeline_id,
                    input.run_id,
                    input.batch_seq,
                    batch,
                )
                .await
                .map_err(into_activity_err)?;
        }
        Ok(MysqlSnapshotChunkOutput {
            rows: chunk.rows as u32,
            last_pk: chunk.last_pk,
            is_final: chunk.is_final,
        })
    }
```

- [ ] **Step 3: Add the snapshot loop to the workflow**

In `crates/worker/src/workflows/mysql_cdc_pipeline.rs`, find the existing `run_inner` body. The current shape is:

```rust
        let schema_out = ctx
            .start_activity(
                MysqlCdcActivities::discover_mysql_schema,
                DiscoverMysqlSchemaInput { ... },
                opts_short(),
            )
            .await?;

        let mut current_gtid = gtid_out.gtid_set;
        let mut window_seq: u32 = 0;
        let mut batch_seq: u32 = 0;
        loop { ... streaming loop ... }
```

Insert a snapshot loop between the schema discovery and the streaming loop. After the `let schema_out = ctx.start_activity(...)?;` block, before the `let mut current_gtid = ...;` line, add:

```rust
        // Snapshot phase: when initial_sync == SnapshotThenStreaming,
        // chunked SELECT against the source until is_final. The captured
        // GTID was already recorded above (capture_start_gtid runs before
        // discover_schema), so streaming will resume from that point and
        // overlap is reconciled at the destination via PK merge.
        if matches!(
            my.initial_sync,
            common_types::pipeline_spec::MysqlInitialSync::SnapshotThenStreaming
        ) {
            let pk_col = my.pk_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "MysqlCdcSourceSpec.pk_column required for snapshot mode"
                )
            })?;
            let mut snap_seq: u32 = 0;
            let mut last_pk: Option<i64> = None;
            loop {
                let snap_out = ctx
                    .start_activity(
                        MysqlCdcActivities::mysql_snapshot_chunk,
                        MysqlSnapshotChunkInput {
                            pipeline_id: input.pipeline_id,
                            run_id: input.run_id,
                            tenant_id: input.tenant_id,
                            principal_id: input.principal_id,
                            jti: input.jti,
                            batch_seq: snap_seq,
                            source_conn: input.source_conn.clone(),
                            schema: my.schema.clone(),
                            table: my.table.clone(),
                            pk_column: pk_col.clone(),
                            last_pk,
                            batch_size: input.spec.batch_size.max(100) as u32,
                            schema_json: schema_out.schema_json.clone(),
                            captured_gtid: gtid_out.gtid_set.clone(),
                            destination: dest.clone(),
                        },
                        opts_long(),
                    )
                    .await?;
                last_pk = snap_out.last_pk;
                snap_seq += 1;
                if snap_out.is_final {
                    break;
                }
            }
        }

```

(Insert immediately before the existing `let mut current_gtid = gtid_out.gtid_set;` line, leaving the streaming loop unchanged.)

The snapshot's `batch_seq` starts at 0 and increments independently of the streaming `batch_seq`. Streaming continues to start at 0 — both write to different sub-paths via `CdcParquetLoader.write`'s `batch_seq` parameter, but since both are independent activities the file naming convention is `batch-{run_id}-{batch_seq}-…` (existing). If snapshot and streaming `batch_seq` collide on filename, the loader's path includes `run_id`, so collisions only matter within the same run. Per the existing loader convention, snapshot files use snap_seq and streaming files use batch_seq under the same run; since both start at 0, the *streaming* loop should bump its starting `batch_seq` past snapshot's count. Update the existing streaming-loop init to:

```rust
        let mut current_gtid = gtid_out.gtid_set;
        let mut window_seq: u32 = 0;
        let mut batch_seq: u32 = if matches!(
            my.initial_sync,
            common_types::pipeline_spec::MysqlInitialSync::SnapshotThenStreaming
        ) {
            // Streaming files start after the highest snapshot batch_seq.
            // Use a high offset so snapshot+streaming don't share filenames.
            10_000
        } else {
            0
        };
```

(The 10_000 offset is a v1 heuristic; the snapshot loop above won't realistically produce >10k batches for any reasonable table. v2 can replace this with a sequence number reservation system.)

Add the `MysqlSnapshotChunkInput` import at the top of `mysql_cdc_pipeline.rs`:

```rust
use crate::activities::mysql_cdc::inputs::*;
```

(This is likely already imported; verify and skip if so.)

- [ ] **Step 4: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/mysql_cdc crates/worker/src/workflows/mysql_cdc_pipeline.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-5-4: mysql_snapshot_chunk activity + workflow snapshot loop

Adds the mysql_snapshot_chunk activity (validates pk_column existence
and integer type before opening the snapshot transaction). The
MysqlCdcPipelineWorkflow runs a chunked snapshot loop between
discover_schema and the streaming loop when initial_sync ==
SnapshotThenStreaming.

The streaming loop's batch_seq starts at 10_000 to avoid filename
collisions with snapshot's batch_seq under the same run; v2 can
replace with a real sequence reservation.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: E2E — modify existing test + add snapshot+streaming test

**Files:**
- Modify: `tests/integration/tests/mysql_cdc_e2e.rs`

- [ ] **Step 1: Modify the existing test to opt into `streaming_only`**

In `tests/integration/tests/mysql_cdc_e2e.rs`, find the existing pipeline spec JSON in `mysql_cdc_streaming_only_e2e`:

```rust
    let spec = json!({
        "source": {
            "type": "mysql_cdc",
            "schema": "test",
            "table": "customers",
            "server_id": 4242,
            "heartbeat_secs": 0
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 100
    });
```

Replace with (adding `"initial_sync": "streaming_only"`):

```rust
    let spec = json!({
        "source": {
            "type": "mysql_cdc",
            "schema": "test",
            "table": "customers",
            "server_id": 4242,
            "heartbeat_secs": 0,
            "initial_sync": "streaming_only"
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 100
    });
```

- [ ] **Step 2: Add the new snapshot-then-streaming test**

Append at the end of `tests/integration/tests/mysql_cdc_e2e.rs`:

```rust
async fn seed_three_existing_rows(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "INSERT INTO customers (id, email, name, created) VALUES \
         (1, 'a@x.com', 'Alice',   '2026-01-01 00:00:00'), \
         (2, 'b@x.com', 'Bob',     '2026-01-01 00:00:01'), \
         (3, 'c@x.com', 'Carol',   '2026-01-01 00:00:02')",
    )
    .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

async fn perform_post_snapshot_iud(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "INSERT INTO customers (id, email, name, created) \
         VALUES (4, 'd@x.com', 'Dave', '2026-01-02 00:00:00')",
    )
    .await?;
    conn.query_drop("UPDATE customers SET email='bob@x.com' WHERE id=2")
        .await?;
    conn.query_drop("DELETE FROM customers WHERE id=1").await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker + temporal stack; ~120s"]
async fn mysql_cdc_snapshot_then_streaming_e2e() -> anyhow::Result<()> {
    build_workspace().await?;

    let (_container, mysql_url) = start_mysql_container().await?;
    seed_table(&mysql_url).await?;
    seed_three_existing_rows(&mysql_url).await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "mysql-snapshot".into(),
            connector_ref: "mysql_cdc@0.1.0".into(),
            config: json!({ "url": mysql_url }),
        })
        .await?;

    let tmp_data = tempfile::tempdir()?;
    let spec = json!({
        "source": {
            "type": "mysql_cdc",
            "schema": "test",
            "table": "customers",
            "server_id": 4243,
            "heartbeat_secs": 0,
            "initial_sync": "snapshot_then_streaming",
            "pk_column": "id"
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 100
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "mysql-customers-snapshot".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut worker = spawn_worker().await?;

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CDC_MAX_WINDOWS", "8")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "pipeline run kickoff failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for snapshot to land 3 rows in Parquet.
    let snap_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() > snap_deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for 3 snapshot rows");
        }
        let ops = read_parquet_ops(tmp_data.path());
        let s_count = ops.iter().filter(|o| *o == "s").count();
        if s_count >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Now perform IUD; streaming picks them up.
    perform_post_snapshot_iud(&mysql_url).await?;

    let total_deadline = Instant::now() + Duration::from_secs(120);
    let mut final_ops: Vec<String> = Vec::new();
    loop {
        if Instant::now() > total_deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for streaming ops; saw: {final_ops:?}");
        }
        final_ops = read_parquet_ops(tmp_data.path());
        let s = final_ops.iter().filter(|o| *o == "s").count();
        let i = final_ops.iter().filter(|o| *o == "i").count();
        let u = final_ops.iter().filter(|o| *o == "u").count();
        let d = final_ops.iter().filter(|o| *o == "d").count();
        if s >= 3 && i >= 1 && u >= 1 && d >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    worker.kill().await?;
    worker.wait().await?;

    let s_count = final_ops.iter().filter(|o| *o == "s").count();
    let i_count = final_ops.iter().filter(|o| *o == "i").count();
    let u_count = final_ops.iter().filter(|o| *o == "u").count();
    let d_count = final_ops.iter().filter(|o| *o == "d").count();
    eprintln!(
        "ops: s={s_count} i={i_count} u={u_count} d={d_count}; total {}",
        final_ops.len()
    );
    assert!(s_count >= 3, "expected ≥3 snapshot rows, got {s_count}");
    assert!(i_count >= 1, "expected ≥1 INSERT, got {i_count}");
    assert!(u_count >= 1, "expected ≥1 UPDATE, got {u_count}");
    assert!(d_count >= 1, "expected ≥1 DELETE, got {d_count}");

    // Verify the typed Parquet schema persists across snapshot+streaming.
    let parquet_schema =
        read_first_parquet_schema(tmp_data.path()).expect("at least one parquet file");
    let id_field = parquet_schema.field_with_name("id").unwrap();
    assert_eq!(
        id_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "id should be Int64, got {:?}",
        id_field.data_type()
    );

    Ok(())
}
```

- [ ] **Step 3: Verify the test compiles**

Run: `cargo build --workspace --tests 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 4: Run both e2e tests against the live stack**

Prerequisite: docker stack up + DOCKER_HOST set:

```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
```

Then:

```bash
DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock \
  cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture 2>&1 | tail -20
```

Expected: both tests pass — `mysql_cdc_streaming_only_e2e` (existing, opted into streaming_only mode) and `mysql_cdc_snapshot_then_streaming_e2e` (new, full snapshot + IUD round-trip).

If snapshot_then_streaming fails with `"snapshot mode requires pk_column"`, the spec JSON is missing the `"pk_column": "id"` field — verify the JSON shape.

If snapshot lands 0 's' rows, the SELECT projection or transaction might be wrong — re-check `build_chunk_sql` against the actual MySQL accepting that syntax (8.x supports `CAST AS CHAR` cleanly).

If streaming doesn't pick up post-snapshot writes, the `captured_gtid` might be too late (captured after snapshot started) — re-check the workflow ordering: `capture_start_gtid` MUST run before any snapshot chunk.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/tests/mysql_cdc_e2e.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-5-5: e2e — modify streaming_only test + add snapshot+streaming

mysql_cdc_streaming_only_e2e now passes "initial_sync": "streaming_only"
explicitly (default flipped to SnapshotThenStreaming in Task 1).

mysql_cdc_snapshot_then_streaming_e2e is the new round-trip:
seed 3 rows, snapshot lands them as op="s", post-snapshot IUD flows
through streaming as i/u/d. Final assertion: 3 snapshot + 1 each i/u/d
+ typed Parquet schema (id: Int64).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find:

```markdown
Currently: **Phase II.3.d.4 — Postgres CDC OID coverage: BYTEA + TIME (complete)** on top of II.3.d.3. BYTEA columns now land as Arrow `Binary`; TIME as `Time64(Microsecond)`. Postgres CDC is fully type-aware end-to-end including binary blobs and time-of-day. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (MySQL initial snapshot, multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

Replace with:

```markdown
Currently: **Phase II.3.d.5 — MySQL CDC initial snapshot (complete)** on top of II.3.d.4. MySQL CDC now snapshots existing rows (op="s") before streaming, with the GTID captured upfront so concurrent updates flow through streaming and reconcile via destination PK merge. Default mode is `snapshot_then_streaming`; skip-snapshot is opt-in via `initial_sync = streaming_only`. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.5 — MySQL CDC initial snapshot

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
