# Phase 2-5a: Snowflake Destination Loader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `SnowflakeLoader` implementing `loader_sdk::DestinationLoader` — append + PK-upsert delivery to Snowflake over the SQL API v2 with key-pair JWT auth and log-table idempotency, dispatched from `load_batch`.

**Architecture:** Module dir `crates/worker/src/loaders/snowflake/` split into `sql.rs` (pure SQL builders — unit-tested), `jwt.rs` (key-pair JWT), `client.rs` (SQL API v2 HTTP client, wiremock-testable via `ETL_SNOWFLAKE_BASE_URL` override), `mod.rs` (loader orchestration). Design memo: `docs/superpowers/specs/2026-07-23-phase-2-5a-snowflake-loader-design.md`.

**Tech Stack:** Existing workspace deps only: reqwest (rustls), jsonwebtoken 10, rsa 0.9, sha2, base64, arrow 53, wiremock 0.6 (new dev-dep on worker).

## Global Constraints

- Rust 1.88, edition 2021. `cargo fmt` + `cargo clippy --workspace` clean before each commit.
- No new external dependencies beyond promoting existing workspace deps into `crates/worker`.
- Secrets never enter the catalog: the spec carries `private_key_env` (an env-var *name*), never key material.
- All identifiers double-quoted, case preserved; reject `"` / NUL / control chars (mirror `resolve_target_table` in `loaders/postgres.rs`).
- Same-`LoadId` retry MUST be a no-op (idempotency log check).
- Commit style: `phase-2-5a-N: <what>` matching repo history.

---

### Task 1: `SnowflakeDestinationSpec` in common-types

**Files:**
- Modify: `crates/common-types/src/pipeline_spec.rs`

**Interfaces:**
- Produces: `DestinationSpec::Snowflake(SnowflakeDestinationSpec)`; struct fields `account, user, private_key_env, warehouse, database, schema, table, role: Option<String>, pk_columns: Vec<String>` — later tasks match on these exact names.

- [ ] **Step 1: Write the failing tests** (append to the `tests` mod in `pipeline_spec.rs`):

```rust
#[test]
fn snowflake_destination_spec_roundtrip() {
    let s = DestinationSpec::Snowflake(SnowflakeDestinationSpec {
        account: "xy12345".into(),
        user: "ETL_LOADER".into(),
        private_key_env: "SNOWFLAKE_PRIVATE_KEY".into(),
        warehouse: "ETL_WH".into(),
        database: "ANALYTICS".into(),
        schema: "RAW".into(),
        table: "customers".into(),
        role: Some("ETL_ROLE".into()),
        pk_columns: vec!["id".into()],
    });
    let j = serde_json::to_string(&s).unwrap();
    assert!(j.contains("\"type\":\"snowflake\""));
    let back: DestinationSpec = serde_json::from_str(&j).unwrap();
    assert!(matches!(back, DestinationSpec::Snowflake(_)));
}

#[test]
fn snowflake_destination_spec_defaults() {
    let j = r#"{"type":"snowflake","account":"xy12345","user":"U",
        "private_key_env":"K","warehouse":"W","database":"D",
        "schema":"S","table":"T"}"#;
    let s: DestinationSpec = serde_json::from_str(j).unwrap();
    if let DestinationSpec::Snowflake(sf) = s {
        assert!(sf.pk_columns.is_empty());
        assert!(sf.role.is_none());
    } else {
        panic!("expected snowflake variant");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p common-types snowflake_destination -- --nocapture`
Expected: FAIL — `SnowflakeDestinationSpec` not found.

- [ ] **Step 3: Implement** — add the variant and struct next to `PostgresDestinationSpec`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DestinationSpec {
    LocalParquet(LocalParquetSpec),
    Postgres(PostgresDestinationSpec),
    Snowflake(SnowflakeDestinationSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnowflakeDestinationSpec {
    /// Account identifier (locator like `xy12345` or `org-account`).
    pub account: String,
    pub user: String,
    /// Name of the env var holding the PKCS#8 PEM private key for
    /// key-pair auth. Never key material itself (invariant #6).
    pub private_key_env: String,
    pub warehouse: String,
    pub database: String,
    /// Snowflake schema owning the target table + idempotency log.
    pub schema: String,
    /// Target table. Created on first non-empty load if absent.
    pub table: String,
    #[serde(default)]
    pub role: Option<String>,
    /// Upsert key columns. Empty ⇒ append (INSERT); non-empty ⇒ MERGE.
    #[serde(default)]
    pub pk_columns: Vec<String>,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p common-types`
Expected: PASS (all existing + 2 new).

- [ ] **Step 5: Commit**

```bash
git add crates/common-types/src/pipeline_spec.rs
git commit -m "phase-2-5a-1: SnowflakeDestinationSpec + DestinationSpec::Snowflake"
```

---

### Task 2: SQL builders — identifiers, types, literals (`sql.rs`)

**Files:**
- Create: `crates/worker/src/loaders/snowflake/sql.rs`
- Create: `crates/worker/src/loaders/snowflake/mod.rs` (module shell only: `pub mod sql;`)
- Modify: `crates/worker/src/loaders/mod.rs` (add `pub mod snowflake;`)

**Interfaces:**
- Produces (used by Tasks 3–4 and the loader):
  - `pub fn quote_ident(name: &str) -> anyhow::Result<String>` — `"name"` or error
  - `pub fn snowflake_type_for(dt: &DataType) -> anyhow::Result<&'static str>`
  - `pub fn sql_literal(col: &dyn arrow::array::Array, row: usize) -> anyhow::Result<String>`

- [ ] **Step 1: Write the failing tests** (bottom of new `sql.rs`, `#[cfg(test)] mod tests`):

```rust
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

#[test]
fn idents_quoted_and_validated() {
    assert_eq!(quote_ident("customers").unwrap(), "\"customers\"");
    assert_eq!(quote_ident("UPPER_case").unwrap(), "\"UPPER_case\"");
    assert!(quote_ident("bad\"quote").is_err());
    assert!(quote_ident("bad\nnewline").is_err());
    assert!(quote_ident("").is_err());
}

#[test]
fn type_mapping_matches_design_table() {
    assert_eq!(snowflake_type_for(&DataType::Int64).unwrap(), "BIGINT");
    assert_eq!(snowflake_type_for(&DataType::Utf8).unwrap(), "TEXT");
    assert_eq!(snowflake_type_for(&DataType::Boolean).unwrap(), "BOOLEAN");
    assert_eq!(snowflake_type_for(&DataType::Float64).unwrap(), "DOUBLE");
    assert_eq!(
        snowflake_type_for(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))).unwrap(),
        "TIMESTAMP_TZ"
    );
    assert_eq!(
        snowflake_type_for(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
        "TIMESTAMP_NTZ"
    );
    assert_eq!(snowflake_type_for(&DataType::Date32).unwrap(), "DATE");
    assert_eq!(snowflake_type_for(&DataType::Binary).unwrap(), "BINARY");
    assert!(snowflake_type_for(&DataType::Duration(TimeUnit::Second)).is_err());
}

#[test]
fn literals_render_and_escape() {
    let strs: ArrayRef = Arc::new(StringArray::from(vec![Some("O'Brien"), None]));
    assert_eq!(sql_literal(strs.as_ref(), 0).unwrap(), "'O''Brien'");
    assert_eq!(sql_literal(strs.as_ref(), 1).unwrap(), "NULL");

    let ints: ArrayRef = Arc::new(Int64Array::from(vec![42]));
    assert_eq!(sql_literal(ints.as_ref(), 0).unwrap(), "42");

    let bools: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(sql_literal(bools.as_ref(), 0).unwrap(), "TRUE");

    let bins: ArrayRef = Arc::new(BinaryArray::from(vec![&[0xDEu8, 0xAD][..]]));
    assert_eq!(sql_literal(bins.as_ref(), 0).unwrap(), "TO_BINARY('dead', 'HEX')");

    let ts: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![1_650_000_000_000_000i64]).with_timezone("UTC"),
    );
    let lit = sql_literal(ts.as_ref(), 0).unwrap();
    assert!(lit.ends_with("::TIMESTAMP_TZ"), "got {lit}");
    assert!(lit.starts_with("'2022-04-15T"), "got {lit}");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p worker snowflake::sql -- --nocapture`
Expected: FAIL to compile — functions missing.

- [ ] **Step 3: Implement** `sql.rs` core:

```rust
//! Pure SQL text builders for the Snowflake loader. No I/O — everything
//! here is unit-testable. Literals are rendered (no bind params) because
//! the SQL API's binding model doesn't do multi-row inserts; batches are
//! dogfood-sized (see design memo §5).

use anyhow::{bail, Context};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};

pub fn quote_ident(name: &str) -> anyhow::Result<String> {
    if name.is_empty() {
        bail!("empty identifier");
    }
    if name.chars().any(|c| c == '"' || c == '\0' || c.is_control()) {
        bail!("invalid character in identifier {name:?}");
    }
    Ok(format!("\"{name}\""))
}

pub fn snowflake_type_for(dt: &DataType) -> anyhow::Result<&'static str> {
    Ok(match dt {
        DataType::Int64 => "BIGINT",
        DataType::Int32 => "INTEGER",
        DataType::Int16 => "SMALLINT",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Boolean => "BOOLEAN",
        DataType::Float64 => "DOUBLE",
        DataType::Float32 => "FLOAT",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMP_TZ",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP_NTZ",
        DataType::Date32 => "DATE",
        DataType::Binary | DataType::LargeBinary => "BINARY",
        DataType::Time64(TimeUnit::Microsecond) => "TIME",
        other => bail!("unsupported Arrow type for Snowflake: {other:?}"),
    })
}

fn escape_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn sql_literal(col: &dyn Array, row: usize) -> anyhow::Result<String> {
    if col.is_null(row) {
        return Ok("NULL".into());
    }
    let any = col.as_any();
    Ok(match col.data_type() {
        DataType::Int64 => any.downcast_ref::<Int64Array>().unwrap().value(row).to_string(),
        DataType::Int32 => any.downcast_ref::<Int32Array>().unwrap().value(row).to_string(),
        DataType::Int16 => any.downcast_ref::<Int16Array>().unwrap().value(row).to_string(),
        DataType::Boolean => {
            if any.downcast_ref::<BooleanArray>().unwrap().value(row) { "TRUE".into() } else { "FALSE".into() }
        }
        DataType::Float64 => {
            let v = any.downcast_ref::<Float64Array>().unwrap().value(row);
            if v.is_finite() { v.to_string() } else { format!("'{v}'::DOUBLE") }
        }
        DataType::Float32 => {
            let v = any.downcast_ref::<Float32Array>().unwrap().value(row);
            if v.is_finite() { v.to_string() } else { format!("'{v}'::FLOAT") }
        }
        DataType::Utf8 => escape_str(any.downcast_ref::<StringArray>().unwrap().value(row)),
        DataType::LargeUtf8 => escape_str(any.downcast_ref::<LargeStringArray>().unwrap().value(row)),
        DataType::Binary => {
            let v = any.downcast_ref::<BinaryArray>().unwrap().value(row);
            format!("TO_BINARY('{}', 'HEX')", hex::encode(v))
        }
        DataType::LargeBinary => {
            let v = any.downcast_ref::<LargeBinaryArray>().unwrap().value(row);
            format!("TO_BINARY('{}', 'HEX')", hex::encode(v))
        }
        DataType::Date32 => {
            let days = any.downcast_ref::<Date32Array>().unwrap().value(row);
            let d = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                + Duration::days(days as i64);
            format!("'{d}'::DATE")
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let us = any.downcast_ref::<Time64MicrosecondArray>().unwrap().value(row);
            let t = NaiveTime::from_num_seconds_from_midnight_opt(
                (us / 1_000_000) as u32,
                ((us % 1_000_000) * 1_000) as u32,
            ).context("time out of range")?;
            format!("'{}'::TIME", t.format("%H:%M:%S%.6f"))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let us = any.downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row);
            let dt: DateTime<Utc> = DateTime::from_timestamp_micros(us)
                .context("timestamp out of range")?;
            let iso = dt.format("%Y-%m-%dT%H:%M:%S%.6fZ");
            if tz.is_some() {
                format!("'{iso}'::TIMESTAMP_TZ")
            } else {
                format!("'{iso}'::TIMESTAMP_NTZ")
            }
        }
        other => bail!("unsupported Arrow type for Snowflake literal: {other:?}"),
    })
}
```

Note: if `hex` is not already a worker dependency, use the existing pattern from the PG loader/examples (`sha2`-adjacent crates); otherwise inline a 4-line `fn hex_encode(bytes: &[u8]) -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() }` — do NOT add a new crate just for hex.

Create `crates/worker/src/loaders/snowflake/mod.rs` containing just `pub mod sql;` and add `pub mod snowflake;` to `crates/worker/src/loaders/mod.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p worker snowflake::sql`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/snowflake/ crates/worker/src/loaders/mod.rs
git commit -m "phase-2-5a-2: snowflake sql.rs — idents, type map, literal rendering"
```

---

### Task 3: SQL builders — DDL + idempotency-log statements

**Files:**
- Modify: `crates/worker/src/loaders/snowflake/sql.rs`

**Interfaces:**
- Consumes: `quote_ident`, `snowflake_type_for` (Task 2); `loader_sdk::LoadId`.
- Produces:
  - `pub fn qualified(db: &str, schema: &str, table: &str) -> anyhow::Result<String>` → `"DB"."SCH"."TBL"`
  - `pub fn ensure_log_table_ddl(db: &str, schema: &str) -> anyhow::Result<String>`
  - `pub fn ensure_target_table_ddl(db: &str, schema: &str, table: &str, arrow_schema: &arrow::datatypes::Schema) -> anyhow::Result<String>`
  - `pub fn check_loaded_sql(db: &str, schema: &str, id: &LoadId) -> anyhow::Result<String>`
  - `pub fn log_insert_sql(db: &str, schema: &str, id: &LoadId, rows: usize) -> anyhow::Result<String>`
  - `pub const LOG_TABLE: &str = "_ETL_LOADED_BATCHES";`

- [ ] **Step 1: Write the failing tests:**

```rust
fn load_id() -> loader_sdk::LoadId {
    loader_sdk::LoadId {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::nil()),
        pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(uuid::Uuid::nil()),
        run_id: common_types::ids::RunId::from_uuid_unchecked(uuid::Uuid::nil()),
        batch_seq: 7,
        stream_name: "public.items".into(),
    }
}

#[test]
fn log_table_ddl_shape() {
    let ddl = ensure_log_table_ddl("DB", "RAW").unwrap();
    assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS \"DB\".\"RAW\".\"_ETL_LOADED_BATCHES\""));
    assert!(ddl.contains("BATCH_SEQ"));
}

#[test]
fn target_ddl_from_arrow_schema() {
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("name", DataType::Utf8, true),
    ]);
    let ddl = ensure_target_table_ddl("DB", "RAW", "customers", &schema).unwrap();
    assert_eq!(
        ddl,
        "CREATE TABLE IF NOT EXISTS \"DB\".\"RAW\".\"customers\" (\"id\" BIGINT, \"name\" TEXT)"
    );
}

#[test]
fn check_and_log_sql_carry_full_load_id() {
    let id = load_id();
    let check = check_loaded_sql("DB", "RAW", &id).unwrap();
    assert!(check.contains("BATCH_SEQ = 7"));
    assert!(check.contains("STREAM_NAME = 'public.items'"));
    let ins = log_insert_sql("DB", "RAW", &id, 42).unwrap();
    assert!(ins.contains("42"));
    assert!(ins.contains(&uuid::Uuid::nil().to_string()));
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p worker snowflake::sql` → compile FAIL.

- [ ] **Step 3: Implement:**

```rust
pub const LOG_TABLE: &str = "_ETL_LOADED_BATCHES";

pub fn qualified(db: &str, schema: &str, table: &str) -> anyhow::Result<String> {
    Ok(format!("{}.{}.{}", quote_ident(db)?, quote_ident(schema)?, quote_ident(table)?))
}

pub fn ensure_log_table_ddl(db: &str, schema: &str) -> anyhow::Result<String> {
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         TENANT_ID TEXT NOT NULL, PIPELINE_ID TEXT NOT NULL, \
         RUN_ID TEXT NOT NULL, STREAM_NAME TEXT NOT NULL, \
         BATCH_SEQ BIGINT NOT NULL, ROWS_LOADED BIGINT NOT NULL, \
         LOADED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), \
         PRIMARY KEY (TENANT_ID, PIPELINE_ID, RUN_ID, STREAM_NAME, BATCH_SEQ))",
        qualified(db, schema, LOG_TABLE)?
    ))
}

pub fn ensure_target_table_ddl(
    db: &str,
    schema: &str,
    table: &str,
    arrow_schema: &arrow::datatypes::Schema,
) -> anyhow::Result<String> {
    let cols: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| Ok(format!("{} {}", quote_ident(f.name())?, snowflake_type_for(f.data_type())?)))
        .collect::<anyhow::Result<_>>()?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified(db, schema, table)?,
        cols.join(", ")
    ))
}

pub fn check_loaded_sql(db: &str, schema: &str, id: &loader_sdk::LoadId) -> anyhow::Result<String> {
    Ok(format!(
        "SELECT ROWS_LOADED FROM {} WHERE TENANT_ID = '{}' AND PIPELINE_ID = '{}' \
         AND RUN_ID = '{}' AND STREAM_NAME = {} AND BATCH_SEQ = {}",
        qualified(db, schema, LOG_TABLE)?,
        id.tenant_id.as_uuid(),
        id.pipeline_id.as_uuid(),
        id.run_id.as_uuid(),
        escape_str(&id.stream_name),
        id.batch_seq,
    ))
}

pub fn log_insert_sql(
    db: &str,
    schema: &str,
    id: &loader_sdk::LoadId,
    rows: usize,
) -> anyhow::Result<String> {
    Ok(format!(
        "INSERT INTO {} (TENANT_ID, PIPELINE_ID, RUN_ID, STREAM_NAME, BATCH_SEQ, ROWS_LOADED) \
         VALUES ('{}', '{}', '{}', {}, {}, {})",
        qualified(db, schema, LOG_TABLE)?,
        id.tenant_id.as_uuid(),
        id.pipeline_id.as_uuid(),
        id.run_id.as_uuid(),
        escape_str(&id.stream_name),
        id.batch_seq,
        rows,
    ))
}
```

(`worker` already depends on `loader-sdk`; UUIDs render via `as_uuid()` — check `ids.rs` for the accessor name and adjust if it differs.)

- [ ] **Step 4: Run tests** — `cargo test -p worker snowflake::sql` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/snowflake/sql.rs
git commit -m "phase-2-5a-3: snowflake DDL + idempotency-log SQL builders"
```

---

### Task 4: SQL builders — append INSERT + upsert MERGE

**Files:**
- Modify: `crates/worker/src/loaders/snowflake/sql.rs`

**Interfaces:**
- Consumes: Task 2–3 helpers.
- Produces:
  - `pub fn append_insert_sql(db: &str, schema: &str, table: &str, batch: &RecordBatch) -> anyhow::Result<String>`
  - `pub fn merge_upsert_sql(db: &str, schema: &str, table: &str, batch: &RecordBatch, pk_columns: &[String]) -> anyhow::Result<String>` — errors if a pk column is missing from the batch or `pk_columns` covers every column with none left to update (that case emits `WHEN NOT MATCHED` only).

- [ ] **Step 1: Write the failing tests:**

```rust
fn two_row_batch() -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("name", DataType::Utf8, true),
    ]));
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        ],
    )
    .unwrap()
}

#[test]
fn append_insert_renders_all_rows() {
    let sql = append_insert_sql("DB", "RAW", "t", &two_row_batch()).unwrap();
    assert_eq!(
        sql,
        "INSERT INTO \"DB\".\"RAW\".\"t\" (\"id\", \"name\") VALUES (1, 'a'), (2, NULL)"
    );
}

#[test]
fn merge_upsert_shape() {
    let sql = merge_upsert_sql("DB", "RAW", "t", &two_row_batch(), &["id".into()]).unwrap();
    assert!(sql.starts_with("MERGE INTO \"DB\".\"RAW\".\"t\" AS t USING (SELECT * FROM VALUES (1, 'a'), (2, NULL) AS v(\"id\", \"name\")) AS s ON t.\"id\" = s.\"id\""), "got: {sql}");
    assert!(sql.contains("WHEN MATCHED THEN UPDATE SET t.\"name\" = s.\"name\""));
    assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (\"id\", \"name\") VALUES (s.\"id\", s.\"name\")"));
}

#[test]
fn merge_all_pk_columns_skips_update_clause() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let sql = merge_upsert_sql("DB", "RAW", "t", &batch, &["id".into()]).unwrap();
    assert!(!sql.contains("WHEN MATCHED"));
    assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
}

#[test]
fn merge_missing_pk_errors() {
    assert!(merge_upsert_sql("DB", "RAW", "t", &two_row_batch(), &["nope".into()]).is_err());
}
```

- [ ] **Step 2: Run to verify failure** — compile FAIL.

- [ ] **Step 3: Implement:**

```rust
fn rendered_rows(batch: &RecordBatch) -> anyhow::Result<Vec<String>> {
    (0..batch.num_rows())
        .map(|row| {
            let vals: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| sql_literal(c.as_ref(), row))
                .collect::<anyhow::Result<_>>()?;
            Ok(format!("({})", vals.join(", ")))
        })
        .collect()
}

fn quoted_columns(batch: &RecordBatch) -> anyhow::Result<Vec<String>> {
    batch.schema().fields().iter().map(|f| quote_ident(f.name())).collect()
}

pub fn append_insert_sql(
    db: &str, schema: &str, table: &str, batch: &RecordBatch,
) -> anyhow::Result<String> {
    let cols = quoted_columns(batch)?;
    let rows = rendered_rows(batch)?;
    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        qualified(db, schema, table)?, cols.join(", "), rows.join(", ")
    ))
}

pub fn merge_upsert_sql(
    db: &str, schema: &str, table: &str, batch: &RecordBatch, pk_columns: &[String],
) -> anyhow::Result<String> {
    let names: Vec<&str> = batch.schema().fields().iter().map(|f| f.name().as_str())
        .collect::<Vec<_>>();
    for pk in pk_columns {
        anyhow::ensure!(names.contains(&pk.as_str()), "pk column {pk:?} not in batch");
    }
    let cols = quoted_columns(batch)?;
    let rows = rendered_rows(batch)?;
    let on: Vec<String> = pk_columns.iter()
        .map(|pk| Ok(format!("t.{q} = s.{q}", q = quote_ident(pk)?)))
        .collect::<anyhow::Result<_>>()?;
    let non_pk: Vec<String> = batch.schema().fields().iter()
        .filter(|f| !pk_columns.iter().any(|pk| pk == f.name()))
        .map(|f| Ok(format!("t.{q} = s.{q}", q = quote_ident(f.name())?)))
        .collect::<anyhow::Result<_>>()?;
    let insert_vals: Vec<String> = cols.iter().map(|c| format!("s.{c}")).collect();
    let matched = if non_pk.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {}", non_pk.join(", "))
    };
    Ok(format!(
        "MERGE INTO {} AS t USING (SELECT * FROM VALUES {} AS v({})) AS s ON {}{} \
         WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        qualified(db, schema, table)?,
        rows.join(", "),
        cols.join(", "),
        on.join(" AND "),
        matched,
        cols.join(", "),
        insert_vals.join(", "),
    ))
}
```

- [ ] **Step 4: Run tests** — `cargo test -p worker snowflake::sql` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/snowflake/sql.rs
git commit -m "phase-2-5a-4: snowflake append INSERT + upsert MERGE builders"
```

---

### Task 5: Key-pair JWT (`jwt.rs`)

**Files:**
- Create: `crates/worker/src/loaders/snowflake/jwt.rs` (+ `pub mod jwt;` in snowflake/mod.rs)
- Modify: `crates/worker/Cargo.toml` — add `jsonwebtoken = { workspace = true }`, `rsa = { workspace = true }`, `sha2 = { workspace = true }` if not present.

**Interfaces:**
- Produces:
  - `pub fn public_key_fingerprint(private_key_pem: &str) -> anyhow::Result<String>` → `"SHA256:<base64-std>"`
  - `pub struct KeyPairAuth; impl KeyPairAuth { pub fn from_pem(account: &str, user: &str, pem: &str) -> anyhow::Result<Self>; pub fn token(&self) -> anyhow::Result<String>; }`

- [ ] **Step 1: Write the failing tests** (generate a throwaway key with `rsa` in the test — mirrors `crates/auth/src/keystore.rs`'s generation code):

```rust
fn test_pem() -> String {
    use rsa::pkcs8::EncodePrivateKey;
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
    key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap().to_string()
}

#[test]
fn fingerprint_is_stable_sha256() {
    let pem = test_pem();
    let fp1 = public_key_fingerprint(&pem).unwrap();
    let fp2 = public_key_fingerprint(&pem).unwrap();
    assert_eq!(fp1, fp2);
    assert!(fp1.starts_with("SHA256:"));
}

#[test]
fn token_claims_shape() {
    let pem = test_pem();
    let auth = KeyPairAuth::from_pem("xy12345", "loader_user", &pem).unwrap();
    let tok = auth.token().unwrap();
    // decode without verification to inspect claims
    let payload_b64 = tok.split('.').nth(1).unwrap();
    use base64::Engine;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(v["sub"], "XY12345.LOADER_USER");
    assert!(v["iss"].as_str().unwrap().starts_with("XY12345.LOADER_USER.SHA256:"));
    assert!(v["exp"].as_i64().unwrap() > v["iat"].as_i64().unwrap());
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p worker snowflake::jwt` → compile FAIL.

- [ ] **Step 3: Implement:**

```rust
//! Snowflake key-pair JWT (SQL API v2, KEYPAIR_JWT token type).

use anyhow::Context;
use base64::Engine;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use sha2::{Digest, Sha256};

pub fn public_key_fingerprint(private_key_pem: &str) -> anyhow::Result<String> {
    let key = rsa::RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .context("parsing PKCS#8 private key")?;
    let der = key.to_public_key().to_public_key_der()
        .context("encoding public key DER")?;
    let hash = Sha256::digest(der.as_bytes());
    Ok(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD.encode(hash)
    ))
}

#[derive(serde::Serialize)]
struct Claims { iss: String, sub: String, iat: i64, exp: i64 }

pub struct KeyPairAuth {
    issuer: String,
    subject: String,
    key: jsonwebtoken::EncodingKey,
}

impl KeyPairAuth {
    pub fn from_pem(account: &str, user: &str, pem: &str) -> anyhow::Result<Self> {
        // Account identifier: uppercase, strip any region suffix after '.'.
        let account = account.split('.').next().unwrap_or(account).to_uppercase();
        let user = user.to_uppercase();
        let fp = public_key_fingerprint(pem)?;
        Ok(Self {
            issuer: format!("{account}.{user}.{fp}"),
            subject: format!("{account}.{user}"),
            key: jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
                .context("loading RSA key for JWT signing")?,
        })
    }

    pub fn token(&self) -> anyhow::Result<String> {
        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            iss: self.issuer.clone(),
            sub: self.subject.clone(),
            iat: now,
            exp: now + 55 * 60,
        };
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &self.key,
        )
        .context("signing snowflake JWT")
    }
}
```

(jsonwebtoken 10 API — if `EncodingKey::from_rsa_pem` moved, check how `crates/auth/src/jwt.rs` constructs its RS256 encoding key and copy that exact call.)

- [ ] **Step 4: Run tests** — `cargo test -p worker snowflake::jwt` → PASS.
- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/snowflake/jwt.rs crates/worker/src/loaders/snowflake/mod.rs crates/worker/Cargo.toml
git commit -m "phase-2-5a-5: snowflake key-pair JWT auth"
```

---

### Task 6: SQL API v2 client (`client.rs`)

**Files:**
- Create: `crates/worker/src/loaders/snowflake/client.rs` (+ `pub mod client;`)
- Modify: `crates/worker/Cargo.toml` — add `[dev-dependencies] wiremock = "0.6"`.

**Interfaces:**
- Consumes: `KeyPairAuth` (Task 5), `SnowflakeDestinationSpec` (Task 1).
- Produces:
  - `pub struct SnowflakeClient { ... }`
  - `pub fn from_spec(spec: &SnowflakeDestinationSpec) -> anyhow::Result<SnowflakeClient>` — reads PEM from `std::env::var(&spec.private_key_env)`; base URL `https://<account>.snowflakecomputing.com`, overridden by env `ETL_SNOWFLAKE_BASE_URL` when set (test seam).
  - `pub async fn execute(&self, sql: &str) -> anyhow::Result<serde_json::Value>` — returns response body JSON.
  - `pub async fn execute_multi(&self, statements: &[String]) -> anyhow::Result<()>` — one request, `MULTI_STATEMENT_COUNT = statements.len()`.
  - `pub fn rowset_len(body: &serde_json::Value) -> usize` — helper: `body["data"]` array length (0 when absent).

- [ ] **Step 1: Write the failing tests** (in `client.rs`, `#[tokio::test]` + wiremock):

```rust
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn spec_for(server_url: &str) -> common_types::pipeline_spec::SnowflakeDestinationSpec {
    std::env::set_var("SNOWTEST_KEY", crate::loaders::snowflake::jwt_test_pem());
    std::env::set_var("ETL_SNOWFLAKE_BASE_URL", server_url);
    common_types::pipeline_spec::SnowflakeDestinationSpec {
        account: "xy12345".into(), user: "u".into(),
        private_key_env: "SNOWTEST_KEY".into(),
        warehouse: "W".into(), database: "DB".into(), schema: "RAW".into(),
        table: "t".into(), role: None, pk_columns: vec![],
    }
}

#[tokio::test]
async fn execute_posts_statement_with_jwt_headers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [["1"]], "statementHandle": "h1"
        })))
        .expect(1)
        .mount(&server).await;
    let client = SnowflakeClient::from_spec(&spec_for(&server.uri())).unwrap();
    let body = client.execute("SELECT 1").await.unwrap();
    assert_eq!(rowset_len(&body), 1);
}

#[tokio::test]
async fn execute_polls_on_202() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(202).set_body_json(
            serde_json::json!({"statementHandle": "abc"})))
        .mount(&server).await;
    Mock::given(method("GET")).and(path("/api/v2/statements/abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"data": []})))
        .mount(&server).await;
    let client = SnowflakeClient::from_spec(&spec_for(&server.uri())).unwrap();
    let body = client.execute("SELECT 1").await.unwrap();
    assert_eq!(rowset_len(&body), 0);
}

#[tokio::test]
async fn non_2xx_surfaces_snowflake_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(422).set_body_json(
            serde_json::json!({"message": "SQL compilation error"})))
        .mount(&server).await;
    let client = SnowflakeClient::from_spec(&spec_for(&server.uri())).unwrap();
    let err = client.execute("BROKEN").await.unwrap_err().to_string();
    assert!(err.contains("SQL compilation error"), "got: {err}");
}
```

Expose a `pub(crate) fn jwt_test_pem() -> String` helper in `snowflake/mod.rs` under `#[cfg(test)]` reusing Task 5's `test_pem` so client tests don't regenerate keys per call. NOTE: tests that set env vars must not run concurrently with each other — mark this test module `#[serial_test::serial]` only if `serial_test` already exists in the workspace; otherwise have each test use a unique env-var name (as `SNOWTEST_KEY` vs `SNOWTEST_KEY2`) and pass the base URL through a `with_base_url` builder instead of env where practical: add `pub fn with_base_url(mut self, url: &str) -> Self` and prefer it in tests; `ETL_SNOWFLAKE_BASE_URL` remains for the integration-test seam only.

- [ ] **Step 2: Run to verify failure** — `cargo test -p worker snowflake::client` → compile FAIL.

- [ ] **Step 3: Implement:**

```rust
//! Minimal Snowflake SQL API v2 client (statements endpoint only).

use anyhow::{bail, Context};
use common_types::pipeline_spec::SnowflakeDestinationSpec;

use super::jwt::KeyPairAuth;

pub struct SnowflakeClient {
    http: reqwest::Client,
    base_url: String,
    auth: KeyPairAuth,
    warehouse: String,
    database: String,
    schema: String,
    role: Option<String>,
}

pub fn rowset_len(body: &serde_json::Value) -> usize {
    body.get("data").and_then(|d| d.as_array()).map(|a| a.len()).unwrap_or(0)
}

impl SnowflakeClient {
    pub fn from_spec(spec: &SnowflakeDestinationSpec) -> anyhow::Result<Self> {
        let pem = std::env::var(&spec.private_key_env).with_context(|| {
            format!("env var {} (private_key_env) not set", spec.private_key_env)
        })?;
        let base_url = std::env::var("ETL_SNOWFLAKE_BASE_URL").unwrap_or_else(|_| {
            format!("https://{}.snowflakecomputing.com", spec.account.to_lowercase())
        });
        Ok(Self {
            http: reqwest::Client::new(),
            base_url,
            auth: KeyPairAuth::from_pem(&spec.account, &spec.user, &pem)?,
            warehouse: spec.warehouse.clone(),
            database: spec.database.clone(),
            schema: spec.schema.clone(),
            role: spec.role.clone(),
        })
    }

    pub fn with_base_url(mut self, url: &str) -> Self {
        self.base_url = url.trim_end_matches('/').to_string();
        self
    }

    async fn post_statement(
        &self,
        statement: &str,
        multi_count: Option<usize>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut body = serde_json::json!({
            "statement": statement,
            "timeout": 60,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        });
        if let Some(role) = &self.role {
            body["role"] = serde_json::json!(role);
        }
        if let Some(n) = multi_count {
            body["parameters"] = serde_json::json!({"MULTI_STATEMENT_COUNT": n.to_string()});
        }
        let token = self.auth.token()?;
        let resp = self.http
            .post(format!("{}/api/v2/statements", self.base_url))
            .bearer_auth(&token)
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header("Accept", "application/json")
            .json(&body)
            .send().await.context("snowflake POST /statements")?;
        self.handle_response(resp, &token).await
    }

    async fn handle_response(
        &self,
        resp: reqwest::Response,
        token: &str,
    ) -> anyhow::Result<serde_json::Value> {
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        if status.as_u16() == 202 {
            let handle = body["statementHandle"].as_str()
                .context("202 without statementHandle")?.to_string();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                if std::time::Instant::now() > deadline {
                    bail!("snowflake statement {handle} still running after 60s");
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let r = self.http
                    .get(format!("{}/api/v2/statements/{handle}", self.base_url))
                    .bearer_auth(token)
                    .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                    .send().await.context("snowflake GET /statements/<handle>")?;
                if r.status().as_u16() == 202 { continue; }
                let s = r.status();
                let b: serde_json::Value = r.json().await.unwrap_or_default();
                if s.is_success() { return Ok(b); }
                bail!("snowflake statement failed ({s}): {}",
                    b["message"].as_str().unwrap_or("<no message>"));
            }
        }
        if status.is_success() { return Ok(body); }
        bail!("snowflake request failed ({status}): {}",
            body["message"].as_str().unwrap_or("<no message>"));
    }

    pub async fn execute(&self, sql: &str) -> anyhow::Result<serde_json::Value> {
        self.post_statement(sql, None).await
    }

    pub async fn execute_multi(&self, statements: &[String]) -> anyhow::Result<()> {
        let joined = statements.join(";\n");
        self.post_statement(&joined, Some(statements.len())).await.map(|_| ())
    }
}
```

- [ ] **Step 4: Run tests** — `cargo test -p worker snowflake::client` → PASS (3 tests).
- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/snowflake/ crates/worker/Cargo.toml
git commit -m "phase-2-5a-6: snowflake SQL API v2 client with 202 polling"
```

---

### Task 7: `SnowflakeLoader` (`mod.rs`)

**Files:**
- Modify: `crates/worker/src/loaders/snowflake/mod.rs`

**Interfaces:**
- Consumes: everything above + `loader_sdk::{DestinationLoader, LoadId, LoadResult}`.
- Produces: `pub struct SnowflakeLoader;` implementing `DestinationLoader` — Task 8 dispatches to it.

- [ ] **Step 1: Write the failing test** (unit — validate paths only; the load() network paths are covered in Task 9's wiremock integration test):

```rust
#[tokio::test]
async fn validate_rejects_missing_env() {
    let spec = common_types::pipeline_spec::DestinationSpec::Snowflake(
        common_types::pipeline_spec::SnowflakeDestinationSpec {
            account: "a".into(), user: "u".into(),
            private_key_env: "DEFINITELY_NOT_SET_VAR_2_5A".into(),
            warehouse: "w".into(), database: "d".into(), schema: "s".into(),
            table: "t".into(), role: None, pk_columns: vec![],
        });
    let err = SnowflakeLoader.validate(&spec).await.unwrap_err().to_string();
    assert!(err.contains("DEFINITELY_NOT_SET_VAR_2_5A"), "got {err}");
}

#[tokio::test]
async fn validate_rejects_wrong_variant() {
    let spec = common_types::pipeline_spec::DestinationSpec::LocalParquet(
        common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() });
    assert!(SnowflakeLoader.validate(&spec).await.is_err());
}
```

- [ ] **Step 2: Run to verify failure** — compile FAIL.

- [ ] **Step 3: Implement** in `snowflake/mod.rs`:

```rust
//! Snowflake destination loader (RFC-9, phase 2-5a MVP).
//!
//! Idempotency: `_ETL_LOADED_BATCHES` log table, checked before writing;
//! data DML + log INSERT execute in one multi-statement transaction
//! (BEGIN; ...; COMMIT). Same LoadId retried ⇒ short-circuit no-op.
//! Append (`pk_columns: []`) uses INSERT; upsert uses MERGE-from-VALUES.
//! Deferred: stage+COPY fast path, CDC apply, schema evolution,
//! multi-table routing — see the 2-5a design memo.

pub mod client;
pub mod jwt;
pub mod sql;

use anyhow::{bail, Context};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};

pub struct SnowflakeLoader;

fn spec_of(dest: &DestinationSpec) -> anyhow::Result<&SnowflakeDestinationSpec> {
    match dest {
        DestinationSpec::Snowflake(s) => Ok(s),
        other => bail!("SnowflakeLoader invoked with non-snowflake destination: {other:?}"),
    }
}

#[async_trait::async_trait]
impl DestinationLoader for SnowflakeLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let spec = spec_of(dest)?;
        let pem = std::env::var(&spec.private_key_env).with_context(|| {
            format!("env var {} (private_key_env) not set", spec.private_key_env)
        })?;
        jwt::KeyPairAuth::from_pem(&spec.account, &spec.user, &pem)?;
        sql::qualified(&spec.database, &spec.schema, &spec.table)?;
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let spec = spec_of(dest)?;
        let path = format!(
            "snowflake://{}/{}/{}/{}",
            spec.account, spec.database, spec.schema, spec.table
        );
        if batch.num_rows() == 0 {
            return Ok(LoadResult { rows_loaded: 0, bytes_written: 0, path });
        }
        let client = client::SnowflakeClient::from_spec(spec)?;
        let (db, sch) = (&spec.database, &spec.schema);

        client.execute(&sql::ensure_log_table_ddl(db, sch)?).await
            .context("ensuring idempotency log table")?;
        client.execute(&sql::ensure_target_table_ddl(db, sch, &spec.table, batch.schema().as_ref())?)
            .await.context("ensuring target table")?;

        let check = client.execute(&sql::check_loaded_sql(db, sch, &load_id)?).await
            .context("checking idempotency log")?;
        if client::rowset_len(&check) > 0 {
            tracing::info!(
                target: "loader.snowflake",
                batch_seq = load_id.batch_seq,
                "batch already loaded; idempotent no-op"
            );
            return Ok(LoadResult { rows_loaded: 0, bytes_written: 0, path });
        }

        let dml = if spec.pk_columns.is_empty() {
            sql::append_insert_sql(db, sch, &spec.table, &batch)?
        } else {
            sql::merge_upsert_sql(db, sch, &spec.table, &batch, &spec.pk_columns)?
        };
        let stmts = vec![
            "BEGIN".to_string(),
            dml,
            sql::log_insert_sql(db, sch, &load_id, batch.num_rows())?,
            "COMMIT".to_string(),
        ];
        client.execute_multi(&stmts).await.context("transactional load")?;

        Ok(LoadResult {
            rows_loaded: batch.num_rows(),
            bytes_written: batch.get_array_memory_size() as u64,
            path,
        })
    }
}
```

- [ ] **Step 4: Run tests** — `cargo test -p worker snowflake` → PASS (all module tests).
- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/snowflake/mod.rs
git commit -m "phase-2-5a-7: SnowflakeLoader — validate + idempotent transactional load"
```

---

### Task 8: Activity dispatch

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs` (three sites: `loader_name` at ~line 24, the `load` match at ~line 287, the dead-letter `unreachable!` arm at ~line 341)

**Interfaces:**
- Consumes: `SnowflakeLoader` (Task 7).

- [ ] **Step 1: Write the failing test** (extend `dispatch_tests` mod at ~line 443):

```rust
#[test]
fn loader_name_snowflake() {
    let spec = DestinationSpec::Snowflake(common_types::pipeline_spec::SnowflakeDestinationSpec {
        account: "a".into(), user: "u".into(), private_key_env: "K".into(),
        warehouse: "w".into(), database: "d".into(), schema: "s".into(),
        table: "t".into(), role: None, pk_columns: vec![],
    });
    assert_eq!(loader_name(&spec), "snowflake");
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p worker dispatch_tests` → FAIL (non-exhaustive match compile errors appear first; that is the point).

- [ ] **Step 3: Implement** — fix every non-exhaustive match:

```rust
use crate::loaders::snowflake::SnowflakeLoader;

// loader_name:
common_types::pipeline_spec::DestinationSpec::Snowflake(_) => "snowflake",

// load dispatch:
common_types::pipeline_spec::DestinationSpec::Snowflake(_) => {
    SnowflakeLoader
        .load(&input.destination, load_id.clone(), batch)
        .await
}

// dead-letter path arm (Snowflake logs-and-drops like Postgres — the
// dl_supported guard already covers it): change the guarded match arm to
common_types::pipeline_spec::DestinationSpec::Postgres(_)
| common_types::pipeline_spec::DestinationSpec::Snowflake(_) => {
    unreachable!("guarded by dl_supported");
}
```

Check for any other exhaustive `match` on `DestinationSpec` across the workspace (`grep -rn "DestinationSpec::" crates/ | grep match` and let the compiler find the rest via `cargo build --workspace`) — the CLI `dsl.rs`/`get_cmd` may print destination kinds; extend those too with the same pattern used for Postgres.

- [ ] **Step 4: Run tests**

Run: `cargo build --workspace && cargo test -p worker`
Expected: builds clean (no non-exhaustive match errors anywhere), all worker tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/sync/mod.rs
git commit -m "phase-2-5a-8: dispatch DestinationSpec::Snowflake in load_batch"
```

---

### Task 9: Integration test against a mock SQL API

**Files:**
- Create: `tests/integration/tests/snowflake_loader_mock.rs`

**Interfaces:**
- Consumes: `worker::loaders::snowflake::SnowflakeLoader` (worker is already a dep of integration-tests), wiremock 0.6 (already a dep).

- [ ] **Step 1: Write the test file** (these are the deliverable; they should pass immediately since Tasks 2–7 are done — if any fail, fix the loader, not the test):

```rust
//! SnowflakeLoader against a wiremock fake of the SQL API v2.
//! Proves the loader's statement sequence without a Snowflake account:
//! DDL → idempotency check → BEGIN/DML/log/COMMIT multi-statement.
//! Real-account coverage: snowflake_loader.rs (env-gated).

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use worker::loaders::snowflake::SnowflakeLoader;

fn test_pem() -> String {
    use rsa::pkcs8::EncodePrivateKey;
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
    key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap().to_string()
}

fn spec(pk: Vec<String>) -> DestinationSpec {
    DestinationSpec::Snowflake(SnowflakeDestinationSpec {
        account: "xy12345".into(),
        user: "loader".into(),
        private_key_env: "SNOWFLAKE_MOCK_KEY".into(),
        warehouse: "W".into(),
        database: "DB".into(),
        schema: "RAW".into(),
        table: "items".into(),
        role: None,
        pk_columns: pk,
    })
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap()
}

fn load_id(seq: u32) -> LoadId {
    LoadId {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::nil()),
        pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(uuid::Uuid::nil()),
        run_id: common_types::ids::RunId::from_uuid_unchecked(uuid::Uuid::nil()),
        batch_seq: seq,
        stream_name: "public.items".into(),
    }
}

async fn mock_env(server: &MockServer) {
    std::env::set_var("SNOWFLAKE_MOCK_KEY", test_pem());
    std::env::set_var("ETL_SNOWFLAKE_BASE_URL", server.uri());
}

/// Not-yet-loaded check returns an empty rowset.
fn empty_ok() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []}))
}

#[tokio::test]
async fn append_flow_issues_insert_in_transaction() {
    let server = MockServer::start().await;
    mock_env(&server).await;
    // catch-all 200 for DDL + check (empty rowset)
    Mock::given(method("POST")).and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server).await;

    let res = SnowflakeLoader.load(&spec(vec![]), load_id(0), batch()).await.unwrap();
    assert_eq!(res.rows_loaded, 2);

    // request log: last POST must contain BEGIN + INSERT INTO target + log insert + COMMIT
    let reqs = server.received_requests().await.unwrap();
    let last = std::str::from_utf8(&reqs.last().unwrap().body).unwrap().to_string();
    assert!(last.contains("BEGIN"), "got {last}");
    assert!(last.contains("INSERT INTO \\\"DB\\\".\\\"RAW\\\".\\\"items\\\""), "got {last}");
    assert!(last.contains("_ETL_LOADED_BATCHES"), "got {last}");
    assert!(last.contains("COMMIT"), "got {last}");
}

#[tokio::test]
async fn upsert_flow_issues_merge() {
    let server = MockServer::start().await;
    mock_env(&server).await;
    Mock::given(method("POST")).and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server).await;

    let res = SnowflakeLoader
        .load(&spec(vec!["id".into()]), load_id(1), batch()).await.unwrap();
    assert_eq!(res.rows_loaded, 2);
    let reqs = server.received_requests().await.unwrap();
    let all: String = reqs.iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    assert!(all.contains("MERGE INTO"), "got {all}");
    assert!(all.contains("WHEN NOT MATCHED THEN INSERT"), "got {all}");
}

#[tokio::test]
async fn replay_short_circuits_without_dml() {
    let server = MockServer::start().await;
    mock_env(&server).await;
    // check-loaded returns one row ⇒ already loaded
    Mock::given(method("POST")).and(path("/api/v2/statements"))
        .and(body_string_contains("SELECT ROWS_LOADED"))
        .respond_with(ResponseTemplate::new(200)
            .set_body_json(serde_json::json!({"data": [["2"]]})))
        .mount(&server).await;
    Mock::given(method("POST")).and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server).await;

    let res = SnowflakeLoader.load(&spec(vec![]), load_id(2), batch()).await.unwrap();
    assert_eq!(res.rows_loaded, 0, "replay must be a no-op");
    let reqs = server.received_requests().await.unwrap();
    for r in &reqs {
        let b = String::from_utf8_lossy(&r.body);
        assert!(!b.contains("BEGIN"), "no transactional DML on replay; got {b}");
    }
}
```

NOTE on env vars: these three tests share `SNOWFLAKE_MOCK_KEY`/`ETL_SNOWFLAKE_BASE_URL` and MUST run single-threaded. Add at the top of the file: `// run with --test-threads=1; env-var seams are process-global` and run them that way; each test setting the base URL to its own server before any request keeps them correct under `--test-threads=1`.

- [ ] **Step 2: Run**

Run: `cargo test -p integration-tests --test snowflake_loader_mock -- --test-threads=1 --nocapture`
Expected: PASS (3 tests). These don't need docker — they're pure wiremock.

- [ ] **Step 3: Fix anything the tests surface** in the loader/client (not in the tests), re-run until green.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/snowflake_loader_mock.rs
git commit -m "phase-2-5a-9: wiremock integration tests for SnowflakeLoader"
```

---

### Task 10: Env-gated real-account test + finish

**Files:**
- Create: `tests/integration/tests/snowflake_loader.rs`
- Modify: `docs/superpowers/specs/2026-07-23-phase-2-5a-snowflake-loader-design.md` (flip Status to Shipped)

- [ ] **Step 1: Write the env-gated test:**

```rust
//! SnowflakeLoader against a REAL Snowflake account. Skips unless the
//! SNOWFLAKE_TEST_* env vars are set (RFC-9: destination tests run
//! against real accounts, not mocks). Uses a throwaway table per run.
//!
//! Required env:
//!   SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PRIVATE_KEY
//!   (PEM content), SNOWFLAKE_TEST_WAREHOUSE, SNOWFLAKE_TEST_DATABASE,
//!   SNOWFLAKE_TEST_SCHEMA

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use worker::loaders::snowflake::SnowflakeLoader;

fn real_spec(table: &str, pk: Vec<String>) -> Option<DestinationSpec> {
    let get = |k: &str| std::env::var(k).ok();
    let (account, user, key, wh, db, sch) = (
        get("SNOWFLAKE_TEST_ACCOUNT")?, get("SNOWFLAKE_TEST_USER")?,
        get("SNOWFLAKE_TEST_PRIVATE_KEY")?, get("SNOWFLAKE_TEST_WAREHOUSE")?,
        get("SNOWFLAKE_TEST_DATABASE")?, get("SNOWFLAKE_TEST_SCHEMA")?,
    );
    std::env::set_var("SNOWFLAKE_TEST_KEY_MATERIAL", key);
    Some(DestinationSpec::Snowflake(SnowflakeDestinationSpec {
        account, user,
        private_key_env: "SNOWFLAKE_TEST_KEY_MATERIAL".into(),
        warehouse: wh, database: db, schema: sch,
        table: table.into(), role: None, pk_columns: pk,
    }))
}

fn batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
    ]).unwrap()
}

fn load_id(seq: u32) -> LoadId {
    LoadId {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::new_v4()),
        pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(uuid::Uuid::new_v4()),
        run_id: common_types::ids::RunId::from_uuid_unchecked(uuid::Uuid::new_v4()),
        batch_seq: seq,
        stream_name: String::new(),
    }
}

#[tokio::test]
#[ignore = "requires a real Snowflake account via SNOWFLAKE_TEST_* env vars"]
async fn snowflake_append_upsert_idempotent_roundtrip() -> anyhow::Result<()> {
    let table = format!("etl_test_{}", uuid::Uuid::new_v4().simple());
    let Some(spec) = real_spec(&table, vec!["id".into()]) else {
        eprintln!("SKIP: SNOWFLAKE_TEST_* env not set");
        return Ok(());
    };

    // Load 1: initial upsert.
    let id0 = load_id(0);
    let r1 = SnowflakeLoader.load(&spec, id0.clone(), batch(vec![1, 2], vec!["a", "b"])).await?;
    assert_eq!(r1.rows_loaded, 2);

    // Replay same LoadId: must be a no-op.
    let r2 = SnowflakeLoader.load(&spec, id0, batch(vec![1, 2], vec!["a", "b"])).await?;
    assert_eq!(r2.rows_loaded, 0, "same LoadId must short-circuit");

    // Load 2: upsert overwrites row 2, inserts row 3.
    let r3 = SnowflakeLoader.load(&spec, load_id(1), batch(vec![2, 3], vec!["B", "c"])).await?;
    assert_eq!(r3.rows_loaded, 2);

    // Verify final state via the loader's own client.
    if let DestinationSpec::Snowflake(s) = &spec {
        let client = worker::loaders::snowflake::client::SnowflakeClient::from_spec(s)?;
        let body = client.execute(&format!(
            "SELECT \"id\", \"name\" FROM \"{}\".\"{}\".\"{}\" ORDER BY \"id\"",
            s.database, s.schema, table
        )).await?;
        let rows = body["data"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1][1], "B", "row 2 must reflect the upsert");
        // cleanup
        client.execute(&format!(
            "DROP TABLE IF EXISTS \"{}\".\"{}\".\"{}\"", s.database, s.schema, table)).await?;
        client.execute(&format!(
            "DELETE FROM \"{}\".\"{}\".\"_ETL_LOADED_BATCHES\" WHERE STREAM_NAME = ''",
            s.database, s.schema)).await.ok();
    }
    Ok(())
}
```

- [ ] **Step 2: Verify skip behavior** (no account needed):

Run: `cargo test -p integration-tests --test snowflake_loader -- --ignored --nocapture`
Expected: prints `SKIP: SNOWFLAKE_TEST_* env not set`, PASS.

- [ ] **Step 3: Full verification sweep**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --lib
cargo test -p worker
cargo test -p integration-tests --test snowflake_loader_mock -- --test-threads=1
```
Expected: all green.

- [ ] **Step 4: Flip design memo Status to `Shipped`, commit**

```bash
git add tests/integration/tests/snowflake_loader.rs docs/superpowers/specs/2026-07-23-phase-2-5a-snowflake-loader-design.md
git commit -m "phase-2-5a-10: env-gated real-account test + design memo"
```

---

## Self-Review Notes

- Spec coverage: design memo §1 In-scope items map to Tasks 1 (spec), 2–4 (SQL), 5 (JWT), 6 (client), 7 (loader + idempotency), 8 (dispatch), 9–10 (tests). Deferred items are documented in the memo §1 Out and the loader's module doc.
- Type consistency: `SnowflakeDestinationSpec` field names in Tasks 1/6/7/9/10 match; `sql::` function names in Tasks 2/3/4/7 match; `rowset_len` defined Task 6, used Task 7.
- Known uncertainty flagged inline: `jsonwebtoken` 10 exact constructor (fall back to auth crate's usage), `as_uuid()` accessor name, `hex` availability. Each has a stated fallback; none blocks a task boundary.
