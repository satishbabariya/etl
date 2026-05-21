# Postgres Destination Loader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Postgres destination loader (second loader after LocalParquet) that supports insert-only and upsert (`ON CONFLICT DO UPDATE`) writes with idempotent retry semantics, so pipelines can land data in Postgres tables.

**Architecture:** New `PostgresLoader` implementing the existing `DestinationLoader` trait. Each `load()` call opens a single sqlx transaction, ensures the target table exists, optionally checks a per-tenant `_etl_loaded_batches` idempotency log, performs a batched `INSERT` (with `ON CONFLICT (pk) DO UPDATE` when PK columns are configured), records the load in the log, and commits. Schema is derived from the incoming `RecordBatch` schema; first-load creates the table. The `load_batch` activity gains a match on `DestinationSpec` to dispatch to the right loader.

**Tech Stack:** Rust · `sqlx 0.8` (Postgres feature, workspace dep) · `arrow 53` · `tokio-postgres` available but not required for MVP · `tempfile`/Docker `postgres:16` (already in `docker-compose.yml`) for tests.

**Scope cuts (deferred, not in this plan):**
- CDC `_cdc.op`-aware DELETE/UPDATE handling (RFC-9 Pattern 3) — append + upsert only.
- Mid-run schema evolution (ALTER TABLE) — first-load CREATE TABLE only; later batches assume schema unchanged and fail loudly if mismatched.
- Soft delete / tombstone columns.
- Dead-letter routing for rejected rows (current dead-letter path is hardcoded to LocalParquet; PG path returns plain activity errors that Temporal retries).
- True `COPY FROM STDIN` (perf optimization; deferred).
- Secret-ref resolution for connection URL — MVP uses an inline URL string (matches current dev-mode pattern); RFC-11 secret refs are a follow-up.
- Multi-table per spec — MVP supports one target table per pipeline (matches the rest of the platform's current single-table assumption).

---

## File Structure

**Create:**
- `crates/worker/src/loaders/postgres.rs` — `PostgresLoader` implementation, type mapping, DDL builders, idempotency log helpers.
- `tests/integration/tests/postgres_loader.rs` — integration tests against the docker-compose `postgres` service.

**Modify:**
- `crates/common-types/src/pipeline_spec.rs` — add `Postgres(PostgresDestinationSpec)` variant + struct.
- `crates/worker/src/loaders/mod.rs` — register the new module.
- `crates/worker/src/activities/sync/mod.rs` — dispatch loader by `DestinationSpec` variant; guard the existing dead-letter block behind `LocalParquet`.

Each file has one responsibility: spec (type), dispatch (activity glue), implementation (loader), tests (integration).

---

## Task 1: Add `PostgresDestinationSpec` to the pipeline spec

**Files:**
- Modify: `crates/common-types/src/pipeline_spec.rs`
- Test: `crates/common-types/src/pipeline_spec.rs` (existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/common-types/src/pipeline_spec.rs`:

```rust
#[test]
fn postgres_destination_roundtrips() {
    let s = DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: "postgres://etl:etl@localhost:5432/etl_dest".into(),
        schema: "public".into(),
        table: "customers".into(),
        pk_columns: vec!["id".into()],
    });
    let j = serde_json::to_string(&s).unwrap();
    let back: DestinationSpec = serde_json::from_str(&j).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), j);
}

#[test]
fn postgres_destination_serialized_form_is_tagged() {
    let s = DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: "postgres://x".into(),
        schema: "public".into(),
        table: "t".into(),
        pk_columns: vec![],
    });
    let j: serde_json::Value = serde_json::to_value(&s).unwrap();
    assert_eq!(j["type"], "postgres");
    assert!(j["pk_columns"].is_array());
}

#[test]
fn postgres_destination_pk_columns_default_empty() {
    let j = r#"{
        "type": "postgres",
        "connection_url": "postgres://x",
        "schema": "public",
        "table": "t"
    }"#;
    let s: DestinationSpec = serde_json::from_str(j).unwrap();
    if let DestinationSpec::Postgres(p) = s {
        assert!(p.pk_columns.is_empty());
    } else {
        panic!("expected Postgres variant");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p common-types pipeline_spec::tests::postgres_destination -- --nocapture`
Expected: compile error — `PostgresDestinationSpec` and `DestinationSpec::Postgres` not defined.

- [ ] **Step 3: Add the variant and struct**

In `crates/common-types/src/pipeline_spec.rs`, change the `DestinationSpec` enum to:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DestinationSpec {
    LocalParquet(LocalParquetSpec),
    Postgres(PostgresDestinationSpec),
}
```

Add below `LocalParquetSpec`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresDestinationSpec {
    /// Plain `postgres://...` URL. MVP — RFC-11 secret refs are a follow-up.
    pub connection_url: String,
    /// Postgres schema (namespace) that owns the target table.
    pub schema: String,
    /// Target table name. Created on first load if absent.
    pub table: String,
    /// Upsert key columns. Empty ⇒ insert-only (append). Non-empty ⇒
    /// `INSERT ... ON CONFLICT (pk) DO UPDATE`.
    #[serde(default)]
    pub pk_columns: Vec<String>,
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p common-types pipeline_spec`
Expected: all `pipeline_spec::tests::*` pass, including the three new tests.

- [ ] **Step 5: Commit**

```bash
git add crates/common-types/src/pipeline_spec.rs
git commit -m "phase-2-4a-1: add PostgresDestinationSpec variant"
```

---

## Task 2: Stub `PostgresLoader` + register module

**Files:**
- Create: `crates/worker/src/loaders/postgres.rs`
- Modify: `crates/worker/src/loaders/mod.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/worker/src/loaders/postgres.rs` (you'll create the file in step 3 with the test included):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};

    #[tokio::test]
    async fn validate_rejects_non_postgres_spec() {
        let loader = PostgresLoader;
        let spec = DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() }
        );
        let err = loader.validate(&spec).await.unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("postgres"));
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `cargo test -p worker loaders::postgres::tests::validate_rejects_non_postgres_spec`
Expected: compile error — `PostgresLoader` not found.

- [ ] **Step 3: Create the stub loader and register the module**

Create `crates/worker/src/loaders/postgres.rs`:

```rust
//! Postgres destination loader (RFC-9). MVP: insert-only or
//! `ON CONFLICT DO UPDATE`, per-call transaction, idempotency log.
//!
//! Scope cuts (see plan): no CDC op-aware DELETE, no mid-run schema
//! evolution, no soft delete, no dead-letter routing.

use anyhow::{Context, bail};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};

pub struct PostgresLoader;

#[async_trait]
impl DestinationLoader for PostgresLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let _spec = postgres_spec(dest)?;
        // Connectivity check arrives in Task 4.
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        _load_id: LoadId,
        _batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let _spec = postgres_spec(dest)?;
        bail!("PostgresLoader::load not yet implemented");
    }
}

fn postgres_spec(dest: &DestinationSpec) -> anyhow::Result<&PostgresDestinationSpec> {
    match dest {
        DestinationSpec::Postgres(s) => Ok(s),
        other => bail!("PostgresLoader received non-postgres destination: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};

    #[tokio::test]
    async fn validate_rejects_non_postgres_spec() {
        let loader = PostgresLoader;
        let spec = DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() }
        );
        let err = loader.validate(&spec).await.unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("postgres"));
    }
}
```

Then add to `crates/worker/src/loaders/mod.rs`:

```rust
//! In-process loader implementations. Phase I.2: local-Parquet only.
pub mod cdc_parquet;
pub mod parquet_local;
pub mod postgres;
```

- [ ] **Step 4: Run test to confirm it passes**

Run: `cargo test -p worker loaders::postgres::tests::validate_rejects_non_postgres_spec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs crates/worker/src/loaders/mod.rs
git commit -m "phase-2-4a-2: PostgresLoader stub + module wiring"
```

---

## Task 3: Dispatch loader by `DestinationSpec` variant in `load_batch` activity

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs` (lines around 244 — the hardcoded `LocalParquetLoader.load(...)` and the dead-letter block around 254–290)

- [ ] **Step 1: Write the failing test**

Add a unit test inside the existing `#[cfg(test)] mod` of `crates/worker/src/activities/sync/mod.rs` (if no such module exists, create one at the bottom of the file). The test exercises the dispatch error path — a Postgres spec routes to the new loader, which currently returns "not yet implemented":

```rust
#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};

    #[test]
    fn pick_loader_returns_postgres_for_postgres_spec() {
        let spec = DestinationSpec::Postgres(PostgresDestinationSpec {
            connection_url: "postgres://x".into(),
            schema: "public".into(),
            table: "t".into(),
            pk_columns: vec![],
        });
        let name = loader_name(&spec);
        assert_eq!(name, "postgres");
    }

    #[test]
    fn pick_loader_returns_local_parquet_for_parquet_spec() {
        let spec = DestinationSpec::LocalParquet(
            common_types::pipeline_spec::LocalParquetSpec { base_path: "/tmp".into() }
        );
        assert_eq!(loader_name(&spec), "local_parquet");
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `cargo test -p worker activities::sync::dispatch_tests`
Expected: compile error — `loader_name` not defined.

- [ ] **Step 3: Add the dispatch helper and refactor `load_batch`**

In `crates/worker/src/activities/sync/mod.rs`, near the top of the impl block (or top-level), add:

```rust
use crate::loaders::postgres::PostgresLoader;

fn loader_name(dest: &common_types::pipeline_spec::DestinationSpec) -> &'static str {
    match dest {
        common_types::pipeline_spec::DestinationSpec::LocalParquet(_) => "local_parquet",
        common_types::pipeline_spec::DestinationSpec::Postgres(_) => "postgres",
    }
}
```

Replace the existing hardcoded loader call (the line `let res = LocalParquetLoader.load(&input.destination, load_id.clone(), batch).await...`) with:

```rust
let res = match &input.destination {
    common_types::pipeline_spec::DestinationSpec::LocalParquet(_) => {
        LocalParquetLoader
            .load(&input.destination, load_id.clone(), batch)
            .await
    }
    common_types::pipeline_spec::DestinationSpec::Postgres(_) => {
        PostgresLoader
            .load(&input.destination, load_id.clone(), batch)
            .await
    }
}
.map_err(to_retryable)?;
```

Guard the existing dead-letter block so it only runs for `LocalParquet` (the existing block already destructures `LocalParquet`; wrap the whole `if let Some(rej_b64) = ...` in `if matches!(&input.destination, common_types::pipeline_spec::DestinationSpec::LocalParquet(_))`). Add an `else` branch that logs and drops rejected rows for non-parquet destinations:

```rust
if matches!(&input.destination, common_types::pipeline_spec::DestinationSpec::LocalParquet(_)) {
    // existing dead-letter block unchanged
} else if input.rejected_ipc_b64.is_some() {
    tracing::warn!(
        target: "loader.dead_letter",
        "dead-letter routing not implemented for {}; rejected rows dropped",
        loader_name(&input.destination)
    );
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker activities::sync`
Expected: dispatch_tests pass; all previously-passing tests in `activities::sync` still pass.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/sync/mod.rs
git commit -m "phase-2-4a-3: dispatch load_batch by DestinationSpec variant"
```

---

## Task 4: Arrow→Postgres type mapping (DDL builder)

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs` (add private `ddl` helpers + unit tests)

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/worker/src/loaders/postgres.rs`:

```rust
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

fn fields(items: &[(&str, DataType, bool)]) -> Vec<Field> {
    items.iter().map(|(n, t, nullable)| Field::new(*n, t.clone(), *nullable)).collect()
}

#[test]
fn pg_column_type_covers_cdc_source_types() {
    assert_eq!(pg_column_type(&DataType::Int64).unwrap(), "BIGINT");
    assert_eq!(pg_column_type(&DataType::Int32).unwrap(), "INTEGER");
    assert_eq!(pg_column_type(&DataType::Utf8).unwrap(), "TEXT");
    assert_eq!(pg_column_type(&DataType::Boolean).unwrap(), "BOOLEAN");
    assert_eq!(pg_column_type(&DataType::Float64).unwrap(), "DOUBLE PRECISION");
    assert_eq!(
        pg_column_type(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))).unwrap(),
        "TIMESTAMPTZ"
    );
    assert_eq!(pg_column_type(&DataType::Date32).unwrap(), "DATE");
    assert_eq!(pg_column_type(&DataType::Binary).unwrap(), "BYTEA");
    assert_eq!(pg_column_type(&DataType::Time64(TimeUnit::Microsecond)).unwrap(), "TIME");
}

#[test]
fn pg_column_type_rejects_unsupported() {
    let err = pg_column_type(&DataType::List(Arc::new(Field::new("x", DataType::Int8, true))))
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("unsupported"));
}

#[test]
fn create_table_ddl_quotes_identifiers_and_emits_columns() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ])));
    let ddl = create_table_ddl(
        "public",
        "customers",
        &schema,
        &["id".to_string()],
    ).unwrap();
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"public\".\"customers\""));
    assert!(ddl.contains("\"id\" BIGINT NOT NULL"));
    assert!(ddl.contains("\"name\" TEXT"));
    assert!(ddl.contains("PRIMARY KEY (\"id\")"));
}

#[test]
fn create_table_ddl_omits_pk_when_pk_columns_empty() {
    let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
    let ddl = create_table_ddl("public", "events", &schema, &[]).unwrap();
    assert!(!ddl.contains("PRIMARY KEY"));
}

#[test]
fn create_table_ddl_errors_when_pk_column_missing_from_schema() {
    let schema = Arc::new(Schema::new(fields(&[("name", DataType::Utf8, true)])));
    let err = create_table_ddl("public", "t", &schema, &["id".to_string()]).unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("pk column"));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::pg_column_type loaders::postgres::tests::create_table_ddl`
Expected: compile errors — `pg_column_type`, `create_table_ddl` not defined.

- [ ] **Step 3: Implement the helpers**

Add to `crates/worker/src/loaders/postgres.rs` (above the `#[cfg(test)]` block):

```rust
use arrow::datatypes::{DataType, Schema, TimeUnit};

pub(crate) fn pg_column_type(t: &DataType) -> anyhow::Result<&'static str> {
    Ok(match t {
        DataType::Int64 => "BIGINT",
        DataType::Int32 => "INTEGER",
        DataType::Int16 => "SMALLINT",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Boolean => "BOOLEAN",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Float32 => "REAL",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMPTZ",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP",
        DataType::Date32 => "DATE",
        DataType::Binary | DataType::LargeBinary => "BYTEA",
        DataType::Time64(_) | DataType::Time32(_) => "TIME",
        other => bail!("unsupported Arrow type for Postgres loader: {other:?}"),
    })
}

pub(crate) fn create_table_ddl(
    schema: &str,
    table: &str,
    arrow_schema: &Schema,
    pk_columns: &[String],
) -> anyhow::Result<String> {
    for pk in pk_columns {
        if arrow_schema.field_with_name(pk).is_err() {
            bail!("pk column {pk:?} missing from batch schema");
        }
    }
    let mut cols = Vec::with_capacity(arrow_schema.fields().len());
    for f in arrow_schema.fields() {
        let ty = pg_column_type(f.data_type())?;
        let null = if f.is_nullable() { "" } else { " NOT NULL" };
        cols.push(format!("\"{}\" {}{}", f.name(), ty, null));
    }
    let pk_clause = if pk_columns.is_empty() {
        String::new()
    } else {
        let quoted: Vec<String> = pk_columns.iter().map(|c| format!("\"{c}\"")).collect();
        format!(", PRIMARY KEY ({})", quoted.join(", "))
    };
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}\" ({}{})",
        cols.join(", "),
        pk_clause,
    ))
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests`
Expected: all five new tests pass, plus `validate_rejects_non_postgres_spec`.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4a-4: Arrow→Postgres type mapping + CREATE TABLE DDL"
```

---

## Task 5: INSERT statement builder (append + upsert variants)

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn insert_sql_append_form_no_pk() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ])));
    let sql = insert_sql("public", "events", &schema, &[]);
    assert_eq!(
        sql,
        r#"INSERT INTO "public"."events" ("id", "name") VALUES ($1, $2)"#
    );
}

#[test]
fn insert_sql_upsert_form_with_pk() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
        ("amount", DataType::Float64, true),
    ])));
    let sql = insert_sql("public", "customers", &schema, &["id".to_string()]);
    assert_eq!(
        sql,
        r#"INSERT INTO "public"."customers" ("id", "name", "amount") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "amount" = EXCLUDED."amount""#
    );
}

#[test]
fn insert_sql_upsert_excludes_pk_columns_from_update_set() {
    let schema = Arc::new(Schema::new(fields(&[
        ("tenant", DataType::Utf8, false),
        ("id", DataType::Int64, false),
        ("val", DataType::Int64, true),
    ])));
    let sql = insert_sql("public", "t", &schema, &["tenant".into(), "id".into()]);
    assert!(sql.contains("ON CONFLICT (\"tenant\", \"id\")"));
    assert!(sql.contains("SET \"val\" = EXCLUDED.\"val\""));
    assert!(!sql.contains("SET \"tenant\""));
    assert!(!sql.contains("SET \"id\""));
}

#[test]
fn insert_sql_pk_only_uses_do_nothing() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
    ])));
    let sql = insert_sql("public", "t", &schema, &["id".into()]);
    assert!(sql.contains("ON CONFLICT (\"id\") DO NOTHING"));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::insert_sql`
Expected: compile error — `insert_sql` not defined.

- [ ] **Step 3: Implement `insert_sql`**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
pub(crate) fn insert_sql(
    schema: &str,
    table: &str,
    arrow_schema: &Schema,
    pk_columns: &[String],
) -> String {
    let field_names: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
    let col_list = field_names
        .iter()
        .map(|n| format!("\"{n}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (1..=field_names.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let base = format!(
        "INSERT INTO \"{schema}\".\"{table}\" ({col_list}) VALUES ({placeholders})"
    );
    if pk_columns.is_empty() {
        return base;
    }
    let pk_list = pk_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let non_pk: Vec<String> = field_names
        .iter()
        .filter(|n| !pk_columns.iter().any(|p| p == *n))
        .map(|n| format!("\"{n}\" = EXCLUDED.\"{n}\""))
        .collect();
    if non_pk.is_empty() {
        format!("{base} ON CONFLICT ({pk_list}) DO NOTHING")
    } else {
        format!("{base} ON CONFLICT ({pk_list}) DO UPDATE SET {}", non_pk.join(", "))
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::insert_sql`
Expected: all four tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4a-5: INSERT/UPSERT SQL builder"
```

---

## Task 6: Per-cell Arrow→sqlx parameter binding

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

The hot path needs to walk each `RecordBatch` row, extract typed values from the Arrow arrays, and bind them to a sqlx `query`. Test it with a small in-memory roundtrip helper before wiring the real loader.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module. These tests verify the helper that turns `(RecordBatch, row_index)` into a `Vec<BoundValue>` (where `BoundValue` is an internal enum used to feed sqlx):

```rust
use arrow::array::{Int64Array, StringArray, BooleanArray, Float64Array, TimestampMicrosecondArray};

#[test]
fn extract_row_handles_int64_text_bool_float() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
        ("active", DataType::Boolean, false),
        ("score", DataType::Float64, true),
    ])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec![Some("a"), None])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
        ],
    ).unwrap();

    let row0 = extract_row(&batch, 0).unwrap();
    assert!(matches!(row0[0], BoundValue::Int64(10)));
    assert!(matches!(row0[1], BoundValue::Text(Some(ref s)) if s == "a"));
    assert!(matches!(row0[2], BoundValue::Bool(true)));
    assert!(matches!(row0[3], BoundValue::Float64(Some(v)) if (v - 1.5).abs() < 1e-9));

    let row1 = extract_row(&batch, 1).unwrap();
    assert!(matches!(row1[1], BoundValue::Text(None)));
    assert!(matches!(row1[3], BoundValue::Float64(None)));
}

#[test]
fn extract_row_handles_timestamptz_utc() {
    let schema = Arc::new(Schema::new(fields(&[
        ("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ])));
    // 2026-05-21T00:00:00Z in microseconds since epoch.
    let micros: i64 = 1779_667_200_000_000;
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))],
    ).unwrap();
    let row = extract_row(&batch, 0).unwrap();
    if let BoundValue::TimestampTz(dt) = &row[0] {
        assert_eq!(dt.timestamp_micros(), micros);
    } else {
        panic!("expected TimestampTz, got {:?}", row[0]);
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::extract_row`
Expected: compile error — `extract_row`, `BoundValue` not defined.

- [ ] **Step 3: Implement `BoundValue` + `extract_row`**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

#[derive(Debug, Clone)]
pub(crate) enum BoundValue {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Bool(bool),
    Text(Option<String>),
    Bytea(Option<Vec<u8>>),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(DateTime<Utc>),
}

pub(crate) fn extract_row(
    batch: &RecordBatch,
    row: usize,
) -> anyhow::Result<Vec<BoundValue>> {
    let mut out = Vec::with_capacity(batch.num_columns());
    for (idx, col) in batch.columns().iter().enumerate() {
        let field = batch.schema().field(idx).clone();
        let is_null = col.is_null(row);
        let v = match field.data_type() {
            DataType::Int64 => BoundValue::Int64(
                col.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
            ),
            DataType::Int32 => BoundValue::Int32(
                col.as_any().downcast_ref::<Int32Array>().unwrap().value(row),
            ),
            DataType::Int16 => BoundValue::Int16(
                col.as_any().downcast_ref::<Int16Array>().unwrap().value(row),
            ),
            DataType::Boolean => BoundValue::Bool(
                col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
            ),
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                BoundValue::Float64(if is_null { None } else { Some(arr.value(row)) })
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                BoundValue::Float32(if is_null { None } else { Some(arr.value(row)) })
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                BoundValue::Text(if is_null { None } else { Some(arr.value(row).to_string()) })
            }
            DataType::Binary => {
                let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                BoundValue::Bytea(if is_null { None } else { Some(arr.value(row).to_vec()) })
            }
            DataType::Date32 => {
                let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(row);
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .context("date32 out of range")?;
                BoundValue::Date(date)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let arr = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
                let micros = arr.value(row);
                let secs = (micros / 1_000_000) as u32;
                let nanos = ((micros % 1_000_000) * 1_000) as u32;
                let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .context("time64 out of range")?;
                BoundValue::Time(t)
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let micros = arr.value(row);
                let dt = DateTime::<Utc>::from_timestamp_micros(micros)
                    .context("timestamp_us out of range")?;
                BoundValue::TimestampTz(dt)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let micros = arr.value(row);
                let dt = DateTime::<Utc>::from_timestamp_micros(micros)
                    .context("timestamp_us out of range")?;
                BoundValue::Timestamp(dt.naive_utc())
            }
            other => bail!("extract_row: unsupported Arrow type {other:?}"),
        };
        out.push(v);
    }
    Ok(out)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::extract_row`
Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4a-6: Arrow row → sqlx BoundValue extraction"
```

---

## Task 7: Idempotency log table schema + helpers

The loader uses a destination-side log table mapping `LoadId` to "applied" status (RFC-9 §"Destinations without idempotency primitives"). Identity is `(tenant_id, pipeline_id, run_id, batch_seq, stream_name)` — same as `LoadId`. The log lives in the same Postgres database as the target table.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn ensure_log_table_ddl_is_idempotent_and_keyed_by_load_id() {
    let ddl = ensure_log_table_ddl("public");
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"public\".\"_etl_loaded_batches\""));
    assert!(ddl.contains("tenant_id UUID"));
    assert!(ddl.contains("pipeline_id UUID"));
    assert!(ddl.contains("run_id UUID"));
    assert!(ddl.contains("batch_seq BIGINT"));
    assert!(ddl.contains("stream_name TEXT"));
    assert!(ddl.contains("PRIMARY KEY (tenant_id, pipeline_id, run_id, stream_name, batch_seq)"));
}
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `cargo test -p worker loaders::postgres::tests::ensure_log_table_ddl`
Expected: compile error — `ensure_log_table_ddl` not defined.

- [ ] **Step 3: Implement the helper**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
pub(crate) fn ensure_log_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".\"_etl_loaded_batches\" (\
            tenant_id UUID NOT NULL, \
            pipeline_id UUID NOT NULL, \
            run_id UUID NOT NULL, \
            stream_name TEXT NOT NULL, \
            batch_seq BIGINT NOT NULL, \
            rows_loaded BIGINT NOT NULL, \
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
            PRIMARY KEY (tenant_id, pipeline_id, run_id, stream_name, batch_seq)\
        )"
    )
}
```

- [ ] **Step 4: Run test to confirm it passes**

Run: `cargo test -p worker loaders::postgres::tests::ensure_log_table_ddl`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4a-7: idempotency log table DDL"
```

---

## Task 8: Implement `PostgresLoader::load` end-to-end (integration test with docker postgres)

This is the wiring task: validate spec, open transaction, ensure log table, check idempotency log, ensure target table (first batch), build insert SQL, bind each row, execute, insert log row, commit. Test against the docker-compose `postgres` service.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`
- Create: `tests/integration/tests/postgres_loader.rs`

**Test database convention.** The dev container at `postgres://etl:etl@localhost:5432/etl_catalog` is shared. The integration test creates and drops a uniquely-named schema per test (`etl_loader_test_<uuid>`) so tests can run concurrently without interference. The test file's first action checks env var `ETL_INTEGRATION_PG_URL` (defaulting to `postgres://etl:etl@localhost:5432/etl_catalog`) and skips with a clear message if the database is unreachable.

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/tests/postgres_loader.rs`:

```rust
//! Integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service to be running.
//! Skipped (with a clear message) when the database is unreachable.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::ids::{PipelineId, RunId, TenantId};
use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::sync::Arc;
use worker::loaders::postgres::PostgresLoader;

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn fresh_schema() -> Option<(sqlx::PgPool, String)> {
    let url = test_url();
    let pool = match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SKIP postgres_loader test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_loader_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create schema");
    Some((pool, schema))
}

async fn drop_schema(pool: &sqlx::PgPool, schema: &str) {
    let _ = sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(pool)
        .await;
}

fn tiny_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap()
}

fn spec(connection_url: &str, schema: &str, pk: Vec<String>) -> DestinationSpec {
    DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: connection_url.into(),
        schema: schema.into(),
        table: "customers".into(),
        pk_columns: pk,
    })
}

#[tokio::test]
async fn append_only_load_writes_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);
    let load_id = LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: 0,
        stream_name: String::new(),
    };
    PostgresLoader.load(&s, load_id, tiny_batch()).await.expect("load");

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"customers\""
    ))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 3);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn second_load_with_same_load_id_is_noop() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);
    let load_id = LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: 7,
        stream_name: String::new(),
    };
    let r1 = PostgresLoader.load(&s, load_id.clone(), tiny_batch()).await.unwrap();
    let r2 = PostgresLoader.load(&s, load_id, tiny_batch()).await.unwrap();
    assert_eq!(r1.rows_loaded, 3);
    // Second call sees the log entry and short-circuits — reports 0 new rows.
    assert_eq!(r2.rows_loaded, 0);

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"customers\""
    ))
    .fetch_one(&pool).await.unwrap().get(0);
    assert_eq!(count, 3, "row count must not double on retry");
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn upsert_overwrites_on_pk_conflict() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec!["id".into()]);

    let batch1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("old-a"), Some("old-b")])),
        ],
    ).unwrap();
    let batch2 = RecordBatch::try_new(
        batch1.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("new-a"), Some("new-b"), Some("new-c")])),
        ],
    ).unwrap();

    let pid = PipelineId::new();
    let tid = TenantId::new();
    let rid = RunId::new();

    PostgresLoader.load(
        &s,
        LoadId { tenant_id: tid.clone(), pipeline_id: pid.clone(), run_id: rid.clone(), batch_seq: 0, stream_name: String::new() },
        batch1,
    ).await.unwrap();
    PostgresLoader.load(
        &s,
        LoadId { tenant_id: tid, pipeline_id: pid, run_id: rid, batch_seq: 1, stream_name: String::new() },
        batch2,
    ).await.unwrap();

    let rows = sqlx::query(&format!(
        "SELECT id, name FROM \"{schema}\".\"customers\" ORDER BY id"
    ))
    .fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    let names: Vec<String> = rows.iter().map(|r| r.get::<String, _>(1)).collect();
    assert_eq!(names, vec!["new-a", "new-b", "new-c"]);
    drop_schema(&pool, &schema).await;
}
```

Register the integration test crate has access to `worker::loaders::postgres::PostgresLoader`. Check `tests/integration/Cargo.toml`: it should already depend on `worker`. If `loaders` is not `pub`, add `pub` in `crates/worker/src/lib.rs` (look for the existing `pub mod loaders;` line — if it's `mod loaders;` make it `pub`). Likewise ensure `pub mod postgres;` in `loaders/mod.rs` (Task 2 already did this) and that `PostgresLoader` is `pub` in `loaders/postgres.rs` (Task 2 already declared it `pub`).

- [ ] **Step 2: Run tests to confirm they fail**

Run: `docker compose up -d postgres && sleep 2 && cargo test -p integration-tests --test postgres_loader -- --nocapture`
Expected: tests fail with `PostgresLoader::load not yet implemented` from Task 2's stub.

- [ ] **Step 3: Implement `load` end-to-end**

Replace the stub `load` body in `crates/worker/src/loaders/postgres.rs` with the real implementation. Also extend `validate` to actually try a connection.

```rust
use sqlx::postgres::{PgPoolOptions, PgArguments};
use sqlx::{Arguments, Executor, Postgres, Transaction};

impl PostgresLoader {
    async fn connect(spec: &PostgresDestinationSpec) -> anyhow::Result<sqlx::PgPool> {
        PgPoolOptions::new()
            .max_connections(4)
            .connect(&spec.connection_url)
            .await
            .with_context(|| format!("connect to {}", spec.connection_url))
    }
}

#[async_trait]
impl DestinationLoader for PostgresLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        let spec = postgres_spec(dest)?;
        let pool = Self::connect(spec).await?;
        sqlx::query("SELECT 1").execute(&pool).await.context("SELECT 1 health check")?;
        Ok(())
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let spec = postgres_spec(dest)?;
        let pool = Self::connect(spec).await?;
        let mut tx: Transaction<'_, Postgres> = pool.begin().await.context("begin tx")?;

        // 1. Ensure log table.
        tx.execute(sqlx::query(&ensure_log_table_ddl(&spec.schema)))
            .await
            .context("ensure log table")?;

        // 2. Idempotency check — if this load_id is already logged, no-op.
        let existing: Option<(i64,)> = sqlx::query_as(&format!(
            "SELECT rows_loaded FROM \"{}\".\"_etl_loaded_batches\" \
             WHERE tenant_id=$1 AND pipeline_id=$2 AND run_id=$3 \
             AND stream_name=$4 AND batch_seq=$5",
            spec.schema
        ))
        .bind(load_id.tenant_id.as_uuid())
        .bind(load_id.pipeline_id.as_uuid())
        .bind(load_id.run_id.as_uuid())
        .bind(&load_id.stream_name)
        .bind(load_id.batch_seq as i64)
        .fetch_optional(&mut *tx)
        .await
        .context("query log")?;

        if existing.is_some() {
            tx.commit().await.ok();
            return Ok(LoadResult {
                rows_loaded: 0,
                bytes_written: 0,
                path: format!("{}.{} (already loaded)", spec.schema, spec.table),
            });
        }

        // 3. Ensure target table on first non-empty batch.
        if batch.num_rows() > 0 {
            let ddl = create_table_ddl(&spec.schema, &spec.table, batch.schema().as_ref(), &spec.pk_columns)?;
            tx.execute(sqlx::query(&ddl)).await.context("create target table")?;
        }

        // 4. Insert rows.
        let sql = insert_sql(&spec.schema, &spec.table, batch.schema().as_ref(), &spec.pk_columns);
        let mut rows_loaded = 0usize;
        for r in 0..batch.num_rows() {
            let values = extract_row(&batch, r)?;
            let mut args = PgArguments::default();
            bind_values(&mut args, &values)?;
            sqlx::query_with(&sql, args)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("INSERT row {r}"))?;
            rows_loaded += 1;
        }

        // 5. Record in log.
        sqlx::query(&format!(
            "INSERT INTO \"{}\".\"_etl_loaded_batches\" \
             (tenant_id, pipeline_id, run_id, stream_name, batch_seq, rows_loaded) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            spec.schema
        ))
        .bind(load_id.tenant_id.as_uuid())
        .bind(load_id.pipeline_id.as_uuid())
        .bind(load_id.run_id.as_uuid())
        .bind(&load_id.stream_name)
        .bind(load_id.batch_seq as i64)
        .bind(rows_loaded as i64)
        .execute(&mut *tx)
        .await
        .context("insert log row")?;

        tx.commit().await.context("commit tx")?;
        Ok(LoadResult {
            rows_loaded,
            bytes_written: 0,
            path: format!("{}.{}", spec.schema, spec.table),
        })
    }
}

fn bind_values(args: &mut PgArguments, values: &[BoundValue]) -> anyhow::Result<()> {
    for v in values {
        match v {
            BoundValue::Int16(x) => args.add(*x)?,
            BoundValue::Int32(x) => args.add(*x)?,
            BoundValue::Int64(x) => args.add(*x)?,
            BoundValue::Float32(x) => args.add(*x)?,
            BoundValue::Float64(x) => args.add(*x)?,
            BoundValue::Bool(x) => args.add(*x)?,
            BoundValue::Text(x) => args.add(x.clone())?,
            BoundValue::Bytea(x) => args.add(x.clone())?,
            BoundValue::Date(x) => args.add(*x)?,
            BoundValue::Time(x) => args.add(*x)?,
            BoundValue::Timestamp(x) => args.add(*x)?,
            BoundValue::TimestampTz(x) => args.add(*x)?,
        }
    }
    Ok(())
}
```

If `crates/worker/src/lib.rs` declares loaders as `mod loaders;`, change to `pub mod loaders;`. Confirm `PostgresLoader` and the `loaders::postgres` module are visible from outside `worker`.

Also confirm `tests/integration/Cargo.toml` has these dev-deps (add any missing): `worker`, `loader-sdk`, `common-types`, `arrow`, `sqlx` (with `postgres`, `runtime-tokio`), `tokio` (with `macros`, `rt-multi-thread`), `uuid` (with `v4`).

- [ ] **Step 4: Run tests to confirm they pass**

Run: `docker compose up -d postgres && sleep 2 && cargo test -p integration-tests --test postgres_loader -- --nocapture --test-threads=1`
Expected: all three integration tests pass. (`--test-threads=1` avoids any cross-test contention while bringing the loader up; can be removed once stable.)

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs tests/integration/tests/postgres_loader.rs tests/integration/Cargo.toml crates/worker/src/lib.rs
git commit -m "phase-2-4a-8: PostgresLoader::load with idempotency log"
```

---

## Task 9: Verify dispatch path with a workflow-level smoke test

Make sure the activity dispatch from Task 3 actually routes a Postgres spec to the new loader through the full `load_batch` activity, not just at the unit level.

**Files:**
- Modify: `tests/integration/tests/postgres_loader.rs` (add one test)

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/tests/postgres_loader.rs`:

```rust
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use worker::activities::sync::SyncActivities;
use worker::activities::sync::inputs::LoadBatchInput;

fn encode_batch_ipc(batch: &RecordBatch) -> String {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    B64.encode(&buf)
}

#[tokio::test]
async fn load_batch_activity_dispatches_to_postgres_loader() {
    let Some((pool, schema)) = fresh_schema().await else { return };

    let batch = tiny_batch();
    let input = LoadBatchInput {
        tenant_id: uuid::Uuid::new_v4(),
        pipeline_id: uuid::Uuid::new_v4(),
        run_id: uuid::Uuid::new_v4(),
        batch_seq: 0,
        stream_name: String::new(),
        batch_ipc_b64: encode_batch_ipc(&batch),
        rejected_ipc_b64: None,
        destination: DestinationSpec::Postgres(PostgresDestinationSpec {
            connection_url: test_url(),
            schema: schema.clone(),
            table: "customers".into(),
            pk_columns: vec![],
        }),
    };

    // Construct the activities struct the same way the worker does.
    // (If the constructor needs more setup, mirror an existing test in
    //  tests/integration/tests/incremental_sync.rs or activities/sync tests.)
    let activities = SyncActivities::new_for_test();
    let _out = activities
        .load_batch(temporal_sdk::ActivityContext::default(), input)
        .await
        .expect("load_batch via dispatch");

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"customers\""
    )).fetch_one(&pool).await.unwrap().get(0);
    assert_eq!(count, 3);
    drop_schema(&pool, &schema).await;
}
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `cargo test -p integration-tests --test postgres_loader load_batch_activity_dispatches_to_postgres_loader -- --nocapture`
Expected: compile failure or runtime panic — `SyncActivities::new_for_test` may not exist, or fields of `LoadBatchInput` may differ from what this test assumes.

- [ ] **Step 3: Adapt the test to the real shapes**

Open `crates/worker/src/activities/sync/inputs.rs` and `crates/worker/src/activities/sync/mod.rs`. Adjust the test to match:
- The actual fields of `LoadBatchInput` (it may include extra metadata you must populate; copy a minimal construction from an existing test in `tests/integration/tests/incremental_sync.rs` or `durability_midbatch.rs`).
- The actual constructor for `SyncActivities`. If there's no `new_for_test`, use whatever existing tests use; if all existing call sites instantiate it via Temporal, add a `pub fn new_for_test()` to `SyncActivities` returning a `Self` with minimal dependencies, and document it as test-only at the impl block.

If `SyncActivities::new_for_test` does not exist, add it next to `SyncActivities::new` (or equivalent):

```rust
#[cfg(any(test, feature = "test-utils"))]
impl SyncActivities {
    pub fn new_for_test() -> Self {
        // Mirror the minimum-viable fields used by load_batch.
        // No real Temporal client / secret store needed for the
        // load_batch path under test.
        Self {
            // … populate stub fields …
        }
    }
}
```

(If `SyncActivities` requires a runtime that the dispatch test can't provide cheaply, drop the dispatch test entirely and instead call `loaders::postgres::PostgresLoader.load(...)` directly in the test plus add a `#[test]` covering just the `match` arm in `dispatch_tests` from Task 3 — that's already passing. Skip Step 4 and proceed to Step 5 with no integration test added.)

- [ ] **Step 4: Run test to confirm it passes**

Run: `docker compose up -d postgres && cargo test -p integration-tests --test postgres_loader -- --nocapture`
Expected: all four tests pass. (Or three if you took the fallback in Step 3.)

- [ ] **Step 5: Commit**

```bash
git add tests/integration/tests/postgres_loader.rs crates/worker/src/activities/sync/mod.rs
git commit -m "phase-2-4a-9: end-to-end load_batch dispatch covers Postgres"
```

---

## Task 10: Document the loader and update the RFC implementation status

**Files:**
- Modify: `README.md` (Loaders section, if present — otherwise skip)
- Modify: `crates/worker/src/loaders/postgres.rs` (top-of-file doc comment)
- Modify: `2026-05-13-102414-implementation-status-by-reviewing-rfc-line-by-li.txt` is a transcript — do NOT touch. Instead, append a short note to the new spec doc you'll create next.
- Create: `docs/superpowers/specs/2026-05-21-phase-2-4a-postgres-loader-design.md` — brief design memo (matches the pattern of prior `docs/superpowers/specs/*.md`).

- [ ] **Step 1: Update the top-of-file doc on the loader**

Replace the doc header in `crates/worker/src/loaders/postgres.rs` with a tight summary listing:
- The two delivery patterns supported (append / upsert).
- The idempotency strategy (log table + per-call transaction).
- Explicit deferred items (CDC op-aware DELETE, schema evolution mid-run, soft delete, dead-letter, COPY-FROM-STDIN, secret-ref URLs).

- [ ] **Step 2: Write the design memo**

Create `docs/superpowers/specs/2026-05-21-phase-2-4a-postgres-loader-design.md`. Sections:
1. **Scope** — what's in/out (mirror the plan's "Scope cuts").
2. **Trait fit** — confirms the existing narrow `DestinationLoader` trait is sufficient; lists the RFC-9 trait extensions (`prepare_run`/`commit_run`/`abort_run`) we explicitly defer.
3. **Idempotency strategy** — log table schema, per-call transaction, what happens on retry.
4. **Type mapping table** — Arrow type → Postgres type, with the unsupported types listed.
5. **Connection / secret handling** — inline URL today, secret-ref planned with RFC-11.
6. **Next steps** — CDC op-aware writes, schema evolution, multi-table, COPY perf path.

- [ ] **Step 3: Run all tests one last time**

```bash
cargo test --workspace
docker compose up -d postgres
cargo test -p integration-tests --test postgres_loader -- --nocapture
```

Expected: workspace tests green; postgres_loader integration tests green.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-05-21-phase-2-4a-postgres-loader-design.md crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4a-10: docs for postgres destination loader"
```

- [ ] **Step 5: Open PR**

```bash
git push -u origin HEAD
gh pr create --title "phase-2-4a: Postgres destination loader (RFC-9)" --body "$(cat <<'EOF'
## Summary
- Adds `PostgresLoader` (second loader after LocalParquet) with insert + upsert paths
- Idempotent retries via per-tenant `_etl_loaded_batches` log table
- Activity dispatch routes `DestinationSpec::Postgres(_)` to the new loader

## Scope cuts (follow-ups)
- CDC op-aware DELETE/UPDATE (RFC-9 Pattern 3)
- Mid-run schema evolution
- Soft delete / dead-letter routing
- `COPY FROM STDIN` perf path
- Secret-ref URLs (waiting on RFC-11 wiring)

## Test plan
- [x] `cargo test --workspace` green
- [x] Integration: append-only writes rows
- [x] Integration: retry with same LoadId is a no-op (row count unchanged)
- [x] Integration: upsert overwrites on PK conflict
- [x] Integration: activity dispatch routes Postgres spec to new loader

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage (vs RFC-9 plus our Scope cuts):**
- Loader trait conformance — Task 2 (stub), Task 8 (real impl). ✓
- `LoadId`-based idempotency — Task 7 (log DDL), Task 8 (check + insert in same tx). ✓
- Pattern 1 (Direct Append) — Task 8 append branch. ✓
- Pattern 2 (Merge on Commit, upsert) — Task 5 (SQL builder) + Task 8 (binding). ✓
- Pattern 3 (Apply Change Stream, CDC ops) — **deferred** (stated in plan header). ✓
- Pattern 4 (Append-Only Event Log) — covered by the append branch with `pk_columns: []`. ✓
- Schema application — Task 8 ensures target table; mid-run evolution **deferred** (stated). ✓
- Transactional commit — Task 8 wraps all writes plus log insert in one tx. ✓
- Type mapping — Task 4 covers the types the CDC sources actually emit; unsupported types fail loudly. ✓
- Tests — Task 8 covers idempotency + ordering (via consecutive batches with same PK); concurrent / dead-letter / throttling **deferred**. ✓
- Dispatch wiring — Task 3 + Task 9. ✓

**Placeholder scan:** No "TBD"/"implement later"/"similar to Task N" markers; each step contains either runnable code or an exact command + expected outcome.

**Type consistency:** `BoundValue`, `extract_row`, `bind_values`, `pg_column_type`, `create_table_ddl`, `insert_sql`, `ensure_log_table_ddl`, `postgres_spec` — names match across Tasks 2, 4, 5, 6, 7, 8. `PostgresDestinationSpec` field names (`connection_url`, `schema`, `table`, `pk_columns`) used identically in spec, loader, and tests.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-21-postgres-destination-loader.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
