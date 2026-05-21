# Phase 2.4.b: Postgres Loader — CDC op-aware writes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `PostgresLoader` route CDC events by `_cdc.op` (insert/update/snapshot ⇒ upsert; delete ⇒ DELETE) so PG-source CDC pipelines can land in a PG destination with correct row state.

**Architecture:** Auto-detect CDC mode by presence of the `_cdc.op` column on the incoming `RecordBatch`. In CDC mode: require `pk_columns`, build the destination DDL from the schema with `_cdc.*` stripped, and walk rows in order — `i`/`u`/`s` rows go through the existing upsert SQL, `d` rows go through a new per-row DELETE keyed on the configured PKs. Non-CDC batches keep today's append/upsert behavior unchanged. All ops still execute inside the single per-call sqlx transaction protected by the `_etl_loaded_batches` idempotency log.

**Tech Stack:** Rust · existing `sqlx 0.8` + `arrow 53` · CDC column convention from `common_types::cdc` (`COL_OP`, `COL_LSN`, `COL_COMMIT_TS`, `COL_TXID`) · docker-compose `postgres:16` for integration.

**Scope cuts (deferred, not in this plan):**
- Schema-change events (`_cdc.op = "c"`) — logged + skipped, not applied. Schema evolution is its own work item.
- Truncate events (`_cdc.op = "t"`, used by PG CDC snapshot per `decode.rs:32`) — logged + skipped for now; treating as full-table DELETE is destructive and warrants explicit pipeline-level opt-in.
- PK-change updates that don't also emit a delete of the old key — out of scope; documented as a known limitation.
- Soft-delete / tombstone columns.
- Per-row dead-letter for CDC failures.
- Audit-log destination mode (keep `_cdc.*` columns in the destination table). Today's default is "destination looks like source" — strip metadata.
- Multi-table per spec (still one target table per pipeline; same as phase-2-4a).

---

## File Structure

**Modify:**
- `crates/worker/src/loaders/postgres.rs` — add CDC detection, `cdc_data_schema`, `delete_sql`, `cdc_op_at`, `extract_data_row`, and dispatch in `load()` for CDC batches.

**Create:**
- `tests/integration/tests/postgres_loader_cdc.rs` — integration tests against docker `postgres:16` covering insert/update/delete/mixed/snapshot-handoff cases.

**No changes to:**
- `loader_sdk` (`DestinationLoader` trait + `LoadId`) — the existing surface is sufficient.
- `PostgresDestinationSpec` — same fields; CDC mode is data-driven (schema has `_cdc.op`).
- The `load_batch` activity dispatch from phase-2-4a — Postgres routing already in place.

---

## Task 1: Detect CDC batches by `_cdc.op` presence

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/worker/src/loaders/postgres.rs`:

```rust
#[test]
fn is_cdc_batch_true_when_op_column_present() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
    ])));
    assert!(is_cdc_batch(&schema));
}

#[test]
fn is_cdc_batch_false_for_plain_data_schema() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ])));
    assert!(!is_cdc_batch(&schema));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::is_cdc_batch`
Expected: compile error — `is_cdc_batch` not defined.

- [ ] **Step 3: Implement `is_cdc_batch`**

Add to `crates/worker/src/loaders/postgres.rs` (top-level, near the other helpers):

```rust
pub(crate) fn is_cdc_batch(schema: &Schema) -> bool {
    schema.field_with_name(common_types::cdc::COL_OP).is_ok()
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::is_cdc_batch`
Expected: 2 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4b-1: detect CDC batches by _cdc.op presence"
```

---

## Task 2: Strip `_cdc.*` columns to build destination schema

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn cdc_data_schema_drops_metadata_columns() {
    let schema = Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
        (common_types::cdc::COL_LSN, DataType::Utf8, false),
        (
            common_types::cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let stripped = cdc_data_schema(&schema);
    let names: Vec<&str> = stripped.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
}

#[test]
fn cdc_data_schema_is_identity_on_non_cdc_schema() {
    let schema = Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ]));
    let stripped = cdc_data_schema(&schema);
    let names: Vec<&str> = stripped.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
}

#[test]
fn cdc_data_field_indices_lists_non_cdc_columns_in_order() {
    let schema = Schema::new(fields(&[
        ("id", DataType::Int64, false),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
        ("name", DataType::Utf8, true),
        (common_types::cdc::COL_LSN, DataType::Utf8, false),
    ]));
    assert_eq!(cdc_data_field_indices(&schema), vec![0usize, 2]);
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::cdc_data_schema loaders::postgres::tests::cdc_data_field_indices`
Expected: compile error — `cdc_data_schema`, `cdc_data_field_indices` not defined.

- [ ] **Step 3: Implement the helpers**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
fn is_cdc_metadata_col(name: &str) -> bool {
    name == common_types::cdc::COL_OP
        || name == common_types::cdc::COL_LSN
        || name == common_types::cdc::COL_COMMIT_TS
        || name == common_types::cdc::COL_TXID
}

pub(crate) fn cdc_data_schema(schema: &Schema) -> Schema {
    let kept: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .filter(|f| !is_cdc_metadata_col(f.name()))
        .map(|f| f.as_ref().clone())
        .collect();
    Schema::new(kept)
}

pub(crate) fn cdc_data_field_indices(schema: &Schema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| (!is_cdc_metadata_col(f.name())).then_some(i))
        .collect()
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::cdc_data`
Expected: 3 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4b-2: strip _cdc.* columns for destination schema"
```

---

## Task 3: Read `_cdc.op` at row index

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
use arrow::array::StringArray as ArrStr;

#[test]
fn cdc_op_at_returns_op_string_per_row() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
    ])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(ArrStr::from(vec!["i", "u", "d"])),
        ],
    )
    .unwrap();
    assert_eq!(cdc_op_at(&batch, 0).unwrap(), "i");
    assert_eq!(cdc_op_at(&batch, 1).unwrap(), "u");
    assert_eq!(cdc_op_at(&batch, 2).unwrap(), "d");
}

#[test]
fn cdc_op_at_errors_when_column_missing() {
    let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let err = cdc_op_at(&batch, 0).unwrap_err();
    assert!(format!("{err}").contains("_cdc.op"));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::cdc_op_at`
Expected: compile error — `cdc_op_at` not defined.

- [ ] **Step 3: Implement `cdc_op_at`**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
pub(crate) fn cdc_op_at<'a>(batch: &'a RecordBatch, row: usize) -> anyhow::Result<&'a str> {
    let idx = batch
        .schema()
        .index_of(common_types::cdc::COL_OP)
        .map_err(|_| anyhow::anyhow!("batch is missing _cdc.op column"))?;
    let col = batch.column(idx);
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("_cdc.op column is not Utf8"))?;
    Ok(arr.value(row))
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::cdc_op_at`
Expected: 2 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4b-3: read _cdc.op per row"
```

---

## Task 4: DELETE SQL builder

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn delete_sql_single_pk() {
    let sql = delete_sql("public", "customers", &["id".to_string()]);
    assert_eq!(sql, r#"DELETE FROM "public"."customers" WHERE "id" = $1"#);
}

#[test]
fn delete_sql_composite_pk() {
    let sql = delete_sql("public", "t", &["tenant".to_string(), "id".to_string()]);
    assert_eq!(
        sql,
        r#"DELETE FROM "public"."t" WHERE "tenant" = $1 AND "id" = $2"#
    );
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::delete_sql`
Expected: compile error — `delete_sql` not defined.

- [ ] **Step 3: Implement `delete_sql`**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
pub(crate) fn delete_sql(schema: &str, table: &str, pk_columns: &[String]) -> String {
    let where_clause = pk_columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("\"{c}\" = ${}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("DELETE FROM \"{schema}\".\"{table}\" WHERE {where_clause}")
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::delete_sql`
Expected: 2 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4b-4: DELETE SQL builder"
```

---

## Task 5: Extract non-CDC values from a row + extract PK values

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn extract_data_row_skips_cdc_columns() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
        ("name", DataType::Utf8, true),
        (common_types::cdc::COL_LSN, DataType::Utf8, false),
    ])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(ArrStr::from(vec!["i"])),
            Arc::new(ArrStr::from(vec![Some("hello")])),
            Arc::new(ArrStr::from(vec!["lsn-1"])),
        ],
    )
    .unwrap();
    let row = extract_data_row(&batch, 0).unwrap();
    assert_eq!(row.len(), 2);
    assert!(matches!(row[0], BoundValue::Int64(42)));
    assert!(matches!(row[1], BoundValue::Text(Some(ref s)) if s == "hello"));
}

#[test]
fn extract_pk_values_picks_pk_columns_in_order() {
    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("region", DataType::Utf8, false),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
    ])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(ArrStr::from(vec!["eu"])),
            Arc::new(ArrStr::from(vec!["d"])),
        ],
    )
    .unwrap();
    let pks = extract_pk_values(&batch, 0, &["region".into(), "id".into()]).unwrap();
    assert_eq!(pks.len(), 2);
    assert!(matches!(pks[0], BoundValue::Text(Some(ref s)) if s == "eu"));
    assert!(matches!(pks[1], BoundValue::Int64(7)));
}

#[test]
fn extract_pk_values_errors_on_missing_pk_column() {
    let schema = Arc::new(Schema::new(fields(&[("id", DataType::Int64, false)])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let err = extract_pk_values(&batch, 0, &["missing".into()]).unwrap_err();
    assert!(format!("{err}").contains("missing"));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::extract_data_row loaders::postgres::tests::extract_pk_values`
Expected: compile errors — `extract_data_row`, `extract_pk_values` not defined.

- [ ] **Step 3: Implement the helpers**

Add to `crates/worker/src/loaders/postgres.rs`:

```rust
pub(crate) fn extract_data_row(
    batch: &RecordBatch,
    row: usize,
) -> anyhow::Result<Vec<BoundValue>> {
    let schema = batch.schema();
    let keep = cdc_data_field_indices(schema.as_ref());
    // Reuse `extract_row` to honor every existing type-mapping rule, then
    // pick the data-column subset. The waste is bounded — schemas are
    // narrow and rows fit in cache.
    let full = extract_row(batch, row)?;
    Ok(keep.into_iter().map(|i| full[i].clone()).collect())
}

pub(crate) fn extract_pk_values(
    batch: &RecordBatch,
    row: usize,
    pk_columns: &[String],
) -> anyhow::Result<Vec<BoundValue>> {
    let schema = batch.schema();
    let full = extract_row(batch, row)?;
    let mut out = Vec::with_capacity(pk_columns.len());
    for pk in pk_columns {
        let idx = schema
            .index_of(pk)
            .map_err(|_| anyhow::anyhow!("pk column {pk:?} missing from batch schema"))?;
        out.push(full[idx].clone());
    }
    Ok(out)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::extract_data_row loaders::postgres::tests::extract_pk_values`
Expected: 3 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4b-5: extract data-row and pk-values excluding CDC cols"
```

---

## Task 6: CDC dispatch in `PostgresLoader::load`

This is the wiring task. Inside the existing `load()` body, after the idempotency check, branch on `is_cdc_batch(batch.schema())`. The non-CDC path is unchanged.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing unit test (error case — CDC mode requires pk_columns)**

Add to the `tests` module. This test exercises the validation that fires before any DB call, so it doesn't need a live database:

```rust
#[tokio::test]
async fn load_cdc_batch_errors_when_pk_columns_empty() {
    use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
    use common_types::ids::{PipelineId, RunId, TenantId};
    use loader_sdk::LoadId;

    let schema = Arc::new(Schema::new(fields(&[
        ("id", DataType::Int64, false),
        (common_types::cdc::COL_OP, DataType::Utf8, false),
    ])));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(ArrStr::from(vec!["i"])),
        ],
    )
    .unwrap();
    let spec = DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: "postgres://127.0.0.1:1/nope".into(),
        schema: "public".into(),
        table: "t".into(),
        pk_columns: vec![],
    });
    let load_id = LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: 0,
        stream_name: String::new(),
    };
    let err = PostgresLoader.load(&spec, load_id, batch).await.unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("cdc") && msg.contains("pk"), "got: {msg}");
}
```

- [ ] **Step 2: Run test to confirm it fails**

Run: `cargo test -p worker loaders::postgres::tests::load_cdc_batch_errors_when_pk_columns_empty`
Expected: FAIL — without CDC branch, the loader tries to connect to `127.0.0.1:1` and returns a connection error, not the expected validation error.

- [ ] **Step 3: Add the CDC dispatch + helper, and short-circuit the pk-check before the DB call**

In `crates/worker/src/loaders/postgres.rs`, modify `PostgresLoader::load` and add a new private async helper. Replace the section starting at `// 3. Ensure target table on first non-empty batch.` with:

```rust
        // 3. CDC vs plain. CDC mode is data-driven: any batch that carries
        //    `_cdc.op` is routed through the CDC path.
        if batch.num_rows() > 0 && is_cdc_batch(batch.schema().as_ref()) {
            if spec.pk_columns.is_empty() {
                bail!(
                    "CDC batch arrived at postgres loader but pk_columns is empty; \
                     CDC ops require a primary key for upsert/delete routing"
                );
            }
            cdc_apply(&mut tx, spec, &batch).await?;
        } else if batch.num_rows() > 0 {
            // Original non-CDC path: ensure target table, then INSERT every row.
            let ddl = create_table_ddl(
                &spec.schema,
                &spec.table,
                batch.schema().as_ref(),
                &spec.pk_columns,
            )?;
            tx.execute(sqlx::query(&ddl))
                .await
                .context("create target table")?;

            let sql = insert_sql(
                &spec.schema,
                &spec.table,
                batch.schema().as_ref(),
                &spec.pk_columns,
            );
            for r in 0..batch.num_rows() {
                let values = extract_row(&batch, r)?;
                let mut q = sqlx::query(&sql);
                for v in &values {
                    q = bind_one(q, v);
                }
                q.execute(&mut *tx)
                    .await
                    .with_context(|| format!("INSERT row {r}"))?;
            }
        }
```

Delete the now-duplicated INSERT loop that lived below this section in `load()` (the original `// 4. Insert rows.` block) — the non-CDC branch above replaces it. Keep step 5 ("Record in log.") and `tx.commit()` exactly as they are. The `rows_loaded` accounting must move to inside both branches; add a counter:

Replace the surrounding lines so the final shape of `load()` body (from the `// 3.` comment through `commit`) reads:

```rust
        let mut rows_loaded = 0usize;

        if batch.num_rows() > 0 && is_cdc_batch(batch.schema().as_ref()) {
            if spec.pk_columns.is_empty() {
                bail!(
                    "CDC batch arrived at postgres loader but pk_columns is empty; \
                     CDC ops require a primary key for upsert/delete routing"
                );
            }
            rows_loaded = cdc_apply(&mut tx, spec, &batch).await?;
        } else if batch.num_rows() > 0 {
            let ddl = create_table_ddl(
                &spec.schema,
                &spec.table,
                batch.schema().as_ref(),
                &spec.pk_columns,
            )?;
            tx.execute(sqlx::query(&ddl))
                .await
                .context("create target table")?;

            let sql = insert_sql(
                &spec.schema,
                &spec.table,
                batch.schema().as_ref(),
                &spec.pk_columns,
            );
            for r in 0..batch.num_rows() {
                let values = extract_row(&batch, r)?;
                let mut q = sqlx::query(&sql);
                for v in &values {
                    q = bind_one(q, v);
                }
                q.execute(&mut *tx)
                    .await
                    .with_context(|| format!("INSERT row {r}"))?;
                rows_loaded += 1;
            }
        }
```

Add the new `cdc_apply` function below `bind_one`:

```rust
async fn cdc_apply(
    tx: &mut Transaction<'_, Postgres>,
    spec: &PostgresDestinationSpec,
    batch: &RecordBatch,
) -> anyhow::Result<usize> {
    let data_schema = cdc_data_schema(batch.schema().as_ref());
    let ddl = create_table_ddl(&spec.schema, &spec.table, &data_schema, &spec.pk_columns)?;
    tx.execute(sqlx::query(&ddl))
        .await
        .context("create target table (cdc)")?;

    let upsert_sql = insert_sql(&spec.schema, &spec.table, &data_schema, &spec.pk_columns);
    let del_sql = delete_sql(&spec.schema, &spec.table, &spec.pk_columns);

    let mut applied = 0usize;
    for r in 0..batch.num_rows() {
        let op = cdc_op_at(batch, r)?;
        match op {
            "i" | "u" | "s" => {
                let values = extract_data_row(batch, r)?;
                let mut q = sqlx::query(&upsert_sql);
                for v in &values {
                    q = bind_one(q, v);
                }
                q.execute(&mut **tx)
                    .await
                    .with_context(|| format!("CDC upsert row {r}"))?;
                applied += 1;
            }
            "d" => {
                let pks = extract_pk_values(batch, r, &spec.pk_columns)?;
                let mut q = sqlx::query(&del_sql);
                for v in &pks {
                    q = bind_one(q, v);
                }
                q.execute(&mut **tx)
                    .await
                    .with_context(|| format!("CDC delete row {r}"))?;
                applied += 1;
            }
            "c" => {
                tracing::warn!(
                    target: "loader.postgres.cdc",
                    "schema-change CDC event skipped (mid-run schema evolution not implemented)"
                );
            }
            "t" => {
                tracing::warn!(
                    target: "loader.postgres.cdc",
                    "truncate CDC event skipped (destructive ops not auto-applied)"
                );
            }
            other => {
                bail!("unknown CDC op {other:?} at row {r}");
            }
        }
    }
    Ok(applied)
}
```

Update the log insertion's `rows_loaded` value at the bottom of `load()` — the existing code already uses `rows_loaded as i64`. Since the variable is now populated in both branches, no change is needed beyond making sure the binding declaration sits before the dispatch. The `LoadResult` constructed at the end already reads `rows_loaded`.

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres`
Expected: all prior tests still green; `load_cdc_batch_errors_when_pk_columns_empty` now passes.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4b-6: route CDC i/u/s/d in PostgresLoader::load"
```

---

## Task 7: Integration tests — insert / update / delete / mixed batch

**Files:**
- Create: `tests/integration/tests/postgres_loader_cdc.rs`

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/tests/postgres_loader_cdc.rs`. The test helpers mirror the layout of the phase-2-4a integration tests (`tests/integration/tests/postgres_loader.rs`), creating a fresh schema per test and skipping if Postgres is unreachable:

```rust
//! CDC-mode integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service to be running.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use common_types::cdc;
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
            eprintln!("SKIP postgres_loader_cdc test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_cdc_loader_test_{}", uuid::Uuid::new_v4().simple());
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

fn cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn batch_of(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(|s| s.to_string())).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.2.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();

    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC"),
            ),
        ],
    )
    .unwrap()
}

fn spec(connection_url: &str, schema: &str) -> DestinationSpec {
    DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: connection_url.into(),
        schema: schema.into(),
        table: "events".into(),
        pk_columns: vec!["id".into()],
    })
}

fn load_id(seq: u32) -> LoadId {
    LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: seq,
        stream_name: String::new(),
    }
}

async fn rows_in(pool: &sqlx::PgPool, schema: &str) -> Vec<(i64, Option<String>)> {
    sqlx::query(&format!(
        "SELECT id, name FROM \"{schema}\".\"events\" ORDER BY id"
    ))
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.get::<i64, _>(0), r.try_get::<String, _>(1).ok()))
    .collect()
}

#[tokio::test]
async fn cdc_inserts_create_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);
    let batch = batch_of(&[(1, Some("a"), "i"), (2, Some("b"), "i")]);
    PostgresLoader.load(&s, load_id(0), batch).await.expect("load");

    let rows = rows_in(&pool, &schema).await;
    assert_eq!(rows, vec![(1, Some("a".into())), (2, Some("b".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_updates_overwrite_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(&s, load_id(0), batch_of(&[(1, Some("old"), "i")]))
        .await
        .unwrap();
    PostgresLoader
        .load(&s, load_id(1), batch_of(&[(1, Some("new"), "u")]))
        .await
        .unwrap();

    assert_eq!(rows_in(&pool, &schema).await, vec![(1, Some("new".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_deletes_remove_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(&s, load_id(0), batch_of(&[(1, Some("a"), "i"), (2, Some("b"), "i")]))
        .await
        .unwrap();
    PostgresLoader
        .load(&s, load_id(1), batch_of(&[(1, None, "d")]))
        .await
        .unwrap();

    assert_eq!(rows_in(&pool, &schema).await, vec![(2, Some("b".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_mixed_batch_applies_in_order() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Seed.
    PostgresLoader
        .load(&s, load_id(0), batch_of(&[(1, Some("a"), "i"), (2, Some("b"), "i")]))
        .await
        .unwrap();
    // Mixed batch: update 1, delete 2, insert 3.
    PostgresLoader
        .load(
            &s,
            load_id(1),
            batch_of(&[
                (1, Some("a-prime"), "u"),
                (2, None, "d"),
                (3, Some("c"), "i"),
            ]),
        )
        .await
        .unwrap();

    assert_eq!(
        rows_in(&pool, &schema).await,
        vec![(1, Some("a-prime".into())), (3, Some("c".into()))]
    );
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_snapshot_then_streaming_converges() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Snapshot phase — `s` rows treated like upserts.
    PostgresLoader
        .load(
            &s,
            load_id(0),
            batch_of(&[(1, Some("snap-1"), "s"), (2, Some("snap-2"), "s")]),
        )
        .await
        .unwrap();
    // Streaming phase: update 1, insert 3.
    PostgresLoader
        .load(
            &s,
            load_id(1),
            batch_of(&[(1, Some("stream-1"), "u"), (3, Some("stream-3"), "i")]),
        )
        .await
        .unwrap();

    assert_eq!(
        rows_in(&pool, &schema).await,
        vec![
            (1, Some("stream-1".into())),
            (2, Some("snap-2".into())),
            (3, Some("stream-3".into())),
        ]
    );
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_retry_with_same_load_id_is_noop() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);
    let lid = load_id(0);

    let r1 = PostgresLoader
        .load(&s, lid.clone(), batch_of(&[(1, Some("a"), "i")]))
        .await
        .unwrap();
    let r2 = PostgresLoader
        .load(&s, lid, batch_of(&[(1, Some("a"), "i")]))
        .await
        .unwrap();
    assert_eq!(r1.rows_loaded, 1);
    assert_eq!(r2.rows_loaded, 0, "retry must short-circuit");

    assert_eq!(rows_in(&pool, &schema).await, vec![(1, Some("a".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_destination_table_has_no_metadata_columns() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);
    PostgresLoader
        .load(&s, load_id(0), batch_of(&[(1, Some("a"), "i")]))
        .await
        .unwrap();

    let cols: Vec<String> = sqlx::query(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'events' \
         ORDER BY ordinal_position",
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.get::<String, _>(0))
    .collect();
    assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
    drop_schema(&pool, &schema).await;
}
```

- [ ] **Step 2: Run tests to confirm they fail (or pass — verify they exercise the new path)**

Run: `docker compose up -d postgres && cargo test -p integration-tests --test postgres_loader_cdc -- --nocapture --test-threads=1`
Expected: tests run; failures (if any) point at concrete CDC handling bugs to fix. If Task 6 implemented `cdc_apply` correctly the suite should pass at first run, but if not, the failure surface should be small (one helper at a time).

- [ ] **Step 3: Iterate on failures until green**

If a test fails, the most likely diagnoses are:
1. `extract_data_row` index off-by-one — re-check `cdc_data_field_indices` returns indices into the *source* schema, not the data-only schema.
2. DDL drift between `create_table_ddl` calls in non-CDC vs CDC mode — the second-call DDL is `IF NOT EXISTS`, so it's a no-op, but if the *first* batch is non-CDC and the second is CDC, the table will already exist with `_cdc.*` columns; that's a known limitation (mixed-mode pipelines are out of scope). Confirm no test triggers this case.
3. Mixed-batch ordering — Postgres applies statements within a transaction in the order they're sent; the test relies on this. If a test fails on ordering, recheck that `cdc_apply` iterates `0..batch.num_rows()` and doesn't reorder.

Fix the responsible helper from Tasks 1–6, not the test, unless the test itself is wrong.

- [ ] **Step 4: Verify all integration tests pass**

Run: `cargo test -p integration-tests --test postgres_loader_cdc -- --test-threads=1`
Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/tests/postgres_loader_cdc.rs
git commit -m "phase-2-4b-7: CDC integration tests against docker postgres"
```

---

## Task 8: Documentation update

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs` (top-of-file doc)
- Create: `docs/superpowers/specs/2026-05-21-phase-2-4b-postgres-loader-cdc-design.md`

- [ ] **Step 1: Update the loader doc header**

Replace the "Deferred" bullet about CDC in `crates/worker/src/loaders/postgres.rs`'s top-of-file comment:

```rust
//! ## CDC mode
//! Auto-detected when the incoming batch carries `_cdc.op`. Requires
//! `pk_columns`. Routes per row:
//!   - `i` / `u` / `s` ⇒ upsert into target (same UPSERT SQL as plain mode)
//!   - `d` ⇒ DELETE keyed on the configured PKs
//!   - `c` ⇒ skipped (schema evolution is a follow-up)
//!   - `t` ⇒ skipped (destructive ops are not auto-applied)
//! `_cdc.*` columns are stripped from the destination table schema.
//!
//! ## Deferred (still)
//! - Mid-run schema evolution (`ALTER TABLE`).
//! - Soft delete / tombstone columns.
//! - Dead-letter routing for Postgres CDC failures.
//! - `COPY FROM STDIN` perf path.
//! - RFC-11 secret-ref URLs.
//! - Multi-table per spec.
//! - Audit-log destination mode (keep `_cdc.*` columns at the destination).
//! - PK-change updates that omit a delete of the old key.
```

(Remove the old "CDC `_cdc.op`-aware DELETE / UPDATE" deferred bullet.)

- [ ] **Step 2: Write the design memo**

Create `docs/superpowers/specs/2026-05-21-phase-2-4b-postgres-loader-cdc-design.md`:

```markdown
# Phase 2.4.b: Postgres Loader — CDC op-aware writes

**Status:** Shipped 2026-05-21 (branch `phase-2-4b-postgres-loader-cdc`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-4b-postgres-loader-cdc.md`
**Builds on:** Phase 2.4.a (`docs/superpowers/specs/2026-05-21-phase-2-4a-postgres-loader-design.md`).
**RFC:** RFC-0009 §"Pattern 3: Apply Change Stream".

## What this adds

The `PostgresLoader` now honors `_cdc.op` on incoming batches. PG-source CDC pipelines can land directly in a PG destination with correct row state.

## Detection

CDC mode is data-driven: any batch whose schema contains `_cdc.op` is routed through the CDC path. There's no spec-level mode flag — the data carries the signal. This means the same `PostgresDestinationSpec` can serve a snapshot phase (`s`) and a streaming phase (`i`/`u`/`d`) without reconfiguration.

## Per-row routing

| `_cdc.op` | Action                                                      |
|-----------|-------------------------------------------------------------|
| `i`       | `INSERT ... ON CONFLICT (pk) DO UPDATE`                     |
| `u`       | `INSERT ... ON CONFLICT (pk) DO UPDATE`                     |
| `s`       | `INSERT ... ON CONFLICT (pk) DO UPDATE` (snapshot)          |
| `d`       | `DELETE FROM target WHERE pk = ...`                         |
| `c`       | skip + `tracing::warn!` (schema-evolution follow-up)        |
| `t`       | skip + `tracing::warn!` (destructive ops not auto-applied)  |
| other     | error                                                       |

All rows in a batch execute in the single per-call transaction, in batch order.

## Schema stripping

`_cdc.op`, `_cdc.lsn`, `_cdc.commit_ts`, `_cdc.txid` are dropped from the destination DDL and from upsert row values. The destination table looks like the source table — no platform metadata leaks. If a customer ever wants the audit-log shape (keep `_cdc.*` columns), that's a separate spec/mode.

## Idempotency

Unchanged from phase 2.4.a. The transaction-level idempotency log in `<schema>._etl_loaded_batches` short-circuits a duplicated `LoadId` regardless of CDC vs plain mode.

## Limitations (known, deferred)

- **Mixed-mode pipelines on the same destination** — if the first batch is non-CDC and a later batch is CDC, the destination table will already exist with whatever shape the first batch produced. `cdc_apply` uses `CREATE TABLE IF NOT EXISTS`, so it won't drop and recreate. Plain → CDC mode switch on the same target requires a manual recreate (or a future schema-reconciliation pass).
- **PK changes without delete-of-old-key** — an `update` whose PK column value differs from the source's prior value will land as an insert/update on the new key and leave the old-key row behind. Sources that emit a `d` of the old key followed by an `i` of the new key (which is what PG logical replication does with replica identity FULL) behave correctly.
- **Schema-change events (`c`) and truncate events (`t`)** are skipped with a warning. Wiring real schema evolution is the next loader-facing work item.

## Tests

- Unit: ~10 new tests in `crates/worker/src/loaders/postgres.rs::tests` (CDC detection, schema stripping, op extraction, DELETE SQL, value extraction, pk-empty validation).
- Integration: 7 new tests in `tests/integration/tests/postgres_loader_cdc.rs` covering insert/update/delete/mixed/snapshot-handoff/retry-idempotency/no-metadata-cols against docker `postgres:16`.

## Follow-ups (priority order)

1. Schema evolution (apply additive `ALTER TABLE` on `c` events; pause on destructive).
2. Multi-table per spec (`stream_name → table` routing).
3. Dead-letter routing for CDC failures.
4. `COPY FROM STDIN` perf path for snapshot-heavy batches.
5. Secret-ref URLs (RFC-11 wiring on the loader side).
6. Audit-log destination mode option.
```

- [ ] **Step 3: Run the full check**

```bash
cargo test -p common-types -p worker -p loader-sdk
docker compose up -d postgres
cargo test -p integration-tests --test postgres_loader -- --test-threads=1
cargo test -p integration-tests --test postgres_loader_cdc -- --test-threads=1
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs docs/superpowers/specs/2026-05-21-phase-2-4b-postgres-loader-cdc-design.md
git commit -m "phase-2-4b-8: docs for CDC-aware postgres loader"
```

- [ ] **Step 5: Open PR**

```bash
git push -u origin HEAD
gh pr create --title "phase-2-4b: Postgres loader — CDC op-aware writes" --body "$(cat <<'EOF'
## Summary
- `PostgresLoader` now routes per-row by `_cdc.op`: `i`/`u`/`s` ⇒ upsert, `d` ⇒ DELETE.
- CDC mode auto-detected from `_cdc.op` column presence; `_cdc.*` stripped from destination DDL.
- Non-CDC batches behave exactly as before; idempotency log unchanged.

## Test plan
- [x] Unit tests for detection, schema stripping, op extraction, DELETE SQL, pk-empty validation
- [x] Integration: insert / update / delete / mixed-batch / snapshot→streaming / retry-idempotency / no-metadata-cols

## Deferred
- Schema-change events (`c`) skipped with a warn
- Truncate events (`t`) skipped with a warn
- Mixed-mode pipelines on the same destination
- PK changes without delete-of-old-key

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage (vs RFC-9 Pattern 3 + phase-2-4a deferral list):**
- "Apply Change Stream" per-row dispatch — Task 6. ✓
- `i`/`u`/`s`/`d` routing — Task 6, Task 7. ✓
- `c` / `t` skipped with warning — Task 6 (explicit `match` arms). ✓
- `_cdc.*` stripped from destination schema — Task 2, Task 6, Task 7 `cdc_destination_table_has_no_metadata_columns`. ✓
- Atomic per-batch — same transaction as phase-2-4a; verified by Task 7 mixed-batch test. ✓
- Idempotency unchanged — Task 7 `cdc_retry_with_same_load_id_is_noop`. ✓
- CDC mode requires `pk_columns` — Task 6 validation + Task 6 step 1 unit test. ✓

**Placeholder scan:** No "TBD" / "implement later" / "similar to" / "add appropriate error handling" markers. Every code step has the actual code. Iteration step in Task 7 names the three most-likely failure modes with concrete diagnoses.

**Type consistency:** `is_cdc_batch`, `cdc_data_schema`, `cdc_data_field_indices`, `cdc_op_at`, `delete_sql`, `extract_data_row`, `extract_pk_values`, `cdc_apply` — names used consistently across Tasks 1–6. `PostgresDestinationSpec` field names unchanged from phase-2-4a (`pk_columns`, `schema`, `table`, `connection_url`). `LoadId` and `BoundValue` reused from phase-2-4a without modification.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-21-phase-2-4b-postgres-loader-cdc.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
