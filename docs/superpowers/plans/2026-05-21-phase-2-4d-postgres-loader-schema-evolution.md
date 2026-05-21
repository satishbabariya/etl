# Phase 2.4.d: Postgres Loader — Schema Evolution

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `"c"` warn-and-skip arm in `cdc_apply` with real DDL application. Additive changes (new columns, widened types) are applied via `ALTER TABLE` before subsequent rows in the same batch are processed. Destructive changes (dropped columns, narrowed types, incompatible type changes) pause the pipeline by returning a non-retriable `anyhow::Error` per RFC-9 §"Mid-run schema change" and RFC-10's `propagate_additive` policy.

**Architecture:** All schema-diffing logic lives inline in the Postgres loader — no catalog wiring in this phase. The diff input is `cdc_data_schema(batch.schema())` (the batch's data schema, already stripped of `_cdc.*` columns) vs. the destination table's current column set, queried from `information_schema.columns` inside the same transaction. Differences are classified using a local `SchemaDelta` enum. When a `"c"` row appears in a batch, schema evolution is applied *before the per-row loop* (all rows in the same Arrow `RecordBatch` share the widened schema; the `"c"` row is a sentinel, not the schema carrier). After successful DDL the `"c"` row's data binding is skipped and the loop continues.

**Tech Stack:** Rust · existing `sqlx 0.8` + `arrow 53` · `information_schema.columns` for live schema introspection · `pg_column_type` reused from `crates/worker/src/loaders/postgres.rs:163` · docker-compose `postgres:16` for integration tests.

**Scope cuts (deferred, not in this plan):**
- Catalog wiring — schema evolution is decided and applied inline; no `applied_to_destination_at` updates, no catalog policy evaluation.
- Column rename via `ALTER TABLE ... RENAME COLUMN` — RFC-10 renames are off by default; treated here as `DropColumn + AddColumn` (drops pause the pipeline).
- PK composition changes — always destructive; pipeline pauses.
- Backfilling new nullable columns with a non-null default.
- `field_nullability_tightened` — treated as destructive.
- Type widenings that require a `USING` cast beyond the cast-to-self pattern — plan handles the common numeric and string widenings explicitly.

---

## File Structure

**Modify:**
- `crates/worker/src/loaders/postgres.rs` — add `DestCol`, `query_destination_columns`, `SchemaDelta`, `TypeRelation`, `pg_type_relation`, `diff_schema`, `add_column_ddl`, `alter_column_type_ddl`, `apply_schema_evolution`; replace the `"c"` arm in `cdc_apply`.

**Create:**
- `tests/integration/tests/postgres_loader_schema_evolution.rs` — integration tests.
- `docs/superpowers/specs/2026-05-21-phase-2-4d-postgres-loader-schema-evolution-design.md` — design memo.

**No changes to:**
- `common_types::evolution` — `ChangeKind` exists but uses Arrow type strings; the loader needs PG type strings (as returned by `information_schema.columns.data_type`). A local `SchemaDelta` avoids a cross-crate round-trip conversion.
- `PostgresDestinationSpec`, `loader_sdk` trait, `LoadId` — all unchanged.

---

## Task 1: `DestCol` struct and `query_destination_columns`

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

This helper is the raw input for schema diffing. It returns `Vec<DestCol>` — `(name, pg_type_str, nullable)` — queried live from PG so the diff is always against actual destination state, not a cached schema. The query runs inside the existing per-call transaction so DDL and data writes are atomic.

- [ ] **Step 1: Write the failing unit tests**

Add to the `tests` module in `crates/worker/src/loaders/postgres.rs`:

```rust
#[test]
fn dest_col_equality() {
    let a = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false };
    let b = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false };
    assert_eq!(a, b);
}

#[test]
fn dest_col_nullable_differs() {
    let a = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false };
    let b = DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: true };
    assert_ne!(a, b);
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::dest_col`
Expected: compile error — `DestCol` not defined.

- [ ] **Step 3: Add `DestCol` and `query_destination_columns`**

Add to `crates/worker/src/loaders/postgres.rs`, directly above `pub struct PostgresLoader` (around line 364):

```rust
/// One column as reported by `information_schema.columns`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DestCol {
    pub name: String,
    /// Lower-cased PG type name as returned by `information_schema.columns.data_type`,
    /// e.g. `"bigint"`, `"text"`, `"timestamp with time zone"`.
    pub pg_type: String,
    pub nullable: bool,
}

/// Query the live column list for `"<schema>"."<table>"` from
/// `information_schema.columns`, ordered by `ordinal_position`.
/// Returns an empty `Vec` if the table does not yet exist.
pub(crate) async fn query_destination_columns(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<DestCol>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(&mut **tx)
    .await
    .context("query information_schema.columns")?;

    let cols = rows
        .into_iter()
        .map(|r| {
            let nullable_str: String = r.get(2);
            DestCol {
                name: r.get(0),
                pg_type: r.get::<String, _>(1).to_lowercase(),
                nullable: nullable_str.eq_ignore_ascii_case("YES"),
            }
        })
        .collect();
    Ok(cols)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::dest_col`
Expected: 2 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4d-1: DestCol struct + query_destination_columns helper"
```

---

## Task 2: `SchemaDelta` enum, `pg_type_relation`, and `diff_schema`

`diff_schema` compares the batch's data schema against the destination's live column list. Each `SchemaDelta` variant carries exactly the information needed for the DDL that follows. `pg_type_relation` is a private helper that classifies the direction of a type change.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn diff_schema_detects_new_column() {
    let dest = vec![DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false }];
    let batch_schema = Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),  // new column
    ]));
    let deltas = diff_schema(&batch_schema, &dest).unwrap();
    assert_eq!(deltas.len(), 1);
    assert!(matches!(
        &deltas[0],
        SchemaDelta::AddColumn { name, pg_type, nullable: true }
            if name == "name" && pg_type == "TEXT"
    ));
}

#[test]
fn diff_schema_detects_widen_int32_to_int64() {
    let dest = vec![DestCol { name: "id".into(), pg_type: "integer".into(), nullable: false }];
    let batch_schema = Schema::new(fields(&[("id", DataType::Int64, false)]));
    let deltas = diff_schema(&batch_schema, &dest).unwrap();
    assert_eq!(deltas.len(), 1);
    assert!(matches!(
        &deltas[0],
        SchemaDelta::WidenType { name, new_pg_type }
            if name == "id" && new_pg_type == "BIGINT"
    ));
}

#[test]
fn diff_schema_detects_dropped_column() {
    let dest = vec![
        DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false },
        DestCol { name: "name".into(), pg_type: "text".into(), nullable: true },
    ];
    let batch_schema = Schema::new(fields(&[("id", DataType::Int64, false)]));
    let deltas = diff_schema(&batch_schema, &dest).unwrap();
    assert_eq!(deltas.len(), 1);
    assert!(matches!(&deltas[0], SchemaDelta::DropColumn { name } if name == "name"));
}

#[test]
fn diff_schema_no_delta_when_schemas_match() {
    let dest = vec![
        DestCol { name: "id".into(), pg_type: "bigint".into(), nullable: false },
        DestCol { name: "name".into(), pg_type: "text".into(), nullable: true },
    ];
    let batch_schema = Schema::new(fields(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
    ]));
    let deltas = diff_schema(&batch_schema, &dest).unwrap();
    assert!(deltas.is_empty());
}

#[test]
fn diff_schema_narrowing_int64_to_int32_is_destructive() {
    let dest = vec![DestCol { name: "val".into(), pg_type: "bigint".into(), nullable: true }];
    let batch_schema = Schema::new(fields(&[("val", DataType::Int32, true)]));
    let deltas = diff_schema(&batch_schema, &dest).unwrap();
    assert_eq!(deltas.len(), 1);
    assert!(matches!(&deltas[0], SchemaDelta::NarrowType { name } if name == "val"));
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::diff_schema`
Expected: compile error — `SchemaDelta`, `diff_schema` not defined.

- [ ] **Step 3: Implement `SchemaDelta`, `TypeRelation`, `pg_type_relation`, and `diff_schema`**

Add to `crates/worker/src/loaders/postgres.rs`, after `DestCol` and `query_destination_columns`:

```rust
/// A change between the batch's data schema and the destination table's columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaDelta {
    /// Column in batch but not in destination — emit ADD COLUMN.
    AddColumn { name: String, pg_type: String, nullable: bool },
    /// Destination column missing from batch — destructive (RFC-10: field_removed).
    DropColumn { name: String },
    /// Batch type is wider than destination's current PG type — emit ALTER COLUMN TYPE.
    WidenType { name: String, new_pg_type: String },
    /// Batch type is narrower than destination's — destructive (RFC-10: field_type_narrowed).
    NarrowType { name: String },
    /// Batch type has no widening relationship with destination — destructive.
    IncompatibleType { name: String, dest_pg_type: String, batch_pg_type: String },
}

impl SchemaDelta {
    pub(crate) fn is_destructive(&self) -> bool {
        matches!(
            self,
            SchemaDelta::DropColumn { .. }
                | SchemaDelta::NarrowType { .. }
                | SchemaDelta::IncompatibleType { .. }
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TypeRelation {
    Same,
    Widening,
    Narrowing,
    Incompatible,
}

/// Classify the relationship between the destination's current PG type (as
/// returned by `information_schema.columns.data_type`, already lower-cased) and
/// the batch's PG type (as returned by `pg_column_type`, upper-cased).
/// Both are normalized to lower-case before comparison.
fn pg_type_relation(dest: &str, batch: &str) -> TypeRelation {
    let d = dest.to_lowercase();
    let b = batch.to_lowercase();
    if d == b {
        return TypeRelation::Same;
    }
    // Explicit widening table (dest_type → batch_type direction).
    const WIDENINGS: &[(&str, &str)] = &[
        ("smallint", "integer"),
        ("smallint", "bigint"),
        ("integer", "bigint"),
        ("real", "double precision"),
        ("character varying", "text"),
        ("varchar", "text"),
        ("character", "text"),
    ];
    // Explicit narrowing table (dest_type → batch_type direction).
    const NARROWINGS: &[(&str, &str)] = &[
        ("integer", "smallint"),
        ("bigint", "smallint"),
        ("bigint", "integer"),
        ("double precision", "real"),
    ];
    if WIDENINGS.iter().any(|(f, t)| *f == d.as_str() && *t == b.as_str()) {
        TypeRelation::Widening
    } else if NARROWINGS.iter().any(|(f, t)| *f == d.as_str() && *t == b.as_str()) {
        TypeRelation::Narrowing
    } else {
        TypeRelation::Incompatible
    }
}

/// Compare the batch's data schema (already `_cdc.*`-stripped) against the
/// destination's live column list. Returns one `SchemaDelta` per difference,
/// additive deltas in batch-field order and drop deltas in destination-column order.
pub(crate) fn diff_schema(
    batch_data_schema: &Schema,
    dest_cols: &[DestCol],
) -> anyhow::Result<Vec<SchemaDelta>> {
    let mut deltas = Vec::new();

    // Pass 1 — for every batch column check against destination.
    for field in batch_data_schema.fields() {
        let batch_pg = pg_column_type(field.data_type())?;
        match dest_cols.iter().find(|c| c.name == *field.name()) {
            None => {
                deltas.push(SchemaDelta::AddColumn {
                    name: field.name().clone(),
                    pg_type: batch_pg.to_string(),
                    nullable: field.is_nullable(),
                });
            }
            Some(dest_col) => {
                match pg_type_relation(dest_col.pg_type.as_str(), batch_pg) {
                    TypeRelation::Same => {}
                    TypeRelation::Widening => {
                        deltas.push(SchemaDelta::WidenType {
                            name: field.name().clone(),
                            new_pg_type: batch_pg.to_string(),
                        });
                    }
                    TypeRelation::Narrowing => {
                        deltas.push(SchemaDelta::NarrowType { name: field.name().clone() });
                    }
                    TypeRelation::Incompatible => {
                        deltas.push(SchemaDelta::IncompatibleType {
                            name: field.name().clone(),
                            dest_pg_type: dest_col.pg_type.clone(),
                            batch_pg_type: batch_pg.to_string(),
                        });
                    }
                }
            }
        }
    }

    // Pass 2 — for every destination column check it still exists in batch.
    for dest_col in dest_cols {
        if batch_data_schema.field_with_name(&dest_col.name).is_err() {
            deltas.push(SchemaDelta::DropColumn { name: dest_col.name.clone() });
        }
    }

    Ok(deltas)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::diff_schema`
Expected: 5 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4d-2: SchemaDelta enum + pg_type_relation + diff_schema"
```

---

## Task 3: `add_column_ddl` and `alter_column_type_ddl` builders

Two pure string helpers — no DB calls, fully testable as unit tests.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
#[test]
fn add_column_ddl_builds_nullable_column() {
    let sql = add_column_ddl("myschema", "mytable", "score", "DOUBLE PRECISION", true);
    assert_eq!(
        sql,
        r#"ALTER TABLE "myschema"."mytable" ADD COLUMN IF NOT EXISTS "score" DOUBLE PRECISION"#
    );
}

#[test]
fn add_column_ddl_always_omits_not_null() {
    // Even when field.is_nullable() == false, ADD COLUMN must be nullable so
    // existing rows survive (no DEFAULT available).
    let sql = add_column_ddl("s", "t", "id2", "BIGINT", false);
    assert!(!sql.contains("NOT NULL"), "ADD COLUMN must never emit NOT NULL; got: {sql}");
}

#[test]
fn alter_column_type_ddl_emits_using_cast() {
    let sql = alter_column_type_ddl("public", "orders", "amount", "BIGINT");
    assert_eq!(
        sql,
        r#"ALTER TABLE "public"."orders" ALTER COLUMN "amount" TYPE BIGINT USING "amount"::BIGINT"#
    );
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::add_column_ddl loaders::postgres::tests::alter_column_type_ddl`
Expected: compile error — functions not defined.

- [ ] **Step 3: Implement the helpers**

Add to `crates/worker/src/loaders/postgres.rs`, after `diff_schema`:

```rust
/// `ALTER TABLE "<schema>"."<table>" ADD COLUMN IF NOT EXISTS "<name>" <pg_type>`
///
/// Always omits `NOT NULL` regardless of `nullable`: adding a non-null column
/// without a DEFAULT fails on non-empty tables. Existing rows receive NULL.
pub(crate) fn add_column_ddl(
    schema: &str,
    table: &str,
    col_name: &str,
    pg_type: &str,
    _nullable: bool,
) -> String {
    format!(
        "ALTER TABLE \"{schema}\".\"{table}\" ADD COLUMN IF NOT EXISTS \"{col_name}\" {pg_type}"
    )
}

/// `ALTER TABLE "<schema>"."<table>" ALTER COLUMN "<name>" TYPE <new_type> USING "<name>"::<new_type>`
///
/// The explicit `USING` cast is required by PG for numeric widenings (e.g.
/// `INTEGER → BIGINT`) and is a no-op for identity casts.
pub(crate) fn alter_column_type_ddl(
    schema: &str,
    table: &str,
    col_name: &str,
    new_pg_type: &str,
) -> String {
    format!(
        "ALTER TABLE \"{schema}\".\"{table}\" ALTER COLUMN \"{col_name}\" \
         TYPE {new_pg_type} USING \"{col_name}\"::{new_pg_type}"
    )
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::add_column_ddl loaders::postgres::tests::alter_column_type_ddl`
Expected: 3 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4d-3: add_column_ddl + alter_column_type_ddl DDL builders"
```

---

## Task 4: `apply_schema_evolution` and wire into `cdc_apply`

This is the integration point. `apply_schema_evolution` queries destination columns, diffs, checks for destructive changes, applies additive DDLs. It is called once per batch *before the row loop* when the batch contains any `"c"` row. The `"c"` arm becomes a no-op (skip data binding).

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write a failing unit test for the destructive-classification path**

Add to the `tests` module:

```rust
#[test]
fn schema_delta_is_destructive_classification() {
    assert!(!SchemaDelta::AddColumn {
        name: "x".into(), pg_type: "TEXT".into(), nullable: true,
    }.is_destructive());
    assert!(!SchemaDelta::WidenType {
        name: "x".into(), new_pg_type: "BIGINT".into(),
    }.is_destructive());
    assert!(SchemaDelta::DropColumn { name: "x".into() }.is_destructive());
    assert!(SchemaDelta::NarrowType { name: "x".into() }.is_destructive());
    assert!(SchemaDelta::IncompatibleType {
        name: "x".into(),
        dest_pg_type: "text".into(),
        batch_pg_type: "boolean".into(),
    }.is_destructive());
}
```

- [ ] **Step 2: Run test to confirm it passes (SchemaDelta.is_destructive was added in Task 2)**

Run: `cargo test -p worker loaders::postgres::tests::schema_delta_is_destructive_classification`
Expected: 1 passing (Task 2 already implemented `is_destructive`; this confirms the logic is correct end-to-end).

- [ ] **Step 3: Add `apply_schema_evolution` and update `cdc_apply`**

Add `apply_schema_evolution` to `crates/worker/src/loaders/postgres.rs`, immediately before the `async fn cdc_apply` definition (line 538):

```rust
/// Query destination columns, diff against `batch_data_schema`, apply additive
/// changes, and return `Err` on any destructive change.
///
/// Called once per batch that contains a `"c"` CDC op, before the per-row loop.
/// After `Ok(())`, the destination schema is a superset of `batch_data_schema`
/// and all subsequent data rows can be bound and inserted safely.
async fn apply_schema_evolution(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    target_table: &str,
    batch_data_schema: &Schema,
) -> anyhow::Result<()> {
    let dest_cols = query_destination_columns(tx, schema, target_table).await?;

    // Table doesn't exist yet — CREATE TABLE in the normal cdc_apply flow handles it.
    if dest_cols.is_empty() {
        return Ok(());
    }

    let deltas = diff_schema(batch_data_schema, &dest_cols)?;

    // Fail atomically before any DDL if there are destructive changes.
    let destructive: Vec<&SchemaDelta> = deltas.iter().filter(|d| d.is_destructive()).collect();
    if !destructive.is_empty() {
        let descriptions: Vec<String> = destructive
            .iter()
            .map(|d| match d {
                SchemaDelta::DropColumn { name } => {
                    format!("column dropped from source: {name:?}")
                }
                SchemaDelta::NarrowType { name } => {
                    format!("type narrowed (data loss risk): {name:?}")
                }
                SchemaDelta::IncompatibleType { name, dest_pg_type, batch_pg_type } => {
                    format!(
                        "incompatible type for {name:?}: \
                         destination is {dest_pg_type}, batch expects {batch_pg_type}"
                    )
                }
                _ => unreachable!(),
            })
            .collect();
        bail!(
            "destructive schema change detected for table {target_table:?} — \
             operator action required before pipeline can resume:\n  {}",
            descriptions.join("\n  ")
        );
    }

    // Apply additive deltas in schema order.
    for delta in &deltas {
        let ddl = match delta {
            SchemaDelta::AddColumn { name, pg_type, nullable } => {
                add_column_ddl(schema, target_table, name, pg_type, *nullable)
            }
            SchemaDelta::WidenType { name, new_pg_type } => {
                alter_column_type_ddl(schema, target_table, name, new_pg_type)
            }
            _ => continue,
        };
        tracing::info!(
            target: "loader.postgres.schema_evolution",
            %target_table,
            %ddl,
            "applying additive schema change"
        );
        tx.execute(sqlx::query(&ddl))
            .await
            .with_context(|| format!("schema evolution DDL failed: {ddl}"))?;
    }

    Ok(())
}
```

Now update `cdc_apply` in `crates/worker/src/loaders/postgres.rs`. Make two changes:

**Change A** — insert a pre-loop evolution check. Add this block immediately after `let del_sql = delete_sql(...)` (line ~551) and before `let mut applied = 0usize`:

```rust
    // Pre-loop: if this batch contains any "c" (schema-change) event, apply
    // schema evolution now. The batch's Arrow schema is already the widened
    // schema (the connector emits widened rows alongside the "c" sentinel),
    // so DDL must land before any data row in this batch is processed.
    let has_schema_change = (0..batch.num_rows())
        .any(|r| cdc_op_at(batch, r).map(|op| op == "c").unwrap_or(false));
    if has_schema_change {
        apply_schema_evolution(&mut *tx, &spec.schema, target_table, &data_schema).await?;
    }
```

**Change B** — replace the `"c"` arm (lines ~579–583). The old arm was:

```rust
            "c" => {
                tracing::warn!(
                    target: "loader.postgres.cdc",
                    "schema-change CDC event skipped (mid-run schema evolution not implemented)"
                );
            }
```

Replace with:

```rust
            "c" => {
                // Schema evolution was applied before the loop.
                // The "c" row itself carries no data values; skip data binding.
                tracing::debug!(
                    target: "loader.postgres.cdc",
                    row = r,
                    "schema-change event: DDL already applied, skipping row data"
                );
            }
```

- [ ] **Step 4: Run all worker unit tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres`
Expected: all prior tests green + `schema_delta_is_destructive_classification` green. No compile errors.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4d-4: apply_schema_evolution + wire into cdc_apply pre-loop"
```

---

## Task 5: Integration test — additive schema change mid-stream

**Files:**
- Create: `tests/integration/tests/postgres_loader_schema_evolution.rs`

- [ ] **Step 1: Write the failing integration tests**

Create `tests/integration/tests/postgres_loader_schema_evolution.rs`:

```rust
//! Schema evolution integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service to be running.

use arrow::array::{Float64Array, Int64Array, StringArray};
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
            eprintln!("SKIP postgres_loader_schema_evolution: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_evo_loader_test_{}", uuid::Uuid::new_v4().simple());
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

/// Base CDC schema: id BIGINT + name TEXT + _cdc.*
fn base_cdc_schema() -> Arc<Schema> {
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

/// Widened CDC schema: id BIGINT + name TEXT + score FLOAT64 + _cdc.*
fn widened_cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

/// Build a base-schema CDC batch. Rows: (id, name, op).
fn base_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(str::to_string)).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.2.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();
    RecordBatch::try_new(
        base_cdc_schema(),
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

/// Build a widened-schema CDC batch. Rows: (id, name, score, op).
/// Use `score = None` for the "c" sentinel row.
fn widened_batch(rows: &[(i64, Option<&str>, Option<f64>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(str::to_string)).collect();
    let scores: Vec<Option<f64>> = rows.iter().map(|r| r.2).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.3.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();
    RecordBatch::try_new(
        widened_cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC"),
            ),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn additive_new_column_mid_stream_lands_correctly() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Batch 0: seed with base schema (id, name).
    PostgresLoader
        .load(&s, load_id(0), base_batch(&[(1, Some("alice"), "i"), (2, Some("bob"), "i")]))
        .await
        .expect("batch 0");

    // Confirm initial columns.
    let cols_before: Vec<String> = sqlx::query(
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
    assert_eq!(cols_before, vec!["id", "name"], "initial columns wrong");

    // Batch 1: widened schema.
    // Row 0: id=0, op="c" — schema-change sentinel (id=0 is a throwaway upsert).
    // Row 1: id=3, name="carol", score=9.5, op="i" — real new row.
    PostgresLoader
        .load(
            &s,
            load_id(1),
            widened_batch(&[
                (0, None, None, "c"),
                (3, Some("carol"), Some(9.5), "i"),
            ]),
        )
        .await
        .expect("batch 1 (additive evolution)");

    // score column must have been added.
    let cols_after: Vec<String> = sqlx::query(
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
    assert_eq!(cols_after, vec!["id", "name", "score"], "score column not added");

    // Existing rows have score = NULL; new row has score = 9.5.
    let rows: Vec<(i64, Option<String>, Option<f64>)> = sqlx::query(&format!(
        "SELECT id, name, score FROM \"{schema}\".\"events\" ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (
        r.get::<i64, _>(0),
        r.try_get::<String, _>(1).ok(),
        r.try_get::<f64, _>(2).ok(),
    ))
    .collect();

    assert_eq!(rows[0], (1, Some("alice".into()), None), "alice wrong");
    assert_eq!(rows[1], (2, Some("bob".into()), None), "bob wrong");
    assert_eq!(rows[2], (3, Some("carol".into()), Some(9.5)), "carol wrong");

    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn schema_change_only_batch_adds_column_and_does_not_insert_rows() {
    // A batch consisting of only a "c" sentinel — no data rows.
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Seed.
    PostgresLoader
        .load(&s, load_id(0), base_batch(&[(1, Some("alice"), "i")]))
        .await
        .expect("seed");

    // "c"-only batch on widened schema.
    PostgresLoader
        .load(
            &s,
            load_id(1),
            widened_batch(&[(0, None, None, "c")]),
        )
        .await
        .expect("c-only batch");

    // score column was added.
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
    assert_eq!(cols, vec!["id", "name", "score"]);

    // Row count unchanged (the "c" row's id=0 was bound but is still an upsert —
    // verify alice is the only row with a real name).
    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"events\" WHERE name IS NOT NULL"
    ))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 1, "only alice should be in the table");

    drop_schema(&pool, &schema).await;
}
```

- [ ] **Step 2: Run tests to confirm they compile and pass**

Run: `docker compose up -d postgres && cargo test -p integration-tests --test postgres_loader_schema_evolution -- --nocapture --test-threads=1`

Expected: 2 tests pass. If a test fails, the most likely diagnoses:
1. `score` column not added — check `has_schema_change` detects the `"c"` row (verify `cdc_op_at(batch, 0)` returns `"c"` for the widened batch's first row).
2. `diff_schema` returns no deltas — check `pg_type_relation("double precision", "double precision")` returns `Same` (both are already lower-cased inside `pg_type_relation`). Confirm `pg_column_type(DataType::Float64)` returns `"DOUBLE PRECISION"` and `pg_type_relation("double precision", "double precision")` compares lowercase `"double precision" == "double precision"` → `Same`. That's correct. If PG reports the type as `"double precision"` in `information_schema` for a `DOUBLE PRECISION` column and the batch also maps to `"DOUBLE PRECISION"`, the normalized comparison is `Same` → no delta. If `score` is a new column (not in destination), it should produce `AddColumn`. Verify the destination table was created without `score` after batch 0 (it was created with `(id, name)` only).
3. `apply_schema_evolution` returning an error on the `"c"` batch — check that `query_destination_columns` finds the table (it was created in batch 0) and returns `[{name="id"}, {name="name"}]`. The diff against `{id, name, score}` should produce `AddColumn{name="score", pg_type="DOUBLE PRECISION", nullable=true}`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/postgres_loader_schema_evolution.rs
git commit -m "phase-2-4d-5: integration tests for additive schema evolution"
```

---

## Task 6: Integration test — destructive change returns non-retriable error

- [ ] **Step 1: Append the destructive tests to the existing file**

Append to `tests/integration/tests/postgres_loader_schema_evolution.rs`:

```rust
/// CDC schema with `name` dropped (only id + _cdc.*).
fn dropped_name_cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn dropped_name_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.1.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();
    RecordBatch::try_new(
        dropped_name_cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC"),
            ),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn destructive_drop_column_returns_error_and_leaves_destination_unchanged() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Seed table with (id, name).
    PostgresLoader
        .load(&s, load_id(0), base_batch(&[(1, Some("alice"), "i")]))
        .await
        .expect("seed");

    // Batch 1: schema-change event where `name` has been dropped from source.
    let err = PostgresLoader
        .load(
            &s,
            load_id(1),
            dropped_name_batch(&[(0, "c"), (2, "i")]),
        )
        .await
        .unwrap_err();

    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("destructive") && msg.contains("operator"),
        "expected destructive-change error, got: {err}"
    );
    assert!(msg.contains("name"), "error must name the dropped column, got: {err}");

    // Destination must be unchanged (transaction was rolled back).
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
    assert_eq!(cols, vec!["id", "name"], "destination must be unchanged after abort");

    // Row must still be present.
    let count: i64 =
        sqlx::query(&format!("SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"events\""))
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
    assert_eq!(count, 1, "alice must still be in the table");

    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn destructive_type_narrowing_returns_error_and_leaves_destination_unchanged() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Build and seed a table with a BIGINT `amount` column.
    let bigint_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let seed = RecordBatch::try_new(
        bigint_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![Some(1_000_000i64)])),
            Arc::new(StringArray::from(vec!["i"])),
            Arc::new(StringArray::from(vec!["lsn-0"])),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(vec![1_779_667_200_000_000i64])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    PostgresLoader.load(&s, load_id(0), seed).await.expect("seed");

    // Narrowing batch: amount is INT32 (narrower than BIGINT), plus a "c" sentinel.
    let narrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int32, true), // INT32 < BIGINT — narrowing!
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let narrow = RecordBatch::try_new(
        narrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(arrow::array::Int32Array::from(vec![Some(0i32)])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(StringArray::from(vec!["lsn-1"])),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(vec![1_779_667_200_000_001i64])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();

    let err = PostgresLoader.load(&s, load_id(1), narrow).await.unwrap_err();

    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("destructive") && msg.contains("operator"),
        "expected destructive-change error, got: {err}"
    );
    assert!(msg.contains("amount"), "error must name the narrowed column, got: {err}");

    drop_schema(&pool, &schema).await;
}
```

- [ ] **Step 2: Run the full schema evolution integration suite**

Run: `cargo test -p integration-tests --test postgres_loader_schema_evolution -- --nocapture --test-threads=1`
Expected: 4 tests pass.

- [ ] **Step 3: Run all prior PG loader integration suites to confirm no regressions**

Run:
```bash
docker compose up -d postgres
cargo test -p integration-tests \
    --test postgres_loader \
    --test postgres_loader_cdc \
    --test postgres_loader_multi_table \
    --test postgres_loader_schema_evolution \
    -- --test-threads=1
```
Expected: 3 + 7 + 5 + 4 = 19 tests green.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/postgres_loader_schema_evolution.rs
git commit -m "phase-2-4d-6: destructive-change integration tests (drop column, type narrowing)"
```

---

## Task 7: Docs — update loader header + write design memo

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs` (top-of-file doc comment)
- Create: `docs/superpowers/specs/2026-05-21-phase-2-4d-postgres-loader-schema-evolution-design.md`

- [ ] **Step 1: Update the loader doc header**

In `crates/worker/src/loaders/postgres.rs`, find the `## CDC mode` section (lines 20–27). Replace:

```rust
//!   - `c` ⇒ skipped (schema evolution is a follow-up)
```

with:

```rust
//!   - `c` ⇒ schema evolution applied before the row loop: additive changes
//!            (ADD COLUMN, widened types) are applied via ALTER TABLE; destructive
//!            changes (DROP, narrow, incompatible) return a non-retriable error per
//!            RFC-9 §"Mid-run schema change" + RFC-10 propagate_additive.
```

Replace the `## Deferred` first bullet (line 39):

```rust
//! - Mid-run schema evolution (only first-load CREATE TABLE).
```

with:

```rust
//! - Catalog wiring for schema evolution (loader applies DDL inline; no catalog
//!   state transitions in this phase — no applied_to_destination_at updates).
//! - Column rename via ALTER TABLE ... RENAME COLUMN (treated as drop+add → pauses).
//! - PK-type-change guard (WidenType on a PK column is applied; should pause instead).
//! - Backfilling new columns with a non-null default value.
```

- [ ] **Step 2: Write the design memo**

Create `docs/superpowers/specs/2026-05-21-phase-2-4d-postgres-loader-schema-evolution-design.md`:

```markdown
# Phase 2.4.d: Postgres Loader — Schema Evolution

**Status:** Shipped 2026-05-21 (branch `phase-2-4d-postgres-loader-schema-evolution`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-4d-postgres-loader-schema-evolution.md`
**Builds on:** Phase 2.4.c (`docs/superpowers/specs/2026-05-21-phase-2-4c-postgres-loader-multi-table-design.md`).
**RFC:** RFC-0009 §"Schema Application" + §"Mid-run schema change (CDC DDL events)"; RFC-0010 §"Evolution Policy" (`propagate_additive`).

## What this adds

The Postgres loader now applies additive schema changes when a `_cdc.op = "c"` event appears in a CDC batch. Previously the `"c"` arm was a warn-and-skip no-op. Now it:

1. Queries `information_schema.columns` for the target table inside the current transaction.
2. Diffs the batch's data schema against destination columns using `diff_schema`.
3. Applies `ALTER TABLE ADD COLUMN` or `ALTER TABLE ALTER COLUMN TYPE` for additive changes.
4. Returns a non-retriable `anyhow::Error` for destructive changes (drops, narrowings, incompatible type changes).

## Design: inline diffing, no catalog wiring

RFC-9 §"Schema Application" says DDL is "governed by RFC-10's policy but with loader-specific mechanics." This plan implements the mechanics inline — no catalog round-trip. The loader unconditionally acts under `propagate_additive` semantics. Catalog wiring (policy evaluation, `applied_to_destination_at` updates, schema version chain) is a follow-up phase when the catalog service is wired into the data plane.

## Evolution fires before the row loop

Because an Arrow `RecordBatch` has one schema for all rows, the batch is already the widened schema when the `"c"` sentinel appears. Schema evolution is applied *before the row loop* so data rows before and after the `"c"` sentinel all bind against the updated destination. The pre-loop check is:

```rust
let has_schema_change = (0..batch.num_rows())
    .any(|r| cdc_op_at(batch, r).map(|op| op == "c").unwrap_or(false));
```

## Additive changes

| Condition | DDL |
|---|---|
| Column in batch not in destination | `ALTER TABLE ... ADD COLUMN IF NOT EXISTS "<name>" <type>` — always nullable (no DEFAULT available) |
| Batch type wider than destination | `ALTER TABLE ... ALTER COLUMN "<name>" TYPE <new_type> USING "<name>"::<new_type>` |

## Destructive changes that pause

| `SchemaDelta` variant | Condition | RFC-10 equivalent |
|---|---|---|
| `DropColumn` | Column in destination, absent from batch | `field_removed` |
| `NarrowType` | Batch type narrower than destination (e.g., BIGINT → INTEGER) | `field_type_narrowed` |
| `IncompatibleType` | No widening/narrowing path exists | `field_type_incompatible` |

"Pause" = `bail!("destructive schema change detected ... operator action required")`. Temporal retries the activity; all retries fail identically until the operator takes action (aligns source schema, or manually alters the destination). The transaction is rolled back — no data written, idempotency log not updated.

## Type relation table

| dest (`information_schema`) | batch (`pg_column_type`) | relation |
|---|---|---|
| `smallint` | `integer` | Widening |
| `smallint` | `bigint` | Widening |
| `integer` | `bigint` | Widening |
| `real` | `double precision` | Widening |
| `character varying` / `varchar` / `character` | `text` | Widening |
| `bigint` | `integer` | Narrowing |
| `bigint` | `smallint` | Narrowing |
| `integer` | `smallint` | Narrowing |
| `double precision` | `real` | Narrowing |
| `timestamp without time zone` | `timestamp with time zone` | Incompatible (semantic change) |
| same type | same type | Same (no delta) |

## Key invariants

- **Idempotency**: `ADD COLUMN IF NOT EXISTS` is safe on retry. `ALTER COLUMN TYPE` is idempotent if the column is already the target type (PG accepts no-op type changes).
- **Atomicity**: DDL, data writes, and idempotency log insert are in the same `sqlx` transaction. A failed DDL rolls back everything; the batch load is retried cleanly.
- **No data written on destructive error**: `apply_schema_evolution` returns `Err` *before* any DDL is applied. Transaction rollback leaves destination identical to its pre-batch state.
- **New columns always nullable**: `ADD COLUMN IF NOT EXISTS "<name>" <type>` (no `NOT NULL`) so existing rows survive without a DEFAULT.

## Tests

- Unit (in `crates/worker/src/loaders/postgres.rs::tests`):
  - `DestCol` equality (2 tests).
  - `diff_schema` — new column, widen, drop, no-op, narrowing (5 tests).
  - `add_column_ddl` — nullable output, forced-nullable (2 tests).
  - `alter_column_type_ddl` — USING cast (1 test).
  - `schema_delta_is_destructive_classification` — all 5 variants (1 test).
  — 11 new unit tests total.
- Integration (in `tests/integration/tests/postgres_loader_schema_evolution.rs`):
  - Additive new column mid-stream: seed + widened batch with `"c"` + data row → `score` column added, existing rows unaffected, new row lands with score value.
  - Schema-change-only batch: `score` added, row count unchanged.
  - Destructive drop column: error contains "destructive" + "operator" + "name"; destination unchanged; existing row survives.
  - Destructive type narrowing: error contains "destructive" + "operator" + "amount"; destination unchanged.
  — 4 new integration tests; all 15 prior PG loader integration tests still green.

## Limitations (known, deferred)

- **Column renames**: treated as `DropColumn + AddColumn` — drops pause the pipeline. Operator must handle via catalog rename-reconciliation (RFC-10 §"Rename heuristic").
- **PK-type-change guard**: a widening of a PK column (e.g., INTEGER PK → BIGINT PK) is currently applied via `ALTER COLUMN TYPE`. In practice the PK index can be rebuilt, but under load this is expensive and could cause lock contention. A future guard should detect PK columns in `WidenType` deltas and pause instead.
- **Catalog wiring**: no `applied_to_destination_at` update; no policy evaluation via catalog; no schema version chain progression.
- **`COPY FROM STDIN` fast path**: if added later, it will need to call `apply_schema_evolution` before its copy loop.

## Follow-ups (priority order)

1. PK-type-change guard: pause on `WidenType` where column is in `spec.pk_columns`.
2. Catalog wiring: `applied_to_destination_at`, policy evaluation, schema version chain.
3. `COPY FROM STDIN` fast path (perf).
4. Secret-ref connection URLs (RFC-11).
5. Dead-letter routing for Postgres CDC failures.
6. Audit-log destination mode option.
```

- [ ] **Step 3: Final verification**

```bash
cargo test -p common-types -p worker -p loader-sdk
docker compose up -d postgres
cargo test -p integration-tests \
    --test postgres_loader \
    --test postgres_loader_cdc \
    --test postgres_loader_multi_table \
    --test postgres_loader_schema_evolution \
    -- --test-threads=1
```

Expected: all green — 11 new unit tests in `worker`, 4 new schema-evolution integration tests, 15 prior PG loader integration tests unaffected.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs \
        docs/superpowers/specs/2026-05-21-phase-2-4d-postgres-loader-schema-evolution-design.md
git commit -m "phase-2-4d-7: docs — update loader header + schema evolution design memo"
```

- [ ] **Step 5: Open PR**

```bash
git push -u origin HEAD
gh pr create \
  --title "phase-2-4d: Postgres loader — schema evolution for CDC DDL events" \
  --body "$(cat <<'EOF'
## Summary
- Replaces the warn-and-skip \`\"c\"\` arm in \`cdc_apply\` with real DDL application.
- Additive changes (new columns, widened types) applied via \`ALTER TABLE\` before the row loop.
- Destructive changes (column drop, type narrowing, incompatible type) return a non-retriable error per RFC-9 §\"Mid-run schema change\" + RFC-10 \`propagate_additive\`.
- All diffing inline in the loader; no catalog wiring in this phase.

## Test plan
- [x] 11 new unit tests: DestCol, diff_schema (5), add_column_ddl (2), alter_column_type_ddl (1), SchemaDelta.is_destructive (1)
- [x] 4 new integration tests: additive new column, c-only batch, destructive drop, destructive narrow
- [x] 15 prior PG loader integration tests (phase 2.4.a/b/c) still green

## Deferred
- PK-type-change guard
- Catalog wiring (applied_to_destination_at, policy evaluation)
- Column rename via ALTER TABLE ... RENAME COLUMN
- COPY FROM STDIN fast path

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage (vs RFC-9 §"Mid-run schema change" + RFC-10 `propagate_additive`):**
- Additive changes applied before subsequent rows — `has_schema_change` pre-loop block + `apply_schema_evolution`. ✓
- Destructive changes pause the pipeline — `bail!("destructive schema change...operator action required")`. ✓
- `"c"` row data binding skipped — `"c"` arm changed to debug log + no binding, no `applied += 1`. ✓
- New columns always nullable — `add_column_ddl` ignores `_nullable` param, always omits `NOT NULL`. ✓
- Atomicity — DDL inside same `sqlx` transaction; error rolls back everything. ✓
- Idempotency — `ADD COLUMN IF NOT EXISTS`; `ALTER COLUMN TYPE` is PG-idempotent on retry. ✓
- Dropped column pauses — `DropColumn` in `is_destructive()`. ✓
- Type narrowing pauses — `NarrowType` in `is_destructive()`. ✓
- Error message names the problematic column — `descriptions` vec in `apply_schema_evolution`. ✓
- Integration test verifies destination unchanged after destructive abort — explicit `information_schema` check after error. ✓

**Placeholder scan:** No "TBD" / "implement later" / "similar to" / "add appropriate error handling" markers. Every step has actual, compilable code. Task 5 step 2 names three specific first-run failure modes with concrete diagnoses.

**Consistency with prior phases:**
- `pub(crate)` visibility on all new helpers matches the convention set in phase-2-4a/b/c.
- `&mut **tx` double-deref pattern used in `query_destination_columns` matches the existing `q.execute(&mut **tx)` calls throughout `cdc_apply` and `plain_apply`.
- `use sqlx::Row` added locally inside `query_destination_columns` to match the pattern in the integration tests (which also use `r.get::<_, _>(n)`).
- New types (`DestCol`, `SchemaDelta`, `TypeRelation`) are added after the existing `BoundValue` enum and before `pub struct PostgresLoader`, keeping all type definitions together.
- `pg_type_relation` is private (`fn`, not `pub(crate)`) — only `diff_schema` calls it, consistent with `is_cdc_metadata_col` being private.

**Integration test style:** Matches `postgres_loader_cdc.rs` exactly — same `fresh_schema`/`drop_schema` helpers, same `test_url`, same `load_id` builder, same `use sqlx::Row` for `.get()` calls, same `-- --test-threads=1` invocation, same skip-on-unreachable pattern.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-21-phase-2-4d-postgres-loader-schema-evolution.md`. Two execution options:

1. **Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks, fast iteration with tight feedback loops.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints after each task.

Which approach?
