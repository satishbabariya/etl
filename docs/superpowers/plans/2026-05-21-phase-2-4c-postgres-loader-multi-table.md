# Phase 2.4.c: Postgres Loader — Multi-Table Routing

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route each batch to a target Postgres table chosen by `LoadId.stream_name`, so a single pipeline can fan out CDC + cursor batches into many tables (e.g. `"public.users"` ⇒ table `"public.users"`).

**Architecture:** Single new helper `resolve_target_table(spec, stream_name)` picks the table name per batch (stream_name if non-empty else `spec.table`) and validates it to forbid quote characters that would break quoted-identifier escaping. Thread the resolved table through `create_table_ddl`, `insert_sql`, `delete_sql`, and `cdc_apply` so every existing code path picks the per-batch destination. The destination spec stays unchanged at the type level (`spec.table` becomes the fallback), so phase-2-4a/b single-table pipelines keep working byte-for-byte. Idempotency log already keys on `stream_name` — multi-table is just multiple distinct keys per run.

**Tech Stack:** Same as 2.4.a/b — Rust · sqlx 0.8 · arrow 53 · docker `postgres:16` for tests. No new deps.

**Scope cuts (deferred, not in this plan):**
- Per-stream `pk_columns` override — every stream uses `spec.pk_columns`. Mixed-PK multi-table pipelines are a follow-up (per-stream config map on `PostgresDestinationSpec`).
- Schema-qualified target names — `stream_name = "public.users"` ⇒ table `"public.users"` (literal dot in the table name), all inside `spec.schema`. Splitting `<src_schema>.<table>` into a destination `<dst_schema>.<table>` is a follow-up.
- Per-stream `target_table` renaming map.
- Cross-stream transactionality. Each batch is atomic on its own; cross-table consistency is a workflow-level concern (RFC-4).
- Schema evolution (still). Mid-run `ALTER TABLE` waits for phase 2.4.d.

---

## File Structure

**Modify:**
- `crates/worker/src/loaders/postgres.rs` — add `resolve_target_table`, thread the resolved table through all SQL builders + `cdc_apply` + the non-CDC INSERT path in `load()`.

**Create:**
- `tests/integration/tests/postgres_loader_multi_table.rs` — integration tests against docker `postgres:16` covering multi-table append, multi-table CDC, and the new validation.

**Unchanged:**
- `PostgresDestinationSpec` — `table` becomes the "fallback when stream_name is empty" field; everyone else (phase-2-4a single-table append, phase-2-4b CDC) sees no behavior change.
- `LoadId` / `loader_sdk` — `stream_name` field already exists since phase-2-3g-0; this plan just teaches the PG loader to use it.
- `load_batch` activity — already populates `stream_name` on the `LoadId` it constructs (see `crates/worker/src/activities/sync/mod.rs` lines around the `LoadId { … stream_name: input.stream_name.clone() }` block).

---

## Task 1: `resolve_target_table` helper with validation

Pick `stream_name` if non-empty, else `spec.table`. Forbid characters that would break PG quoted identifiers (`"`, NUL, control chars). The existing `validate_stream_name` in `parquet_local.rs` is path-focused and lets through `"`; we need a Postgres-flavored variant here.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/worker/src/loaders/postgres.rs`:

```rust
#[test]
fn resolve_target_table_uses_stream_name_when_present() {
    let spec = PostgresDestinationSpec {
        connection_url: "postgres://x".into(),
        schema: "public".into(),
        table: "fallback".into(),
        pk_columns: vec![],
    };
    assert_eq!(
        resolve_target_table(&spec, "public.users").unwrap(),
        "public.users"
    );
}

#[test]
fn resolve_target_table_falls_back_to_spec_table_when_stream_empty() {
    let spec = PostgresDestinationSpec {
        connection_url: "postgres://x".into(),
        schema: "public".into(),
        table: "fallback".into(),
        pk_columns: vec![],
    };
    assert_eq!(resolve_target_table(&spec, "").unwrap(), "fallback");
}

#[test]
fn resolve_target_table_rejects_double_quote() {
    let spec = PostgresDestinationSpec {
        connection_url: "postgres://x".into(),
        schema: "public".into(),
        table: "t".into(),
        pk_columns: vec![],
    };
    let err = resolve_target_table(&spec, "evil\"name").unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("illegal"));
}

#[test]
fn resolve_target_table_rejects_control_chars() {
    let spec = PostgresDestinationSpec {
        connection_url: "postgres://x".into(),
        schema: "public".into(),
        table: "t".into(),
        pk_columns: vec![],
    };
    for bad in &["with\0nul", "with\nnewline", "with\rcr"] {
        let err = resolve_target_table(&spec, bad).unwrap_err();
        assert!(
            format!("{err}").to_lowercase().contains("illegal"),
            "expected illegal-char rejection for {bad:?}"
        );
    }
}

#[test]
fn resolve_target_table_errors_when_both_empty() {
    let spec = PostgresDestinationSpec {
        connection_url: "postgres://x".into(),
        schema: "public".into(),
        table: "".into(),
        pk_columns: vec![],
    };
    let err = resolve_target_table(&spec, "").unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("table") && msg.contains("empty"), "got: {msg}");
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p worker loaders::postgres::tests::resolve_target_table`
Expected: compile errors — `resolve_target_table` not defined.

- [ ] **Step 3: Implement `resolve_target_table`**

Add to `crates/worker/src/loaders/postgres.rs` (near the other SQL-builder helpers, above `pg_column_type` is fine):

```rust
pub(crate) fn resolve_target_table<'a>(
    spec: &'a PostgresDestinationSpec,
    stream_name: &'a str,
) -> anyhow::Result<&'a str> {
    let candidate = if stream_name.is_empty() {
        spec.table.as_str()
    } else {
        stream_name
    };
    if candidate.is_empty() {
        bail!(
            "postgres loader: target table is empty (both stream_name and spec.table are empty)"
        );
    }
    for ch in candidate.chars() {
        if ch == '"' || ch == '\0' || ch.is_control() {
            bail!(
                "postgres loader: illegal character {ch:?} in target table name {candidate:?}"
            );
        }
    }
    Ok(candidate)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p worker loaders::postgres::tests::resolve_target_table`
Expected: 5 passing.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4c-1: resolve_target_table — per-batch table picker + validation"
```

---

## Task 2: Thread resolved table through `cdc_apply`

`cdc_apply` currently hard-codes `spec.table` in every SQL builder call. Pass the resolved table in, so the caller in `load()` (Task 4) can vary it per batch.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Modify `cdc_apply` to take a `target_table` parameter**

In `crates/worker/src/loaders/postgres.rs`, change the signature and the three internal builder calls:

```rust
async fn cdc_apply(
    tx: &mut Transaction<'_, Postgres>,
    spec: &PostgresDestinationSpec,
    target_table: &str,
    batch: &RecordBatch,
) -> anyhow::Result<usize> {
    let data_schema = cdc_data_schema(batch.schema().as_ref());
    let ddl = create_table_ddl(&spec.schema, target_table, &data_schema, &spec.pk_columns)?;
    tx.execute(sqlx::query(&ddl))
        .await
        .context("create target table (cdc)")?;

    let upsert_sql = insert_sql(&spec.schema, target_table, &data_schema, &spec.pk_columns);
    let del_sql = delete_sql(&spec.schema, target_table, &spec.pk_columns);
    // …rest of the function body is unchanged (same per-row match).
}
```

Update the only caller in `PostgresLoader::load` to pass the resolved table — for now wire it as `&spec.table` (Task 4 swaps it for the per-batch resolution):

```rust
rows_loaded = cdc_apply(&mut tx, spec, &spec.table, &batch).await?;
```

- [ ] **Step 2: Verify the change compiles and existing tests still pass**

Run: `cargo test -p worker loaders::postgres`
Expected: all 26 existing tests still green (Task 2 is a refactor; behavior is unchanged because the caller still passes `spec.table`).

Run: `cargo test -p integration-tests --test postgres_loader --test postgres_loader_cdc -- --test-threads=1`
Expected: 3 + 7 = 10 PG loader integration tests still green.

- [ ] **Step 3: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4c-2: thread target_table through cdc_apply"
```

---

## Task 3: Thread resolved table through the non-CDC INSERT path

The non-CDC branch of `load()` also builds DDL/INSERT against `spec.table`. Refactor that block into a helper that takes `target_table`, mirroring Task 2.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Extract a helper and switch `load()` to call it**

Inside `crates/worker/src/loaders/postgres.rs`, add a new helper next to `cdc_apply`:

```rust
async fn plain_apply(
    tx: &mut Transaction<'_, Postgres>,
    spec: &PostgresDestinationSpec,
    target_table: &str,
    batch: &RecordBatch,
) -> anyhow::Result<usize> {
    let ddl = create_table_ddl(
        &spec.schema,
        target_table,
        batch.schema().as_ref(),
        &spec.pk_columns,
    )?;
    tx.execute(sqlx::query(&ddl))
        .await
        .context("create target table")?;

    let sql = insert_sql(
        &spec.schema,
        target_table,
        batch.schema().as_ref(),
        &spec.pk_columns,
    );
    let mut rows_loaded = 0usize;
    for r in 0..batch.num_rows() {
        let values = extract_row(batch, r)?;
        let mut q = sqlx::query(&sql);
        for v in &values {
            q = bind_one(q, v);
        }
        q.execute(&mut **tx)
            .await
            .with_context(|| format!("INSERT row {r}"))?;
        rows_loaded += 1;
    }
    Ok(rows_loaded)
}
```

In `PostgresLoader::load`, replace the entire `else if batch.num_rows() > 0 { … }` block (the one immediately after the CDC `if` branch, currently spans the inline `let ddl = create_table_ddl(...); tx.execute(...).await…; let sql = insert_sql(...); for r in … }`) with:

```rust
        } else if batch.num_rows() > 0 {
            rows_loaded = plain_apply(&mut tx, spec, &spec.table, &batch).await?;
        }
```

(The `if batch.num_rows() > 0 && is_cdc_batch(...)` arm above stays — the new helper is the equivalent of the original inline non-CDC code; behavior is unchanged.)

- [ ] **Step 2: Verify**

Run: `cargo test -p worker loaders::postgres`
Expected: 26 existing tests still green.

Run: `cargo test -p integration-tests --test postgres_loader --test postgres_loader_cdc -- --test-threads=1`
Expected: 10 PG loader integration tests still green.

- [ ] **Step 3: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4c-3: extract plain_apply helper for non-CDC path"
```

---

## Task 4: Use `resolve_target_table` per batch in `load()`

The plumbing from Tasks 2–3 means the only behavioral change is to compute the target table from `load_id.stream_name` instead of always passing `spec.table`.

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs`

- [ ] **Step 1: Resolve the target table once and pass it to both helpers**

In `crates/worker/src/loaders/postgres.rs`, modify `PostgresLoader::load`. Find the section starting `// 3. CDC vs plain.` and the two branches that currently pass `&spec.table`. Add a `target_table` binding just before the dispatch and pass it through:

```rust
        // 3. CDC vs plain. Resolve the per-batch destination table first.
        let target_table = resolve_target_table(spec, &load_id.stream_name)?;
        let mut rows_loaded = 0usize;

        if batch.num_rows() > 0 && is_cdc_batch(batch.schema().as_ref()) {
            if spec.pk_columns.is_empty() {
                bail!(
                    "CDC batch arrived at postgres loader but pk_columns is empty; \
                     CDC ops require a primary key for upsert/delete routing"
                );
            }
            rows_loaded = cdc_apply(&mut tx, spec, target_table, &batch).await?;
        } else if batch.num_rows() > 0 {
            rows_loaded = plain_apply(&mut tx, spec, target_table, &batch).await?;
        }
```

Also update the `LoadResult.path` returned at the bottom of `load()` so it reflects the resolved target — find the existing two `format!("{}.{}", spec.schema, spec.table)` calls (the "already loaded" short-circuit at idempotency check time, and the success-path return at the end of `load`). For the success-path return:

```rust
        tx.commit().await.context("commit tx")?;
        Ok(LoadResult {
            rows_loaded,
            bytes_written: 0,
            path: format!("{}.{}", spec.schema, target_table),
        })
```

Leave the idempotency-shortcut path's `path` as-is (using `spec.table`) — that branch fires before `resolve_target_table` and a retry of an already-loaded batch doesn't need the resolved name. Don't move `resolve_target_table` above the idempotency check; doing so would cause a malformed `stream_name` to fail before the cheap log lookup short-circuits a retry.

Actually we want the validation to fire on every call, not just on cache misses, so that a bad `stream_name` is reported immediately. Move `resolve_target_table` above the idempotency check too. Replace the section starting `// 2. Idempotency check` with this order:

```rust
        // 2. Resolve target table (validates stream_name even for retried/dup loads).
        let target_table = resolve_target_table(spec, &load_id.stream_name)?;

        // 3. Idempotency check — if this load_id is already logged, no-op.
        let existing = sqlx::query(&format!(
```

Then remove the second `let target_table = …` from step 1 above, and update the "already loaded" short-circuit `path` to use the resolved name:

```rust
        if existing.is_some() {
            tx.commit().await.ok();
            return Ok(LoadResult {
                rows_loaded: 0,
                bytes_written: 0,
                path: format!("{}.{} (already loaded)", spec.schema, target_table),
            });
        }
```

After this edit, the only remaining `target_table` binding is the one declared just before the idempotency check; the section starting `// 3. CDC vs plain.` should no longer redeclare it. The branch bodies use the already-bound `target_table`.

- [ ] **Step 2: Verify single-table pipelines still work**

Run: `cargo test -p worker loaders::postgres`
Expected: 26 green.

Run: `cargo test -p integration-tests --test postgres_loader --test postgres_loader_cdc -- --test-threads=1`
Expected: 10 green. These tests construct `LoadId` with `stream_name: String::new()`, so they exercise the `spec.table` fallback path.

- [ ] **Step 3: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs
git commit -m "phase-2-4c-4: route per-batch by LoadId.stream_name in PostgresLoader::load"
```

---

## Task 5: Multi-table append integration test

**Files:**
- Create: `tests/integration/tests/postgres_loader_multi_table.rs`

- [ ] **Step 1: Write the integration tests**

Create `tests/integration/tests/postgres_loader_multi_table.rs`:

```rust
//! Multi-table routing integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service.

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
            eprintln!("SKIP postgres_loader_multi_table test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_multi_loader_test_{}", uuid::Uuid::new_v4().simple());
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

fn plain_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn plain_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(|s| s.to_string())).collect();
    RecordBatch::try_new(
        plain_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
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

fn cdc_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(|s| s.to_string())).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.2.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len())
        .map(|i| 1_779_667_200_000_000 + i as i64)
        .collect();
    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

fn spec(connection_url: &str, schema: &str, pk: Vec<String>) -> DestinationSpec {
    DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: connection_url.into(),
        schema: schema.into(),
        // table is the fallback when stream_name is empty; multi-table
        // tests always set stream_name, so this value should never be used.
        table: "unused_fallback".into(),
        pk_columns: pk,
    })
}

fn lid(stream: &str, seq: u32) -> LoadId {
    LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: seq,
        stream_name: stream.into(),
    }
}

async fn count(pool: &sqlx::PgPool, schema: &str, table: &str) -> i64 {
    sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"{table}\""
    ))
    .fetch_one(pool)
    .await
    .unwrap()
    .get(0)
}

#[tokio::test]
async fn multi_table_append_lands_in_distinct_tables() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    PostgresLoader
        .load(&s, lid("users", 0), plain_batch(&[(1, Some("alice"))]))
        .await
        .unwrap();
    PostgresLoader
        .load(
            &s,
            lid("orders", 0),
            plain_batch(&[(100, Some("o-1")), (101, Some("o-2"))]),
        )
        .await
        .unwrap();

    assert_eq!(count(&pool, &schema, "users").await, 1);
    assert_eq!(count(&pool, &schema, "orders").await, 2);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_cdc_routes_per_stream() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec!["id".into()]);

    // Two streams, each with a mixed CDC batch.
    PostgresLoader
        .load(
            &s,
            lid("users", 0),
            cdc_batch(&[(1, Some("alice"), "i"), (2, Some("bob"), "i")]),
        )
        .await
        .unwrap();
    PostgresLoader
        .load(
            &s,
            lid("orders", 0),
            cdc_batch(&[(10, Some("o-10"), "i"), (11, Some("o-11"), "i")]),
        )
        .await
        .unwrap();
    // Streaming batch on users only.
    PostgresLoader
        .load(
            &s,
            lid("users", 1),
            cdc_batch(&[(1, Some("alice-v2"), "u"), (2, None, "d")]),
        )
        .await
        .unwrap();

    let users: Vec<(i64, Option<String>)> = sqlx::query(&format!(
        "SELECT id, name FROM \"{schema}\".\"users\" ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.get::<i64, _>(0), r.try_get::<String, _>(1).ok()))
    .collect();
    assert_eq!(users, vec![(1, Some("alice-v2".into()))]);
    assert_eq!(count(&pool, &schema, "orders").await, 2);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_idempotency_keys_include_stream_name() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    // Same (run, batch_seq) but different stream_name — must NOT collide
    // in the _etl_loaded_batches log (stream_name is part of the PK).
    let tid = TenantId::new();
    let pid = PipelineId::new();
    let rid = RunId::new();
    let mk = |stream: &str| LoadId {
        tenant_id: tid.clone(),
        pipeline_id: pid.clone(),
        run_id: rid.clone(),
        batch_seq: 0,
        stream_name: stream.into(),
    };

    PostgresLoader
        .load(&s, mk("users"), plain_batch(&[(1, Some("a"))]))
        .await
        .unwrap();
    PostgresLoader
        .load(&s, mk("orders"), plain_batch(&[(10, Some("o"))]))
        .await
        .unwrap();

    assert_eq!(count(&pool, &schema, "users").await, 1);
    assert_eq!(count(&pool, &schema, "orders").await, 1);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_stream_with_dot_in_name() {
    // The connector convention is `<src_schema>.<table>` (e.g. "public.users").
    // The PG loader should accept the literal dot as part of the table name.
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    PostgresLoader
        .load(
            &s,
            lid("public.users", 0),
            plain_batch(&[(1, Some("a"))]),
        )
        .await
        .unwrap();

    assert_eq!(count(&pool, &schema, "public.users").await, 1);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_rejects_quote_char_in_stream_name() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    let err = PostgresLoader
        .load(
            &s,
            lid("bad\"name", 0),
            plain_batch(&[(1, Some("a"))]),
        )
        .await
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("illegal"));
    drop_schema(&pool, &schema).await;
}
```

- [ ] **Step 2: Run the integration tests**

Run: `docker compose up -d postgres && cargo test -p integration-tests --test postgres_loader_multi_table -- --nocapture --test-threads=1`
Expected: 5 tests pass. Likely first-time failures and their causes:
1. `multi_table_rejects_quote_char_in_stream_name` fails — `resolve_target_table` not on the hot path before the connect (check Task 4 step 1 placed `resolve_target_table` above `pool.begin()`); if it's after, the test still passes (an error is returned), but the call hits the DB unnecessarily. Move the resolution earlier if needed.
2. Counts mismatch on `multi_table_cdc_routes_per_stream` — verify `cdc_apply` is using the `target_table` parameter and not still hard-coding `spec.table` (re-grep for `spec.table` inside that function).
3. `multi_table_idempotency_keys_include_stream_name` fails — confirm the idempotency log's PK is `(tenant_id, pipeline_id, run_id, stream_name, batch_seq)` (it is, from phase-2-4a `ensure_log_table_ddl`); if it errors, the query bind for `stream_name` may be wrong.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/postgres_loader_multi_table.rs
git commit -m "phase-2-4c-5: multi-table integration tests"
```

---

## Task 6: Documentation update

**Files:**
- Modify: `crates/worker/src/loaders/postgres.rs` (top-of-file doc)
- Create: `docs/superpowers/specs/2026-05-21-phase-2-4c-postgres-loader-multi-table-design.md`

- [ ] **Step 1: Update the loader doc header**

In `crates/worker/src/loaders/postgres.rs`, replace the "Deferred" section's "Multi-table per spec — MVP is one target table per pipeline." line with:

```
//! ## Multi-table routing
//! `LoadId.stream_name` selects the target table per batch:
//!   - `stream_name = ""` ⇒ `spec.table` (single-table pipeline).
//!   - `stream_name = "<n>"` ⇒ table `<n>` inside `spec.schema`. Connectors
//!     today emit `"<src_schema>.<src_table>"`, which lands as a literal
//!     `<src_schema>.<src_table>` table name inside `spec.schema`.
//! Validation forbids `"`, NUL, and control chars in the resolved name
//! to prevent quoted-identifier escapes.
//!
//! ## Deferred (still)
//! - Mid-run schema evolution (only first-load CREATE TABLE).
//! - Per-stream `pk_columns` override — every stream uses `spec.pk_columns`.
//! - Soft delete / tombstone columns.
//! - Dead-letter routing (rejected rows are logged + dropped by the activity).
//! - `COPY FROM STDIN` fast path (perf optimization).
//! - RFC-11 secret-ref connection URLs (MVP takes an inline `postgres://`).
//! - Audit-log destination mode (keep `_cdc.*` columns at the destination).
//! - PK-change updates that omit a delete of the old key.
//! - Destination-schema split: `stream_name = "<src_schema>.<table>"` ⇒
//!   destination `<src_schema>."<table>"` instead of one literal-dot table.
```

- [ ] **Step 2: Write the design memo**

Create `docs/superpowers/specs/2026-05-21-phase-2-4c-postgres-loader-multi-table-design.md`:

```markdown
# Phase 2.4.c: Postgres Loader — Multi-Table Routing

**Status:** Shipped 2026-05-21 (branch `phase-2-4c-postgres-loader-multi-table`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-4c-postgres-loader-multi-table.md`
**Builds on:** Phase 2.4.b (`docs/superpowers/specs/2026-05-21-phase-2-4b-postgres-loader-cdc-design.md`).
**RFC:** RFC-0009 §"Per-Destination Specifics — Postgres".

## What this adds

`PostgresLoader` now reads `LoadId.stream_name` to pick a per-batch target table inside `spec.schema`. Multi-table CDC pipelines (e.g. `public.users` + `public.orders` from the same Postgres source) land each table's batches in its own destination table, with no spec changes needed.

## Resolution

- `stream_name = ""` ⇒ `spec.table` (single-table behavior — phase 2.4.a/b unchanged).
- `stream_name = "<n>"` ⇒ table `<n>` inside `spec.schema`.

The connector convention is `<src_schema>.<src_table>` (see `docs/superpowers/specs/2026-05-05-phase-2-3g-multi-table-cdc-design.md`), so a source row from `public.users` lands in `<spec.schema>."public.users"` (literal dot in the destination table name). Future work splits this into a `<src_schema>` schema namespace at the destination.

## Validation

`resolve_target_table` rejects target names containing:
- `"` (would break quoted-identifier escaping)
- NUL byte
- Any Unicode `is_control()` character

Dots, hyphens, underscores, mixed case all pass.

## Idempotency

Unchanged from phase 2.4.a. The `_etl_loaded_batches` PK is `(tenant_id, pipeline_id, run_id, stream_name, batch_seq)`, so two streams with the same `batch_seq` in the same run do not collide.

## Limitations (known, deferred)

- **Per-stream `pk_columns` override** — every stream uses `spec.pk_columns`. Mixed-PK multi-table pipelines need a per-stream config (future `tables: HashMap<String, TableConfig>` field on the spec).
- **Destination-schema split** — `public.users` lands as a literal `"public.users"` table; a future variant could route it to `<spec.schema>."users"` (taking the post-dot segment) or `"public"."users"` (separate schema).
- **Schema evolution** still deferred to phase 2.4.d.
- **Cross-stream atomicity** — each batch is atomic; cross-table consistency is a workflow concern (RFC-4).

## Tests

- Unit: 5 new tests in `loaders::postgres::tests` covering `resolve_target_table` (fallback, stream-wins, quote rejection, control-char rejection, both-empty error).
- Integration: 5 new tests in `tests/integration/tests/postgres_loader_multi_table.rs` covering multi-table append, multi-table CDC, idempotency-by-stream, dot-in-name, and quote rejection at runtime.
- Phase 2.4.a/b's 10 integration tests still green (no regression — they use empty `stream_name`).

## Follow-ups (priority order)

1. Schema evolution (apply additive `ALTER TABLE` on `c` events; pause on destructive).
2. Per-stream `pk_columns` override / per-stream renaming map.
3. Dead-letter routing for Postgres failures.
4. `COPY FROM STDIN` perf path for snapshot-heavy batches.
5. Secret-ref URLs (RFC-11 wiring on the loader side).
6. Audit-log destination mode option.
7. Destination-schema split for `<src>.<tbl>` stream names.
```

- [ ] **Step 3: Final verification**

```bash
cargo test -p common-types -p worker -p loader-sdk
docker compose up -d postgres
cargo test -p integration-tests --test postgres_loader \
                                --test postgres_loader_cdc \
                                --test postgres_loader_multi_table \
                                -- --test-threads=1
```

Expected: all green — 5 new unit tests in worker, 5 new multi-table integration tests, 10 prior PG loader integration tests still green.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/loaders/postgres.rs docs/superpowers/specs/2026-05-21-phase-2-4c-postgres-loader-multi-table-design.md docs/superpowers/plans/2026-05-21-phase-2-4c-postgres-loader-multi-table.md
git commit -m "phase-2-4c-6: docs for multi-table postgres loader"
```

- [ ] **Step 5: Open PR**

```bash
git push -u origin HEAD
gh pr create --title "phase-2-4c: Postgres loader — multi-table routing" --body "$(cat <<'EOF'
## Summary
- `LoadId.stream_name` now selects the per-batch target table in `PostgresLoader`.
- `stream_name = ""` falls back to `spec.table` (phase 2.4.a/b single-table behavior preserved).
- Validation rejects `"`, NUL, and control chars in resolved table names.

## Test plan
- [x] 5 new `resolve_target_table` unit tests
- [x] 5 new multi-table integration tests (append, CDC, idempotency-by-stream, dot-in-name, quote rejection)
- [x] Phase 2.4.a/b's 10 integration tests still green

## Deferred
- Per-stream `pk_columns` override
- Destination-schema split for `<src>.<tbl>` stream names
- Schema evolution

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage (vs the scope statement):**
- `LoadId.stream_name` ⇒ per-batch target table — Tasks 1, 4. ✓
- Fallback to `spec.table` when stream_name empty — Task 1 unit test + Task 4 (existing tests still pass). ✓
- Quoted-identifier safety — Task 1 unit tests + Task 5 integration test. ✓
- Multi-table append works — Task 5 `multi_table_append_lands_in_distinct_tables`. ✓
- Multi-table CDC routes per stream — Task 5 `multi_table_cdc_routes_per_stream`. ✓
- Idempotency log keys correctly on `stream_name` — Task 5 `multi_table_idempotency_keys_include_stream_name`. ✓
- Dot-in-name (the actual connector convention) supported — Task 5 `multi_table_stream_with_dot_in_name`. ✓
- Single-table behavior unchanged — verified by re-running phase-2-4a/b integration suites at Task 2/3/4 step 2. ✓

**Placeholder scan:** No "TBD" / "implement later" / "similar to" / "add appropriate error handling" markers. Every code step has the actual code. Task 5 Step 2 names the three most-likely failure modes with concrete diagnoses.

**Type consistency:** `resolve_target_table` returns `&str`; the four call sites (`cdc_apply` arg, `plain_apply` arg, `LoadResult.path` format strings) all accept `&str`. `PostgresDestinationSpec` field names unchanged from phase-2-4a (`connection_url`, `schema`, `table`, `pk_columns`). `LoadId.stream_name` is `String` everywhere (set by `activities/sync/mod.rs` from `input.stream_name.clone()`); helper takes `&str` borrowing from it cleanly.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-21-phase-2-4c-postgres-loader-multi-table.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
