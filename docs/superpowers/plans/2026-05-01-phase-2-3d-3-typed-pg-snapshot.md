# Phase II.3.d.3 — Typed Postgres CDC Snapshot Batches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the Postgres CDC snapshot path to type-parity with streaming. Today the snapshot batch carries only the PK column as Utf8; this phase captures all data columns and emits them with typed Arrow types matching the streaming schema.

**Architecture:** Discover the table's column list + Postgres OIDs once at snapshot start via `information_schema.columns`. Reuse the OID→Arrow type map and `parse_pg_text` parser from Phase II.3.d.2. Snapshot SQL casts every column to text (`SELECT col1::text, ...`) so the same parse_pg_text path used by streaming works unchanged. Per-column ArrayBuilder dispatch (extracted from streaming into shared helpers in `types.rs`) builds the typed RecordBatch.

**Tech Stack:** `sqlx::PgConnection` (already used by snapshot), `arrow::array::*Builder` family, the `pg_oid_to_arrow_type` + `parse_pg_text` + `PgScalarValue` infrastructure landed in PR #31.

**Predecessor:** Phase II.3.d.2 (PR #31). Streaming path already emits typed columns; this phase finishes the typed-CDC arc by bringing snapshot to parity. Closes the deferred follow-up flagged in PR #31's body.

---

## File Map

- **`crates/worker/src/connectors/postgres/cdc/types.rs`** — Add `discover_pg_table_oids` (sqlx-driven, queries `information_schema.columns`). Move the existing `make_pg_builder` / `append_pg_scalar` helpers from `stream.rs` into this module so both snapshot and stream can call them.
- **`crates/worker/src/connectors/postgres/cdc/stream.rs`** — Drops the now-shared helpers, imports them from `types.rs`. No behavior change.
- **`crates/worker/src/connectors/postgres/cdc/snapshot.rs`** — `read_chunk` accepts a typed Arrow schema (it already does — we just stop forcing all-Utf8). `rows_to_cdc_batch` rewritten to iterate every data column (not just PK), parse each via `parse_pg_text`, and use type-aware ArrayBuilders. Snapshot SQL now `SELECT col::text FROM ...` for every data column.
- **`crates/worker/src/activities/cdc/mod.rs`** — `snapshot_chunk` activity discovers column OIDs via `discover_pg_table_oids` and passes the typed schema to `read_chunk`. Replaces the `[(pk_col, Utf8)]` single-column hack.
- **`tests/integration/tests/cdc_snapshot_streaming_handoff.rs`** — Add Parquet-shape assertions on snapshot rows: `id` is `Int64`, `customer` is `Utf8`, `amount` is `Utf8`. Existing op-count assertions preserved.

No new files. No new deps.

---

## Task 1: `discover_pg_table_oids` in `types.rs`

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/types.rs`

- [ ] **Step 1: Add the discovery function**

Append to `crates/worker/src/connectors/postgres/cdc/types.rs` (after the existing `parse_pg_timestamp_to_micros` function, before the `#[cfg(test)] mod tests` block):

```rust
/// One column's identity from a Postgres table's catalog row.
#[derive(Clone, Debug, PartialEq)]
pub struct PgColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub is_nullable: bool,
    pub ordinal_position: u32,
}

/// Live `information_schema.columns` query keyed by the column's
/// Postgres OID via `pg_type`. Returns columns in `ordinal_position`
/// order. Fails if the table has zero columns visible to the
/// connecting role.
pub async fn discover_pg_table_oids(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    table: &str,
) -> Result<Vec<PgColumnInfo>> {
    use sqlx::Row;
    // information_schema.columns gives us names + nullability + position
    // but not the Postgres OID — we need the type from pg_attribute.
    // One round-trip joining the two tables.
    let rows = sqlx::query(
        "SELECT a.attname AS name, \
                a.atttypid::int8 AS type_oid, \
                NOT a.attnotnull AS is_nullable, \
                a.attnum::int4 AS ordinal_position \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 \
           AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(conn)
    .await
    .context("query pg_attribute")?;

    if rows.is_empty() {
        return Err(anyhow!(
            "table {schema}.{table} not found (or no visible columns)"
        ));
    }

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let name: String = r.try_get("name").context("name")?;
        let type_oid: i64 = r.try_get("type_oid").context("type_oid")?;
        let is_nullable: bool = r.try_get("is_nullable").context("is_nullable")?;
        let ordinal_position: i32 =
            r.try_get("ordinal_position").context("ordinal_position")?;
        out.push(PgColumnInfo {
            name,
            type_oid: type_oid as u32,
            is_nullable,
            ordinal_position: ordinal_position as u32,
        });
    }
    Ok(out)
}
```

The function is integration-tested in Task 5's e2e (which exercises a real Postgres). No unit test is needed for this query — its behavior depends on the live catalog.

- [ ] **Step 2: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 3: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/types.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-3-1: discover_pg_table_oids

Queries pg_attribute + pg_class + pg_namespace to fetch a
table's columns by name + Postgres OID + nullability +
ordinal position. Used by the snapshot path in Task 3 to
build a typed Arrow schema before reading rows.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Move `make_pg_builder` + `append_pg_scalar` to `types.rs`

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/types.rs`
- Modify: `crates/worker/src/connectors/postgres/cdc/stream.rs`

These two helpers are about Arrow builders driven by `PgScalarValue` — naturally shared infrastructure rather than streaming-specific.

- [ ] **Step 1: Move `make_pg_builder` into `types.rs`**

In `crates/worker/src/connectors/postgres/cdc/stream.rs`, find the `make_pg_builder` function (immediately after the `events_to_batch` function from PR #31). Cut its full body. Paste it into `crates/worker/src/connectors/postgres/cdc/types.rs` at the end of the file, before the `#[cfg(test)]` block. Change visibility from `fn` to `pub fn`.

The function signature in `types.rs` is:

```rust
pub fn make_pg_builder(
    dt: &DataType,
) -> Result<Box<dyn arrow::array::ArrayBuilder>> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
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
```

- [ ] **Step 2: Move `append_pg_scalar` into `types.rs`**

Same operation: cut from `stream.rs`, paste into `types.rs`, change to `pub fn`. The function body is unchanged; the imports inside the function reference `super::types::PgScalarValue` — replace those with bare `PgScalarValue` since the function now lives in `types.rs`. The signature in `types.rs`:

```rust
pub fn append_pg_scalar(
    builder: &mut dyn arrow::array::ArrayBuilder,
    scalar: Option<&PgScalarValue>,
    dt: &DataType,
) -> Result<()> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    };
    match (scalar, dt) {
        (None, DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int32(v)), DataType::Int32) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int32Builder"))?
            .append_value(*v),
        (None, DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Int64(v)), DataType::Int64) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Int64Builder"))?
            .append_value(*v),
        (None, DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float32(v)), DataType::Float32) => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float32Builder"))?
            .append_value(*v),
        (None, DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_null(),
        (Some(PgScalarValue::Float64(v)), DataType::Float64) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Float64Builder"))?
            .append_value(*v),
        (None, DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Utf8(s)), DataType::Utf8) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: StringBuilder"))?
            .append_value(s),
        (None, DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_null(),
        (Some(PgScalarValue::Boolean(b)), DataType::Boolean) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: BooleanBuilder"))?
            .append_value(*b),
        (None, DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_null(),
        (Some(PgScalarValue::Date32(d)), DataType::Date32) => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .ok_or_else(|| anyhow!("type mismatch: Date32Builder"))?
            .append_value(*d),
        (None, DataType::Timestamp(TimeUnit::Microsecond, _)) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
            .append_null(),
        (Some(PgScalarValue::TimestampMicros(t)), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| anyhow!("type mismatch: TimestampMicrosecondBuilder"))?
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

- [ ] **Step 3: Update `stream.rs` to import the moved helpers**

The `events_to_batch`, `append_pg_row`, and `append_pg_row_partial` functions in `stream.rs` reference `make_pg_builder`, `append_pg_scalar`, and `super::types::PgScalarValue`. After the move, they need to reference `super::types::make_pg_builder` and `super::types::append_pg_scalar`.

In `crates/worker/src/connectors/postgres/cdc/stream.rs`, find this call inside `events_to_batch`:

```rust
    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data_cols)
        .map(|i| make_pg_builder(schema.field(i).data_type()))
        .collect::<anyhow::Result<Vec<_>>>()?;
```

Replace with:

```rust
    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data_cols)
        .map(|i| super::types::make_pg_builder(schema.field(i).data_type()))
        .collect::<anyhow::Result<Vec<_>>>()?;
```

In `append_pg_row` and `append_pg_row_partial`, find:

```rust
        append_pg_scalar(&mut **builders.get_mut(i).unwrap(), parsed.as_ref(), dt)?;
```

(twice — once in each function). Replace both with:

```rust
        super::types::append_pg_scalar(&mut **builders.get_mut(i).unwrap(), parsed.as_ref(), dt)?;
```

- [ ] **Step 4: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 5: Run lib tests**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all crates green; worker count unchanged from baseline (no new tests, no removed tests).

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc
git commit -m "$(cat <<'EOF'
phase-2-3d-3-2: move make_pg_builder + append_pg_scalar to types.rs

These helpers are about Arrow builders driven by PgScalarValue —
shared infrastructure rather than streaming-specific. Moving them
to types.rs lets the snapshot path in Task 4 use the same
type-aware dispatch.

stream.rs is mechanically updated to import via super::types::*;
no behavior change.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `snapshot_chunk` activity discovers columns + builds typed schema

**Files:**
- Modify: `crates/worker/src/activities/cdc/mod.rs`

- [ ] **Step 1: Add the column-discovery step at activity start**

In `crates/worker/src/activities/cdc/mod.rs`, find the `snapshot_chunk` activity. The current body builds a single-column schema:

```rust
        let cdc_schema =
            snapshot::cdc_schema_for(&[(input.pk_col.as_str(), DataType::Utf8)]);
```

Replace this with:

```rust
        // Discover the table's full column list + Postgres OIDs once
        // per snapshot chunk (a few hundred microseconds; not a hot
        // path). The resulting typed Arrow schema is passed to read_chunk.
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            crate::secrets::auditing::ResolveContext {
                tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
                principal_id: (!input.principal_id.is_nil())
                    .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
                jti: (!input.jti.is_nil()).then_some(input.jti),
            },
        )
        .await
        .map_err(retryable)?;
        let mut conn_for_discovery = sqlx::PgConnection::connect(resolved.expect_url())
            .await
            .map_err(|e| retryable(anyhow::anyhow!(e)))?;
        let cols_oids = crate::connectors::postgres::cdc::types::discover_pg_table_oids(
            &mut conn_for_discovery,
            &input.schema,
            &input.table,
        )
        .await
        .map_err(retryable)?;
        drop(conn_for_discovery);
        let typed_cols: Vec<(&str, DataType)> = cols_oids
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    crate::connectors::postgres::cdc::types::pg_oid_to_arrow_type(c.type_oid),
                )
            })
            .collect();
        let cdc_schema = snapshot::cdc_schema_for(&typed_cols);
```

This block does the secret-resolve once at the top (it's needed both for column discovery and by `read_chunk`), discovers all columns, builds the typed schema. The existing `let resolve_ctx = ...` block lower in the function then becomes redundant — remove it (the resolve already happened above; reuse `resolved` directly in the existing `read_chunk(resolved.expect_url(), ...)` call).

- [ ] **Step 2: Add `sqlx::Connection` import**

At the top of `crates/worker/src/activities/cdc/mod.rs`, find the existing `sqlx` imports. Add `Connection` to the use list (or add a new line `use sqlx::Connection;`) so `PgConnection::connect` resolves. If the file already has `use sqlx::*` or similar, this step is a no-op.

- [ ] **Step 3: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 4: Run lib tests**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all crates green.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/cdc/mod.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-3-3: snapshot_chunk discovers columns + builds typed schema

Replaces the [(pk_col, Utf8)] single-column hack with a fresh
discover_pg_table_oids call that captures the full table shape
with Postgres OIDs. The typed Arrow schema is passed to
read_chunk; rows_to_cdc_batch is updated in Task 4 to actually
populate every column.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Type-aware `read_chunk` + `rows_to_cdc_batch`

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/snapshot.rs`

- [ ] **Step 1: Update `read_chunk` SQL to text-cast every data column**

In `crates/worker/src/connectors/postgres/cdc/snapshot.rs`, find the existing `read_chunk` function. Change the SELECT statement to text-cast every column. Replace this block:

```rust
    let where_clause = match last_pk {
        Some(_) => format!(" WHERE \"{pk_col}\" > $1"),
        None => String::new(),
    };
    let stmt = format!(
        "SELECT * FROM \"{schema}\".\"{table}\"{where_clause} ORDER BY \"{pk_col}\" LIMIT {batch_size}"
    );
```

with:

```rust
    // Build a `SELECT col1::text AS col1, col2::text AS col2, ..."
    // projection so every value lands as a Postgres text-format
    // string. parse_pg_text in rows_to_cdc_batch then produces
    // typed Arrow values per the schema's declared DataType.
    let data_field_names: Vec<&str> = cdc_schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with("_cdc"))
        .map(|f| f.name().as_str())
        .collect();
    let projection = data_field_names
        .iter()
        .map(|n| format!("\"{n}\"::text AS \"{n}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = match last_pk {
        Some(_) => format!(" WHERE \"{pk_col}\" > $1"),
        None => String::new(),
    };
    let stmt = format!(
        "SELECT {projection} FROM \"{schema}\".\"{table}\"{where_clause} ORDER BY \"{pk_col}\" LIMIT {batch_size}"
    );
```

- [ ] **Step 2: Rewrite `rows_to_cdc_batch` for typed dispatch**

In the same file, replace the existing `rows_to_cdc_batch` function (lines ~51-115) with:

```rust
fn rows_to_cdc_batch(
    rows: Vec<sqlx::postgres::PgRow>,
    pk_col: &str,
    schema: SchemaRef,
    consistent_point: &str,
) -> anyhow::Result<(RecordBatch, Option<i64>)> {
    use arrow::array::{ArrayBuilder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
    use crate::connectors::postgres::cdc::types::{
        append_pg_scalar, make_pg_builder, parse_pg_text,
    };

    let mut last_pk: Option<i64> = None;
    let n_data = schema
        .fields()
        .iter()
        .filter(|f| !f.name().starts_with("_cdc"))
        .count();

    let mut col_builders: Vec<Box<dyn ArrayBuilder>> = (0..n_data)
        .map(|i| make_pg_builder(schema.field(i).data_type()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    let mut tx_b = Int64Builder::new();

    for r in &rows {
        for i in 0..n_data {
            let f = schema.field(i);
            let dt = f.data_type();
            // Every column was selected as ::text — extract as Option<String>
            // and let parse_pg_text do the typed conversion.
            let raw: Option<String> = r
                .try_get::<Option<String>, _>(f.name().as_str())
                .with_context(|| format!("try_get text for column {}", f.name()))?;
            let parsed = match raw.as_deref() {
                Some(s) => parse_pg_text(s, dt).with_context(|| {
                    format!("parse_pg_text col={} dt={:?} raw={:?}", f.name(), dt, s)
                })?,
                None => None,
            };
            append_pg_scalar(&mut *col_builders[i], parsed.as_ref(), dt)?;
        }
        op_b.append_value("s");
        lsn_b.append_value(consistent_point);
        ts_b.append_null();
        tx_b.append_null();
        // pk extraction: the column was selected as ::text, so try_get
        // as String and re-parse to i64. Snapshot only supports i64 PKs
        // for now (matches the cursor type elsewhere in CDC).
        if let Ok(Some(s)) = r.try_get::<Option<String>, _>(pk_col) {
            if let Ok(n) = s.parse::<i64>() {
                last_pk = Some(n);
            }
        }
    }

    let mut cols: Vec<ArrayRef> = col_builders.into_iter().map(|mut b| b.finish()).collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish()));
    cols.push(Arc::new(tx_b.finish()));
    let batch = RecordBatch::try_new(schema, cols)?;
    Ok((batch, last_pk))
}
```

The shape mirrors `events_to_batch` from the streaming side: per-column `Box<dyn ArrayBuilder>`, parse_pg_text, append_pg_scalar.

- [ ] **Step 3: Verify the build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty. (`StringBuilder` and `Int64Builder` imports at the top of `snapshot.rs` may now be unused — if cargo warns, remove the dead imports.)

- [ ] **Step 4: Run lib tests**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/snapshot.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-3-4: type-aware snapshot RecordBatch

read_chunk's SELECT now text-casts every data column
("col1"::text AS "col1", ...). rows_to_cdc_batch iterates all
data columns via the typed schema, parses each via parse_pg_text,
and appends to per-column ArrayBuilders. Mirrors the streaming
side's events_to_batch shape.

The snapshot batch now carries the full table data (not just
the PK) with native Arrow types.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: E2E asserts typed snapshot shape + content

**Files:**
- Modify: `tests/integration/tests/cdc_snapshot_streaming_handoff.rs`

- [ ] **Step 1: Inspect the test's table shape and tmp Parquet dir**

Run: `grep -n 'CREATE TABLE\|tempdir\|local_parquet\|read_ops\|assert' /Users/satishbabariya/Desktop/etl/tests/integration/tests/cdc_snapshot_streaming_handoff.rs`
Expected: a `CREATE TABLE orders (id BIGINT, customer TEXT, amount TEXT)` and a tmp dir for the Parquet destination. The test asserts on op-counts (≥3 's' + ≥1 'i') but not on column shape.

- [ ] **Step 2: Add a `read_snapshot_parquet_schema` helper**

Append to the test file (anywhere above the test function):

```rust
fn read_snapshot_parquet_schema(dir: &std::path::Path) -> Option<arrow::datatypes::Schema> {
    use arrow::array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut files: Vec<std::path::PathBuf> = walkdir::WalkDir::new(dir)
        .into_iter()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        .map(|e| e.into_path())
        .collect();
    files.sort();
    for path in files {
        let f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let builder = match ParquetRecordBatchReaderBuilder::try_new(f) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let schema = builder.schema().as_ref().clone();
        let reader = match builder.build() {
            Ok(r) => r,
            Err(_) => continue,
        };
        // Identify a snapshot batch: contains at least one 's' op.
        for batch in reader.flatten() {
            if let Ok(idx) = batch.schema().index_of("_cdc.op") {
                if let Some(arr) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    for i in 0..arr.len() {
                        if arr.value(i) == "s" {
                            return Some(schema);
                        }
                    }
                }
            }
        }
    }
    None
}
```

- [ ] **Step 3: Add type assertions before the existing kill+wait**

In the test body, after the existing `loop { ... }` that polls for op counts and just before `w.kill().await?;`, insert:

```rust
    // Verify the snapshot Parquet batch carries typed columns and
    // includes every data column (not just the PK).
    let snapshot_schema =
        read_snapshot_parquet_schema(tmp.path()).expect("at least one snapshot parquet file");
    let names: Vec<String> = snapshot_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert!(names.contains(&"id".to_string()), "id missing: {names:?}");
    assert!(names.contains(&"customer".to_string()), "customer missing: {names:?}");
    assert!(names.contains(&"amount".to_string()), "amount missing: {names:?}");
    let id_field = snapshot_schema.field_with_name("id").unwrap();
    assert_eq!(
        id_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "snapshot id should be Int64, got {:?}",
        id_field.data_type()
    );
    let customer_field = snapshot_schema.field_with_name("customer").unwrap();
    assert_eq!(
        customer_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "snapshot customer should be Utf8, got {:?}",
        customer_field.data_type()
    );
```

- [ ] **Step 4: Verify the test compiles**

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
cargo test -p integration-tests --test cdc_snapshot_streaming_handoff -- --ignored --nocapture 2>&1 | tail -15
```

Expected: PASS. The new assertions confirm `id: Int64`, `customer: Utf8`, and that all three data columns are present in the snapshot batch.

If a column is missing, Task 3's column discovery isn't seeing it — likely a permissions issue on `pg_attribute` for the connecting role. Verify with `psql` directly.

If types are wrong (e.g. `id: Utf8` instead of `Int64`), the typed schema isn't reaching `rows_to_cdc_batch` — re-check Task 3's wiring.

- [ ] **Step 6: Run the streaming-only e2e for completeness**

```bash
cargo test -p integration-tests --test cdc_insert_update_delete -- --ignored --nocapture 2>&1 | tail -10
```

Expected: PASS. Streaming path is unchanged by this PR; this is just a regression check.

- [ ] **Step 7: Commit**

```bash
git add tests/integration/tests/cdc_snapshot_streaming_handoff.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-3-5: e2e asserts typed snapshot shape + full data columns

Adds Parquet schema-shape assertions to the snapshot+streaming
handoff test: snapshot batch now contains all three columns
(id, customer, amount), id is Int64, customer is Utf8.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find the line set during Phase II.3.d.2:

```markdown
Currently: **Phase II.3.d.2 — Type-aware Postgres CDC columns (complete)** on top of II.3.d.1. Both MySQL and Postgres CDC streaming paths now emit typed Arrow columns (Int32/Int64/Float64/Utf8/Boolean/Date32/Timestamp). Snapshot path for both connectors is still Utf8 (deferred). Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (typed snapshot, MySQL initial snapshot, multi-table) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

Replace with:

```markdown
Currently: **Phase II.3.d.3 — Typed Postgres CDC snapshot batches (complete)** on top of II.3.d.2. Snapshot now captures all data columns (not just PK) with native Arrow types matching the streaming schema. Postgres CDC is fully type-aware end-to-end. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (MySQL initial snapshot, multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.3 — typed Postgres CDC snapshot

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
