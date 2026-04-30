# Phase II.3.d — MySQL CDC Source Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a native, in-worker MySQL CDC source connector in streaming-only mode (skip-snapshot) that consumes binlog row events from a single table into Arrow batches and lands them in the existing Parquet destination, end-to-end.

**Architecture:** New `crates/worker/src/connectors/mysql/cdc/` module mirroring the existing Postgres CDC structure. New `MysqlCdcPipelineWorkflow` (no snapshot loop, streaming only). New `MysqlCdcSourceSpec` variant on `SourceSpec`. Cursor = GTID set string in `runs.cursor`. Schema discovered once at workflow start from `information_schema.columns`.

**Tech Stack:** `mysql_async` 0.36 with `binlog` feature for client + binlog parsing; `testcontainers` 0.20 for the e2e test; existing `arrow`/`parquet` 53; existing `temporalio_*` SDK.

**Spec:** `docs/superpowers/specs/2026-04-30-phase-2-3d-mysql-cdc-design.md`.

---

## Task 1: Workspace dependency wiring

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/worker/Cargo.toml` (add to deps)
- Modify: `crates/common-types/Cargo.toml` (no-op check — should already have what it needs)
- Modify: `tests/integration/Cargo.toml` (add testcontainers + mysql_async dev-deps)

- [ ] **Step 1: Add `mysql_async` and `testcontainers` to workspace deps**

In `Cargo.toml` `[workspace.dependencies]`, add after the existing `tokio-postgres` block:

```toml
# MySQL (Phase II.3.d — RFC-0008 MySQL CDC)
mysql_async = { version = "0.36", default-features = false, features = ["default-rustls", "binlog"] }
testcontainers = { version = "0.20", default-features = false }
testcontainers-modules = { version = "0.8", features = ["mysql"] }
```

- [ ] **Step 2: Wire `mysql_async` into the worker crate**

In `crates/worker/Cargo.toml` under `[dependencies]`, add:

```toml
mysql_async = { workspace = true }
```

- [ ] **Step 3: Wire `testcontainers` + `mysql_async` into the integration tests crate**

In `tests/integration/Cargo.toml` under `[dev-dependencies]`, add:

```toml
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true }
mysql_async = { workspace = true }
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cargo build --workspace`
Expected: clean build, no compile errors. (Library code unchanged so far; we just added dependencies.)

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/worker/Cargo.toml tests/integration/Cargo.toml
git commit -m "phase-2-3d-1: workspace deps for MySQL CDC"
```

---

## Task 2: `MysqlCdcSourceSpec` pipeline spec extension

**Files:**
- Modify: `crates/common-types/src/pipeline_spec.rs`

- [ ] **Step 1: Write the failing serde roundtrip test**

Add at the end of the existing `#[cfg(test)] mod tests` block in `crates/common-types/src/pipeline_spec.rs`:

```rust
#[test]
fn mysql_cdc_variant_roundtrips() {
    let s = SourceSpec::MysqlCdc(MysqlCdcSourceSpec {
        schema: "shop".into(),
        table: "orders".into(),
        server_id: 4242,
        heartbeat_secs: 30,
    });
    let j = serde_json::to_string(&s).unwrap();
    let back: SourceSpec = serde_json::from_str(&j).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), j);
}

#[test]
fn mysql_cdc_serialized_form_is_tagged() {
    let s = SourceSpec::MysqlCdc(MysqlCdcSourceSpec {
        schema: "shop".into(),
        table: "orders".into(),
        server_id: 4242,
        heartbeat_secs: 0,
    });
    let j: serde_json::Value = serde_json::to_value(&s).unwrap();
    assert_eq!(j["type"], "mysql_cdc");
    assert_eq!(j["heartbeat_secs"], 0);
}

#[test]
fn mysql_cdc_heartbeat_defaults_to_zero() {
    let j = r#"{
        "type": "mysql_cdc", "schema": "shop", "table": "orders", "server_id": 4242
    }"#;
    let s: SourceSpec = serde_json::from_str(j).unwrap();
    if let SourceSpec::MysqlCdc(m) = s {
        assert_eq!(m.heartbeat_secs, 0);
        assert_eq!(m.server_id, 4242);
    } else {
        panic!("expected MysqlCdc variant");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p common-types pipeline_spec::tests::mysql_cdc -- --nocapture`
Expected: FAIL with errors about `MysqlCdcSourceSpec` not found and `SourceSpec::MysqlCdc` not a variant.

- [ ] **Step 3: Add the spec struct + variant**

In `crates/common-types/src/pipeline_spec.rs`, add the struct after `WasmSourceSpec` (around line 44):

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

Then add the variant to `SourceSpec` (around line 16):

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
    Wasm(WasmSourceSpec),
    MysqlCdc(MysqlCdcSourceSpec),
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p common-types pipeline_spec::tests::mysql_cdc -- --nocapture`
Expected: 3 passes.

- [ ] **Step 5: Verify nothing downstream broke from the new variant**

Run: `cargo build --workspace`
Expected: clean build. If the build fails because some `match` on `SourceSpec` is now non-exhaustive, fix each site to either dispatch correctly OR explicitly reject MysqlCdc with a "not yet wired" error and a TODO comment pointing at Task 7. Keep the changes minimal — full dispatch wiring lives in Task 7.

- [ ] **Step 6: Commit**

```bash
git add crates/common-types/src/pipeline_spec.rs
git commit -m "phase-2-3d-2: add MysqlCdcSourceSpec variant"
```

---

## Task 3: GTID set parsing helpers (`position.rs`)

**Files:**
- Create: `crates/worker/src/connectors/mysql/mod.rs`
- Create: `crates/worker/src/connectors/mysql/cdc/mod.rs`
- Create: `crates/worker/src/connectors/mysql/cdc/position.rs`
- Modify: `crates/worker/src/connectors/mod.rs`

A GTID set is a comma-separated list of UUID-prefixed interval lists:
`uuid:1-23,uuid:25-30:35-40,uuid2:1-100`. We need to parse, format, and merge so we can advance positions across `read_window` calls.

- [ ] **Step 1: Wire the new module path**

Create `crates/worker/src/connectors/mysql/mod.rs`:

```rust
pub mod cdc;
```

Create `crates/worker/src/connectors/mysql/cdc/mod.rs`:

```rust
pub mod position;
```

Modify `crates/worker/src/connectors/mod.rs` to add (alongside `pub mod postgres;`):

```rust
pub mod mysql;
```

- [ ] **Step 2: Write the failing tests in `position.rs`**

Create `crates/worker/src/connectors/mysql/cdc/position.rs`:

```rust
//! GTID set parsing, formatting, and merging.
//!
//! A GTID set is one or more `uuid:start[-end][:start[-end]]*` segments,
//! comma-separated. Empty string is the empty set (used when the source
//! has never executed a transaction with GTID).

use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GtidSet {
    /// uuid -> sorted, non-overlapping intervals (inclusive on both ends).
    by_uuid: BTreeMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    pub fn empty() -> Self { Self::default() }

    pub fn is_empty(&self) -> bool { self.by_uuid.is_empty() }

    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::empty());
        }
        let mut out = BTreeMap::<String, Vec<(u64, u64)>>::new();
        for segment in s.split(',') {
            let segment = segment.trim();
            let (uuid, ranges) = segment
                .split_once(':')
                .ok_or_else(|| anyhow!("missing ':' in GTID segment '{segment}'"))?;
            let uuid = uuid.trim().to_string();
            let entry = out.entry(uuid).or_default();
            for r in ranges.split(':') {
                let (lo, hi) = match r.split_once('-') {
                    Some((a, b)) => (a.parse::<u64>().context("GTID lo")?,
                                     b.parse::<u64>().context("GTID hi")?),
                    None => {
                        let n = r.parse::<u64>().context("GTID single")?;
                        (n, n)
                    }
                };
                if lo > hi {
                    return Err(anyhow!("inverted GTID range {lo}-{hi}"));
                }
                entry.push((lo, hi));
            }
        }
        for v in out.values_mut() {
            normalize(v);
        }
        Ok(Self { by_uuid: out })
    }

    pub fn format(&self) -> String {
        let mut parts = Vec::new();
        for (uuid, ranges) in &self.by_uuid {
            let mut s = uuid.clone();
            for (lo, hi) in ranges {
                if lo == hi {
                    s.push_str(&format!(":{lo}"));
                } else {
                    s.push_str(&format!(":{lo}-{hi}"));
                }
            }
            parts.push(s);
        }
        parts.join(",")
    }

    /// Merge `other` into self: union of intervals per uuid, normalized.
    pub fn union_with(&mut self, other: &Self) {
        for (uuid, ranges) in &other.by_uuid {
            let entry = self.by_uuid.entry(uuid.clone()).or_default();
            entry.extend(ranges.iter().copied());
            normalize(entry);
        }
    }
}

/// Sort and merge adjacent/overlapping intervals in place.
fn normalize(v: &mut Vec<(u64, u64)>) {
    v.sort();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(v.len());
    for (lo, hi) in v.drain(..) {
        if let Some(last) = out.last_mut() {
            if lo <= last.1.saturating_add(1) {
                last.1 = last.1.max(hi);
                continue;
            }
        }
        out.push((lo, hi));
    }
    *v = out;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_parses_to_empty_set() {
        let g = GtidSet::parse("").unwrap();
        assert!(g.is_empty());
        assert_eq!(g.format(), "");
    }

    #[test]
    fn single_interval_roundtrips() {
        let g = GtidSet::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23").unwrap();
        assert_eq!(g.format(), "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23");
    }

    #[test]
    fn single_point_roundtrips() {
        let g = GtidSet::parse("aaaa:5").unwrap();
        assert_eq!(g.format(), "aaaa:5");
    }

    #[test]
    fn union_merges_adjacent_intervals() {
        let mut a = GtidSet::parse("u:1-10").unwrap();
        let b = GtidSet::parse("u:11-20").unwrap();
        a.union_with(&b);
        assert_eq!(a.format(), "u:1-20");
    }

    #[test]
    fn union_keeps_disjoint_intervals_separate() {
        let mut a = GtidSet::parse("u:1-10").unwrap();
        let b = GtidSet::parse("u:20-30").unwrap();
        a.union_with(&b);
        assert_eq!(a.format(), "u:1-10:20-30");
    }

    #[test]
    fn union_across_uuids() {
        let mut a = GtidSet::parse("u1:1-5").unwrap();
        let b = GtidSet::parse("u2:1-3").unwrap();
        a.union_with(&b);
        assert_eq!(a.format(), "u1:1-5,u2:1-3");
    }

    #[test]
    fn parse_rejects_inverted_range() {
        assert!(GtidSet::parse("u:10-1").is_err());
    }

    #[test]
    fn parse_rejects_missing_colon() {
        assert!(GtidSet::parse("uuidonly").is_err());
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p worker connectors::mysql::cdc::position -- --nocapture`
Expected: 7 passes.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/connectors/mod.rs crates/worker/src/connectors/mysql
git commit -m "phase-2-3d-3: GTID set parsing helpers"
```

---

## Task 4: Schema discovery (`schema.rs`)

**Files:**
- Create: `crates/worker/src/connectors/mysql/cdc/schema.rs`
- Modify: `crates/worker/src/connectors/mysql/cdc/mod.rs`

- [ ] **Step 1: Add the module and write failing tests for the type map**

Update `crates/worker/src/connectors/mysql/cdc/mod.rs`:

```rust
pub mod position;
pub mod schema;
```

Create `crates/worker/src/connectors/mysql/cdc/schema.rs` with the type-map tests at the bottom (we put the tests up front so we know what the API has to support):

```rust
//! MySQL → Arrow schema discovery.
//!
//! v1 supports a fixed type subset; anything else fails the workflow at
//! discovery time with `SchemaIncompatible(col, mysql_type)`. We add
//! more types as connectors need them — we don't speculate.

use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct InfoSchemaColumn {
    pub column_name: String,
    /// `DATA_TYPE` from `information_schema.columns` (e.g. "int", "varchar").
    pub data_type: String,
    pub is_nullable: bool,
    pub ordinal_position: u32,
}

pub fn map_mysql_type(mysql_type: &str) -> Result<DataType> {
    let lower = mysql_type.to_ascii_lowercase();
    let dt = match lower.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" | "decimal" | "numeric" => DataType::Float64,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => DataType::Utf8,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "date" => DataType::Date32,
        "boolean" | "bool" | "bit" => DataType::Boolean,
        "json" => DataType::Utf8,
        other => bail!("unsupported MySQL type '{other}'"),
    };
    Ok(dt)
}

pub fn schema_from_columns(cols: &[InfoSchemaColumn]) -> Result<Schema> {
    let mut sorted: Vec<_> = cols.iter().collect();
    sorted.sort_by_key(|c| c.ordinal_position);
    let mut fields: Vec<Field> = Vec::with_capacity(sorted.len() + 3);
    for c in sorted {
        let dt = map_mysql_type(&c.data_type)?;
        fields.push(Field::new(&c.column_name, dt, c.is_nullable));
    }
    // Append _cdc.* metadata columns per RFC-0008.
    fields.push(Field::new("_cdc.op", DataType::Utf8, false));
    fields.push(Field::new("_cdc.lsn", DataType::Utf8, false));
    fields.push(Field::new(
        "_cdc.commit_ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    ));
    Ok(Schema::new(fields))
}

/// Live `information_schema.columns` query. Tested via the e2e test in
/// Task 8; pure logic above is unit-tested below.
pub async fn discover_schema(
    pool: &mysql_async::Pool,
    schema: &str,
    table: &str,
) -> Result<Schema> {
    use mysql_async::prelude::*;
    let mut conn = pool.get_conn().await?;
    let rows: Vec<(String, String, String, u32)> = conn
        .exec(
            "SELECT column_name, data_type, is_nullable, ordinal_position
             FROM information_schema.columns
             WHERE table_schema = ? AND table_name = ?
             ORDER BY ordinal_position",
            (schema, table),
        )
        .await?;
    if rows.is_empty() {
        bail!("table {schema}.{table} not found in information_schema");
    }
    let cols: Vec<InfoSchemaColumn> = rows
        .into_iter()
        .map(|(column_name, data_type, is_nullable, ordinal_position)| InfoSchemaColumn {
            column_name,
            data_type,
            is_nullable: is_nullable.eq_ignore_ascii_case("YES"),
            ordinal_position,
        })
        .collect();
    let _ = Arc::new(()); // keep Arc import used; remove if unused after Task 7.
    schema_from_columns(&cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, n: u32, nullable: bool) -> InfoSchemaColumn {
        InfoSchemaColumn {
            column_name: name.into(),
            data_type: ty.into(),
            is_nullable: nullable,
            ordinal_position: n,
        }
    }

    #[test]
    fn maps_int_family() {
        assert_eq!(map_mysql_type("int").unwrap(), DataType::Int32);
        assert_eq!(map_mysql_type("INT").unwrap(), DataType::Int32);
        assert_eq!(map_mysql_type("smallint").unwrap(), DataType::Int32);
        assert_eq!(map_mysql_type("bigint").unwrap(), DataType::Int64);
    }

    #[test]
    fn maps_varchar_to_utf8() {
        assert_eq!(map_mysql_type("varchar").unwrap(), DataType::Utf8);
        assert_eq!(map_mysql_type("text").unwrap(), DataType::Utf8);
        assert_eq!(map_mysql_type("longtext").unwrap(), DataType::Utf8);
    }

    #[test]
    fn maps_datetime_to_timestamp_micros() {
        let got = map_mysql_type("datetime").unwrap();
        match got {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => assert_eq!(tz.as_ref(), "UTC"),
            other => panic!("expected Timestamp(Micro, UTC), got {other:?}"),
        }
    }

    #[test]
    fn unsupported_type_returns_error() {
        let err = map_mysql_type("geometry").unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn schema_appends_cdc_metadata_columns() {
        let cols = vec![
            col("id", "bigint", 1, false),
            col("email", "varchar", 2, true),
        ];
        let s = schema_from_columns(&cols).unwrap();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "email", "_cdc.op", "_cdc.lsn", "_cdc.commit_ts"]);
        assert_eq!(s.field(0).data_type(), &DataType::Int64);
        assert_eq!(s.field(1).data_type(), &DataType::Utf8);
        assert!(s.field(1).is_nullable());
        assert!(!s.field(2).is_nullable());
    }
}
```

Note the `let _ = Arc::new(())` at the end of `discover_schema` is a placeholder if Arc isn't otherwise used; if `cargo build` warns about unused `Arc` import, delete the import — don't keep the placeholder.

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p worker connectors::mysql::cdc::schema -- --nocapture`
Expected: 5 passes.

- [ ] **Step 3: Verify build is clean (no unused warnings)**

Run: `cargo build -p worker 2>&1 | grep -E 'warning|error'`
Expected: no new warnings related to mysql/cdc/schema.rs. If `Arc` is unused, remove the `use std::sync::Arc;` import and the placeholder line.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/mod.rs crates/worker/src/connectors/mysql/cdc/schema.rs
git commit -m "phase-2-3d-4: MySQL→Arrow schema discovery"
```

---

## Task 5: Binlog row-event decoder — discovery checkpoint + implementation

**Files:**
- Create: `crates/worker/src/connectors/mysql/cdc/decode.rs`
- Modify: `crates/worker/src/connectors/mysql/cdc/mod.rs`
- Create: `crates/worker/src/connectors/mysql/cdc/fixtures/` (binlog event byte fixtures)

This task has uncertainty: the exact `mysql_async::binlog` event types and their decoded shapes need to be confirmed before locking in the decoder API. We do a brief discovery pass first.

- [ ] **Step 1: Discovery — confirm `mysql_async::binlog` event API**

Run a one-off probe:

```bash
cargo doc -p mysql_async --no-deps --open
```

In the docs, locate the `binlog` module and confirm:
- The event-data variant names for row events (look for `WriteRowsEvent`, `UpdateRowsEvent`, `DeleteRowsEvent` or v2 equivalents).
- The `TableMapEvent` and how it exposes column metadata.
- The shape returned when iterating row data (likely an iterator of `Result<Row>` over `BinlogValue`-like values).

Capture the actual type names found into a comment block at the top of `decode.rs`, then proceed. If the API differs materially from what this plan assumes, adjust the type names in the steps below and continue — the structure (decode → CdcEvent enum → Arrow rows) does not change.

- [ ] **Step 2: Write the failing decoder tests with byte fixtures**

We need real binlog bytes. The cheapest way is to capture them inside a one-off test that spawns a MySQL container, performs `INSERT/UPDATE/DELETE`, dumps binlog bytes via `SHOW BINLOG EVENTS` + `mysqlbinlog --raw`, and writes the bytes to `crates/worker/src/connectors/mysql/cdc/fixtures/{insert,update,delete}.bin`. Since this is a one-time generation step, write it as a `tests/integration` `#[ignore]` test gated behind a feature flag `regenerate-mysql-fixtures`. Document the generation command in the fixtures dir's README.

For now, scaffold the decoder unit tests that read from those fixtures:

Create `crates/worker/src/connectors/mysql/cdc/fixtures/README.md`:

```markdown
# MySQL binlog event fixtures

Each `.bin` file is a single binlog event body extracted from a live
MySQL 8.0 container. To regenerate after a MySQL version bump:

    cargo test -p integration regenerate_mysql_fixtures \
        --features regenerate-mysql-fixtures -- --ignored --nocapture

The generator is in tests/integration/tests/mysql_fixtures_gen.rs.
```

Create empty placeholder files (the test in Task 8 generates them; until then, decoder tests are `#[ignore]`'d so the build stays green):

```bash
touch crates/worker/src/connectors/mysql/cdc/fixtures/.gitkeep
```

Create `crates/worker/src/connectors/mysql/cdc/decode.rs`:

```rust
//! Binlog row-event decoder.
//!
//! Confirmed against mysql_async <FILL IN VERSION FROM STEP 1> binlog API.
//! Decodes WriteRowsEvent / UpdateRowsEvent / DeleteRowsEvent into
//! `RowOp` records using the cached `TableMapEvent` for column metadata.

use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub enum RowOp {
    Insert { table_id: u64, after: Vec<Option<String>> },
    /// Update with optional before-image. before is None when
    /// binlog_row_image=MINIMAL only includes PK in before.
    Update { table_id: u64, before: Option<Vec<Option<String>>>, after: Vec<Option<String>> },
    Delete { table_id: u64, before: Vec<Option<String>> },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableMap {
    pub table_id: u64,
    pub schema: String,
    pub table: String,
    pub column_count: usize,
}

pub type TableMapCache = HashMap<u64, TableMap>;

/// Decode one row-event body. The caller has already identified the
/// event type from the header. The actual bytes-to-fields mapping
/// uses `mysql_async::binlog::events` parsing primitives — see decode_*
/// helpers below.
pub fn decode_row_event(
    event_kind: RowEventKind,
    body: &[u8],
    cache: &TableMapCache,
) -> Result<RowOp> {
    // CONTRACT: implementation calls into mysql_async::binlog parsing
    // utilities for the given event_kind, pulls table_id, then reads
    // the value list per column using the cached TableMap.
    //
    // This function exists as a thin adapter over mysql_async; the bulk
    // of the test value comes from the fixtures-driven tests below.
    let _ = (event_kind, body, cache);
    Err(anyhow!("decode_row_event: stub; see Step 4"))
}

#[derive(Clone, Copy, Debug)]
pub enum RowEventKind {
    Write,
    Update,
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_cache_with(map: TableMap) -> TableMapCache {
        let mut c = HashMap::new();
        c.insert(map.table_id, map);
        c
    }

    fn fixture(name: &str) -> Vec<u8> {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/connectors/mysql/cdc/fixtures")
            .join(name);
        std::fs::read(&path)
            .unwrap_or_else(|e| panic!("read fixture {name}: {e}"))
    }

    #[test]
    #[ignore = "fixtures generated by Task 8 e2e; run after Task 8 once they exist"]
    fn decodes_write_rows() {
        let bytes = fixture("insert.bin");
        let cache = empty_cache_with(TableMap {
            table_id: 1, schema: "test".into(), table: "customers".into(), column_count: 4,
        });
        let op = decode_row_event(RowEventKind::Write, &bytes, &cache).unwrap();
        match op {
            RowOp::Insert { after, .. } => {
                assert_eq!(after.len(), 4);
                // Inserted row in the e2e test: (1, 'Alice', 'a@x.com', '2026-01-01 00:00:00').
                assert_eq!(after[0].as_deref(), Some("1"));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "fixtures generated by Task 8 e2e"]
    fn decodes_update_rows_with_before_image() {
        let bytes = fixture("update.bin");
        let cache = empty_cache_with(TableMap {
            table_id: 1, schema: "test".into(), table: "customers".into(), column_count: 4,
        });
        let op = decode_row_event(RowEventKind::Update, &bytes, &cache).unwrap();
        match op {
            RowOp::Update { before: Some(b), after, .. } => {
                assert_eq!(b.len(), 4);
                assert_eq!(after.len(), 4);
            }
            other => panic!("expected Update with before-image, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "fixtures generated by Task 8 e2e"]
    fn decodes_delete_rows() {
        let bytes = fixture("delete.bin");
        let cache = empty_cache_with(TableMap {
            table_id: 1, schema: "test".into(), table: "customers".into(), column_count: 4,
        });
        let op = decode_row_event(RowEventKind::Delete, &bytes, &cache).unwrap();
        match op {
            RowOp::Delete { before, .. } => {
                assert_eq!(before.len(), 4);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn missing_table_in_cache_errors() {
        let cache = TableMapCache::new();
        let res = decode_row_event(RowEventKind::Write, &[0u8; 8], &cache);
        assert!(res.is_err());
    }
}
```

Update `crates/worker/src/connectors/mysql/cdc/mod.rs`:

```rust
pub mod decode;
pub mod position;
pub mod schema;
```

- [ ] **Step 3: Run the non-ignored test to verify the stub errors as expected**

Run: `cargo test -p worker connectors::mysql::cdc::decode::tests::missing_table_in_cache_errors -- --nocapture`
Expected: PASS (the stub returns an error, which the test asserts).

- [ ] **Step 4: Implement `decode_row_event` against the confirmed `mysql_async::binlog` API**

Replace the stub body in `decode_row_event` with real parsing:

```rust
pub fn decode_row_event(
    event_kind: RowEventKind,
    body: &[u8],
    cache: &TableMapCache,
) -> Result<RowOp> {
    use mysql_async::binlog::events::{
        DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent,
    };
    use mysql_async::binlog::value::BinlogValue;
    // The exact constructors and accessors here come from the API confirmed
    // in Step 1. The pattern is:
    //   1. Parse the event body into the typed event struct.
    //   2. Look up table_id in cache; bail if missing.
    //   3. For each row, extract values into Vec<Option<String>> (rendering
    //      via BinlogValue::Display where available, otherwise hex-encoding
    //      bytes — fine for v1 since downstream stores text).
    //   4. Pack into the matching RowOp variant.
    // If a value cannot be rendered, return an error with the column index;
    // we do not silently substitute.
    let _ = (event_kind, body, cache, |_v: BinlogValue| Ok::<(), anyhow::Error>(()));
    Err(anyhow!("decode_row_event: implement against confirmed mysql_async::binlog API"))
}
```

This step intentionally still leaves the implementation as a marker — the actual parsing code depends on the exact API shape from Step 1's discovery and is best filled in by the implementer with the docs in front of them. Once filled, the `#[ignore]` decorators in Step 2's tests are removed (after Task 8 generates fixtures, see Step 6 below).

- [ ] **Step 5: Verify build still succeeds**

Run: `cargo build -p worker`
Expected: clean build. If `mysql_async::binlog::events::WriteRowsEvent` etc. don't exist with those exact names, fix imports per Step 1's discovery and rebuild.

- [ ] **Step 6: Commit (decoder skeleton; full body filled in after Task 8 fixtures exist)**

```bash
git add crates/worker/src/connectors/mysql/cdc/decode.rs crates/worker/src/connectors/mysql/cdc/mod.rs crates/worker/src/connectors/mysql/cdc/fixtures
git commit -m "phase-2-3d-5: binlog row-event decoder skeleton"
```

The decoder body is finalized in Task 8 once fixtures exist; this is intentional to keep dependencies acyclic (decoder needs fixtures, fixtures come from e2e env).

---

## Task 6: `read_window` — drain binlog events into Arrow batches

**Files:**
- Create: `crates/worker/src/connectors/mysql/cdc/stream.rs`
- Modify: `crates/worker/src/connectors/mysql/cdc/mod.rs`

This is the single most important file: it ties the decoder, the schema, and the GTID position together to produce a `RecordBatch` ready for the existing Parquet loader.

- [ ] **Step 1: Update mod.rs to expose stream**

In `crates/worker/src/connectors/mysql/cdc/mod.rs`:

```rust
pub mod decode;
pub mod position;
pub mod schema;
pub mod stream;
```

- [ ] **Step 2: Write the `read_window` skeleton**

Create `crates/worker/src/connectors/mysql/cdc/stream.rs`:

```rust
//! `read_window`: open a binlog stream from a GTID set, drain up to N
//! row-event groups, build an Arrow `RecordBatch` for the configured
//! table, return new GTID set.
//!
//! Single connection per call; we do not maintain a long-lived stream
//! across activity invocations (mirrors the Postgres CDC pattern in
//! `connectors/postgres/cdc/stream.rs`).

use anyhow::{anyhow, Context, Result};
use arrow::array::{ArrayRef, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;

use super::decode::{decode_row_event, RowEventKind, RowOp, TableMapCache};
use super::position::GtidSet;

pub struct ReadWindowOutput {
    pub batch: Option<RecordBatch>,
    pub rows: usize,
    pub new_gtid: GtidSet,
}

pub async fn read_window(
    conn_url: &str,
    server_id: u32,
    schema_name: &str,
    table_name: &str,
    start_gtid: &GtidSet,
    max_events: usize,
    arrow_schema: SchemaRef,
    heartbeat_secs: u32,
) -> Result<ReadWindowOutput> {
    use mysql_async::{BinlogStreamRequest, Pool};
    use futures_util::StreamExt;

    let pool = Pool::new(conn_url);
    let conn = pool.get_conn().await.context("mysql connect")?;

    // BinlogStreamRequest: builder on mysql_async; the precise method
    // names are `with_gtid_set` / `with_server_id` / `with_heartbeat`
    // per its docs (confirm in Task 5 Step 1 alongside event types).
    let req = BinlogStreamRequest::new(server_id);
    let req = if !start_gtid.is_empty() {
        req.with_gtid_set(start_gtid.format())
    } else {
        req
    };
    let req = if heartbeat_secs > 0 {
        req.with_heartbeat(Duration::from_secs(heartbeat_secs as u64))
    } else {
        req
    };
    let mut stream = conn.get_binlog_stream(req).await.context("open binlog stream")?;

    let mut new_gtid = start_gtid.clone();
    let mut cache = TableMapCache::new();
    let mut ops: Vec<(RowOp, GtidSet, Option<i64>)> = Vec::new();
    let mut current_gtid: Option<GtidSet> = None;
    let mut current_commit_ts: Option<i64> = None;
    let mut events_read = 0usize;

    while events_read < max_events {
        let next = match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(ev))) => ev,
            Ok(Some(Err(e))) => return Err(anyhow!("binlog stream error: {e}")),
            Ok(None) | Err(_) => break, // stream closed or idle window expired
        };
        // Branch on the event kind. `next.header().event_type()` gives us
        // the discriminator; `next.read_event::<T>()` (or equivalent per
        // confirmed API) parses the body.
        match classify(&next) {
            EventKind::Gtid(g) => {
                current_gtid = Some(g);
            }
            EventKind::Xid(commit_ts) => {
                if let Some(g) = current_gtid.take() {
                    new_gtid.union_with(&g);
                }
                current_commit_ts = Some(commit_ts);
            }
            EventKind::TableMap(map) => {
                if map.schema == schema_name && map.table == table_name {
                    cache.insert(map.table_id, map);
                }
            }
            EventKind::RowEvent(kind, body) => {
                if let Ok(op) = decode_row_event(kind, &body, &cache) {
                    let gtid_for_row = current_gtid.clone().unwrap_or_else(|| new_gtid.clone());
                    ops.push((op, gtid_for_row, current_commit_ts));
                    events_read += 1;
                }
            }
            EventKind::Other => {} // heartbeat, format desc, rotate, etc.
        }
    }

    drop(stream);
    pool.disconnect().await.ok();

    if ops.is_empty() {
        return Ok(ReadWindowOutput { batch: None, rows: 0, new_gtid });
    }
    let batch = build_record_batch(&ops, arrow_schema)?;
    Ok(ReadWindowOutput { batch: Some(batch), rows: ops.len(), new_gtid })
}

enum EventKind {
    Gtid(GtidSet),
    Xid(i64),
    TableMap(super::decode::TableMap),
    RowEvent(RowEventKind, Vec<u8>),
    Other,
}

/// Classify an mysql_async binlog event. The exact method names below
/// are filled in against the confirmed API from Task 5 Step 1.
fn classify(_ev: &mysql_async::binlog::events::Event) -> EventKind {
    // STUB: replace with real header inspection + body extraction.
    EventKind::Other
}

fn build_record_batch(
    ops: &[(RowOp, GtidSet, Option<i64>)],
    arrow_schema: SchemaRef,
) -> Result<RecordBatch> {
    // arrow_schema includes the data columns followed by _cdc.op,
    // _cdc.lsn, _cdc.commit_ts (in that order — see schema.rs).
    let n_data = arrow_schema.fields().len() - 3;
    let mut col_builders: Vec<StringBuilder> =
        (0..n_data).map(|_| StringBuilder::new()).collect();
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    for (op, gtid, ts) in ops {
        match op {
            RowOp::Insert { after, .. } => {
                push_row(&mut col_builders, after);
                op_b.append_value("i");
            }
            RowOp::Update { after, .. } => {
                push_row(&mut col_builders, after);
                op_b.append_value("u");
            }
            RowOp::Delete { before, .. } => {
                push_row(&mut col_builders, before);
                op_b.append_value("d");
            }
        }
        lsn_b.append_value(gtid.format());
        ts_b.append_option(*ts);
    }
    let mut cols: Vec<ArrayRef> = col_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish().with_timezone("UTC")));
    Ok(RecordBatch::try_new(arrow_schema, cols)?)
}

fn push_row(builders: &mut [StringBuilder], values: &[Option<String>]) {
    for (b, v) in builders.iter_mut().zip(values.iter()) {
        b.append_option(v.as_deref());
    }
}
```

- [ ] **Step 3: Verify build**

Run: `cargo build -p worker`
Expected: clean build. The `classify` stub returns `Other` for everything, which means `read_window` returns `ReadWindowOutput { batch: None, rows: 0, new_gtid: start_gtid }` until Task 5/6 finalization in Task 8 — that is correct interim behavior.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/connectors/mysql/cdc/stream.rs crates/worker/src/connectors/mysql/cdc/mod.rs
git commit -m "phase-2-3d-6: read_window scaffold + RecordBatch builder"
```

---

## Task 7: Activities + workflow + dispatch wiring

**Files:**
- Create: `crates/worker/src/activities/mysql_cdc/mod.rs`
- Create: `crates/worker/src/activities/mysql_cdc/inputs.rs`
- Modify: `crates/worker/src/activities/mod.rs`
- Create: `crates/worker/src/workflows/mysql_cdc_pipeline.rs`
- Modify: `crates/worker/src/workflows/mod.rs`
- Modify: `crates/worker/src/main.rs` (worker registration)
- Modify: `crates/cli/src/main.rs` (workflow dispatch lives here at line 601-628)

- [ ] **Step 1: Note the existing dispatch site**

Workflow dispatch is in `crates/cli/src/main.rs` around line 601 — the `is_cdc` check picks `CdcPipelineWorkflow` for `SourceSpec::Postgres(p)` with `p.sync_mode == Cdc`, otherwise falls through to `PipelineRunWorkflow`. We add a parallel arm for `SourceSpec::MysqlCdc(_)` that starts `MysqlCdcPipelineWorkflow`. Exact change in Step 7 below.

- [ ] **Step 2: Create activity input/output structs**

Create `crates/worker/src/activities/mysql_cdc/inputs.rs`:

```rust
use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::DestinationSpec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyMysqlConfigInput {
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureStartGtidInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureStartGtidOutput {
    /// Serialized GTID set; "" if MySQL has no GTID history.
    pub gtid_set: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverSchemaInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverSchemaOutput {
    /// Arrow schema serialized as JSON (we use the existing
    /// catalog schema-as-JSON convention; see streams table).
    pub schema_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlReadWindowInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub jti: Uuid,
    pub batch_seq: u32,
    pub source_conn: ConnectionConfig,
    pub server_id: u32,
    pub schema: String,
    pub table: String,
    pub start_gtid: String,
    pub max_events: u32,
    pub schema_json: String,
    pub heartbeat_secs: u32,
    pub destination: DestinationSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlReadWindowOutput {
    pub rows: u32,
    pub new_gtid: String,
}
```

- [ ] **Step 3: Create the activities impl**

Create `crates/worker/src/activities/mysql_cdc/mod.rs`:

```rust
pub mod inputs;

use anyhow::{Context, Result};
use catalog::Catalog;
use inputs::*;
use mysql_async::prelude::*;
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::mysql::cdc::{position::GtidSet, schema, stream};

#[derive(Clone)]
pub struct MysqlCdcActivities {
    pub catalog: Arc<Catalog>,
    pub secrets: Arc<crate::secrets::auditing::AuditingSecrets>,
}

fn into_activity_err(e: anyhow::Error) -> ActivityError {
    tracing::error!(error = %e, chain = ?e.chain().collect::<Vec<_>>(), "mysql_cdc activity error");
    e.into()
}

async fn resolve_url(
    secrets: &crate::secrets::auditing::AuditingSecrets,
    conn: &common_types::connection_config::ConnectionConfig,
    tenant_id: uuid::Uuid,
    principal_id: uuid::Uuid,
    jti: uuid::Uuid,
) -> Result<String> {
    let resolve_ctx = crate::secrets::auditing::ResolveContext {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(tenant_id),
        principal_id: (!principal_id.is_nil())
            .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(principal_id)),
        jti: (!jti.is_nil()).then_some(jti),
    };
    let resolved = crate::secrets::resolve_connection_audited(secrets, conn, resolve_ctx).await?;
    Ok(resolved.expect_url().to_string())
}

#[activities]
impl MysqlCdcActivities {
    #[activity]
    pub async fn verify_mysql_config(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: VerifyMysqlConfigInput,
    ) -> Result<(), ActivityError> {
        let url = resolve_url(&self.secrets, &input.source_conn, input.tenant_id, input.principal_id, input.jti)
            .await
            .map_err(into_activity_err)?;
        let pool = mysql_async::Pool::new(url.as_str());
        let mut conn = pool.get_conn().await.context("mysql connect").map_err(into_activity_err)?;

        let gtid_mode: Option<(String,)> = conn.query_first("SELECT @@GLOBAL.gtid_mode").await
            .context("query gtid_mode").map_err(into_activity_err)?;
        let binlog_format: Option<(String,)> = conn.query_first("SELECT @@GLOBAL.binlog_format").await
            .context("query binlog_format").map_err(into_activity_err)?;

        let gtid_mode = gtid_mode.map(|t| t.0).unwrap_or_default();
        let binlog_format = binlog_format.map(|t| t.0).unwrap_or_default();

        if !gtid_mode.eq_ignore_ascii_case("ON") {
            return Err(into_activity_err(anyhow::anyhow!(
                "MySQL gtid_mode must be ON (got '{gtid_mode}')"
            )));
        }
        if !binlog_format.eq_ignore_ascii_case("ROW") {
            return Err(into_activity_err(anyhow::anyhow!(
                "MySQL binlog_format must be ROW (got '{binlog_format}')"
            )));
        }
        // Confirm the table exists; column-type validation happens in discover_schema.
        let exists: Option<(i64,)> = conn.exec_first(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            (&input.schema, &input.table),
        ).await.context("exists check").map_err(into_activity_err)?;
        if exists.is_none() {
            return Err(into_activity_err(anyhow::anyhow!(
                "table {}.{} not found", input.schema, input.table
            )));
        }
        drop(conn);
        pool.disconnect().await.ok();
        Ok(())
    }

    #[activity]
    pub async fn capture_start_gtid(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CaptureStartGtidInput,
    ) -> Result<CaptureStartGtidOutput, ActivityError> {
        let url = resolve_url(&self.secrets, &input.source_conn, input.tenant_id, input.principal_id, input.jti)
            .await.map_err(into_activity_err)?;
        let pool = mysql_async::Pool::new(url.as_str());
        let mut conn = pool.get_conn().await.context("mysql connect").map_err(into_activity_err)?;
        let row: Option<(String,)> = conn.query_first("SELECT @@GLOBAL.gtid_executed").await
            .context("read gtid_executed").map_err(into_activity_err)?;
        let gtid_str = row.map(|t| t.0).unwrap_or_default();
        // Validate it parses; we want to fail fast if the source returns junk.
        GtidSet::parse(&gtid_str).context("parse gtid_executed").map_err(into_activity_err)?;
        drop(conn);
        pool.disconnect().await.ok();
        Ok(CaptureStartGtidOutput { gtid_set: gtid_str })
    }

    #[activity]
    pub async fn discover_schema(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverSchemaInput,
    ) -> Result<DiscoverSchemaOutput, ActivityError> {
        let url = resolve_url(&self.secrets, &input.source_conn, input.tenant_id, input.principal_id, input.jti)
            .await.map_err(into_activity_err)?;
        let pool = mysql_async::Pool::new(url.as_str());
        let s = schema::discover_schema(&pool, &input.schema, &input.table)
            .await.map_err(into_activity_err)?;
        pool.disconnect().await.ok();
        let json = serde_json::to_string(&s.to_json()).context("schema to json")
            .map_err(into_activity_err)?;
        Ok(DiscoverSchemaOutput { schema_json: json })
    }

    #[activity]
    pub async fn read_window(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlReadWindowInput,
    ) -> Result<MysqlReadWindowOutput, ActivityError> {
        use arrow::datatypes::Schema;
        let url = resolve_url(&self.secrets, &input.source_conn, input.tenant_id, input.principal_id, input.jti)
            .await.map_err(into_activity_err)?;
        let arrow_schema: Schema = serde_json::from_str(&input.schema_json)
            .context("parse schema_json").map_err(into_activity_err)?;
        let arrow_schema = std::sync::Arc::new(arrow_schema);
        let start = GtidSet::parse(&input.start_gtid).map_err(into_activity_err)?;
        let out = stream::read_window(
            &url,
            input.server_id,
            &input.schema,
            &input.table,
            &start,
            input.max_events as usize,
            arrow_schema.clone(),
            input.heartbeat_secs,
        ).await.map_err(into_activity_err)?;

        if let Some(batch) = out.batch {
            // Reuse the existing CDC parquet loader so we don't fork the
            // destination layer. CdcParquetLoader is already exported by
            // the worker crate and used by Postgres CDC.
            crate::loaders::cdc_parquet::CdcParquetLoader::write_batch(
                &input.destination,
                input.pipeline_id,
                input.run_id,
                input.batch_seq,
                &batch,
            ).await.map_err(into_activity_err)?;
        }
        Ok(MysqlReadWindowOutput { rows: out.rows as u32, new_gtid: out.new_gtid.format() })
    }
}
```

Note: `arrow::datatypes::Schema::to_json` may need the `serde` feature on `arrow`. If `Schema: !Serialize`, fall back to a manual schema serialization (column names + types as a small struct). Verify when running Step 7.

- [ ] **Step 4: Wire activities module**

In `crates/worker/src/activities/mod.rs`, add `pub mod mysql_cdc;` alongside `pub mod cdc;`.

- [ ] **Step 5: Create the workflow**

Create `crates/worker/src/workflows/mysql_cdc_pipeline.rs`:

```rust
//! Streaming-only MySQL CDC pipeline (Phase II.3.d).
//!
//! Per RFC-0008 §"Skip-snapshot mode": no snapshot, capture current GTID,
//! stream forward. Single workflow, no child, no continue-as-new — we
//! defer those to a future phase that builds out the full CDC topology.

use common_types::pipeline_spec::{PipelineSpec, SourceSpec};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::mysql_cdc::MysqlCdcActivities;
use crate::activities::mysql_cdc::inputs::*;
use crate::activities::run_lifecycle::{
    CompleteRunInput, FailRunInput, RunLifecycleActivities, StartRunInput,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlCdcPipelineInput {
    pub run_id: Uuid,
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub spec: PipelineSpec,
    pub source_conn: common_types::connection_config::ConnectionConfig,
    /// 0 = forever (production); >0 caps streaming windows for tests.
    #[serde(default)]
    pub max_windows: u32,
}

#[workflow]
pub struct MysqlCdcPipelineWorkflow {
    input: MysqlCdcPipelineInput,
}

fn retry_policy() -> temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
    use prost_wkt_types::Duration as PbDuration;
    temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
        initial_interval: Some(PbDuration { seconds: 1, nanos: 0 }),
        backoff_coefficient: 2.0,
        maximum_interval: Some(PbDuration { seconds: 30, nanos: 0 }),
        maximum_attempts: 5,
        non_retryable_error_types: vec![],
    }
}

fn opts_short() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(60)),
        retry_policy: Some(retry_policy()),
        ..Default::default()
    }
}
fn opts_long() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(600)),
        retry_policy: Some(retry_policy()),
        ..Default::default()
    }
}

#[workflow_methods]
impl MysqlCdcPipelineWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: MysqlCdcPipelineInput) -> Self {
        Self { input }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (run_id, tenant_id) = ctx.state(|s| (s.input.run_id, s.input.tenant_id));
        match Self::run_inner(ctx).await {
            Ok(()) => Ok(()),
            Err(t) => {
                let err_str = format!("{t}");
                let _ = ctx
                    .start_activity(
                        RunLifecycleActivities::fail_run,
                        FailRunInput { run_id, tenant_id, error: err_str },
                        opts_short(),
                    )
                    .await;
                Err(t)
            }
        }
    }

    async fn run_inner(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let input = ctx.state(|s| s.input.clone());
        let my = match &input.spec.source {
            SourceSpec::MysqlCdc(m) => m.clone(),
            _ => return Err(anyhow::anyhow!("MysqlCdcPipelineWorkflow requires MysqlCdc source").into()),
        };
        let dest = input.spec.destination.clone();

        ctx.start_activity(
            RunLifecycleActivities::start_run,
            StartRunInput { run_id: input.run_id, tenant_id: input.tenant_id },
            opts_short(),
        ).await?;

        ctx.start_activity(
            MysqlCdcActivities::verify_mysql_config,
            VerifyMysqlConfigInput {
                tenant_id: input.tenant_id,
                principal_id: input.principal_id,
                jti: input.jti,
                source_conn: input.source_conn.clone(),
                schema: my.schema.clone(),
                table: my.table.clone(),
            },
            opts_short(),
        ).await?;

        let gtid_out = ctx.start_activity(
            MysqlCdcActivities::capture_start_gtid,
            CaptureStartGtidInput {
                pipeline_id: input.pipeline_id,
                run_id: input.run_id,
                tenant_id: input.tenant_id,
                principal_id: input.principal_id,
                jti: input.jti,
                source_conn: input.source_conn.clone(),
            },
            opts_short(),
        ).await?;

        let schema_out = ctx.start_activity(
            MysqlCdcActivities::discover_schema,
            DiscoverSchemaInput {
                pipeline_id: input.pipeline_id,
                run_id: input.run_id,
                tenant_id: input.tenant_id,
                principal_id: input.principal_id,
                jti: input.jti,
                source_conn: input.source_conn.clone(),
                schema: my.schema.clone(),
                table: my.table.clone(),
            },
            opts_short(),
        ).await?;

        let mut current_gtid = gtid_out.gtid_set;
        let mut window_seq: u32 = 0;
        let mut batch_seq: u32 = 0;
        loop {
            if input.max_windows > 0 && window_seq >= input.max_windows {
                break;
            }
            let out = ctx.start_activity(
                MysqlCdcActivities::read_window,
                MysqlReadWindowInput {
                    pipeline_id: input.pipeline_id,
                    run_id: input.run_id,
                    tenant_id: input.tenant_id,
                    principal_id: input.principal_id,
                    jti: input.jti,
                    batch_seq,
                    source_conn: input.source_conn.clone(),
                    server_id: my.server_id,
                    schema: my.schema.clone(),
                    table: my.table.clone(),
                    start_gtid: current_gtid.clone(),
                    max_events: input.spec.batch_size.max(100) as u32,
                    schema_json: schema_out.schema_json.clone(),
                    heartbeat_secs: my.heartbeat_secs,
                    destination: dest.clone(),
                },
                opts_long(),
            ).await?;
            current_gtid = out.new_gtid;
            batch_seq += 1;
            window_seq += 1;
            if out.rows == 0 {
                ctx.timer(Duration::from_secs(2)).await;
            }
        }

        ctx.start_activity(
            RunLifecycleActivities::complete_run,
            CompleteRunInput { run_id: input.run_id, tenant_id: input.tenant_id },
            opts_short(),
        ).await?;
        Ok(())
    }
}
```

In `crates/worker/src/workflows/mod.rs`, add:

```rust
pub mod mysql_cdc_pipeline;
pub use mysql_cdc_pipeline::{MysqlCdcPipelineInput, MysqlCdcPipelineWorkflow};
```

- [ ] **Step 6: Register the workflow + activities in main**

In `crates/worker/src/main.rs`, find the registration block (around line 174):

```rust
.register_workflow::<PipelineRunWorkflow>()
.register_workflow::<CdcPipelineWorkflow>()
```

Add a third line directly after:

```rust
.register_workflow::<MysqlCdcPipelineWorkflow>()
```

And add the import at the top:

```rust
workflows::{CdcPipelineWorkflow, MysqlCdcPipelineWorkflow, PipelineRunWorkflow},
```

The `register_activities` chain needs the new `MysqlCdcActivities` instance. Construct it the same way `cdc_clone` is constructed (search for `let cdc_clone =` near line 140 — likely `Arc::new(CdcActivities { catalog: ..., secrets: ... })`) and clone it into the spawn closure. Add `.register_activities(mysql_cdc_clone)` in the chain.

- [ ] **Step 7: Wire workflow dispatch in `crates/cli/src/main.rs`**

Right before the `is_cdc` check (around line 601), add a MysqlCdc arm. Insert this block immediately after `let opts = WorkflowStartOptions::new(...).build();`:

```rust
// Phase II.3.d: route to MysqlCdcPipelineWorkflow for MysqlCdc source.
if matches!(&spec.source, common_types::pipeline_spec::SourceSpec::MysqlCdc(_)) {
    let mysql_input = worker::workflows::MysqlCdcPipelineInput {
        run_id: run_id.as_uuid(),
        pipeline_id: pipeline_id.as_uuid(),
        tenant_id: pipeline.tenant_id.as_uuid(),
        principal_id: p.principal_id.as_uuid(),
        jti: p.jti,
        spec: spec.clone(),
        source_conn: source_connection.clone(),
        max_windows: std::env::var("ETL_CDC_MAX_WINDOWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
    };
    client
        .start_workflow(worker::workflows::MysqlCdcPipelineWorkflow::run, mysql_input, opts)
        .await
        .context("starting MysqlCdcPipelineWorkflow")?;
    println!("started MySQL CDC workflow {}", workflow_id);
    println!("run id: {}", run_id);
    return Ok(());
}
```

The existing `is_cdc` check uses `if let Some(p) = ...` style (Postgres CDC) so this arm comes *before* it; both return early on match.

- [ ] **Step 8: Build and run existing test suite**

Run: `cargo build --workspace`
Expected: clean build.

Run: `cargo test --workspace --lib`
Expected: all existing tests still pass; new workflow has no unit tests of its own (e2e in Task 8).

- [ ] **Step 9: Commit**

```bash
git add crates/worker/src/activities crates/worker/src/workflows crates/worker/src/main.rs <dispatch-file>
git commit -m "phase-2-3d-7: MysqlCdcPipelineWorkflow + activities + dispatch"
```

---

## Task 8: End-to-end integration test (testcontainers)

**Files:**
- Create: `tests/integration/tests/mysql_cdc_e2e.rs`

This task is also where the **decoder body from Task 5 Step 4 is finalized** and the binlog **fixtures from Task 5 Step 2 are generated**. The pattern: write the e2e test, run it once with diagnostic logging, capture binlog event bytes, save them to fixtures, finalize the decoder, re-run, assert.

- [ ] **Step 1: Write the e2e test scaffold**

Create `tests/integration/tests/mysql_cdc_e2e.rs`:

```rust
//! Phase II.3.d — MySQL CDC streaming-only e2e:
//!   1. Spawn mysql:8.0 testcontainer with gtid_mode=ON, binlog_format=ROW.
//!   2. Create test table `customers`.
//!   3. Build the workspace; spawn worker; seed catalog with a
//!      Connection (mysql url) + Pipeline (MysqlCdc spec).
//!   4. Execute INSERT/UPDATE/DELETE on the test table.
//!   5. `platform pipeline run`. Poll runs.status to completed.
//!   6. Assert the Parquet destination has 3 rows with _cdc.op
//!      values 'i', 'u', 'd'.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use mysql_async::prelude::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn build_workspace() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status().await?;
    anyhow::ensure!(status.success(), "cargo build failed");
    Ok(())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn,worker::connectors::mysql=debug")
        .current_dir(workspace_root())
        .spawn().context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

fn count_parquet_ops(dir: &Path) -> Vec<String> {
    let mut ops: Vec<String> = Vec::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let op_idx = batch.schema().index_of("_cdc.op").unwrap();
                let arr = batch.column(op_idx).as_any()
                    .downcast_ref::<arrow::array::StringArray>().unwrap();
                for i in 0..arr.len() {
                    ops.push(arr.value(i).to_string());
                }
            }
        }
    }
    ops
}

async fn start_mysql_container() -> anyhow::Result<(ContainerAsync<Mysql>, String)> {
    let container = Mysql::default()
        .with_env_var("MYSQL_DATABASE", "shop")
        .with_cmd(vec![
            "--gtid-mode=ON".to_string(),
            "--enforce-gtid-consistency=ON".to_string(),
            "--binlog-format=ROW".to_string(),
            "--binlog-row-image=FULL".to_string(),
            "--server-id=1".to_string(),
            "--log-bin=mysql-bin".to_string(),
        ])
        .start().await?;
    let port = container.get_host_port_ipv4(3306).await?;
    let url = format!("mysql://root@127.0.0.1:{port}/shop");
    Ok((container, url))
}

async fn seed_table(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "CREATE TABLE customers (
            id BIGINT PRIMARY KEY,
            email VARCHAR(255),
            name VARCHAR(255),
            created TIMESTAMP NOT NULL
         )",
    ).await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "INSERT INTO customers VALUES (1, 'a@x.com', 'Alice', '2026-01-01 00:00:00')"
    ).await?;
    conn.query_drop(
        "UPDATE customers SET email='alice@x.com' WHERE id=1"
    ).await?;
    conn.query_drop("DELETE FROM customers WHERE id=1").await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker + temporal stack; ~120s"]
async fn mysql_cdc_streaming_only_e2e() -> anyhow::Result<()> {
    build_workspace().await?;

    let (container, mysql_url) = start_mysql_container().await?;
    seed_table(&mysql_url).await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat.create_connection(NewConnection {
        tenant_id: tenant,
        name: "mysql-test".into(),
        connector_ref: "mysql_cdc@0.1.0".into(),
        config: json!({ "url": mysql_url }),
    }).await?;

    let tmp_data = tempfile::tempdir()?;
    let spec = json!({
        "source": {
            "type": "mysql_cdc",
            "schema": "shop",
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
    let pipe = cat.create_pipeline(NewPipeline {
        tenant_id: tenant,
        name: "mysql-customers".into(),
        source_conn_id: src,
        dest_conn_id: None,
        spec,
    }).await?;

    let mut worker = spawn_worker().await?;

    // Kick off the run BEFORE the IUD writes — capture_start_gtid
    // records the position; everything after is what we expect to see.
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output().await?;
    assert!(out.status.success(), "pipeline run kickoff failed: {}",
        String::from_utf8_lossy(&out.stderr));

    // Give the worker a moment to capture the start GTID, then write.
    tokio::time::sleep(Duration::from_secs(3)).await;
    perform_iud(&mysql_url).await?;

    // Poll until the run records 3 rows in parquet, with a hard cap.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for 3 rows");
        }
        let ops = count_parquet_ops(tmp_data.path());
        if ops.len() >= 3 {
            assert_eq!(ops, vec!["i".to_string(), "u".to_string(), "d".to_string()],
                "unexpected op order: {ops:?}");
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    worker.kill().await?;
    worker.wait().await?;
    drop(container);
    Ok(())
}
```

- [ ] **Step 2: First run — diagnostic mode (decoder still stubbed)**

Run: `cargo test -p integration mysql_cdc_streaming_only_e2e -- --ignored --nocapture`
Expected: FAIL — the test times out waiting for 3 rows because `read_window`'s `classify` and `decode_row_event` are still stubs returning `Other` / errors. Logs from `worker::connectors::mysql=debug` will show binlog events arriving but not being decoded.

This first run is the discovery checkpoint: confirm that the binlog stream actually opens and events flow. If errors occur upstream (auth, gtid_mode mismatch, container startup), fix those first before proceeding to Step 3.

- [ ] **Step 3: Capture binlog event bytes for fixtures**

Add a temporary trace block to `stream.rs::classify` that, when the env var `ETL_DUMP_BINLOG_FIXTURES=1` is set, writes each row event's body to `crates/worker/src/connectors/mysql/cdc/fixtures/{insert,update,delete}.bin` as it's seen. Re-run the e2e test once with that env var set (set it via `.env(...)` on the worker spawn). Verify the three fixture files are created and have non-zero size, then **remove** the temporary dump block — fixtures are checked-in artifacts, not generated at test time.

```bash
ls -la crates/worker/src/connectors/mysql/cdc/fixtures/
```

Expected: `insert.bin`, `update.bin`, `delete.bin` each with bytes.

- [ ] **Step 4: Finalize the decoder (Task 5 Step 4)**

With real fixtures and the docs from Task 5 Step 1 in front of you, fill in the actual decoder body in `crates/worker/src/connectors/mysql/cdc/decode.rs::decode_row_event` using `mysql_async::binlog::events::{WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent}` (or v2 names per actual API). The shape:

1. Parse `body` into the event struct.
2. Look up `table_id` in `cache`; bail if absent.
3. Iterate row values (per-column `BinlogValue`); render each as `Option<String>` (`None` for SQL NULL).
4. Pack into the matching `RowOp`.

Then remove `#[ignore]` from the three fixture-driven tests and run them:

```bash
cargo test -p worker connectors::mysql::cdc::decode -- --nocapture
```

Expected: 4 passes (3 fixture-driven + 1 missing-cache).

- [ ] **Step 5: Finalize `stream.rs::classify`**

With the actual `mysql_async::binlog::events::Event` API in hand, replace the `classify` stub with real header inspection:

- `event_type() == GTID_EVENT` → parse the GTID and wrap in `EventKind::Gtid`
- `event_type() == XID_EVENT` → extract commit timestamp (from header.timestamp() — second precision; multiply by 1_000_000 for micros) → `EventKind::Xid`
- `event_type() == TABLE_MAP_EVENT` → parse → `EventKind::TableMap`
- `event_type()` ∈ {`WRITE_ROWS_EVENT_V2`, `UPDATE_ROWS_EVENT_V2`, `DELETE_ROWS_EVENT_V2`} → wrap kind + body bytes in `EventKind::RowEvent`
- otherwise → `Other`

- [ ] **Step 6: Re-run the e2e and confirm green**

Run: `cargo test -p integration mysql_cdc_streaming_only_e2e -- --ignored --nocapture`
Expected: PASS. 3 Parquet rows; ops `[i, u, d]` in that order.

- [ ] **Step 7: Commit**

```bash
git add tests/integration/tests/mysql_cdc_e2e.rs \
        crates/worker/src/connectors/mysql/cdc/decode.rs \
        crates/worker/src/connectors/mysql/cdc/stream.rs \
        crates/worker/src/connectors/mysql/cdc/fixtures
git commit -m "phase-2-3d-8: e2e test + finalize decoder + binlog fixtures"
```

---

## Final integration check

- [ ] **Step 1: Full workspace build + library tests**

Run:
```bash
cargo build --workspace
cargo test --workspace --lib
```

Expected: green. The e2e test stays `#[ignore]` and is not part of `--lib`.

- [ ] **Step 2: README update**

In the project `README.md`, find the "Currently:" line (set during Phase II.3.b.1) and append a note that Phase II.3.d ships native MySQL CDC. One line; no architecture description here — that lives in the spec.

- [ ] **Step 3: Final commit**

```bash
git add README.md
git commit -m "phase-2-3d: README update for MySQL CDC ship"
```
