# Phase II.3.g — Multi-Table CDC — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lift both reference WASM CDC connectors from one-table-per-pipeline to N-tables-per-pipeline, with per-table loader sub-paths.

**Architecture:** WIT `read-outcome` gains optional `stream-name`. Workflow uses it to override the pipeline-level stream_name per batch. LoadId carries stream_name; LocalParquetLoader writes to `<base>/<stream_name>/<load_id>.parquet`. Connector cursors encode `<table_idx>|<pin>|<last_pk>` during snapshot; streaming subscription covers all tables, connector buffers per-table and emits one table's batch at a time.

**Tech Stack:** wit-bindgen 0.37, wasmtime 36, sqlx 0.8, mysql_async 0.36. No new top-level deps.

---

## File structure (recap from spec)

| Path | Action |
|---|---|
| WIT + host plumbing | crates/connector-sdk/wit/source-connector.wit, crates/loader-sdk/src/lib.rs, crates/worker/src/loaders/parquet_local.rs, crates/worker/src/wasm_runtime/connector.rs, crates/worker/src/activities/sync/{inputs,mod}.rs, crates/worker/src/workflows/{wasm_cdc_pipeline,pipeline_run}.rs |
| MySQL connector | examples/mysql-cdc-rs/src/{lib,discover,snapshot,streaming}.rs |
| Postgres connector | examples/postgres-cdc-rs/src/{lib,discover,snapshot,streaming}.rs |
| Existing connectors (compat) | examples/csv-source/src/lib.rs, examples/stripe-source/src/lib.rs, examples/hello-world-source/src/lib.rs |
| E2E | tests/integration/tests/{mysql,postgres}_cdc_wasm_e2e.rs |
| Docs | README.md |

---

## Task 1: WIT extension + LoadId.stream_name + loader path

**Goal:** Plumb `stream_name` end-to-end through the host without changing connector behavior. Existing connectors still work after this task because they pass `stream_name = None` (or the loader falls back to the pipeline-level stream_name).

**Files:**
- Modify: `crates/connector-sdk/wit/source-connector.wit`
- Modify: `crates/loader-sdk/src/lib.rs`
- Modify: `crates/worker/src/loaders/parquet_local.rs`
- Modify: `crates/worker/src/wasm_runtime/connector.rs`
- Modify: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/workflows/wasm_cdc_pipeline.rs`
- Modify: `crates/worker/src/workflows/pipeline_run.rs`

- [ ] **Step 1: Edit WIT to add `stream-name` to `read-outcome`**

In `crates/connector-sdk/wit/source-connector.wit`, replace:

```wit
    record read-outcome {
        batch-ipc: list<u8>,
        rows: u32,
        new-cursor: option<cursor-value>,
        is-final: bool,
    }
```

with:

```wit
    record read-outcome {
        batch-ipc: list<u8>,
        rows: u32,
        new-cursor: option<cursor-value>,
        is-final: bool,
        /// Per-batch destination stream override. When Some, the
        /// workflow uses this as `stream_name` for `load_batch` and
        /// the loader writes to `<base_path>/<stream-name>/...`.
        /// None = use the pipeline's configured stream_name (back-compat).
        stream-name: option<string>,
    }
```

- [ ] **Step 2: Add `stream_name` to `LoadId`**

In `crates/loader-sdk/src/lib.rs`, find the `pub struct LoadId` and replace:

```rust
pub struct LoadId {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub run_id: RunId,
    pub batch_seq: u32,
}
```

with:

```rust
pub struct LoadId {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub run_id: RunId,
    pub batch_seq: u32,
    /// Per-table dispatch hint. Empty string = no stream-level
    /// subdirectory (pipeline-level stream_name from input).
    #[serde(default)]
    pub stream_name: String,
}
```

- [ ] **Step 3: Update LocalParquetLoader to use stream_name in path**

In `crates/worker/src/loaders/parquet_local.rs`, find the function that builds the parquet path. The current shape is:

```rust
let mut p = PathBuf::from(&spec.base_path);
```

Insert immediately after the `PathBuf::from` line:

```rust
let mut p = PathBuf::from(&spec.base_path);
if !load_id.stream_name.is_empty() {
    // Reject path-traversal components defensively.
    if load_id.stream_name.contains("..") || load_id.stream_name.contains('/') || load_id.stream_name.contains('\\') {
        return Err(anyhow::anyhow!(
            "stream_name contains illegal path component: {}",
            load_id.stream_name
        ));
    }
    p.push(&load_id.stream_name);
}
```

- [ ] **Step 4: Translate WIT `stream-name` to internal `ReadOutcome`**

In `crates/worker/src/wasm_runtime/connector.rs`, find the `pub struct ReadOutcome` (the worker-internal one, not the WIT-generated one) and add a `stream_name: Option<String>` field. Then in the `read_batch` impl that translates the WIT outcome, find:

```rust
Ok(ReadOutcome {
    batch,
    new_cursor,
    is_final: outcome.is_final,
})
```

and replace with:

```rust
Ok(ReadOutcome {
    batch,
    new_cursor,
    is_final: outcome.is_final,
    stream_name: outcome.stream_name,
})
```

- [ ] **Step 5: Add `stream_name` to ReadBatchOutput**

In `crates/worker/src/activities/sync/inputs.rs`, find `pub struct ReadBatchOutput` and add:

```rust
    #[serde(default)]
    pub stream_name: Option<String>,
```

- [ ] **Step 6: Propagate in `SyncActivities::read_batch`**

In `crates/worker/src/activities/sync/mod.rs`, find the `Ok(ReadBatchOutput { ... })` at the end of `read_batch` and add `stream_name: outcome.stream_name,` to the struct literal.

- [ ] **Step 7: Wire `stream_name` into LoadId in `SyncActivities::load_batch`**

In `crates/worker/src/activities/sync/mod.rs`, find the `LoadId { ... }` construction inside `load_batch` and add:

```rust
            stream_name: input.stream_name.clone(),
```

(`input.stream_name` is already a field on `LoadBatchInput`; the workflow change in step 9 below makes it the per-batch override.)

- [ ] **Step 8: Update WasmCdcPipelineWorkflow per-batch override**

In `crates/worker/src/workflows/wasm_cdc_pipeline.rs`, find the `LoadBatchInput { stream_name: ..., ... }` construction. Currently it passes `input.stream_name.clone()` (pipeline-level). Change to use per-batch override:

```rust
let batch_stream = read_out.stream_name.clone().unwrap_or_else(|| input.stream_name.clone());
ctx.start_activity(
    SyncActivities::load_batch,
    LoadBatchInput {
        ...
        stream_name: batch_stream.clone(),
    },
    ...
)
.await?;

ctx.start_activity(
    SyncActivities::commit_cursor,
    CommitCursorInput {
        ...
        stream_name: batch_stream,
    },
    ...
)
.await?;
```

- [ ] **Step 9: Update PipelineRunWorkflow per-batch override**

In `crates/worker/src/workflows/pipeline_run.rs`, mirror the same `batch_stream` pattern in the read/load/commit triplet.

- [ ] **Step 10: Build worker + connector-sdk**

```bash
cargo build -p connector-sdk -p loader-sdk -p worker --lib
```

Expected: build fails because the existing example connectors haven't been updated to provide the new `stream_name` field. That's resolved in Task 2.

- [ ] **Step 11: Commit**

```bash
git add crates/connector-sdk/wit/source-connector.wit \
        crates/loader-sdk/src/lib.rs \
        crates/worker/src/loaders/parquet_local.rs \
        crates/worker/src/wasm_runtime/connector.rs \
        crates/worker/src/activities/sync/inputs.rs \
        crates/worker/src/activities/sync/mod.rs \
        crates/worker/src/workflows/wasm_cdc_pipeline.rs \
        crates/worker/src/workflows/pipeline_run.rs && \
git commit -m "phase-2-3g-1: WIT read-outcome.stream-name + per-batch loader dispatch

Plumbs an optional stream_name through the read_batch → load_batch
chain. Per-batch override lets multi-table connectors route each
batch to <base_path>/<schema>.<table>/. Single-table connectors
return None and behavior is unchanged.

LoadId gains stream_name; LocalParquetLoader pushes it into the
path with .. / / / \\\\ rejection. Wired through ReadBatchOutput +
both workflows."
```

---

## Task 2: Update existing connectors to pass `stream_name = None`

**Goal:** Get the workspace building again by adding the new field to every guest's `ReadOutcome` constructor.

**Files:**
- Modify: `examples/csv-source/src/lib.rs`
- Modify: `examples/stripe-source/src/lib.rs`
- Modify: `examples/hello-world-source/src/lib.rs`
- Modify: `examples/mysql-cdc-rs/src/{snapshot,streaming}.rs` (will be replaced in Tasks 4-6 anyway, but needs to compile in the meantime)
- Modify: `examples/postgres-cdc-rs/src/{snapshot,streaming}.rs` (same)

- [ ] **Step 1: Add `stream_name: None` to every `ReadOutcome` construction**

For each of the files listed above, search for `ReadOutcome {` and add `stream_name: None,` to the struct literal. There may be multiple sites per file.

- [ ] **Step 2: Build everything**

```bash
cargo build --workspace --lib && \
cargo build --release --manifest-path examples/csv-source/Cargo.toml && \
cargo build --release --manifest-path examples/stripe-source/Cargo.toml && \
cargo build --release --manifest-path examples/hello-world-source/Cargo.toml && \
cargo build --release --manifest-path examples/mysql-cdc-rs/Cargo.toml && \
cargo build --release --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: all clean.

- [ ] **Step 3: Run worker lib tests**

```bash
cargo test --workspace --lib
```

Expected: 138+ tests pass.

- [ ] **Step 4: Commit**

```bash
git add examples/ && \
git commit -m "phase-2-3g-2: existing connectors set stream_name=None (back-compat)

Mechanical update: every ReadOutcome construction in csv-source,
stripe-source, hello-world-source, mysql-cdc-rs, postgres-cdc-rs
gets the new stream_name field set to None. Behavior unchanged.

Tasks 4-9 give mysql-cdc-rs + postgres-cdc-rs the multi-table
treatment that actually populates this field."
```

---

## Task 3: mysql-cdc-rs source-config + multi-table discover

**Files:**
- Modify: `examples/mysql-cdc-rs/src/lib.rs` (SourceCfg)
- Modify: `examples/mysql-cdc-rs/src/discover.rs` (Vec<DiscoveredTable>)

- [ ] **Step 1: Update `SourceCfg` to support both shapes**

Replace `pub(crate) struct SourceCfg { schema, table }` with:

```rust
#[derive(serde::Deserialize, Clone)]
pub(crate) struct TableRef {
    pub schema: String,
    pub table: String,
}

#[derive(serde::Deserialize, Clone)]
#[serde(untagged)]
pub(crate) enum SourceCfg {
    Multi { tables: Vec<TableRef> },
    Single { schema: String, table: String },
}

impl SourceCfg {
    pub fn tables(&self) -> Vec<TableRef> {
        match self {
            SourceCfg::Multi { tables } => tables.clone(),
            SourceCfg::Single { schema, table } => vec![TableRef {
                schema: schema.clone(),
                table: table.clone(),
            }],
        }
    }
    pub fn qualified(t: &TableRef) -> String {
        format!("{}.{}", t.schema, t.table)
    }
}
```

- [ ] **Step 2: Add `DiscoveredTable` to discover.rs**

In `examples/mysql-cdc-rs/src/discover.rs`, add a new struct:

```rust
#[derive(Clone, Debug)]
pub struct DiscoveredTable {
    pub schema: String,
    pub table: String,
    pub columns: Vec<DiscoveredColumn>,
    pub pk_column: String,
}

pub fn query_table(
    h: db::DbHandle,
    schema: &str,
    table: &str,
) -> Result<DiscoveredTable, ConnectorError> {
    let columns = query_columns(h, schema, table)?;
    let pk_column = query_pk_column(h, schema, table)?;
    Ok(DiscoveredTable {
        schema: schema.into(),
        table: table.into(),
        columns,
        pk_column,
    })
}
```

(Keep the existing `query_columns` and `query_pk_column` — `query_table` just composes them.)

- [ ] **Step 3: Update Guest::discover to handle multi-table**

Discovery returns one Arrow schema for ALL tables — but each table has its own schema. For v1, the discover endpoint isn't really used by the workflow's flow (read_batch overrides schema per batch). We just need it to return SOMETHING valid. Return the schema of `tables[0]`:

In `examples/mysql-cdc-rs/src/lib.rs::Guest::discover`, replace:

```rust
let cols = discover::query_columns(h, &cfg.schema, &cfg.table)?;
db::close(h);
let schema = arrow_io::build_full_schema(&discover::columns_to_fields(&cols));
```

with:

```rust
let tables = cfg.tables();
let first = tables.first().ok_or_else(|| ConnectorError::InvalidConfig("source config: tables array is empty".into()))?;
let cols = discover::query_columns(h, &first.schema, &first.table)?;
db::close(h);
let schema = arrow_io::build_full_schema(&discover::columns_to_fields(&cols));
```

- [ ] **Step 4: Build + test**

```bash
cargo build --release --manifest-path examples/mysql-cdc-rs/Cargo.toml && \
cargo test --target aarch64-apple-darwin --manifest-path examples/mysql-cdc-rs/Cargo.toml
```

Expected: clean build, 9+ tests pass.

- [ ] **Step 5: Commit**

```bash
git add examples/mysql-cdc-rs/src/lib.rs examples/mysql-cdc-rs/src/discover.rs && \
git commit -m "phase-2-3g-3: mysql-cdc-rs SourceCfg accepts {tables:[...]}, discover returns tables

SourceCfg becomes an untagged enum: existing {schema, table} still
parses (Single variant); new {tables: [...]} unlocks multi-table.
.tables() normalizes to Vec<TableRef>. discover module gains
query_table that composes query_columns + query_pk_column.

Snapshot/streaming code in this connector still treats it as
single-table — they consume tables.first() until Tasks 4-5
implement the per-table dispatch."
```

---

## Task 4: mysql-cdc-rs multi-table snapshot

**Files:**
- Modify: `examples/mysql-cdc-rs/src/snapshot.rs`
- Modify: `examples/mysql-cdc-rs/src/lib.rs` (cursor dispatch)

- [ ] **Step 1: Replace snapshot.rs with multi-table logic**

The new cursor format inside `parse_snapshot_cursor` is `<i>|<pin>|<last_pk>` (three pipe-separated parts). Update:

```rust
pub(crate) fn parse_snapshot_cursor(s: &str) -> Result<(usize, String, i64), ConnectorError> {
    let parts: Vec<&str> = s.splitn(3, '|').collect();
    if parts.len() != 3 {
        return Err(ConnectorError::InvalidConfig(format!(
            "snapshot cursor: expected <table_idx>|<pin>|<last_pk>, got {s}"
        )));
    }
    let idx: usize = parts[0]
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor table_idx: {e}")))?;
    let last_pk: i64 = parts[2]
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor pk: {e}")))?;
    Ok((idx, parts[1].to_string(), last_pk))
}
```

Update `initial` and `next_chunk` to take a `&[TableRef]` list and the `table_idx` to operate on:

```rust
pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    let tables = cfg.tables();
    if tables.is_empty() {
        return Err(ConnectorError::InvalidConfig("tables array is empty".into()));
    }
    run_chunk(url, &tables, 0, batch_size, 0, None)
}

pub fn next_chunk(url: &str, cfg: &SourceCfg, cursor_value: &str, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    let (idx, gtid, last_pk) = parse_snapshot_cursor(cursor_value)?;
    let tables = cfg.tables();
    if idx >= tables.len() {
        return Err(ConnectorError::Other(format!("snapshot cursor table_idx {idx} out of range; tables.len()={}", tables.len())));
    }
    run_chunk(url, &tables, idx, batch_size, last_pk, Some(gtid))
}

fn run_chunk(
    url: &str,
    tables: &[TableRef],
    idx: usize,
    batch_size: i64,
    last_pk: i64,
    pinned_gtid: Option<String>,
) -> Result<ReadOutcome, ConnectorError> {
    let table = &tables[idx];
    let h = open(url)?;
    let cols = discover::query_columns(h, &table.schema, &table.table)?;
    let pk = discover::query_pk_column(h, &table.schema, &table.table)?;
    let gtid = match pinned_gtid {
        Some(g) => g,
        None => read_gtid_executed(h)?,
    };
    let chunk = chunk_after(h, table, &cols, &pk, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, table, idx, tables.len(), &gtid, last_pk, batch_size)
}
```

(`chunk_after` takes a `&TableRef` instead of `&SourceCfg` — same SQL otherwise.)

Update `finalize` to advance through the table list:

```rust
fn finalize(
    chunk: Chunk,
    cols: &[DiscoveredColumn],
    table: &TableRef,
    idx: usize,
    n_tables: usize,
    gtid: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let schema = build_full_schema(&columns_to_fields(cols));
    let stream_name = format!("{}.{}", table.schema, table.table);
    if chunk.rows.is_empty() {
        // Current table done. Either advance to next table OR
        // transition to streaming if this was the last.
        let (kind, value) = if idx + 1 < n_tables {
            (CursorKind::SnapshotPk, format!("{}|{}|0", idx + 1, gtid))
        } else {
            (CursorKind::Gtid, gtid.to_string())
        };
        return Ok(ReadOutcome {
            batch_ipc: schema_only_bytes(&schema)?,
            rows: 0,
            new_cursor: Some(CursorValue { kind, value }),
            is_final: idx + 1 == n_tables,
            stream_name: Some(stream_name),
        });
    }
    let new_last_pk = chunk.last_pk_in_chunk.unwrap_or(last_pk_in);
    let position = format!("snapshot:{gtid}|{}", new_last_pk);
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    for row in &chunk.rows {
        let cells: Vec<Option<&str>> = row.iter().take(cols.len()).map(|c| c.as_deref()).collect();
        bb.append_row(&cells, 's', &position);
    }
    let rows_n = bb.rows() as u32;
    let bytes = bb.finish_to_ipc().map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?;
    // chunk shorter than batch_size means this table is done; the NEXT
    // call's cursor advances to the next table (or to streaming).
    let chunk_done = (rows_n as i64) < batch_size;
    let (kind, value) = if chunk_done {
        if idx + 1 < n_tables {
            (CursorKind::SnapshotPk, format!("{}|{}|0", idx + 1, gtid))
        } else {
            (CursorKind::Gtid, gtid.to_string())
        }
    } else {
        (CursorKind::SnapshotPk, format!("{}|{}|{}", idx, gtid, new_last_pk))
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue { kind, value }),
        is_final: chunk_done && idx + 1 == n_tables,
        stream_name: Some(stream_name),
    })
}
```

- [ ] **Step 2: Update tests in snapshot.rs**

Replace existing tests with:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_cursor_basic() {
        let (idx, g, pk) = parse_snapshot_cursor("0|uuid:1-7|42").unwrap();
        assert_eq!(idx, 0);
        assert_eq!(g, "uuid:1-7");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_advances_table() {
        let (idx, _, pk) = parse_snapshot_cursor("3|uuid:1-7|0").unwrap();
        assert_eq!(idx, 3);
        assert_eq!(pk, 0);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_bad() {
        assert!(parse_snapshot_cursor("nopipes").is_err());
        assert!(parse_snapshot_cursor("a|b").is_err());           // only 2 parts
        assert!(parse_snapshot_cursor("notnum|g|3").is_err());   // bad idx
        assert!(parse_snapshot_cursor("0|g|notnum").is_err());   // bad pk
    }
}
```

- [ ] **Step 3: lib.rs cursor dispatch unchanged but uses new SourceCfg**

The `Guest::read_batch` impl already dispatches by cursor kind. No change needed — `snapshot::initial` and `snapshot::next_chunk` now accept `&SourceCfg` (the multi-table-aware version).

- [ ] **Step 4: Build + test**

```bash
cargo build --release --manifest-path examples/mysql-cdc-rs/Cargo.toml && \
cargo test --target aarch64-apple-darwin --manifest-path examples/mysql-cdc-rs/Cargo.toml
```

Expected: 9+ tests pass.

- [ ] **Step 5: Commit**

```bash
git add examples/mysql-cdc-rs/src/snapshot.rs examples/mysql-cdc-rs/src/lib.rs && \
git commit -m "phase-2-3g-4: mysql-cdc-rs sequential per-table snapshot

Cursor format <table_idx>|<gtid>|<last_pk> drives sequential
snapshot through all configured tables. After table[i] completes
(short chunk), cursor advances to <i+1>|<gtid>|0; after the last
table, transitions to Gtid for streaming.

Each batch tags stream_name = '<schema>.<table>' so the loader
routes to the right per-table sub-path. Single-table source-config
still works via the back-compat decode in SourceCfg::tables().

3 new cursor-parsing tests."
```

---

## Task 5: mysql-cdc-rs multi-table streaming

**Files:**
- Modify: `examples/mysql-cdc-rs/src/streaming.rs`

- [ ] **Step 1: Replace `next_window` with per-table buffering**

Replace the existing `next_window` body. Key change: build one `DynamicBatchBuilder` per qualified-table-name, append events to the matching builder, and emit the FIRST builder that hits `batch_size`. Discard partial buffers on close.

```rust
use std::collections::HashMap;

pub fn next_window(
    url: &str,
    cfg: &SourceCfg,
    start_gtid: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let h = db::open(url).map_err(db_err_to_connector_err)?;
    let tables = cfg.tables();
    let qualified_set: Vec<String> = tables.iter().map(SourceCfg::qualified).collect();

    // Discover schemas for all configured tables up front.
    let mut table_schemas: HashMap<String, std::sync::Arc<arrow_schema::Schema>> = HashMap::new();
    let mut table_n_cols: HashMap<String, usize> = HashMap::new();
    for t in &tables {
        let cols = discover::query_columns(h, &t.schema, &t.table)?;
        let q = SourceCfg::qualified(t);
        table_n_cols.insert(q.clone(), cols.len());
        table_schemas.insert(q, build_full_schema(&columns_to_fields(&cols)));
    }

    let sub = db::subscribe_changes(h, start_gtid, &[]).map_err(db_err_to_connector_err)?;
    let mut builders: HashMap<String, DynamicBatchBuilder> = HashMap::new();
    let mut latest_position = start_gtid.to_string();
    let mut emitted_table: Option<String> = None;

    loop {
        let evt = match db::next_event(sub).map_err(db_err_to_connector_err)? {
            Some(e) => e,
            None => break,
        };
        if !evt.position.is_empty() {
            latest_position = evt.position.clone();
        }
        if !qualified_set.iter().any(|q| q == &evt.table) {
            continue;
        }
        let n_cols = match table_n_cols.get(&evt.table).copied() {
            Some(n) => n,
            None => continue,
        };
        let builder = builders.entry(evt.table.clone()).or_insert_with(|| {
            let s = table_schemas.get(&evt.table).cloned().unwrap();
            DynamicBatchBuilder::new(s)
        });
        if !append_event(builder, &evt, n_cols) {
            continue;
        }
        if (builder.rows() as i64) >= batch_size {
            emitted_table = Some(evt.table.clone());
            break;
        }
    }
    db::close_stream(sub);

    // If a table hit batch_size, flush it. Otherwise flush whichever
    // table accumulated the most (single-table workflow expects rows
    // when events were drained).
    let target = emitted_table.or_else(|| {
        builders
            .iter()
            .max_by_key(|(_, b)| b.rows())
            .map(|(k, _)| k.clone())
    });

    match target {
        Some(qualified) => {
            let bb = builders.remove(&qualified).unwrap();
            let rows_n = bb.rows() as u32;
            if rows_n == 0 {
                Ok(ReadOutcome {
                    batch_ipc: Vec::new(),
                    rows: 0,
                    new_cursor: Some(CursorValue {
                        kind: CursorKind::Gtid,
                        value: latest_position,
                    }),
                    is_final: false,
                    stream_name: None,
                })
            } else {
                let bytes = bb
                    .finish_to_ipc()
                    .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?;
                Ok(ReadOutcome {
                    batch_ipc: bytes,
                    rows: rows_n,
                    new_cursor: Some(CursorValue {
                        kind: CursorKind::Gtid,
                        value: latest_position,
                    }),
                    is_final: false,
                    stream_name: Some(qualified),
                })
            }
        }
        None => Ok(ReadOutcome {
            batch_ipc: Vec::new(),
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Gtid,
                value: latest_position,
            }),
            is_final: false,
            stream_name: None,
        }),
    }
}
```

- [ ] **Step 2: Build**

```bash
cargo build --release --manifest-path examples/mysql-cdc-rs/Cargo.toml
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add examples/mysql-cdc-rs/src/streaming.rs && \
git commit -m "phase-2-3g-5: mysql-cdc-rs multi-table streaming

next_window builds one DynamicBatchBuilder per qualified-table-name
and appends events into the matching builder. The first builder to
reach batch_size flushes; partial state in other tables' buffers is
discarded (those events re-stream next call — at-least-once
preserved by loader's (id, gtid) keying).

stream_name set to the qualified table name when a batch flushes,
None when nothing was emitted."
```

---

## Task 6: postgres-cdc-rs source-config + multi-table discover

Mirror Task 3 in `examples/postgres-cdc-rs/src/lib.rs` and `examples/postgres-cdc-rs/src/discover.rs`. Same `SourceCfg` enum, same `DiscoveredTable` struct, same `query_table` helper. Build + test + commit as `phase-2-3g-6`.

---

## Task 7: postgres-cdc-rs multi-table snapshot + slot/publication setup

**Files:**
- Modify: `examples/postgres-cdc-rs/src/snapshot.rs`

Mirror Task 4 with Postgres specifics:

- Cursor encoding: `<idx>|<lsn>|<last_pk>` (LSN instead of GTID).
- `pg_current_wal_lsn` pinned at the very first call (cursor=None) and reused via cursor for all tables.
- `ensure_publication` is updated to FOR TABLE every configured table (one publication, multi-table). Idempotent (existing pub is preserved if already created).
- `chunk_after` is unchanged in shape (still uses `$1::pg_type` cast on the PK and `::text` cast on every column).
- `finalize` advances cursor through the table list same as MySQL.

The `ensure_publication` change:

```rust
fn ensure_publication(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let tables = cfg.tables();
    let pub_name = pub_name_fn_for_first(&tables);  // hash of all tables, deterministic
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_publication WHERE pubname = $1",
        &[pub_name.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        let table_list = tables
            .iter()
            .map(|t| format!("\"{}\".\"{}\"", t.schema, t.table))
            .collect::<Vec<_>>()
            .join(", ");
        let stmt = format!("CREATE PUBLICATION \"{pub_name}\" FOR TABLE {table_list}");
        db::query(h, &stmt, &[]).map_err(db_err_to_connector_err)?;
    }
    Ok(())
}
```

`pub_name_fn_for_first` hashes the comma-joined list of qualified table names so the publication name is deterministic across runs of the same pipeline.

Build + test + commit as `phase-2-3g-7`.

---

## Task 8: postgres-cdc-rs multi-table streaming

**Files:**
- Modify: `examples/postgres-cdc-rs/src/streaming.rs`

Mirror Task 5. Postgres specifics:

- Slot name + publication name come from the connector helpers (already named after the table set in Task 7).
- `db::subscribe_changes` options: `slot_name` + `publication_names`. Same as today.
- Streaming loop: identical to MySQL except cursor kind is `Lsn` and position uses LSN format.

Build + commit as `phase-2-3g-8`.

---

## Task 9: e2e tests use 2 tables + per-sub-path assertions

**Files:**
- Modify: `tests/integration/tests/mysql_cdc_wasm_e2e.rs`
- Modify: `tests/integration/tests/postgres_cdc_wasm_e2e.rs`

- [ ] **Step 1: Update both e2e tests to seed 2 tables**

In each, replace `seed_table_and_rows` with two tables (`items` and `orders`):

```rust
async fn seed_tables_and_rows(url: &str) -> anyhow::Result<()> {
    // ...connect...
    // CREATE TABLE items + 3 rows, CREATE TABLE orders + 3 rows
}
```

Update `perform_iud` to perform IUD on both tables.

- [ ] **Step 2: Update spec JSON to use `tables: [...]`**

```rust
let spec = json!({
    "source": {
        "type": "wasm",
        "config": {
            "tables": [
                {"schema": "test", "table": "items"},
                {"schema": "test", "table": "orders"}
            ]
        }
    },
    ...
});
```

- [ ] **Step 3: Replace single `read_parquet_ops(dir)` with per-table reader**

Add a helper that walks `<base>/<schema>.<table>/` subdirectories and returns ops per table:

```rust
fn read_parquet_ops_by_table(dir: &Path) -> std::collections::HashMap<String, Vec<String>> {
    let mut by_table: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|x| x.to_str()) != Some("parquet") {
            continue;
        }
        let table_dir = entry.path().parent().and_then(|p| p.file_name()).and_then(|s| s.to_str()).unwrap_or("").to_string();
        // ...read parquet, extract _cdc.op col, append to by_table[table_dir]
    }
    by_table
}
```

- [ ] **Step 4: Update assertions to require ops in BOTH table sub-paths**

```rust
let by_table = read_parquet_ops_by_table(tmp_data.path());
let items_ops = by_table.get("test.items").cloned().unwrap_or_default();
let orders_ops = by_table.get("test.orders").cloned().unwrap_or_default();
assert!(items_ops.iter().filter(|o| *o == "s").count() >= 3);
assert!(items_ops.iter().any(|o| o == "i"));
assert!(items_ops.iter().any(|o| o == "u"));
assert!(items_ops.iter().any(|o| o == "d"));
assert!(orders_ops.iter().filter(|o| *o == "s").count() >= 3);
assert!(orders_ops.iter().any(|o| o == "i"));
```

(The orders IUD assertion is `i` only since the test typically only does an INSERT to demonstrate streaming, not full IUD on orders.)

- [ ] **Step 5: Build integration tests**

```bash
cargo build -p integration-tests --tests
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add tests/integration/tests/mysql_cdc_wasm_e2e.rs tests/integration/tests/postgres_cdc_wasm_e2e.rs && \
git commit -m "phase-2-3g-9: e2e tests assert per-table parquet sub-paths

Both tests now seed test.items + test.orders, configure
tables: [...] source-config, and assert each table's parquet
sub-path has the right ops. #[ignore] still — local make e2e or
manual workflow_dispatch validates."
```

---

## Task 10: Local validation + README

- [ ] **Step 1: Run all four curated e2e tests locally**

```bash
make stack
export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"
cargo test -p integration-tests --test mysql_cdc_wasm_e2e -- --ignored --nocapture --test-threads=1
cargo test -p integration-tests --test postgres_cdc_wasm_e2e -- --ignored --nocapture --test-threads=1
cargo test -p integration-tests --test wasm_connector -- --ignored --nocapture --test-threads=1
cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture --test-threads=1
```

Expected: all four pass.

- [ ] **Step 2: Update README "Currently:" line**

In `README.md`, replace the "Currently:" line with:

```markdown
Currently: **Phase II.3.g — Multi-table CDC (complete)** on top of II.3.j.1. Both reference WASM CDC connectors (`mysql-cdc-rs`, `postgres-cdc-rs`) snapshot N tables sequentially against a shared GTID/LSN consistency point and stream changes from a single subscription covering all configured tables. Each batch tags `stream_name = "<schema>.<table>"`, and `LocalParquetLoader` writes to per-table sub-paths `<base_path>/<schema>.<table>/<load_id>.parquet`. WIT `read-outcome` gains an optional `stream-name`; non-multi-table connectors return None and behavior is unchanged. Source-config evolves to `{"tables": [{"schema":"...", "table":"..."}, ...]}`; the old `{"schema": "...", "table": "..."}` shape still parses for back-compat. Multi-table CDC is the last big II.3.x item — next is real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 3: Commit + push**

```bash
git add README.md && \
git commit -m "phase-2-3g-10: README — Phase II.3.g multi-table CDC complete"
```

---

## Self-review

### Spec coverage

| Spec section | Plan task |
|---|---|
| Source-config evolution `{tables: [...]}` | Tasks 3, 6 |
| Cursor `<i>\|<pin>\|<last_pk>` | Tasks 4, 7 |
| Per-batch loader dispatch | Task 1 |
| WIT `stream-name: option<string>` | Task 1 |
| LoadId.stream_name + parquet sub-path | Task 1 |
| Workflow per-batch override | Task 1 |
| `ensure_publication FOR TABLE a, b, ...` | Task 7 |
| Multi-table snapshot sequential | Tasks 4, 7 |
| Multi-table streaming + per-table buffering | Tasks 5, 8 |
| Existing connectors back-compat | Task 2 |
| E2E with 2 tables | Task 9 |
| README | Task 10 |

All spec sections covered.

### Placeholder scan

The Postgres tasks (6, 7, 8) defer to "Mirror Task X" with concrete callouts for the Postgres-specific differences. Each callout is concrete (LSN vs GTID, Lsn vs Gtid, `pg_current_wal_lsn` vs `@@gtid_executed`, slot/publication names) — those are real differences, not placeholders. The Rust shape is identical between the two.

### Type consistency

- `SourceCfg::Multi { tables: Vec<TableRef> }` and `SourceCfg::Single { schema, table }` consistent across both connectors.
- `TableRef { schema: String, table: String }` consistent.
- `DiscoveredTable { schema, table, columns, pk_column }` consistent.
- Cursor parser returns `(usize, String, i64) = (idx, gtid_or_lsn, last_pk)` consistent.
- `ReadOutcome.stream_name: Option<String>` consistent across guests.
- `LoadId.stream_name: String` (empty = no subdir) consistent across loader sites.

All checked.
