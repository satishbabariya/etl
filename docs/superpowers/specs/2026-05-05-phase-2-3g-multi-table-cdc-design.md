# Phase II.3.g — Multi-Table CDC — Design Spec

> **Status:** Draft 2026-05-05. Approved by agent (user delegated all 6 architecture decisions). Predecessors: II.3.e (MySQL SDK lift), II.3.f (Postgres SDK port), II.3.h (schema discovery), II.3.i (CI), II.3.j + II.3.j.1 (e2e fixes).

## Goal

Lift both reference WASM CDC connectors (`mysql-cdc-rs`, `postgres-cdc-rs`) from one-table-per-pipeline to N-tables-per-pipeline, end-to-end. Snapshot all configured tables sequentially against a single shared consistency point, then stream changes from all of them through one subscription. Each batch lands at a per-table sub-path under the pipeline's destination, so downstream consumers can read each table independently.

## Non-goals

- **Cross-table transactional consistency on streaming.** Each event is committed independently per the source's logical replication semantics; we don't reorder or buffer for cross-table atomicity.
- **Heterogeneous schemas in the same parquet file.** That ugliness is what the per-table sub-path dispatch avoids.
- **Per-table workflow isolation.** One workflow per pipeline drives all its tables; failure in one table fails the whole workflow (acceptable for v1; finer-grained recovery is a future patch).
- **Glob/pattern table selection.** Explicit array only. Globs can land later as sugar.
- **Backfilling existing pipelines.** A pipeline created with one table can't be migrated to multiple — recreate the pipeline.

---

## Architecture overview

### Source-config evolution

Old (single table):
```json
{ "schema": "test", "table": "items" }
```

New (array):
```json
{
  "tables": [
    { "schema": "test", "table": "items" },
    { "schema": "test", "table": "orders" }
  ]
}
```

Backward compat: the connector accepts the old shape and treats it as a single-element array. Existing pipelines keep working.

### Cursor lifecycle

| State | cursor-kind | value | Meaning |
|---|---|---|---|
| Initial | `None` | — | Pin shared GTID/LSN; ensure publication+slot; start snapshotting tables[0]. |
| Snapshotting tables[i] | `snapshot-pk` | `<i>\|<pin>\|<last_pk>` | Continue snapshotting tables[i] from `last_pk`. |
| Snapshot of tables[i] done; advance to tables[i+1] | `snapshot-pk` | `<i+1>\|<pin>\|0` | Start the next table. |
| All snapshots done | `gtid` (MySQL) / `lsn` (Postgres) | `<pin>` | Streaming. |
| Streaming | `gtid` / `lsn` | `<latest_position>` | Continue streaming all tables. |

The leading `<i>\|` is new in `snapshot-pk`. The connector parses the cursor and dispatches accordingly. Streaming cursor is unchanged.

### Per-batch loader dispatch

`read-outcome` gains a new optional WIT field: `stream-name: option<string>`. When set, the workflow uses it to override the pipeline's default `stream_name` when calling `load_batch`. The loader writes `<base_path>/<stream_name>/<load_id>.parquet` instead of `<base_path>/<load_id>.parquet`.

For non-CDC `wasm:` connectors and existing single-table CDC, the connector returns `stream_name = None` and behavior is unchanged.

### One subscription, all tables

Streaming opens a single `db.subscribe-changes` covering the publication that includes all configured tables. Postgres: `CREATE PUBLICATION ... FOR TABLE a, b`. MySQL: the binlog is single-stream-per-server; the connector filters by `evt.table ∈ qualified_tables` host-side. Each event the connector emits goes into a batch tagged with that event's `evt.table`. When the connector accumulates a batch's worth of rows for any single table, it emits that batch with `stream_name = "<schema>.<table>"`.

Implication: a streaming `read_batch` call returns rows from ONE table at a time, even though the underlying subscription serves all of them. Different tables alternate batches as their event volumes dictate. Since each batch's `stream_name` routes the loader output, this is fine.

---

## WIT change

`crates/connector-sdk/wit/source-connector.wit`:

```wit
record read-outcome {
    batch-ipc: list<u8>,
    rows: u32,
    new-cursor: option<cursor-value>,
    is-final: bool,
    /// Per-batch destination stream override. When set, the workflow
    /// passes this to load_batch as stream_name; the loader writes to
    /// <destination>/<stream-name>/. None means "use the pipeline's
    /// configured stream_name" (backward compat for non-multi-table
    /// connectors).
    stream-name: option<string>,
}
```

Source-level breaking change to all guests that construct `ReadOutcome`. Three example connectors (`mysql-cdc-rs`, `postgres-cdc-rs`, `csv-source`) update with one extra field each.

Bindgen also regenerates the host-side type. The host's `wasm_runtime/connector.rs::WasmSourceConnector::read_batch` translates the new field to the existing Rust `ReadOutcome` (which is internal — not part of any cross-crate contract). The internal `ReadOutcome` gains a matching `stream_name: Option<String>` field.

---

## Workflow change

`WasmCdcPipelineWorkflow::run_inner` passes the per-batch `stream_name` from `read_out.stream_name` into `LoadBatchInput.stream_name` and `CommitCursorInput.stream_name`, falling back to the pipeline-level `stream_name` when `None`:

```rust
let batch_stream = read_out.stream_name.clone().unwrap_or_else(|| input.stream_name.clone());
ctx.start_activity(
    SyncActivities::load_batch,
    LoadBatchInput { stream_name: batch_stream.clone(), ... },
    ...
);
ctx.start_activity(
    SyncActivities::commit_cursor,
    CommitCursorInput { stream_name: batch_stream, ... },
    ...
);
```

`PipelineRunWorkflow` (the bounded sync workflow) gets the same treatment since the `wasm:` (non-CDC) prefix routes through it. Behavior unchanged when connectors don't set the field.

The `cursor` state on the workflow stays a single `Option<CursorValue>`. The connector encodes per-table progress into the cursor value itself (the leading `<i>` index in `snapshot-pk`).

---

## Catalog implication

`stream_state` is keyed on `(pipeline_id, stream_name)` already. With per-batch `stream_name = "<schema>.<table>"`, each table gets its own row in `stream_state` automatically. **No migration needed.**

The `commit_cursor` activity already takes `stream_name` and writes per-(pipeline, stream) — the workflow change above just passes the right value.

Caveat: when a pipeline switches from one table set to another (which we said is non-goal), stale `stream_state` rows for the old tables would persist. Acceptable for v1; cleanup is the operator's problem.

---

## Loader dispatch

`crates/worker/src/loaders/parquet_local.rs::LocalParquetLoader::load`:

```rust
async fn load(&self, dest: &DestinationSpec, load_id: LoadId, batch: RecordBatch) -> Result<LoadResult> {
    let mut p = PathBuf::from(&spec.base_path);
    if !load_id.stream_name.is_empty() {
        p.push(&load_id.stream_name);  // NEW: per-stream subdir
    }
    p.push(format!("{}.parquet", load_id_to_filename(&load_id)));
    fs::create_dir_all(p.parent().unwrap())?;
    // ... existing write logic
}
```

`LoadId` gains a `stream_name: String` field (added in II.3.g). The activity wires `LoadBatchInput.stream_name` → `LoadId.stream_name`.

For backward compat: existing pipelines with a single non-multi-table connector continue to write to `<base_path>/<old_stream_name>/<load_id>.parquet`. That's a directory-shape change. Existing dev/test data on disk would need to be moved or wiped — flagged as a known break for any local dev with persisted parquet output. Acceptable since the dev workflow is already "wipe and re-run".

---

## Connector lifts

### Common

Both connectors:
- `parse_source_cfg` accepts `{ tables: [...] }` OR `{ schema, table }` (single-table compat).
- `discover_all_tables_to_outer_schema` returns a `Vec<DiscoveredTable>` where each entry has the columns + PK + qualified name. Each table's schema is independent.
- `read_batch` cursor dispatch:
  - `None` → snapshot::initial(tables[0]).
  - `SnapshotPk(<i>|<pin>|<last_pk>)` → continue tables[i].
  - `SnapshotPk(<i+1>|<pin>|0)` (when previous chunk was short) → start tables[i+1].
  - When `i == tables.len() - 1` and chunk is short → transition cursor to streaming kind.
  - `Gtid` / `Lsn` → streaming.

### MySQL (`mysql-cdc-rs`)

- Snapshot: `read_gtid_executed` once at very first call (cursor=None); cursor carries it forward via `<pin>`. Each chunk runs `SELECT ... FROM <schema>.<table> WHERE <pk> > ? LIMIT N`.
- Streaming: `db.subscribe_changes` once with the shared GTID set. The host's `MysqlSubscription::next` already filters and emits events with `evt.table` populated; the connector's `append_event` checks if `evt.table ∈ qualified_tables` and accumulates per-table batches.
- Per-batch emission: when the first table accumulates `batch_size` rows OR the idle timeout fires, emit that batch with `stream_name = "<schema>.<table>"`. Other tables' partially-collected rows stay buffered for the next call.

### Postgres (`postgres-cdc-rs`)

- Slot+publication setup: `CREATE PUBLICATION <pub> FOR TABLE <s1>.<t1>, <s2>.<t2>, ...` covers all configured tables. Slot creation unchanged.
- Snapshot: `read_current_lsn` once; cursor carries it forward; each chunk casts cols to text + `WHERE pk > $1::<pk_sql_type>`.
- Streaming: same `db.subscribe_changes` as today; the host's `PgSubscription::next` already pairs Begin/Commit (II.3.j.1) and emits events with `evt.table` populated. Connector's `append_event` filters and routes to per-table batches.

### Per-table batch buffering inside the connector

Both connectors get a small `MultiTableStreamingBuffer` (per-call, not persisted) that maps `qualified_table_name → DynamicBatchBuilder`. As events stream in, they're appended to the matching builder. When any builder hits `batch_size`, that table's batch is flushed and returned to the workflow. Other builders' partial state is discarded (it'll be reconstructed from the next subscription).

Honest trade-off: discarding partial buffers means each table sees at-least-once duplication on the boundary (the same events that filled buffer N+1 partially get re-streamed next call). The loader's `(id, lsn)`/`(id, gtid)` keying makes this idempotent; it's the same property the spec already accepts for the snapshot/streaming overlap window. Acceptable.

---

## E2E test changes

`tests/integration/tests/{mysql,postgres}_cdc_wasm_e2e.rs`:

- Seed two tables: `items` (existing 4-column shape) plus a new `orders { id BIGINT PK, item_id BIGINT, qty INT }`.
- Pre-seed 3 rows in each table.
- Pipeline source-config:
  ```json
  { "tables": [{"schema": "test", "table": "items"}, {"schema": "test", "table": "orders"}] }
  ```
- After 5s settle, perform IUD on both tables.
- Assert: each table's parquet sub-directory exists; each has ≥3 snapshot 's' rows; collectively at least one `i`/`u`/`d` per table.

---

## File structure

| Path | Action |
|---|---|
| `crates/connector-sdk/wit/source-connector.wit` | Modify — add `stream-name: option<string>` to `read-outcome` |
| `crates/loader-sdk/src/lib.rs` | Modify — add `stream_name: String` to `LoadId` |
| `crates/worker/src/loaders/parquet_local.rs` | Modify — push `stream_name` into the path |
| `crates/worker/src/wasm_runtime/connector.rs` | Modify — translate WIT `stream-name` to internal `ReadOutcome` |
| `crates/worker/src/wasm_runtime/connector.rs` (struct `ReadOutcome`) | Modify — add `stream_name: Option<String>` |
| `crates/worker/src/activities/sync/inputs.rs` | Modify — add `stream_name` to `ReadBatchOutput` |
| `crates/worker/src/activities/sync/mod.rs` | Modify — propagate `stream_name` into `LoadId` |
| `crates/worker/src/workflows/wasm_cdc_pipeline.rs` | Modify — per-batch `stream_name` override |
| `crates/worker/src/workflows/pipeline_run.rs` | Modify — same |
| `examples/mysql-cdc-rs/src/lib.rs` | Modify — accept `tables: [...]` config + multi-table cursor dispatch |
| `examples/mysql-cdc-rs/src/discover.rs` | Modify — return `Vec<DiscoveredTable>` |
| `examples/mysql-cdc-rs/src/snapshot.rs` | Modify — sequential per-table snapshot with `<i>\|<pin>\|<last_pk>` cursor |
| `examples/mysql-cdc-rs/src/streaming.rs` | Modify — per-table buffering, emit one table's batch at a time |
| `examples/postgres-cdc-rs/src/{lib,discover,snapshot,streaming}.rs` | Modify — same shape as mysql |
| `examples/csv-source/src/lib.rs` | Modify — set `stream_name = None` (no behavior change) |
| `tests/integration/tests/mysql_cdc_wasm_e2e.rs` | Modify — 2 tables, assert per-table parquet sub-paths |
| `tests/integration/tests/postgres_cdc_wasm_e2e.rs` | Modify — same |
| `README.md` | Modify — bump Currently |

~17 files. Single-pipeline-per-multi-table is the surface; the changes are mostly mechanical once the cursor encoding is locked.

---

## Open concerns

1. **Backward compat surface.** The WIT change to `read-outcome` is a guest-side breaking change for all connectors. Three update sites (`mysql-cdc-rs`, `postgres-cdc-rs`, `csv-source`); also the third-party-ish `stripe-source` and `hello-world-source` examples. Trivial mechanical update (one extra `stream_name: None` in the constructor).

2. **`stream_name` validation.** The connector returns arbitrary strings. We don't validate that `<schema>.<table>` is well-formed — a malicious or buggy connector could pass `../../etc/passwd` as a `stream_name` and the loader would write there. The loader normalizes via `Path::push` which DOES respect path semantics; defensive code in the loader rejects components containing `..` or path separators. Adding that validation is part of this phase.

3. **Per-table snapshot consistency on Postgres.** We pin `pg_current_wal_lsn()` once and reuse it across all tables' snapshot SELECTs. But each table's SELECT runs in its own implicit transaction. If a write happens to table B mid-snapshot-of-A, table B's snapshot sees the post-write state, while the streaming events from before the write are also re-streamed (slot's restart_lsn semantic). At-least-once is preserved; exact-once isn't promised. Same as today.

4. **Per-batch table starvation.** If table A has 1000 events/sec and table B has 1 event/min, the streaming buffer fills A's batch fast and B's batch sits half-full forever. Mitigation v1: idle-timeout flushes partial batches. Properly fair scheduling is a future patch.

5. **Loader path conflicts on directory-rename.** If a pipeline switches from `stream_name = "foo"` to `stream_name = "foo.bar"`, the old path stays and the new path appears. Acceptable for v1; spec'd as non-goal.

---

## Acceptance

- `cargo test --workspace --lib` passes 138+ tests.
- Both example connectors compile to `wasm32-wasip2`.
- `make e2e` curated 4 all green:
  - `mysql_cdc_wasm_e2e`: each of `test.items` + `test.orders` shows ≥3 snapshot rows in its own parquet subdir, plus ≥1 `i`/`u`/`d` each.
  - `postgres_cdc_wasm_e2e`: same.
  - `wasm_connector` (CSV): unchanged single-stream behavior.
  - `mysql_cdc_e2e` (native): unchanged.
- README "Currently:" line updated.
