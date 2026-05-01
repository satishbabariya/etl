# Phase II.3.d.5 — MySQL CDC Initial Snapshot Design

**Status:** Approved 2026-05-01
**Related RFCs:** RFC-0008 (CDC Architecture), §"Initial Snapshot and Streaming Handoff", §"Per-source variation: MySQL"
**Predecessor phases:** II.3.d (PR #28, MySQL CDC streaming-only). The snapshot path is the explicit "deferred" follow-up flagged in II.3.d's spec.

## Goal

Add an initial snapshot phase to the MySQL CDC connector. Today the connector runs in skip-snapshot mode only — capturing the current GTID and streaming forward. This phase makes pre-existing rows land in the destination as `_cdc.op = "s"` rows before streaming begins.

## Design decisions (resolved during brainstorm)

### 1. Cross-chunk consistency

**Per-chunk `START TRANSACTION WITH CONSISTENT SNAPSHOT`.** Each chunk reads at its own point-in-time. With PK-monotonic chunking (`WHERE pk > ? ORDER BY pk LIMIT ?`), each row appears at most once in the snapshot at *some* point in time; concurrent updates flow through streaming and are reconciled at the destination via PK merge.

Rejected alternatives:
- Single long transaction across all chunks: memory/lock duration risk on large tables.
- `FLUSH TABLES WITH READ LOCK` + capture position + release: brief writer block (typically <1s but variable in production), and worth waiting for production data before adopting.
- Replica-style read at a pinned binlog position: substantially more complex.

**v2 upgrade path:** when customers report inconsistency in practice, we can add `FLUSH TABLES WITH READ LOCK` as an opt-in `consistency_mode` field. v1 ships per-chunk only.

### 2. GTID capture timing

**GTID captured before snapshot starts** (matches RFC-0008's "capture-and-stream" invariant). Streaming begins from the captured GTID after snapshot completes; updates during the snapshot window appear in both the snapshot (with their value at chunk-time) and the stream (with their post-update value). Destination loader's PK merge resolves the overlap.

The capture-then-snapshot order is non-negotiable: capturing GTID *after* snapshot risks data loss because writes during the snapshot window would be invisible to both phases.

### 3. Workflow shape

**Single workflow, no child.** Mirrors the existing Postgres CDC pattern (`CdcPipelineWorkflow` does snapshot loop + streaming loop in one workflow). Don't introduce a new topology for MySQL when Postgres hasn't migrated to RFC-0008's child-workflow shape yet — both connectors take the refactor in lockstep when scaling pressures justify it.

### 4. Configuration default

**Default `initial_sync = SnapshotThenStreaming`.** Backward-compat counter: II.3.d shipped a few hours before this phase, no production user base. `streaming_only` is the niche "I only care about changes from now" use case and deserves an explicit opt-in.

The existing `mysql_cdc_e2e.rs` test (which assumes streaming-only) is updated to pass `"initial_sync": "streaming_only"` explicitly in its pipeline spec.

### 5. PK type for snapshot ordering

**Integer PKs only in v1** (i64/i32). Matches the existing Postgres CDC constraint (`SnapshotChunkInput::last_pk: Option<i64>`). Composite/UUID/string PKs are an explicit v2 follow-up.

## Architecture

Add a snapshot phase to `MysqlCdcPipelineWorkflow` between `discover_schema` and the streaming loop. Two new activities (`mysql_capture_snapshot_position` is folded into existing `capture_start_gtid`; `mysql_snapshot_chunk` is new) plus a new module `crates/worker/src/connectors/mysql/cdc/snapshot.rs`. Snapshot uses **per-chunk `START TRANSACTION WITH CONSISTENT SNAPSHOT`** with PK-monotonic chunking; GTID is captured **before** snapshot starts.

### File map

```
crates/worker/src/connectors/mysql/cdc/
├── decode.rs       — adds parse_mysql_text(s, target_type)
├── position.rs     (existing)
├── schema.rs       (existing)
├── snapshot.rs     (NEW — read_chunk via mysql_async)
└── stream.rs       (existing)

crates/worker/src/activities/mysql_cdc/
├── inputs.rs       — adds MysqlSnapshotChunkInput/Output
└── mod.rs          — adds mysql_snapshot_chunk activity

crates/worker/src/workflows/mysql_cdc_pipeline.rs
   — adds conditional snapshot loop between discover_schema and streaming loop

crates/common-types/src/pipeline_spec.rs
   — adds MysqlInitialSync enum + initial_sync + pk_column fields to MysqlCdcSourceSpec
```

### Pipeline spec extension

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MysqlInitialSync {
    #[default]
    SnapshotThenStreaming,
    StreamingOnly,
}

pub struct MysqlCdcSourceSpec {
    pub schema: String,
    pub table: String,
    pub server_id: u32,
    #[serde(default)]
    pub heartbeat_secs: u32,
    #[serde(default)]
    pub initial_sync: MysqlInitialSync,
    /// PK column for snapshot chunking. Required when initial_sync ==
    /// SnapshotThenStreaming. v1 supports i64 PKs only.
    #[serde(default)]
    pub pk_column: Option<String>,
}
```

### Workflow flow

```
start_run
→ verify_mysql_config              (existing precondition)
→ capture_start_gtid               (records gtid_executed BEFORE snapshot)
→ discover_mysql_schema            (existing one-shot)
→ if initial_sync == SnapshotThenStreaming:
      validate pk_column is set + integer-typed
      snapshot loop:
          mysql_snapshot_chunk { last_pk } → { rows, last_pk, is_final }
          batch_seq += 1
      until is_final
→ streaming loop                   (unchanged from II.3.d)
→ complete_run
```

### `mysql_snapshot_chunk` activity

Input: `pipeline_id, run_id, tenant_id, principal_id, jti, batch_seq, source_conn, schema, table, pk_column, last_pk: Option<i64>, batch_size: u32, schema_json, captured_gtid: String, destination`.
Output: `rows: u32, last_pk: Option<i64>, is_final: bool`.

The activity:

1. Resolves the URL via `secrets::resolve_connection_audited` (existing pattern).
2. Calls `snapshot::read_chunk` (new) to:
   - Open `mysql_async::Pool`, get a `Conn`.
   - `START TRANSACTION WITH CONSISTENT SNAPSHOT` (single statement; sets isolation + takes the consistent point).
   - `SELECT CAST(col1 AS CHAR) AS col1, CAST(col2 AS CHAR) AS col2, ... FROM schema.table WHERE pk_col > ? ORDER BY pk_col LIMIT ?` — text-cast all columns so per-row parsing produces typed scalars via the same `ScalarValue` enum used by streaming.
   - Iterate rows, parse each value via `parse_mysql_text(s, target_type)`, append to per-column `ArrayBuilder`s (reuse existing `make_pg_builder`-style dispatch — see "Builder reuse" below).
   - Append `_cdc.op = "s"`, `_cdc.lsn = captured_gtid`, `_cdc.commit_ts = NULL` per row.
   - `COMMIT`.
3. Writes the batch via `CdcParquetLoader.write(...)` (existing).
4. Returns `(rows, last_pk_from_final_row, is_final = rows < batch_size)`.

### Builder reuse

The streaming-side `RowOp` and Arrow builder dispatch live in MySQL CDC's `stream.rs`. Snapshot uses a different intermediate (text strings via `CAST AS CHAR`, not typed `BinlogValue`). Two options:

- **(a) Duplicate the per-column builder logic in `snapshot.rs`** — tightest scope, ~50 lines of dispatch.
- **(b) Extract shared helpers** to a sibling `value.rs` module — cleaner long-term.

**Plan opts for (a) for v1.** When the duplication is real, lift to (b) — premature now since stream-side and snapshot-side dispatch are subtly different (stream consumes typed `BinlogValue`s, snapshot consumes `String`s).

### `parse_mysql_text`

New function in `decode.rs` mirroring `parse_pg_text`. Converts text from `CAST AS CHAR` queries to `ScalarValue`. Reuses the existing `ScalarValue` enum (variants: `Int32`, `Int64`, `Float32`, `Float64`, `Utf8`, `Boolean`, `TimestampMicros`, `Date32`).

Coverage:
- Int32/Int64: `s.parse::<i32>()` / `s.parse::<i64>()`.
- Float64: `s.parse::<f64>()`.
- Utf8: `String::from(s)`.
- Boolean: `"0"` → false, `"1"` → true (MySQL's BIT/TINYINT(1) cast).
- Date32: `NaiveDate::parse_from_str(s, "%Y-%m-%d")` → days since epoch.
- TimestampMicros: `NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")` → micros UTC.

The `ScalarValue` enum already has these variants from II.3.d.1 — no enum change needed.

### Cursor persistence

- `runs.cursor` continues to hold the GTID set; advances only during streaming, not during snapshot.
- Snapshot's `last_pk` lives in workflow state across iterations. Temporal replay reconstructs from activity outputs.
- **No catalog persistence for `last_pk` in v1** — failed workflows don't recover their `last_pk`; restart starts from scratch. Matches Postgres CDC's existing behavior; documented in "Out of scope".

## Error handling

### 1. Configuration / precondition (terminal)

Checked at the start of the snapshot loop, before any rows are read:
- `initial_sync == SnapshotThenStreaming` but `pk_column == None` → `InvalidConfig("snapshot mode requires pk_column")`.
- `pk_column` not present in discovered columns → `InvalidConfig("pk_column 'X' not found in table")`.
- `pk_column`'s OID maps to a non-integer Arrow type → `InvalidConfig("snapshot only supports integer pk columns in v1")`.

### 2. Transient (retryable)

Same Temporal retry policy as `read_window` (5 attempts, 1s→30s exponential). Per-chunk transactions restart at the same `last_pk` on retry — no partial state to reconcile.

### 3. Mid-snapshot schema change

If a column is dropped mid-snapshot, `SELECT CAST(col AS CHAR)` fails on subsequent chunks. We surface as non-retryable `SchemaIncompatible` and the workflow fails. Operator action: re-create pipeline. Documented limitation matching the broader "schema evolution mid-stream is hard" caveat from RFC-0008.

### 4. Snapshot/stream handoff overlap

Per RFC-0008's invariant: every change after `captured_gtid` appears in stream; every change before is reflected in snapshot. Updates during the snapshot window appear twice (once in snapshot, once in stream). Destination loader's PK merge handles this; no platform-side dedup needed.

### 5. Per-chunk inconsistency (acknowledged trade-off)

Different chunks see different points in time. PK-monotonic chunking ensures each row appears at most once in the snapshot. Final destination state converges via streaming. **No data loss; some staleness during the snapshot window.** Documented as a known limitation; `FLUSH TABLES WITH READ LOCK` is the v2 upgrade path.

## Out of scope (deferred)

- Snapshot resume after worker crash (requires `last_pk` persistence to catalog — same gap exists in Postgres CDC).
- Composite / non-integer PKs (UUID, string).
- Snapshot-during-streaming (RFC-0008 §"add-a-table flow").
- Very-large-snapshot history bounding via `continue-as-new`.
- `FLUSH TABLES WITH READ LOCK` for stricter cross-chunk consistency (opt-in v2 mode).
- Skip-snapshot for individual tables in a multi-table pipeline (multi-table itself is a separate phase).

## Testing strategy

### Layer 1 — `parse_mysql_text` unit tests (`connectors/mysql/cdc/decode.rs`)

~6 tests covering Int32/Int64/Float64/Utf8/Boolean/Date32/TimestampMicros. Same shape as `parse_pg_text` tests from II.3.d.2.

### Layer 2 — `read_chunk` SQL composition (no DB needed)

~2 tests: WHERE clause shape with/without `last_pk`, projection includes `CAST AS CHAR` for every column.

### Layer 3 — End-to-end (`tests/integration/tests/mysql_cdc_e2e.rs`)

Two changes:

1. **Modify the existing test** to pass `"initial_sync": "streaming_only"` explicitly in the spec JSON. Same assertions. Confirms backward compatibility.

2. **Add a new test `mysql_cdc_snapshot_then_streaming_e2e`** that:
   - Spins up `mysql:8.1` testcontainer with the same flags.
   - Seeds 3 rows in `customers` BEFORE pipeline start.
   - Starts pipeline with `initial_sync = SnapshotThenStreaming`, `pk_column = "id"`.
   - Polls for ≥3 's' ops in Parquet (snapshot complete).
   - Performs `INSERT id=4`, `UPDATE id=2`, `DELETE id=1` post-snapshot.
   - Polls for streaming ops `[i, u, d]` (alongside the existing 's' rows).
   - Asserts: 3 's' rows + 1 'i' + 1 'u' + 1 'd' total. Asserts streaming Parquet schema is `id: Int64, email: Utf8, name: Utf8, created: Timestamp(Micro, UTC)`.

Both e2e tests `#[ignore]`-gated, run via `DOCKER_HOST` env var (established in II.3.d).

### Test budget

8 unit tests + 2 e2e tests (1 modified + 1 new).

## Build sequence

1. **Pipeline spec extension** — `MysqlInitialSync` enum + new fields. Serde tests.
2. **`parse_mysql_text`** — text → `ScalarValue` parser in `decode.rs`. Unit tests per type.
3. **`snapshot.rs`** — `read_chunk` opens conn, runs `START TRANSACTION WITH CONSISTENT SNAPSHOT` + chunked SELECT with `CAST AS CHAR`, builds typed RecordBatch. SQL-composition unit test.
4. **`mysql_snapshot_chunk` activity + workflow snapshot loop** — new activity; workflow conditional loop. PK-column precondition validation.
5. **E2E** — modify existing test to opt into `streaming_only`; add new `snapshot_then_streaming` e2e.
6. **README + final verification.**

Each task is a separate commit. TDD: failing tests first where the unit is internal logic (1, 2, 3); for network-touching units (4) the e2e test in step 5 is the verification.

## Decision

Approved 2026-05-01. Implementation plan to follow at `docs/superpowers/plans/2026-05-01-phase-2-3d-5-mysql-snapshot.md`.
