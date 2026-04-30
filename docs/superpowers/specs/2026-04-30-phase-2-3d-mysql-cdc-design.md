# Phase II.3.d — MySQL CDC Source Connector (Streaming-Only)

**Status:** Approved 2026-04-30
**Related RFCs:** RFC-0006 (Connector Protocol), RFC-0007 (Incremental Sync), RFC-0008 (CDC Architecture)
**Predecessor phases:** II.3.b.1 (TS connector worker execution), II.3.c (Stripe Rust connector)

## Goal

Ship the platform's first non-HTTP source connector: native, in-worker MySQL CDC consuming binlog row events into Arrow batches, validated end-to-end through the existing destination loader.

This phase explicitly does **not** validate the connector SDK's generality to streaming sources — see "Path Choice" below for why.

## Path Choice (gating decision)

**Native, in-worker.** The existing Postgres CDC connector (`crates/worker/src/connectors/postgres/cdc/`) sets the precedent: CDC connectors live in the worker, not in the WASM sandbox. MySQL CDC follows the same shape.

Rationale:
- The current WASM host exposes only `http_fetch`. Binlog streaming needs raw TCP plus persistent connection state — extending the host to support that is its own design problem (Phase II.3.e candidate, deferred).
- Going native first gives us a second CDC data point (Postgres + MySQL) to inform what host capabilities the SDK actually needs, instead of designing in a vacuum.
- The "first non-HTTP connector" architectural validation comes from binlog → Arrow → Parquet flowing through unchanged destination loaders, which is achievable natively.

## Scope (what ships)

**In scope:**
- One new source variant: `SourceSpec::MysqlCdc(MysqlCdcSourceSpec)`.
- Streaming-only mode (RFC-0008 §"Skip-snapshot mode"): capture current GTID set at pipeline start; stream binlog from there forward; destination starts empty and accumulates.
- Single-table filter per pipeline.
- Insert / Update / Delete row events.
- Schema discovery from `information_schema.columns` against a fixed type subset.
- GTID-only cursor (require `gtid_mode=ON`).
- Cursor persistence and resume across worker restarts via existing `runs.cursor` text column.
- One end-to-end integration test using `testcontainers` (`#[ignore]`).

**Out of scope (deferred):**
- Initial snapshot.
- Multi-table filter per pipeline.
- DDL events / schema evolution mid-stream (RFC-0008 §"Schema Change Handling").
- Truncate events.
- File+position cursor fallback.
- Slot-lag alerting / backpressure tiers (RFC-0008 §"Backpressure").
- Parent/child workflow topology with `continue-as-new` (RFC-0008 §"CDC Workflow Topology").
- Lifting to the WASM connector SDK.

## Architecture

Native worker connector mirroring the existing Postgres CDC structure. Single workflow (no child); streaming loop only (no snapshot loop).

### File layout

```
crates/worker/src/connectors/mysql/cdc/
├── mod.rs        — pub re-exports
├── decode.rs     — binlog row events → Arrow rows (i/u/d only)
├── stream.rs     — read_window: drain binlog up to N events
├── schema.rs     — information_schema → Arrow schema; type map
└── position.rs   — GTID set parse / format / merge helpers

crates/worker/src/workflows/mysql_cdc_pipeline.rs
  — MysqlCdcPipelineWorkflow: streaming loop only

crates/worker/src/activities/mysql_cdc/
├── mod.rs        — activity registration
└── inputs.rs     — input/output structs
```

### Pipeline spec extension

In `crates/common-types/src/pipeline_spec.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlCdcSourceSpec {
    pub schema: String,        // MySQL "database" name
    pub table: String,
    pub server_id: u32,        // unique-per-consumer client id for binlog
    #[serde(default)]
    pub heartbeat_secs: u32,   // 0 = use server default
}

pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
    Wasm(WasmSourceSpec),
    MysqlCdc(MysqlCdcSourceSpec),  // new
}
```

Connection URL (`mysql://user:pass@host:3306/`) lives in the existing `ConnectionConfig.url` — no new shape.

### Workflow

`MysqlCdcPipelineWorkflow`:
```
start_run
→ verify_mysql_config        (one-shot precondition check)
→ capture_start_gtid         (records @@GLOBAL.gtid_executed at T₀)
→ discover_schema            (one-shot; persists Arrow schema to streams table)
→ streaming loop (max_windows budget for tests):
    read_window {gtid, max_events} → ReadWindowOutput {rows, new_gtid}
    if rows == 0: timer(2s)
→ complete_run
```

Retry policy: same `retry_policy()` shape as `CdcPipelineWorkflow` (5 attempts, 1s→30s exponential).

Workflow dispatch in `workflows/mod.rs` adds a third arm: routes `SourceSpec::MysqlCdc` to `MysqlCdcPipelineWorkflow`.

### `read_window` activity (the heart)

Opens a `mysql_async::BinlogStream` from the input GTID, reads up to `max_events` row-event groups (`WRITE_ROWS_EVENT_V2`, `UPDATE_ROWS_EVENT_V2`, `DELETE_ROWS_EVENT_V2`), filters by table id matching configured `schema.table` (via cached `TableMapEvent`), decodes rows, builds a `RecordBatch` with `_cdc.op` + `_cdc.lsn` (value = GTID set string) + `_cdc.commit_ts` columns per RFC-0008 §"Per-row metadata", writes Parquet via the existing destination loader, returns `(rows, new_gtid)`. Closes the stream — non-streaming protocol; same model as Postgres `pg_logical_slot_get_binary_changes`.

### Cursor storage

Existing `runs.cursor` text column. Value = serialized GTID set (e.g.
`3E11FA47-71CA-11E1-9E33-C80AA9429562:1-23,4F22GB58-82DB-22F2-AF44-D91BBA53A673:1-100`).
Parse / format / merge helpers in `position.rs`.

### Schema discovery

`schema.rs::discover_schema(conn, schema, table) → arrow::Schema`:

| MySQL type            | Arrow type                          |
|-----------------------|-------------------------------------|
| TINYINT/SMALLINT/INT  | Int32                               |
| BIGINT                | Int64                               |
| FLOAT                 | Float32                             |
| DOUBLE/DECIMAL        | Float64                             |
| VARCHAR/TEXT/CHAR     | Utf8                                |
| DATETIME/TIMESTAMP    | Timestamp(Microsecond, UTC)         |
| DATE                  | Date32                              |
| BOOLEAN/BIT(1)        | Boolean                             |
| JSON                  | Utf8 (raw JSON text)                |
| anything else         | error: `SchemaIncompatible(col, ty)`|

Discovered schema persisted once at workflow start to existing `streams` table. Any unsupported column type fails the run fast.

## Error handling

Three categories:

### 1. Configuration / precondition failures (terminal — non-retryable)

Checked once at workflow start in `verify_mysql_config` activity:
- `gtid_mode != ON` → `InvalidConfig("MySQL gtid_mode must be ON")`
- `binlog_format != ROW` → `InvalidConfig("MySQL binlog_format must be ROW")`
- `binlog_row_image != FULL` → warn (we can still decode), proceed
- Missing table in `information_schema` → `InvalidConfig` with table name
- Unsupported column type → `SchemaIncompatible(col, mysql_type)`

### 2. Transient connection / IO failures (retryable)

TCP drop, server restart, binlog reader hiccup → activity error bubbles up to Temporal retry policy. On retry, `read_window` reopens the binlog stream from the persisted GTID — idempotent because GTID set replay is precise.

### 3. Position-lost (terminal — `failed_fatal` per RFC-0008)

`mysql_async` returns `ER_MASTER_FATAL_ERROR_READING_BINLOG (1236)` when the requested GTID set has been purged. Detected by error code; mapped to non-retryable `SourceUnavailable("requested GTID purged; reinit required")`. Operator action: re-create pipeline (captures a new start GTID — same skip-snapshot semantics as initial start).

### Idempotency

Every successful `read_window` writes Parquet **before** persisting `new_gtid` to the run. On worker crash mid-window, replay re-reads the same binlog range and produces a new Parquet file — destination dedup is the loader's responsibility (existing pattern). The "no silent drops" RFC-0008 invariant holds: we either commit the new GTID or we re-read.

## Testing strategy

Three layers:

### Layer 1 — Decoder unit tests (`decode.rs`)

Pre-recorded binlog event bytes as fixtures under `tests/fixtures/mysql/`, captured once from a real MySQL via `mysqlbinlog --raw`.

Tests:
- `decodes_write_rows`
- `decodes_update_rows_with_before_image`
- `decodes_delete_rows`
- `respects_table_map_filter`
- `unsupported_column_type_errors`

Fast (<100ms each), no Docker, no network. Runs in default `cargo test`.

### Layer 2 — Schema discovery unit tests (`schema.rs`)

Mock `information_schema.columns` row sets; test the MySQL→Arrow type mapping table directly.

Tests:
- `maps_int_family`
- `maps_varchar_to_utf8`
- `maps_datetime_to_timestamp_micros`
- `unsupported_type_returns_schema_incompatible`

### Layer 3 — End-to-end integration test (`tests/integration/tests/mysql_cdc_e2e.rs`, `#[ignore]`)

Uses `testcontainers` to spawn `mysql:8.0` with `--gtid-mode=ON --enforce-gtid-consistency=ON --binlog-format=ROW`. Seeds a `customers` table, captures start GTID, performs `INSERT/UPDATE/DELETE`, runs the pipeline via `platform pipeline run`, polls `runs.status`, asserts Parquet output has 3 rows with expected `_cdc.op` values (`i, u, d`).

Pattern matches `stripe_ts_e2e.rs`: spawn worker subprocess + container source + assert Parquet rows. `#[ignore]` because it requires Docker.

### Test budget

11 new tests total: 5 decoder unit, 4 schema unit, 1 e2e, 1 spec roundtrip.

## Build sequence (eight tasks)

1. **Workspace deps** — add `mysql_async` (with `binlog`, `default-rustls` features), `testcontainers` (dev-dep).
2. **`MysqlCdcSourceSpec`** — add to `pipeline_spec.rs` with serde roundtrip + tagged-form tests.
3. **`position.rs`** — GTID set parse/format/merge helpers + unit tests.
4. **`schema.rs`** — MySQL→Arrow type map + `discover_schema` query function + unit tests.
5. **`decode.rs`** — binlog row events → Arrow rows; fixture-based unit tests.
6. **`stream.rs`** — `read_window`: open BinlogStream from GTID, drain N events, return rows + new_gtid.
7. **Activities + workflow** — `verify_mysql_config`, `capture_start_gtid`, `discover_schema`, `read_window` activities; `MysqlCdcPipelineWorkflow`; dispatch wiring in `workflows/mod.rs`.
8. **E2E integration test** — `mysql_cdc_e2e.rs` with testcontainers; `#[ignore]`-gated.

Each task is a separate commit. TDD order: tests first where the unit is internal logic (3, 4, 5); for network-touching units (6, 7) the e2e test in step 8 is the verification.

## Open questions punted to follow-up phases

- **Phase II.3.e candidate — Lift CDC to SDK.** Now that we have Postgres CDC + MySQL CDC + Stripe HTTP as data points, design the host-capability extension (raw TCP? typed `db-stream` verb?) informed by what these connectors actually need. Brainstorm-worthy.
- **MySQL initial snapshot.** RFC-0008 §"Per-source variation" describes the `FLUSH TABLES WITH READ LOCK` + consistent-read dance. Worth its own phase; meaningfully harder than Postgres `pg_export_snapshot`.
- **Multi-table support + parent/child workflow.** The full RFC-0008 topology.
- **DDL handling.** Parsing `Query_event`s from the binlog.
- **Backpressure / slot-lag alerting.** RFC-0008 §"Backpressure" tiers.

## Decision

Approved 2026-04-30. Implementation plan to follow at `docs/superpowers/plans/2026-04-30-phase-2-3d-mysql-cdc.md`.
