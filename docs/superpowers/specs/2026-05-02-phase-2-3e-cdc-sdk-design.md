# Phase II.3.e — Lift CDC capabilities to the WASM SDK

**Status:** Approved 2026-05-02
**Related RFCs:** RFC-0006 (Connector Protocol), RFC-0008 (CDC Architecture), RFC-0020 (SDK Extensibility)
**Predecessor phases:** II.3.d (PR #28, MySQL native CDC) — explicit "Path Choice" punted lifting CDC to the SDK to a future phase. With II.3.d through II.3.d.7 done, we have three CDC data points (Postgres native, MySQL native streaming, MySQL native snapshot+streaming) plus the Stripe TS HTTP SDK connector — enough to settle the host-capability shape.

## Goal

Make CDC connectors authorable as WASM Component Model guests via the platform's connector SDK. Existing native MySQL/Postgres CDC connectors remain in-tree as the production path; this phase ships the SDK extension and proves it with one green-field example connector.

## Resolved design decisions

### 1. Host capability shape

**Typed `db` host verbs.** A new WIT interface `platform:connector/db@0.1.0` exposes `open` / `query` / `subscribe-changes` / `next-event` / `close`. The host owns `mysql_async` (and tokio-postgres for v2) clients behind the WIT; the guest writes business logic only.

Rejected alternatives:
- **Raw `tcp_connect`:** Maximum sandbox surface, every connector reimplements wire protocols, blocks TS authoring.
- **Component Model `stream<T>` resources:** Cleanest abstraction; upstream support not stable; defers entire phase indefinitely.

### 2. Migration path

**Green-field.** Native MySQL and Postgres CDC stay in-tree as the production default. The SDK extension ships alongside an example WASM connector (`examples/mysql-cdc-rs`) that proves third-party authoring works. Migrating native connectors to WASM is a separate phase if/when production data shows WASM CDC perf is acceptable.

### 3. Snapshot + streaming export

**Reuse the existing `read-batch` export.** No new exports; cursor encodes phase implicitly via the new `snapshot-pk` cursor-kind variant. The connector returns `is_final = true` to mark snapshot done; subsequent calls drain streaming changes until idle timeout.

### 4. State persistence

**Cursor round-trip via the existing connector protocol.** Guest is stateless: pure function of `(config, cursor) → (batch, new-cursor)`. Host activity persists the new cursor to `cdc_snapshots` between calls — same pattern as Stripe TS for HTTP polling.

### 5. Cursor format

**Extend `cursor-kind` enum.** Three new variants:
- `gtid` — MySQL GTID set string.
- `lsn` — Postgres LSN string.
- `snapshot-pk` — composite `"<gtid_or_lsn>|<last_pk_as_int64>"` during snapshot phase.

The connector encodes `snapshot-pk` while snapshotting, switches to plain `gtid`/`lsn` after `is_final = true`.

### 6. v1 scope

**MySQL CDC SDK example only.** Postgres CDC via SDK is II.3.f (mechanical work, deferred until concrete user need). Multi-table is a separate brainstorm.

## Architecture

A new WIT interface `platform:connector/db@0.1.0` exposes typed DB primitives so WASM CDC connectors author business logic without reimplementing wire protocols. Cursor-kind extends with `gtid` / `lsn` / `snapshot-pk` variants. Existing `WasmSourceSpec { config: serde_json::Value }` carries CDC config; CLI dispatches `connector_ref = "wasm-cdc:<name>@<version>"` to a new `WasmCdcPipelineWorkflow` mirroring `MysqlCdcPipelineWorkflow`'s loop shape. State persistence stays opaque-cursor: host activity writes `cdc_snapshots` between calls.

### File map

```
crates/connector-sdk/wit/source-connector.wit
   — extend cursor-kind with gtid | lsn | snapshot-pk
   — add `import db;` to the source-connector world

crates/connector-sdk/wit/db.wit                     (NEW)
   — interface db with open/query/subscribe_changes/next_event/close

crates/worker/src/wasm_runtime/db_host.rs           (NEW)
   — host-side implementation of db verbs, backed by mysql_async (v1)
     + tokio-postgres (v2 — wire stub)

crates/worker/src/wasm_runtime/host.rs              (modified)
   — extend HostState with DbHostState

crates/worker/src/workflows/wasm_cdc_pipeline.rs    (NEW)
   — WasmCdcPipelineWorkflow with snapshot+streaming loop

crates/worker/src/activities/wasm_cdc/              (NEW)
   — read_batch_wasm_cdc activity that loads the .cwasm and dispatches

examples/mysql-cdc-rs/                              (NEW)
   — Rust SDK template + a working MySQL CDC connector

tests/integration/tests/mysql_cdc_wasm_e2e.rs       (NEW, #[ignore])
   — full snapshot+streaming round-trip via the WASM connector
```

### WIT extension

```wit
package platform:connector@0.1.0;

interface db {
    record db-handle { id: u32 }
    record change-stream { id: u32 }

    record change-event {
        op: char,                 // 'i' / 'u' / 'd' / 'h' (heartbeat)
        position: string,         // GTID set / LSN
        commit-ts: s64,           // micros since epoch; 0 if unknown
        txid: s64,                // 0 if unknown
        table: string,            // schema-qualified
        row-json: string,         // JSON object; empty for heartbeat
    }

    variant db-error {
        invalid-config(string),
        connect-failed(string),
        query-failed(string),
        position-lost(string),
        unsupported(string),
    }

    open: func(url: string) -> result<db-handle, db-error>;
    query: func(h: db-handle, sql: string, params: list<string>)
        -> result<list<list<option<string>>>, db-error>;
    subscribe-changes: func(h: db-handle, position: string)
        -> result<change-stream, db-error>;
    next-event: func(s: change-stream) -> result<option<change-event>, db-error>;
    close: func(h: db-handle);
    close-stream: func(s: change-stream);
}

// In source-connector.wit:
enum cursor-kind {
    int64,
    timestamp-tz,
    gtid,
    lsn,
    snapshot-pk,
}

world source-connector {
    use types.{...};
    import host;
    import db;                    // NEW
    export discover: func(...) -> ...;
    export read-batch: func(...) -> ...;
}
```

### Workflow flow (`WasmCdcPipelineWorkflow`)

```
start_run
→ verify_run                        (lifecycle)
→ snapshot loop:
    read_batch_wasm_cdc { cursor }
       → guest: db.open + db.query("SELECT ::text...")
       → returns ReadOutcome { batch_ipc, rows, new_cursor (snapshot-pk), is_final }
    until is_final
→ streaming loop (max_windows for tests):
    read_batch_wasm_cdc { cursor }
       → guest: db.subscribe_changes + db.next_event drain loop
       → returns ReadOutcome with new_cursor (gtid|lsn) + rows
    forever (or until max_windows)
→ complete_run
```

After each `read_batch_wasm_cdc` call, the activity persists `new_cursor` to `cdc_snapshots` (`snapshot-pk` triggers snapshot-state upsert; `gtid`/`lsn` triggers position-only upsert).

### Host implementation of `db` verbs

`HostState` extends with:

```rust
pub struct DbHostState {
    next_id: u32,
    handles: HashMap<u32, DbConn>,
    streams: HashMap<u32, DbStream>,
}

enum DbConn {
    Mysql(mysql_async::Conn),
    Postgres,  // v2 stub
}

enum DbStream {
    Mysql(mysql_async::BinlogStream),
    Postgres,  // v2 stub
}
```

Per-verb implementation:

- **`db.open(url)`**: parse url scheme, open `mysql_async::Conn::from_url`. Postgres returns `Unsupported` in v1.
- **`db.query`**: bind params, run, return `Vec<Vec<Option<String>>>`. Connector decides projection (CAST/HEX) — host doesn't.
- **`db.subscribe_changes`**: parse position via existing `GtidSet::parse`, build `BinlogStreamRequest` (mirroring `connectors/mysql/cdc/stream.rs::build_request`), call `conn.get_binlog_stream(req)`. Critical: the call consumes `Conn`, so the corresponding handle is invalidated — track this in `DbHostState`.
- **`db.next_event`**: pull from `BinlogStream::next()` with a 5s host-side idle timeout. Decode events using existing `connectors/mysql/cdc/stream.rs::classify`. Return `change-event` with row data as JSON. Heartbeats fire when 5s elapses with no event but stream is healthy.
- **`db.close` / `close-stream`**: drop from HashMap; idempotent silent no-op for unknown ids.

Error mapping: `ER_MASTER_FATAL_ERROR_READING_BINLOG (1236)` → `position-lost`. Connection drops → `connect-failed`. Other → `query-failed`.

### Example connector — `examples/mysql-cdc-rs`

Rust SDK template that exercises every host verb. Single-table snapshot+streaming, integer PK, mirrors the production native connector's behavior shape.

```
examples/mysql-cdc-rs/
├── Cargo.toml          (wit-bindgen, serde_json, anyhow; cdylib)
├── wit/source-connector.wit
├── src/
│   ├── lib.rs          (entry: discover + read-batch exports)
│   ├── snapshot.rs     (db.query against information_schema + the table)
│   ├── streaming.rs    (db.subscribe-changes + next-event loop)
│   └── arrow.rs        (rows → Arrow IPC stream bytes)
└── README.md
```

`read-batch` dispatches on cursor kind:
- `None` → start snapshot from PK 0 (capture GTID via `SELECT @@GLOBAL.gtid_executed`).
- `SnapshotPk(value)` → continue snapshot from `last_pk` parsed from value; if final, switch cursor to `Gtid`.
- `Gtid(value)` → drain streaming changes via `db.subscribe-changes` + `db.next-event` until idle timeout.

Build & publish via existing CLI:
```
platform connector test examples/mysql-cdc-rs
platform connector publish examples/mysql-cdc-rs --registry ./connectors
```

Artifact registers as `wasm-cdc:mysql-cdc-rs@0.1.0`. CLI dispatches `connector_ref` starting with `wasm-cdc:` to `WasmCdcPipelineWorkflow`.

## Error handling

### 1. Configuration / precondition (terminal)

Invalid URL, unsupported DB scheme, missing `source.config` fields → `ConnectorError::InvalidConfig` from guest or `db-error::invalid-config` from host. Workflow fails fast in first `read_batch_wasm_cdc` call.

### 2. Transient (retryable)

TCP drops, server restarts, query timeouts → `query-failed` / `connect-failed`. Activity bubbles to Temporal's 5-attempt 1s→30s policy. On retry, activity reopens the component and re-acquires DB handles; cursor state in `cdc_snapshots` is durable so progress is preserved.

### 3. Position-lost (terminal — `failed_fatal`)

Host detects `ER_MASTER_FATAL_ERROR_READING_BINLOG (1236)` → `db-error::position-lost`. Workflow goes to `fail_run` with non-retryable error. Operator action: re-create pipeline (clears persisted `last_pk`, captures fresh GTID).

### 4. Guest panics

Wasmtime traps the component — activity surfaces a generic retryable error. Connector authoring bugs reach `tracing::error!` via existing host log import.

## Testing strategy

### Layer 1 — Host `db_host.rs` unit tests (~6 tests)

`DbHostState` id allocation/free, error-mapping of `mysql_async::Error` variants to `db-error`. No DB needed (mock or trait abstraction).

### Layer 2 — Guest connector unit tests (~4 tests)

In `examples/mysql-cdc-rs/src/snapshot.rs::tests`: cursor-format round-trip (`snapshot-pk` parse/format), JSON-row → Arrow IPC conversion. No host needed.

### Layer 3 — End-to-end (1 test, `#[ignore]`)

`tests/integration/tests/mysql_cdc_wasm_e2e.rs` mirrors `mysql_cdc_snapshot_then_streaming_e2e` shape but points at the WASM artifact. mysql:8.1 testcontainer + 3 seeded rows + post-snapshot IUD. Asserts `[s,s,s,i,u,d]` ops and typed Parquet schema (`id: Int64`, etc.). ~120s test, Docker-gated.

## Out of scope (deferred)

- **Postgres CDC SDK example.** Mechanical work; ships as II.3.f when there's a concrete user.
- **TS authoring template for CDC.** Works for free thanks to the typed-host capability shape but no v1 example.
- **Multi-table.** Separate brainstorm-required phase.
- **Per-event row-data Arrow IPC inside the host.** Host emits `row-json`; guest converts. Keeps host capability surface narrow.
- **Connection pooling across activity invocations.** Each activity reopens; revisit if hot-path data shows it matters.
- **Migrating native CDC connectors to WASM.** Separate phase if/when production WASM CDC perf is validated.

## Build sequence

1. **WIT extension** — add `db` interface + extend `cursor-kind`. Regenerate bindings.
2. **Host `db_host.rs`** — `DbHostState`, `db.open`/`query`/`close` for MySQL. Stub Postgres. Unit tests for handle lifecycle.
3. **Host `db.subscribe-changes` + `next-event`** — bridge to existing `connectors/mysql/cdc/stream.rs` decode logic. Unit tests for error mapping.
4. **`WasmCdcPipelineWorkflow` + `read_batch_wasm_cdc` activity** — workflow loop + activity that loads `.cwasm` and persists cursor to `cdc_snapshots`.
5. **CLI dispatch** — route `connector_ref` starting with `wasm-cdc:` to `WasmCdcPipelineWorkflow` (similar to existing II.3.d.5 dispatch arm).
6. **`examples/mysql-cdc-rs`** — Rust SDK template + working MySQL CDC connector. Unit tests for cursor format + Arrow conversion.
7. **E2E** — `mysql_cdc_wasm_e2e.rs`; reuse existing testcontainer setup.
8. **README + final verification.**

Each task is a separate commit. TDD where applicable (1, 2, 6); for network-touching units (3, 4, 5) the e2e test in step 7 is the verification.

## Decision

Approved 2026-05-02. Implementation plan to follow at `docs/superpowers/plans/2026-05-02-phase-2-3e-cdc-sdk.md`.
