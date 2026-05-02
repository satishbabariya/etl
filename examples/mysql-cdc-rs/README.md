# mysql-cdc-rs

Phase II.3.e reference WASM CDC connector. Demonstrates snapshot-then-streaming
MySQL CDC authored entirely in the connector SDK using the typed `db.*` host
verbs — no raw TCP, no per-connector binlog parser.

## Build

```
rustup target add wasm32-wasip2     # one-time
cargo build --release
```

The component lands at `target/wasm32-wasip2/release/mysql_cdc_rs.wasm`. Use
`platform connector build` to precompile it for the worker.

## Configure

```yaml
# pipeline spec
source:
  kind: wasm
  config:
    schema: test
    table: items
connection:
  connector_ref: "wasm-cdc:mysql-cdc-rs@0.1.0"
  url: mysql://user:pass@host:3306
```

The `wasm-cdc:` prefix routes the run to `WasmCdcPipelineWorkflow` (long-lived,
sleep-on-empty), distinct from the bounded `wasm:` flow.

## Cursor lifecycle

1. **Initial** (cursor=None): pin GTID via `SELECT @@gtid_executed`, fetch one
   snapshot chunk, return `snapshot-pk` cursor `<gtid>|<last_pk>`.
2. **Snapshot loop**: `snapshot-pk` cursor advances until a chunk returns fewer
   rows than `batch_size`, then transitions to `gtid` with `is_final=true`.
3. **Streaming forever**: each `read_batch` call opens a short-lived
   `subscribe-changes`, drains up to `batch_size` events, returns the new
   `<gtid>` position. Idle windows return rows=0 and the workflow sleeps.

## Schema

Hardcoded for the demo: `id BIGINT PRIMARY KEY, name TEXT NULL`. The connector
emits four Arrow columns: `id`, `name`, `_cdc.op` (`s`/`i`/`u`/`d`),
`_cdc.position` (snapshot:`<gtid>|<pk>` or `<gtid>` during streaming).
