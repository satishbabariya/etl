# postgres-cdc-rs

Phase II.3.f reference WASM CDC connector for Postgres. Mirror of
`mysql-cdc-rs`, using the same `db.*` host imports — the SDK is now
DB-family agnostic.

## Build

```
rustup target add wasm32-wasip2     # one-time
cargo build --release
```

## Configure

```yaml
source:
  kind: wasm
  config:
    schema: public
    table: items
connection:
  connector_ref: "wasm-cdc:postgres-cdc-rs@0.1.0"
  url: postgres://user:pass@host:5432/dbname
```

## Cursor lifecycle

1. **Initial** (cursor=None): pin LSN via `SELECT pg_current_wal_lsn()`,
   create publication+slot if missing, fetch one snapshot chunk, return
   `snapshot-pk` cursor `<lsn>|<last_pk>`.
2. **Snapshot loop**: `snapshot-pk` cursor advances until a chunk returns
   fewer rows than `batch_size`, then transitions to `lsn`.
3. **Streaming forever**: each `read_batch` opens a short-lived
   `subscribe-changes`, drains up to `batch_size` events, returns the
   new `<lsn>`. Idle windows return rows=0 and the workflow sleeps.

## Schema

Hardcoded: `id BIGINT PRIMARY KEY, name TEXT NULL`. Arrow output:
`id`, `name`, `_cdc.op` (`s`/`i`/`u`/`d`), `_cdc.position`.
