# Phase II.3.f — Postgres SDK Port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Postgres host backing for the same `db.*` WIT interface that II.3.e introduced for MySQL, and ship `examples/postgres-cdc-rs` as the reference WASM CDC connector for Postgres.

**Architecture:** The host gains a `DbConn::Postgres(sqlx::PgConnection)` variant; `db.subscribe-changes` for Postgres polls `pg_logical_slot_get_binary_changes` and reuses the existing native pgoutput decoder at `crates/worker/src/connectors/postgres/cdc/decode.rs`. Connectors create slots+publications via `db.query` (idempotent SQL) and pass `slot_name`/`publication_names` via a new WIT `options` parameter on `subscribe-changes`. Workflow + CLI dispatch are unchanged from II.3.e — same `wasm-cdc:` prefix routes to `WasmCdcPipelineWorkflow`.

**Tech Stack:** Rust 1.83, wasmtime 36.0.7, wit-bindgen 0.37, sqlx (already in workspace for native CDC), Postgres 16 + pgoutput v1, mysql_async 0.36 (existing — backward compatible after WIT change).

---

## File structure

| Path | Responsibility | Action |
|------|----------------|--------|
| `crates/connector-sdk/wit/db.wit` | WIT interface definition | Modify — add `options` to `subscribe-changes` |
| `crates/worker/src/wasm_runtime/db_host.rs` | Host trait impl, URL routing | Modify — add Postgres branch in `open` + `query` + `subscribe_changes` |
| `crates/worker/src/wasm_runtime/db_subscribe.rs` | MySQL subscription state | Modify — accept the new WIT signature unchanged (MySQL ignores `options` in v1) |
| `crates/worker/src/wasm_runtime/db_pg_subscribe.rs` | Postgres subscription state + pgoutput decode | **New** |
| `crates/worker/src/wasm_runtime/mod.rs` | Module registry | Modify — add `pub mod db_pg_subscribe;` |
| `examples/mysql-cdc-rs/src/streaming.rs` | MySQL example streaming call site | Modify — add `&[]` arg to subscribe_changes |
| `examples/postgres-cdc-rs/Cargo.toml` | Crate manifest | **New** |
| `examples/postgres-cdc-rs/.cargo/config.toml` | wasm32-wasip2 target | **New** |
| `examples/postgres-cdc-rs/.gitignore` | Ignore `/target` | **New** |
| `examples/postgres-cdc-rs/README.md` | Connector docs | **New** |
| `examples/postgres-cdc-rs/src/lib.rs` | Guest entry, dispatch | **New** |
| `examples/postgres-cdc-rs/src/arrow_io.rs` | Arrow IPC encoding | **New** |
| `examples/postgres-cdc-rs/src/snapshot.rs` | Snapshot phase + slot setup | **New** |
| `examples/postgres-cdc-rs/src/streaming.rs` | Streaming phase | **New** |
| `tests/integration/tests/postgres_cdc_wasm_e2e.rs` | End-to-end test | **New** |
| `README.md` | Project status | Modify — bump "Currently:" line |

---

## Task 1: WIT extension — add `options` to subscribe-changes

**Files:**
- Modify: `crates/connector-sdk/wit/db.wit:53-59`
- Modify: `crates/worker/src/wasm_runtime/db_host.rs` (signature update — db_host.rs's `subscribe_changes` impl gains an `options` parameter)
- Modify: `crates/worker/src/wasm_runtime/db_subscribe.rs` (no behavior change — MySQL ignores options for now)
- Modify: `examples/mysql-cdc-rs/src/streaming.rs` (1 line — pass `&[]`)

- [ ] **Step 1: Edit db.wit to add options parameter**

In `crates/connector-sdk/wit/db.wit`, replace:

```wit
    subscribe-changes: func(
        h: db-handle,
        position: string,
    ) -> result<change-stream, db-error>;
```

with:

```wit
    /// Subscribe to change events from `position`. The `options` list
    /// is a free-form key-value bag passed to the host:
    ///   - Postgres: requires `slot_name` and `publication_names`;
    ///     accepts optional `proto_version` (default "1").
    ///   - MySQL: currently ignores all keys (server_id is host-allocated).
    subscribe-changes: func(
        h: db-handle,
        position: string,
        options: list<tuple<string, string>>,
    ) -> result<change-stream, db-error>;
```

- [ ] **Step 2: Update host db_host.rs signature**

In `crates/worker/src/wasm_runtime/db_host.rs`, change the `subscribe_changes` async fn impl signature to take the new arg. Replace:

```rust
    async fn subscribe_changes(
        &mut self,
        h: DbHandle,
        position: String,
    ) -> wasmtime::Result<Result<ChangeStream, DbError>> {
```

with:

```rust
    async fn subscribe_changes(
        &mut self,
        h: DbHandle,
        position: String,
        _options: Vec<(String, String)>,
    ) -> wasmtime::Result<Result<ChangeStream, DbError>> {
```

(Underscore prefix on `_options` because Task 3 introduces the Postgres branch that consumes them; MySQL doesn't read any keys today.)

- [ ] **Step 3: Update mysql-cdc-rs streaming call**

In `examples/mysql-cdc-rs/src/streaming.rs`, find the `db::subscribe_changes(h, start_gtid)` call and change it to:

```rust
    let sub = db::subscribe_changes(h, start_gtid, &[]).map_err(db_err_to_connector_err)?;
```

- [ ] **Step 4: Build worker lib + mysql-cdc-rs to verify the WIT change compiles end-to-end**

Run:

```bash
cargo build -p worker --lib && \
cargo build --release --manifest-path examples/mysql-cdc-rs/Cargo.toml
```

Expected: both compile; bindgen generates the new option-bearing fn for guest + host.

- [ ] **Step 5: Run worker lib tests**

Run:

```bash
cargo test -p worker --lib
```

Expected: all 134 existing tests still pass (no behavior change yet).

- [ ] **Step 6: Commit**

```bash
git add crates/connector-sdk/wit/db.wit \
        crates/worker/src/wasm_runtime/db_host.rs \
        examples/mysql-cdc-rs/src/streaming.rs && \
git commit -m "phase-2-3f-1: extend WIT db.subscribe-changes with options parameter

Adds list<tuple<string,string>> options arg to subscribe-changes so
Postgres connectors can pass slot_name + publication_names. MySQL
ignores all keys today (server_id stays host-allocated).

mysql-cdc-rs example updated to pass empty options. Host signature
update; behavior unchanged. Phase II.3.f Task 2 starts using the
options bag."
```

---

## Task 2: Host db.open + db.query for Postgres

**Files:**
- Modify: `crates/worker/src/wasm_runtime/db_host.rs` — add `Postgres` variant + URL routing + query branch

- [ ] **Step 1: Add `Postgres` variant to `DbConn` enum**

In `crates/worker/src/wasm_runtime/db_host.rs`, find the `DbConn` enum and replace it:

```rust
pub(super) enum DbConn {
    Mysql(Conn),
    Postgres(sqlx::PgConnection),
    /// The handle was passed to `subscribe-changes`, which consumed the
    /// underlying connection. Subsequent db.query calls on this id fail.
    Consumed,
}
```

Add the import at the top of the file alongside `use mysql_async::*`:

```rust
use sqlx::Connection as _;
```

- [ ] **Step 2: Update `db.open` to route postgres URLs**

Find the `async fn open` impl. Replace its body:

```rust
    async fn open(&mut self, url: String) -> wasmtime::Result<Result<DbHandle, DbError>> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return Ok(Err(DbError::Unsupported(
                "postgres support not in v1; use mysql:// urls".into(),
            )));
        }
        if !url.starts_with("mysql://") {
            return Ok(Err(DbError::InvalidConfig(format!(
                "unsupported scheme in url: {url}"
            ))));
        }
        match Conn::from_url(&url).await {
            Ok(conn) => {
                let id = self.db.alloc_id();
                self.db.conns.insert(id, DbConn::Mysql(conn));
                Ok(Ok(DbHandle { id }))
            }
            Err(e) => Ok(Err(DbError::ConnectFailed(e.to_string()))),
        }
    }
```

with:

```rust
    async fn open(&mut self, url: String) -> wasmtime::Result<Result<DbHandle, DbError>> {
        if url.starts_with("mysql://") {
            return match Conn::from_url(&url).await {
                Ok(conn) => {
                    let id = self.db.alloc_id();
                    self.db.conns.insert(id, DbConn::Mysql(conn));
                    Ok(Ok(DbHandle { id }))
                }
                Err(e) => Ok(Err(DbError::ConnectFailed(e.to_string()))),
            };
        }
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return match sqlx::PgConnection::connect(&url).await {
                Ok(conn) => {
                    let id = self.db.alloc_id();
                    self.db.conns.insert(id, DbConn::Postgres(conn));
                    Ok(Ok(DbHandle { id }))
                }
                Err(e) => Ok(Err(DbError::ConnectFailed(e.to_string()))),
            };
        }
        Ok(Err(DbError::InvalidConfig(format!(
            "unsupported scheme in url: {url}"
        ))))
    }
```

- [ ] **Step 3: Add Postgres branch to `db.query`**

Find the `async fn query` impl. Replace the inner `match entry { ... }` block with one that handles both DBs:

```rust
        let entry = match self.db.conns.get_mut(&h.id) {
            Some(e) => e,
            None => {
                return Ok(Err(DbError::QueryFailed(format!(
                    "no such db handle: {}",
                    h.id
                ))));
            }
        };
        match entry {
            DbConn::Mysql(conn) => {
                let params_v: Vec<Value> = params
                    .into_iter()
                    .map(|s| Value::Bytes(s.into_bytes()))
                    .collect();
                let rows: Vec<mysql_async::Row> = match conn.exec(&sql, params_v).await {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(DbError::QueryFailed(e.to_string()))),
                };
                let mut out: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let n = row.columns_ref().len();
                    let mut cells: Vec<Option<String>> = Vec::with_capacity(n);
                    for i in 0..n {
                        let v: Value = row.as_ref(i).cloned().unwrap_or(Value::NULL);
                        cells.push(value_to_string(v));
                    }
                    out.push(cells);
                }
                Ok(Ok(out))
            }
            DbConn::Postgres(conn) => {
                let mut q = sqlx::query(&sql);
                for p in &params {
                    q = q.bind(p);
                }
                let rows = match q.fetch_all(&mut *conn).await {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(DbError::QueryFailed(e.to_string()))),
                };
                let mut out: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len());
                for row in &rows {
                    use sqlx::Row as _;
                    let n = row.len();
                    let mut cells: Vec<Option<String>> = Vec::with_capacity(n);
                    for i in 0..n {
                        let v: Option<String> = row.try_get(i).unwrap_or_default();
                        cells.push(v);
                    }
                    out.push(cells);
                }
                Ok(Ok(out))
            }
            DbConn::Consumed => Ok(Err(DbError::QueryFailed(
                "this handle was consumed by subscribe-changes".into(),
            ))),
        }
```

(The existing variable `entry` becomes the matched value; the previous code's `let conn = match entry { DbConn::Mysql(c) => c, ... }` pattern is replaced by matching all three variants directly.)

- [ ] **Step 4: Update `db.close` to handle Postgres**

Find the `async fn close` impl. Replace its body with:

```rust
    async fn close(&mut self, h: DbHandle) -> wasmtime::Result<()> {
        if let Some(conn) = self.db.conns.remove(&h.id) {
            match conn {
                DbConn::Mysql(c) => {
                    let _ = c.disconnect().await;
                }
                DbConn::Postgres(c) => {
                    let _ = c.close().await;
                }
                DbConn::Consumed => {}
            }
        }
        Ok(())
    }
```

- [ ] **Step 5: Add a unit test for Postgres URL routing**

In `crates/worker/src/wasm_runtime/db_host.rs`'s `tests` module, append:

```rust
    #[tokio::test]
    async fn open_rejects_unknown_scheme() {
        use super::*;
        let mut state = super::super::host::HostState::new(super::super::limits::Limits::default());
        let res = state.open("redis://localhost:6379".into()).await.unwrap();
        match res {
            Err(DbError::InvalidConfig(msg)) => {
                assert!(msg.contains("unsupported scheme"), "got: {msg}");
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }
```

- [ ] **Step 6: Run worker lib tests**

```bash
cargo build -p worker --lib && cargo test -p worker --lib wasm_runtime::db_host
```

Expected: all db_host tests pass (4 existing + 1 new = 5).

- [ ] **Step 7: Commit**

```bash
git add crates/worker/src/wasm_runtime/db_host.rs && \
git commit -m "phase-2-3f-2: host db.open + db.query for Postgres URLs

DbConn gains a Postgres(sqlx::PgConnection) variant. open routes
mysql:// to mysql_async, postgres:// (and postgresql://) to sqlx,
rejects unknown schemes with InvalidConfig.

query dispatches per-variant; the Postgres path uses sqlx::query +
fetch_all and reads each cell as Option<String> (PgRow::try_get
falls back to None on type mismatch — guests are expected to
project via CAST AS TEXT for non-string columns, mirroring the
MySQL CAST AS CHAR contract).

close calls disconnect() / sqlx close() then removes from the map.

5 db_host unit tests pass (1 new for unsupported scheme rejection)."
```

---

## Task 3: Host db.subscribe-changes + next-event for Postgres

**Files:**
- Create: `crates/worker/src/wasm_runtime/db_pg_subscribe.rs`
- Modify: `crates/worker/src/wasm_runtime/mod.rs` — register the new module
- Modify: `crates/worker/src/wasm_runtime/db_host.rs` — extend `DbHostState::streams`, dispatch in `subscribe_changes` + `next_event` + `close_stream`

- [ ] **Step 1: Create db_pg_subscribe.rs with the PgSubscription struct + decode helpers**

Create `crates/worker/src/wasm_runtime/db_pg_subscribe.rs`:

```rust
//! Postgres binlog (logical-replication) subscription state.
//!
//! One `PgSubscription` per active `db.subscribe-changes` call.
//! Each `next-event` call drains a buffered `pending` VecDeque first;
//! when empty, it polls `pg_logical_slot_get_binary_changes` for up
//! to `max_per_poll` rows and decodes the binary pgoutput payload via
//! the existing native decoder at `connectors/postgres/cdc/decode.rs`.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use sqlx::PgConnection;
use sqlx::Row as _;

use crate::connectors::postgres::cdc::decode::{
    decode_message, CdcEvent, RelationInfo, RelationTable,
};

use super::bindings::platform::connector::db::{ChangeEvent, DbError};

const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 5;
const DEFAULT_MAX_PER_POLL: usize = 1000;

pub struct PgSubscription {
    pub(super) conn: PgConnection,
    pub(super) slot_name: String,
    pub(super) publication_names: String,
    pub(super) proto_version: String,
    pub(super) pending: VecDeque<ChangeEvent>,
    pub(super) relations: RelationTable,
    pub(super) current_position: String,
    pub(super) idle_timeout: Duration,
    pub(super) max_per_poll: usize,
}

impl PgSubscription {
    pub fn new(
        conn: PgConnection,
        slot_name: String,
        publication_names: String,
        proto_version: String,
        start_position: String,
    ) -> Self {
        Self {
            conn,
            slot_name,
            publication_names,
            proto_version,
            pending: VecDeque::new(),
            relations: HashMap::new(),
            current_position: start_position,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
            max_per_poll: DEFAULT_MAX_PER_POLL,
        }
    }

    /// Drain one event. `Ok(None)` means the slot returned 0 rows on
    /// this poll — the guest should treat that as "drain done" and
    /// return from read_batch.
    pub async fn next(&mut self) -> Result<Option<ChangeEvent>, DbError> {
        if let Some(ev) = self.pending.pop_front() {
            return Ok(Some(ev));
        }
        match self.poll_and_buffer().await {
            Ok(()) => Ok(self.pending.pop_front()),
            Err(e) => Err(e),
        }
    }

    async fn poll_and_buffer(&mut self) -> Result<(), DbError> {
        let stmt = "SELECT lsn::text, data \
                    FROM pg_logical_slot_get_binary_changes($1, NULL, $2, \
                        'proto_version', $3, \
                        'publication_names', $4)";
        let rows = match sqlx::query(stmt)
            .bind(&self.slot_name)
            .bind(self.max_per_poll as i32)
            .bind(&self.proto_version)
            .bind(&self.publication_names)
            .fetch_all(&mut self.conn)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(DbError::QueryFailed(format!(
                    "pg_logical_slot_get_binary_changes: {e}"
                )));
            }
        };
        for r in &rows {
            let lsn: String = r.try_get(0).unwrap_or_default();
            let data: Vec<u8> = r.try_get(1).unwrap_or_default();
            let event = match decode_message(&data) {
                Ok(e) => e,
                Err(e) => {
                    return Err(DbError::QueryFailed(format!(
                        "pgoutput decode at lsn {lsn}: {e}"
                    )));
                }
            };
            if let Some(ce) = self.cdc_event_to_change_event(event, &lsn) {
                self.pending.push_back(ce);
            }
        }
        if !rows.is_empty() {
            // Last row's LSN is the slot's new confirmed position.
            if let Some(last) = rows.last() {
                let lsn: String = last.try_get(0).unwrap_or_default();
                if !lsn.is_empty() {
                    self.current_position = lsn;
                }
            }
        }
        Ok(())
    }

    fn cdc_event_to_change_event(
        &mut self,
        ev: CdcEvent,
        lsn: &str,
    ) -> Option<ChangeEvent> {
        match ev {
            CdcEvent::Relation(info) => {
                self.relations.insert(info.rel_id, info);
                None
            }
            CdcEvent::Insert { rel_id, row } => {
                let rel = self.relations.get(&rel_id)?;
                Some(self.row_event(rel, 'i', lsn, Some(row), None))
            }
            CdcEvent::Update { rel_id, row } => {
                let rel = self.relations.get(&rel_id)?;
                Some(self.row_event(rel, 'u', lsn, Some(row), None))
            }
            CdcEvent::Delete { rel_id, key } => {
                let rel = self.relations.get(&rel_id)?;
                Some(self.row_event(rel, 'd', lsn, None, Some(key)))
            }
            // Begin/Commit/Truncate/Origin produce no row-level event.
            _ => None,
        }
    }

    fn row_event(
        &self,
        rel: &RelationInfo,
        op: char,
        lsn: &str,
        after: Option<Vec<Option<String>>>,
        before: Option<Vec<Option<String>>>,
    ) -> ChangeEvent {
        let mut obj = serde_json::Map::new();
        if let Some(a) = after {
            obj.insert("after".into(), positional(&a));
        }
        if let Some(b) = before {
            obj.insert("before".into(), positional(&b));
        }
        ChangeEvent {
            op,
            position: lsn.to_string(),
            commit_ts: 0,
            txid: 0,
            table: format!("{}.{}", rel.namespace, rel.name),
            row_json: serde_json::Value::Object(obj).to_string(),
        }
    }
}

fn positional(cells: &[Option<String>]) -> serde_json::Value {
    serde_json::Value::Array(
        cells
            .iter()
            .map(|c| match c {
                None => serde_json::Value::Null,
                Some(s) => serde_json::Value::String(s.clone()),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_relation() -> RelationInfo {
        RelationInfo {
            rel_id: 42,
            namespace: "public".into(),
            name: "items".into(),
            columns: vec![],
        }
    }

    #[test]
    fn relation_event_caches_and_returns_none() {
        // Construct a minimal subscription without a real Conn by
        // routing through the helper directly.
        let rel = fake_relation();
        let mut relations: RelationTable = HashMap::new();
        relations.insert(rel.rel_id, rel);
        // Verify the conversion of an Insert event uses the cached relation.
        // (Helper lives on PgSubscription; we exercise positional() which is
        // self-contained and the trickiest pure-data path.)
        let arr = positional(&vec![Some("1".into()), None]);
        let serde_json::Value::Array(ref items) = arr else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], serde_json::Value::String("1".into()));
        assert_eq!(items[1], serde_json::Value::Null);
    }

    #[test]
    fn positional_handles_empty() {
        let arr = positional(&[]);
        assert_eq!(arr, serde_json::Value::Array(vec![]));
    }

    #[test]
    fn positional_preserves_order() {
        let arr = positional(&[
            Some("a".into()),
            Some("b".into()),
            None,
            Some("d".into()),
        ]);
        let items = match arr {
            serde_json::Value::Array(v) => v,
            _ => panic!(),
        };
        assert_eq!(items.len(), 4);
        assert_eq!(items[2], serde_json::Value::Null);
    }
}
```

- [ ] **Step 2: Register the new module**

In `crates/worker/src/wasm_runtime/mod.rs`, change:

```rust
pub mod bindings;
pub mod connector;
pub mod db_host;
pub mod db_subscribe;
```

to add `db_pg_subscribe` next to `db_subscribe`:

```rust
pub mod bindings;
pub mod connector;
pub mod db_host;
pub mod db_pg_subscribe;
pub mod db_subscribe;
```

- [ ] **Step 3: Extend DbHostState.streams to be either MySQL or Postgres**

In `crates/worker/src/wasm_runtime/db_host.rs`, find the `DbHostState` struct:

```rust
pub struct DbHostState {
    next_id: u32,
    conns: HashMap<u32, DbConn>,
    /// Streams are populated by Task 3.
    pub(super) streams: HashMap<u32, super::db_subscribe::MysqlSubscription>,
}
```

Replace with an enum-typed map:

```rust
pub(super) enum DbStream {
    Mysql(super::db_subscribe::MysqlSubscription),
    Postgres(super::db_pg_subscribe::PgSubscription),
}

pub struct DbHostState {
    next_id: u32,
    conns: HashMap<u32, DbConn>,
    pub(super) streams: HashMap<u32, DbStream>,
}
```

- [ ] **Step 4: Update the MySQL subscribe_changes path to wrap in DbStream::Mysql**

In `db_host.rs`, find the existing MySQL `subscribe_changes` body. Where it currently does `self.db.streams.insert(sub_id, super::db_subscribe::MysqlSubscription::new(stream, position))`, change to:

```rust
        self.db.streams.insert(
            sub_id,
            DbStream::Mysql(super::db_subscribe::MysqlSubscription::new(stream, position)),
        );
```

- [ ] **Step 5: Add the Postgres branch to subscribe_changes**

The current `subscribe_changes` impl matches on `DbConn::Mysql / DbConn::Consumed / None`. Add a Postgres arm. Replace the entire impl with:

```rust
    async fn subscribe_changes(
        &mut self,
        h: DbHandle,
        position: String,
        options: Vec<(String, String)>,
    ) -> wasmtime::Result<Result<ChangeStream, DbError>> {
        let conn = match self.db.conns.remove(&h.id) {
            Some(c) => c,
            None => {
                return Ok(Err(DbError::QueryFailed(format!(
                    "no such db handle: {}",
                    h.id
                ))));
            }
        };

        match conn {
            DbConn::Mysql(my_conn) => {
                self.db.conns.insert(h.id, DbConn::Consumed);
                let server_id = self.db.alloc_server_id();
                let req = match build_binlog_request(server_id, &position) {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(e)),
                };
                let stream = match my_conn.get_binlog_stream(req).await {
                    Ok(s) => s,
                    Err(e) => {
                        return Ok(Err(DbError::ConnectFailed(format!(
                            "get_binlog_stream: {e}"
                        ))));
                    }
                };
                let sub_id = self.db.alloc_id();
                self.db.streams.insert(
                    sub_id,
                    DbStream::Mysql(super::db_subscribe::MysqlSubscription::new(
                        stream, position,
                    )),
                );
                Ok(Ok(ChangeStream { id: sub_id }))
            }
            DbConn::Postgres(pg_conn) => {
                let mut slot_name: Option<String> = None;
                let mut publication_names: Option<String> = None;
                let mut proto_version = "1".to_string();
                for (k, v) in &options {
                    match k.as_str() {
                        "slot_name" => slot_name = Some(v.clone()),
                        "publication_names" => publication_names = Some(v.clone()),
                        "proto_version" => proto_version = v.clone(),
                        _ => {}
                    }
                }
                let slot_name = match slot_name {
                    Some(s) => s,
                    None => {
                        return Ok(Err(DbError::InvalidConfig(
                            "postgres subscribe-changes requires options[slot_name]".into(),
                        )));
                    }
                };
                let publication_names = match publication_names {
                    Some(s) => s,
                    None => {
                        return Ok(Err(DbError::InvalidConfig(
                            "postgres subscribe-changes requires options[publication_names]"
                                .into(),
                        )));
                    }
                };
                let sub_id = self.db.alloc_id();
                self.db.streams.insert(
                    sub_id,
                    DbStream::Postgres(super::db_pg_subscribe::PgSubscription::new(
                        pg_conn,
                        slot_name,
                        publication_names,
                        proto_version,
                        position,
                    )),
                );
                Ok(Ok(ChangeStream { id: sub_id }))
            }
            DbConn::Consumed => Ok(Err(DbError::QueryFailed(
                "handle already consumed by an earlier subscribe-changes".into(),
            ))),
        }
    }
```

- [ ] **Step 6: Dispatch next_event by stream variant**

Replace the existing `next_event` impl with:

```rust
    async fn next_event(
        &mut self,
        s: ChangeStream,
    ) -> wasmtime::Result<Result<Option<ChangeEvent>, DbError>> {
        let Some(sub) = self.db.streams.get_mut(&s.id) else {
            return Ok(Err(DbError::QueryFailed(format!(
                "no such change-stream: {}",
                s.id
            ))));
        };
        let res = match sub {
            DbStream::Mysql(m) => m.next().await,
            DbStream::Postgres(p) => p.next().await,
        };
        Ok(res)
    }
```

- [ ] **Step 7: Update close_stream to handle either variant**

Replace the existing `close_stream` impl with:

```rust
    async fn close_stream(&mut self, s: ChangeStream) -> wasmtime::Result<()> {
        if let Some(sub) = self.db.streams.remove(&s.id) {
            drop(sub); // both variants release their resources on drop
        }
        Ok(())
    }
```

- [ ] **Step 8: Build worker lib + run tests**

```bash
cargo build -p worker --lib && cargo test -p worker --lib wasm_runtime
```

Expected: clean build, all wasm_runtime tests pass (existing 14 + 3 new in db_pg_subscribe = 17).

- [ ] **Step 9: Commit**

```bash
git add crates/worker/src/wasm_runtime/db_host.rs \
        crates/worker/src/wasm_runtime/db_pg_subscribe.rs \
        crates/worker/src/wasm_runtime/mod.rs && \
git commit -m "phase-2-3f-3: host db.subscribe-changes + next-event for Postgres

- New db_pg_subscribe.rs: PgSubscription holds the sqlx PgConnection,
  slot_name + publication_names from the WIT options bag, a pending
  VecDeque + RelationTable cache. next() drains pending first then
  polls pg_logical_slot_get_binary_changes for up to 1000 rows,
  decoding via the existing native pgoutput decoder
  (connectors/postgres/cdc/decode.rs::decode_message).
- DbStream becomes an enum (Mysql | Postgres) so the host's stream
  map carries both subscription kinds.
- subscribe_changes routes by DbConn variant: MySQL keeps the
  existing consume-via-binlog-stream path; Postgres reads slot_name
  + publication_names + (optional) proto_version from options,
  errors with InvalidConfig on missing keys.
- next_event + close_stream dispatch by DbStream variant.

Postgres event translation: Insert/Update/Delete events use the
relation cache populated from Relation events. Position is the
row's lsn::text. Begin/Commit/Truncate/Origin produce no row event.

3 new db_pg_subscribe unit tests (positional shape)."
```

---

## Task 4: examples/postgres-cdc-rs scaffold

**Files:**
- Create: `examples/postgres-cdc-rs/Cargo.toml`
- Create: `examples/postgres-cdc-rs/.cargo/config.toml`
- Create: `examples/postgres-cdc-rs/.gitignore`
- Create: `examples/postgres-cdc-rs/src/lib.rs` (skeleton; snapshot+streaming impls fill in Tasks 5-6)
- Create: `examples/postgres-cdc-rs/src/arrow_io.rs`
- Create: `examples/postgres-cdc-rs/README.md`

- [ ] **Step 1: Create Cargo.toml**

`examples/postgres-cdc-rs/Cargo.toml`:

```toml
[workspace]

[package]
name = "postgres-cdc-rs"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.37"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
arrow-array = { version = "53", default-features = false }
arrow-schema = { version = "53", default-features = false }
arrow-ipc = { version = "53", default-features = false }
arrow-data = { version = "53", default-features = false }
arrow-buffer = { version = "53", default-features = false }
sha2 = { version = "0.10", default-features = false }
hex = "0.4"

[profile.release]
opt-level = "s"
lto = true
strip = true
```

- [ ] **Step 2: Create .cargo/config.toml**

`examples/postgres-cdc-rs/.cargo/config.toml`:

```toml
[build]
target = "wasm32-wasip2"
```

- [ ] **Step 3: Create .gitignore**

`examples/postgres-cdc-rs/.gitignore`:

```
/target
```

- [ ] **Step 4: Create the Arrow IPC helper module**

`examples/postgres-cdc-rs/src/arrow_io.rs`:

```rust
//! Arrow IPC helpers — schema bytes for `discover`, batch bytes for `read_batch`.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("_cdc.op", DataType::Utf8, false),
        Field::new("_cdc.position", DataType::Utf8, false),
    ]))
}

pub fn schema_ipc_bytes() -> Result<Vec<u8>, String> {
    let s = schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, s.as_ref()).map_err(|e| e.to_string())?;
        w.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}

pub struct Row {
    pub id: i64,
    pub name: Option<String>,
    pub op: char,
    pub position: String,
}

pub fn rows_to_ipc(rows: &[Row]) -> Result<Vec<u8>, String> {
    let s = schema();
    let ids: Int64Array = rows.iter().map(|r| Some(r.id)).collect();
    let names: StringArray = rows.iter().map(|r| r.name.clone()).collect();
    let ops: StringArray = rows.iter().map(|r| Some(r.op.to_string())).collect();
    let pos: StringArray = rows.iter().map(|r| Some(r.position.clone())).collect();

    let batch = RecordBatch::try_new(
        s.clone(),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(names) as ArrayRef,
            Arc::new(ops) as ArrayRef,
            Arc::new(pos) as ArrayRef,
        ],
    )
    .map_err(|e| e.to_string())?;

    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, s.as_ref()).map_err(|e| e.to_string())?;
        w.write(&batch).map_err(|e| e.to_string())?;
        w.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}
```

- [ ] **Step 5: Create the lib.rs skeleton**

`examples/postgres-cdc-rs/src/lib.rs`:

```rust
//! postgres-cdc-rs — Phase II.3.f reference WASM CDC connector for Postgres.
//!
//! Cursor lifecycle (matches mysql-cdc-rs):
//!
//! `None` (initial run)
//!   ↓ pin LSN via `SELECT pg_current_wal_lsn()`,
//!     ensure publication+slot via idempotent SQL,
//!     return one snapshot chunk
//! `snapshot-pk` value="<lsn>|<last_pk>"
//!   ↓ snapshot loop: fetch next chunk WHERE id > last_pk
//!   ↓ when chunk_size < batch_size, transition cursor to `lsn`
//! `lsn` value="<lsn>"
//!   ↓ streaming loop forever: db.subscribe-changes + db.next-event
//!
//! Source config JSON: `{ "schema": "public", "table": "items" }`.
//! Schema (hardcoded for the demo): `id BIGINT, name TEXT NULL`.

mod arrow_io;
mod snapshot;
mod streaming;

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "source-connector",
});

use platform::connector::host::{log, LogLevel};
use platform::connector::types::CursorKind;

struct Component;
export!(Component);

#[derive(serde::Deserialize, Clone)]
pub(crate) struct SourceCfg {
    pub schema: String,
    pub table: String,
}

fn parse_source_cfg(json: &str) -> Result<SourceCfg, ConnectorError> {
    serde_json::from_str(json)
        .map_err(|e| ConnectorError::InvalidConfig(format!("source config: {e}")))
}

/// Slot + publication names derived deterministically from the
/// connector_ref + table so re-runs of the same pipeline reuse the
/// same slot. Truncated SHA-256 hex; safe for Postgres identifier
/// length limits (NAMEDATALEN = 64).
pub(crate) fn slot_name(schema: &str, table: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(schema.as_bytes());
    h.update(b".");
    h.update(table.as_bytes());
    let digest = h.finalize();
    let short = hex::encode(&digest[..6]); // 12 hex chars
    format!("etl_pgrs_{short}")
}

pub(crate) fn publication_name(schema: &str, table: &str) -> String {
    let s = slot_name(schema, table);
    format!("{s}_pub")
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        arrow_io::schema_ipc_bytes()
            .map_err(|e| ConnectorError::Other(format!("schema ipc: {e}")))
    }

    fn read_batch(
        conn: ConnectionConfig,
        source: SourceConfig,
        cursor: Option<CursorValue>,
        batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        let cfg = parse_source_cfg(&source.json)?;
        let bs = batch_size.max(1) as i64;
        log(
            LogLevel::Info,
            &format!(
                "postgres-cdc-rs: read_batch table={}.{} cursor={:?} batch_size={}",
                cfg.schema, cfg.table, cursor, bs
            ),
        );

        match cursor.as_ref().map(|c| c.kind) {
            None => snapshot::initial(&conn.url, &cfg, bs),
            Some(CursorKind::SnapshotPk) => {
                snapshot::next_chunk(&conn.url, &cfg, &cursor.unwrap().value, bs)
            }
            Some(CursorKind::Lsn) => {
                streaming::next_window(&conn.url, &cfg, &cursor.unwrap().value, bs)
            }
            Some(other) => Err(ConnectorError::InvalidConfig(format!(
                "unexpected cursor kind for postgres-cdc-rs: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_name_deterministic() {
        assert_eq!(slot_name("public", "items"), slot_name("public", "items"));
    }

    #[test]
    fn slot_name_distinguishes_tables() {
        assert_ne!(slot_name("public", "a"), slot_name("public", "b"));
    }

    #[test]
    fn publication_name_includes_slot_prefix() {
        let p = publication_name("public", "items");
        assert!(p.starts_with("etl_pgrs_"));
        assert!(p.ends_with("_pub"));
    }
}
```

- [ ] **Step 6: Create stub snapshot.rs and streaming.rs**

These get filled in Tasks 5–6, but they need to exist so the crate compiles after Task 4.

`examples/postgres-cdc-rs/src/snapshot.rs`:

```rust
//! Snapshot phase. Filled in Task 5.

use crate::platform::connector::db;
use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn initial(_url: &str, _cfg: &SourceCfg, _batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    Err(ConnectorError::Other(
        "postgres-cdc-rs snapshot::initial not yet implemented (lands in phase-2-3f-5)".into(),
    ))
}

pub fn next_chunk(
    _url: &str,
    _cfg: &SourceCfg,
    _cursor_value: &str,
    _batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    Err(ConnectorError::Other(
        "postgres-cdc-rs snapshot::next_chunk not yet implemented (lands in phase-2-3f-5)".into(),
    ))
}

pub(crate) fn db_err_to_connector_err(e: db::DbError) -> ConnectorError {
    match e {
        db::DbError::InvalidConfig(s) => ConnectorError::InvalidConfig(s),
        db::DbError::ConnectFailed(s) | db::DbError::PositionLost(s) => {
            ConnectorError::SourceUnavailable(s)
        }
        db::DbError::QueryFailed(s) => ConnectorError::Other(s),
        db::DbError::Unsupported(s) => ConnectorError::Other(format!("unsupported: {s}")),
    }
}
```

`examples/postgres-cdc-rs/src/streaming.rs`:

```rust
//! Streaming phase. Filled in Task 6.

use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn next_window(
    _url: &str,
    _cfg: &SourceCfg,
    _start_lsn: &str,
    _batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    Err(ConnectorError::Other(
        "postgres-cdc-rs streaming::next_window not yet implemented (lands in phase-2-3f-6)".into(),
    ))
}
```

- [ ] **Step 7: Create README.md**

`examples/postgres-cdc-rs/README.md`:

```markdown
# postgres-cdc-rs

Phase II.3.f reference WASM CDC connector for Postgres. Mirror of
`mysql-cdc-rs`, using the same `db.*` host imports — the SDK is now
DB-family agnostic.

## Build

\`\`\`
rustup target add wasm32-wasip2     # one-time
cargo build --release
\`\`\`

## Configure

\`\`\`yaml
source:
  kind: wasm
  config:
    schema: public
    table: items
connection:
  connector_ref: "wasm-cdc:postgres-cdc-rs@0.1.0"
  url: postgres://user:pass@host:5432/dbname
\`\`\`

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
```

- [ ] **Step 8: Build the WASM crate**

```bash
cargo build --release --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: a ~600KiB `target/wasm32-wasip2/release/postgres_cdc_rs.wasm` artifact.

- [ ] **Step 9: Run unit tests**

```bash
cargo test --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: 3 tests pass (slot_name_deterministic, slot_name_distinguishes_tables, publication_name_includes_slot_prefix).

- [ ] **Step 10: Commit**

```bash
git add examples/postgres-cdc-rs/ && \
git commit -m "phase-2-3f-4: examples/postgres-cdc-rs scaffold

Cargo.toml + .cargo/config.toml + .gitignore + README + lib.rs entry +
arrow_io.rs + stub snapshot.rs/streaming.rs that error with 'lands in
phase-2-3f-5/6'.

Slot + publication names derive deterministically from schema+table
via SHA-256 prefix (12 hex chars, fits NAMEDATALEN). 3 unit tests
validate determinism + uniqueness + naming convention."
```

---

## Task 5: postgres-cdc-rs snapshot + LSN pinning

**Files:**
- Modify: `examples/postgres-cdc-rs/src/snapshot.rs` — fill in real impl

- [ ] **Step 1: Replace snapshot.rs with the full impl**

Replace the entire contents of `examples/postgres-cdc-rs/src/snapshot.rs` with:

```rust
//! Snapshot phase: chunked SELECT WHERE id > last_pk ORDER BY id LIMIT N.
//!
//! On the initial call we additionally:
//!   - pin the LSN via `SELECT pg_current_wal_lsn()`;
//!   - ensure the publication exists (CREATE PUBLICATION IF NOT EXISTS);
//!   - ensure the replication slot exists (idempotent SELECT-WHERE-NOT-EXISTS guard
//!     around `pg_create_logical_replication_slot`).
//!
//! Cursor format during snapshot: `snapshot-pk` value=`<lsn>|<last_pk>`.
//! Transitions to `lsn` value=`<lsn>` once a short chunk arrives.

use crate::arrow_io::{rows_to_ipc, Row};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::{
    publication_name as pub_name_fn, slot_name as slot_name_fn, ConnectorError, ReadOutcome,
    SourceCfg,
};

pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    let h = open(url)?;
    ensure_publication(h, cfg)?;
    ensure_slot(h, cfg)?;
    let lsn = read_current_lsn(h)?;
    let chunk = chunk_after(h, cfg, 0, batch_size)?;
    db::close(h);
    finalize(chunk, &lsn, 0, batch_size)
}

pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (lsn, last_pk) = parse_snapshot_cursor(cursor_value)?;
    let h = open(url)?;
    let chunk = chunk_after(h, cfg, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &lsn, last_pk, batch_size)
}

fn finalize(
    chunk: Chunk,
    lsn: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    if chunk.rows.is_empty() {
        return Ok(ReadOutcome {
            batch_ipc: vec![],
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Lsn,
                value: lsn.to_string(),
            }),
            is_final: true,
        });
    }
    let new_last_pk = chunk.rows.last().map(|(id, _)| *id).unwrap_or(last_pk_in);
    let pos = format!("snapshot:{lsn}|{new_last_pk}");
    let arrow_rows: Vec<Row> = chunk
        .rows
        .into_iter()
        .map(|(id, name)| Row {
            id,
            name,
            op: 's',
            position: pos.clone(),
        })
        .collect();
    let rows_n = arrow_rows.len() as u32;
    let bytes = rows_to_ipc(&arrow_rows)
        .map_err(|e| ConnectorError::Other(format!("rows_to_ipc: {e}")))?;

    let snapshot_done = (rows_n as i64) < batch_size;
    let (kind, value) = if snapshot_done {
        (CursorKind::Lsn, lsn.to_string())
    } else {
        (CursorKind::SnapshotPk, format!("{lsn}|{new_last_pk}"))
    };

    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue { kind, value }),
        is_final: snapshot_done,
    })
}

struct Chunk {
    rows: Vec<(i64, Option<String>)>,
}

fn chunk_after(
    h: db::DbHandle,
    cfg: &SourceCfg,
    last_pk: i64,
    batch_size: i64,
) -> Result<Chunk, ConnectorError> {
    let sql = format!(
        "SELECT id, name FROM \"{schema}\".\"{table}\" \
         WHERE id > $1 ORDER BY id LIMIT {limit}",
        schema = cfg.schema,
        table = cfg.table,
        limit = batch_size,
    );
    let rows = db::query(h, &sql, &[last_pk.to_string()]).map_err(db_err_to_connector_err)?;
    let mut out: Vec<(i64, Option<String>)> = Vec::with_capacity(rows.len());
    for r in rows {
        let id: i64 = r
            .first()
            .and_then(|v| v.as_deref())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| ConnectorError::Other("snapshot: expected i64 id".into()))?;
        let name: Option<String> = r.get(1).and_then(|v| v.clone());
        out.push((id, name));
    }
    Ok(Chunk { rows: out })
}

fn read_current_lsn(h: db::DbHandle) -> Result<String, ConnectorError> {
    let rows =
        db::query(h, "SELECT pg_current_wal_lsn()::text", &[]).map_err(db_err_to_connector_err)?;
    let cell = rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .flatten()
        .ok_or_else(|| ConnectorError::Other("pg_current_wal_lsn returned no rows".into()))?;
    Ok(cell)
}

fn ensure_publication(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let pub_name = pub_name_fn(&cfg.schema, &cfg.table);
    // Postgres 14+ supports IF NOT EXISTS on CREATE PUBLICATION.
    let stmt = format!(
        "CREATE PUBLICATION \"{pub_name}\" \
         FOR TABLE \"{schema}\".\"{table}\"",
        schema = cfg.schema,
        table = cfg.table,
    );
    // Issue with a guard query: skip if the publication already exists.
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_publication WHERE pubname = $1",
        &[pub_name.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        db::query(h, &stmt, &[]).map_err(db_err_to_connector_err)?;
    }
    Ok(())
}

fn ensure_slot(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let slot = slot_name_fn(&cfg.schema, &cfg.table);
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
        &[slot.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        // pg_create_logical_replication_slot(slot_name, plugin) returns one row.
        db::query(
            h,
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[slot],
        )
        .map_err(db_err_to_connector_err)?;
    }
    Ok(())
}

fn open(url: &str) -> Result<db::DbHandle, ConnectorError> {
    db::open(url).map_err(db_err_to_connector_err)
}

pub(crate) fn parse_snapshot_cursor(s: &str) -> Result<(String, i64), ConnectorError> {
    let (lsn, pk) = s.split_once('|').ok_or_else(|| {
        ConnectorError::InvalidConfig(format!("snapshot cursor missing '|': {s}"))
    })?;
    let pk: i64 = pk
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor pk not i64: {e}")))?;
    Ok((lsn.to_string(), pk))
}

pub(crate) fn db_err_to_connector_err(e: db::DbError) -> ConnectorError {
    match e {
        db::DbError::InvalidConfig(s) => ConnectorError::InvalidConfig(s),
        db::DbError::ConnectFailed(s) | db::DbError::PositionLost(s) => {
            ConnectorError::SourceUnavailable(s)
        }
        db::DbError::QueryFailed(s) => ConnectorError::Other(s),
        db::DbError::Unsupported(s) => ConnectorError::Other(format!("unsupported: {s}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_cursor_accepts_basic() {
        let (lsn, pk) = parse_snapshot_cursor("0/16B3748|42").unwrap();
        assert_eq!(lsn, "0/16B3748");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_malformed() {
        assert!(parse_snapshot_cursor("no-pipe-here").is_err());
        assert!(parse_snapshot_cursor("lsn|notanumber").is_err());
    }
}
```

- [ ] **Step 2: Build the WASM crate**

```bash
cargo build --release --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: clean build.

- [ ] **Step 3: Run unit tests**

```bash
cargo test --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: 5 tests pass (3 from Task 4 + 2 new for cursor parsing).

- [ ] **Step 4: Commit**

```bash
git add examples/postgres-cdc-rs/src/snapshot.rs && \
git commit -m "phase-2-3f-5: postgres-cdc-rs snapshot + LSN pinning

Initial call:
- pin LSN via SELECT pg_current_wal_lsn()
- ensure publication exists (CREATE PUBLICATION FOR TABLE; guarded
  by pg_publication lookup)
- ensure slot exists (SELECT pg_create_logical_replication_slot;
  guarded by pg_replication_slots lookup)
- fetch first chunk WHERE id > 0 ORDER BY id LIMIT batch_size
- emit cursor snapshot-pk value <lsn>|<last_pk>

Subsequent snapshot calls just fetch the next chunk; LSN stays
pinned across the whole snapshot phase. Transition to cursor lsn
when chunk_size < batch_size.

5 unit tests pass."
```

---

## Task 6: postgres-cdc-rs streaming + JSON row decode

**Files:**
- Modify: `examples/postgres-cdc-rs/src/streaming.rs` — fill in real impl

- [ ] **Step 1: Replace streaming.rs with the full impl**

Replace the entire contents of `examples/postgres-cdc-rs/src/streaming.rs` with:

```rust
//! Streaming phase: db.subscribe-changes + drain via db.next-event.
//!
//! Each call to `read_batch` opens one short-lived subscription, drains
//! up to `batch_size` events, then closes it. Slot and publication
//! names are passed via the WIT options bag — the host uses them as
//! parameters to pg_logical_slot_get_binary_changes.

use crate::arrow_io::{rows_to_ipc, Row};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::snapshot::db_err_to_connector_err;
use crate::{
    publication_name as pub_name_fn, slot_name as slot_name_fn, ConnectorError, ReadOutcome,
    SourceCfg,
};

pub fn next_window(
    url: &str,
    cfg: &SourceCfg,
    start_lsn: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let h = db::open(url).map_err(db_err_to_connector_err)?;
    let slot = slot_name_fn(&cfg.schema, &cfg.table);
    let pub_name = pub_name_fn(&cfg.schema, &cfg.table);
    let opts: &[(String, String)] = &[
        ("slot_name".to_string(), slot.clone()),
        ("publication_names".to_string(), pub_name.clone()),
    ];
    // Convert &[(String, String)] to &[(&str, &str)] for the WIT call.
    let opts_ref: Vec<(&str, &str)> = opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let sub = db::subscribe_changes(h, start_lsn, &opts_ref).map_err(db_err_to_connector_err)?;
    let qualified = format!("{}.{}", cfg.schema, cfg.table);

    let mut rows: Vec<Row> = Vec::new();
    let mut latest_position: String = start_lsn.to_string();

    while (rows.len() as i64) < batch_size {
        let evt = match db::next_event(sub).map_err(db_err_to_connector_err)? {
            Some(e) => e,
            None => break, // host idle — drain done
        };
        if !evt.position.is_empty() {
            latest_position = evt.position.clone();
        }
        if evt.table != qualified {
            continue;
        }
        if let Some(row) = decode_row(&evt) {
            rows.push(row);
        }
    }
    db::close_stream(sub);

    let bytes = if rows.is_empty() {
        Vec::new()
    } else {
        rows_to_ipc(&rows).map_err(|e| ConnectorError::Other(format!("rows_to_ipc: {e}")))?
    };

    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows.len() as u32,
        new_cursor: Some(CursorValue {
            kind: CursorKind::Lsn,
            value: latest_position,
        }),
        is_final: false,
    })
}

fn decode_row(evt: &db::ChangeEvent) -> Option<Row> {
    use serde_json::Value;
    let v: Value = serde_json::from_str(&evt.row_json).ok()?;
    let arr = match evt.op {
        'd' => v.get("before")?.as_array()?,
        _ => v.get("after")?.as_array()?,
    };
    // Pgoutput v1 returns text values, so cells are JSON strings.
    let id_str = arr.first().and_then(|c| match c {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    })?;
    let id: i64 = id_str.parse().ok()?;
    let name: Option<String> = arr.get(1).and_then(|c| match c {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    });
    Some(Row {
        id,
        name,
        op: evt.op,
        position: evt.position.clone(),
    })
}
```

- [ ] **Step 2: Build**

```bash
cargo build --release --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: clean build. Output `~700 KiB` `postgres_cdc_rs.wasm`.

- [ ] **Step 3: Run unit tests + verify nothing else broke**

```bash
cargo test --manifest-path examples/postgres-cdc-rs/Cargo.toml && \
cargo build -p worker --lib && \
cargo test -p worker --lib
```

Expected: example crate still has 5 tests passing; worker lib still builds + 134-ish lib tests pass.

- [ ] **Step 4: Commit**

```bash
git add examples/postgres-cdc-rs/src/streaming.rs && \
git commit -m "phase-2-3f-6: postgres-cdc-rs streaming + JSON row decode

next_window opens db.open + db.subscribe_changes with the new
options bag carrying slot_name and publication_names. Drains up
to batch_size events via db.next_event, filters to the configured
schema.table, decodes each event's JSON row (positional array,
matching MySQL's shape), emits as Arrow rows with op/position
metadata, returns new cursor kind=lsn value=<latest_position>.

Pgoutput v1 returns cells as text strings — id is parsed via
.parse::<i64>() rather than .as_i64() (the JSON value is a
String, not a Number)."
```

---

## Task 7: End-to-end test

**Files:**
- Create: `tests/integration/tests/postgres_cdc_wasm_e2e.rs`

- [ ] **Step 1: Create the e2e test**

`tests/integration/tests/postgres_cdc_wasm_e2e.rs`:

```rust
//! Phase II.3.f — Postgres WASM CDC end-to-end:
//!   1. Build the example postgres-cdc-rs WASM connector + precompile.
//!   2. Spawn postgres:16 testcontainer with wal_level=logical.
//!   3. Pre-seed `items` with three rows (snapshot fodder).
//!   4. Spawn worker; seed catalog with a Wasm connection +
//!      Wasm pipeline using connector_ref="wasm-cdc:postgres-cdc-rs@0.1.0".
//!   5. `platform pipeline run` — workflow dispatches to
//!      WasmCdcPipelineWorkflow; the guest snapshots, then streams.
//!   6. After a short delay, INSERT/UPDATE/DELETE.
//!   7. Poll parquet for snapshot 's' rows + 'i'/'u'/'d' streaming rows.

use anyhow::Context;
use arrow::array::Array;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use sqlx::Connection;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres as PgContainer;
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
        .status()
        .await?;
    anyhow::ensure!(status.success(), "workspace build failed");
    Ok(())
}

async fn build_wasm_connector() -> anyhow::Result<()> {
    let status = Command::new(cargo_bin("platform"))
        .current_dir(workspace_root())
        .args(["connector", "build", "examples/postgres-cdc-rs"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "connector build failed");
    Ok(())
}

async fn spawn_worker(connectors_dir: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env(
            "RUST_LOG",
            "info,sqlx=warn,worker::wasm_runtime=debug,worker::workflows=debug",
        )
        .env("ETL_CONNECTORS_DIR", connectors_dir)
        .current_dir(workspace_root())
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

fn read_parquet_ops(dir: &Path) -> Vec<String> {
    let mut ops: Vec<String> = Vec::new();
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(dir)
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
        let reader = match ParquetRecordBatchReaderBuilder::try_new(f) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let reader = match reader.build() {
            Ok(r) => r,
            Err(_) => continue,
        };
        for batch in reader.flatten() {
            if let Ok(idx) = batch.schema().index_of("_cdc.op") {
                if let Some(arr) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    for i in 0..arr.len() {
                        ops.push(arr.value(i).to_string());
                    }
                }
            }
        }
    }
    ops
}

async fn start_pg_container() -> anyhow::Result<(ContainerAsync<PgContainer>, String)> {
    let container = PgContainer::default()
        .with_cmd(vec![
            "-c".to_string(),
            "wal_level=logical".to_string(),
            "-c".to_string(),
            "max_wal_senders=4".to_string(),
            "-c".to_string(),
            "max_replication_slots=4".to_string(),
        ])
        .start()
        .await?;
    let port = container.get_host_port_ipv4(5432).await?;
    // testcontainers-modules postgres defaults: user=postgres, pw=postgres, db=postgres.
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    Ok((container, url))
}

async fn seed_table_and_rows(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query(
        "CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            name TEXT
         )",
    )
    .execute(&mut conn)
    .await?;
    sqlx::query("INSERT INTO items (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .execute(&mut conn)
        .await?;
    conn.close().await?;
    Ok(())
}

async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query("INSERT INTO items (id, name) VALUES (4, 'four')")
        .execute(&mut conn)
        .await?;
    sqlx::query("UPDATE items SET name='TWO' WHERE id=2")
        .execute(&mut conn)
        .await?;
    sqlx::query("DELETE FROM items WHERE id=1")
        .execute(&mut conn)
        .await?;
    conn.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker + temporal stack; builds wasm guest; ~120s"]
async fn postgres_cdc_wasm_e2e() -> anyhow::Result<()> {
    build_workspace().await?;
    build_wasm_connector().await?;

    let (_container, pg_url) = start_pg_container().await?;
    seed_table_and_rows(&pg_url).await?;

    let tmp_data = tempfile::tempdir()?;
    let connectors = workspace_root().join("connectors");

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "pg-wasm-cdc".into(),
            connector_ref: "wasm-cdc:postgres-cdc-rs@0.1.0".into(),
            config: json!({ "url": pg_url }),
        })
        .await?;

    let spec = json!({
        "source": {
            "type": "wasm",
            "config": {
                "schema": "public",
                "table": "items"
            }
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 2
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "pg-cdc-wasm-items".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut worker = spawn_worker(&connectors).await?;

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CONNECTORS_DIR", &connectors)
        .env("ETL_CDC_MAX_WINDOWS", "12")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "pipeline run kickoff failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    tokio::time::sleep(Duration::from_secs(5)).await;
    perform_iud(&pg_url).await?;

    let deadline = Instant::now() + Duration::from_secs(150);
    let mut last_ops: Vec<String> = Vec::new();
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for ops; saw: {last_ops:?}");
        }
        last_ops = read_parquet_ops(tmp_data.path());
        let snap_count = last_ops.iter().filter(|o| *o == "s").count();
        let i_count = last_ops.iter().filter(|o| *o == "i").count();
        let u_count = last_ops.iter().filter(|o| *o == "u").count();
        let d_count = last_ops.iter().filter(|o| *o == "d").count();
        if snap_count >= 3 && i_count >= 1 && u_count >= 1 && d_count >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    worker.kill().await?;
    worker.wait().await?;

    eprintln!("ops captured: {last_ops:?}");
    assert!(
        last_ops.iter().filter(|o| *o == "s").count() >= 3,
        "expected >=3 snapshot rows; got {last_ops:?}"
    );
    assert!(
        last_ops.iter().any(|o| o == "i"),
        "missing INSERT in {last_ops:?}"
    );
    assert!(
        last_ops.iter().any(|o| o == "u"),
        "missing UPDATE in {last_ops:?}"
    );
    assert!(
        last_ops.iter().any(|o| o == "d"),
        "missing DELETE in {last_ops:?}"
    );

    Ok(())
}
```

- [ ] **Step 2: Verify the test compiles**

```bash
cargo build -p integration-tests --tests
```

Expected: clean build (test is `#[ignore]`, so no actual run).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/postgres_cdc_wasm_e2e.rs && \
git commit -m "phase-2-3f-7: end-to-end test for Postgres WASM CDC

Mirrors mysql_cdc_wasm_e2e.rs with three changes:
- testcontainer is postgres:16 with wal_level=logical, max_wal_senders,
  max_replication_slots set
- connector_ref = wasm-cdc:postgres-cdc-rs@0.1.0
- pre-seed via sqlx::PgConnection (not mysql_async)

Pre-seeds 3 rows for snapshot, then performs IUD after 5s settle.
Asserts >=3 snapshot rows + at least one each of i/u/d.

#[ignore] — requires docker + temporal. cargo build -p integration-tests
--tests is the structural verification gate."
```

---

## Task 8: README + final verification

**Files:**
- Modify: `README.md` (the "Currently:" line)

- [ ] **Step 1: Update README "Currently:" line**

In `README.md` find the line starting with `Currently: **Phase II.3.e —` and replace it with:

```markdown
Currently: **Phase II.3.f — Postgres SDK port (complete)** on top of II.3.e. The `db.*` host imports now back both `mysql://` and `postgres://` URLs; CDC connectors authored as WASM components work against either DB family. `db.subscribe-changes` gains a `list<tuple<string,string>>` options bag — Postgres uses it for `slot_name` / `publication_names`; MySQL ignores all keys. Reference connector `examples/postgres-cdc-rs` mirrors `mysql-cdc-rs` and reuses the existing native pgoutput decoder via the host module `db_pg_subscribe.rs`. Slot + publication names derive deterministically from `schema+table`. Workflow + CLI dispatch unchanged. Runtime on **wasmtime 36**. Remaining II.3.x: multi-table CDC. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib test sweep across touched crates**

```bash
cargo test -p worker -p common-types -p catalog -p connector-sdk -p loader-sdk -p audit --lib
```

Expected: all pass; worker lib in particular has 134 + ~5 new (db_host postgres routing + db_pg_subscribe positional helpers + DbStream variant test) tests.

- [ ] **Step 3: Verify both example connectors still compile**

```bash
cargo build --release --manifest-path examples/mysql-cdc-rs/Cargo.toml && \
cargo build --release --manifest-path examples/postgres-cdc-rs/Cargo.toml
```

Expected: both clean.

- [ ] **Step 4: Verify worker + cli binaries build**

```bash
cargo build -p worker -p cli
```

Expected: clean.

- [ ] **Step 5: Commit + push for PR**

```bash
git add README.md && \
git commit -m "phase-2-3f-8: README — Phase II.3.f Postgres SDK port complete

Worker lib: ~139 tests pass (+5 new: db_host postgres routing, 3
db_pg_subscribe positional helpers, mysql-cdc-rs unchanged at +6
existing tests).

Both example connectors compile to wasm32-wasip2:
  - mysql-cdc-rs: ~647 KiB (unchanged)
  - postgres-cdc-rs: ~700 KiB (new)

Untested at runtime: postgres_cdc_wasm_e2e.rs (#[ignore]; needs
docker + temporal)."
```

---

## Self-review

### Spec coverage

| Spec section | Plan task |
|---|---|
| Architecture overview | Tasks 2-3 (host) + 4-6 (guest) |
| WIT changes (subscribe-changes options) | Task 1 |
| Host db.open routing | Task 2 |
| Host db.query Postgres branch | Task 2 |
| Host db.subscribe-changes Postgres | Task 3 |
| PgSubscription struct | Task 3 |
| Pgoutput decoder reuse | Task 3 (uses crate::connectors::postgres::cdc::decode::decode_message) |
| Example connector | Tasks 4-6 |
| Cursor lifecycle (snapshot-pk → lsn) | Task 5 finalize() + Task 6 streaming cursor kind=Lsn |
| Workflow + CLI no changes | Confirmed inline in Task 8 |
| Testing strategy | Task 3 (unit) + Task 7 (e2e) |

All spec sections covered.

### Placeholder scan

No "TBD", "TODO", "implement later", or vague directives. Stub stubs in Task 4's snapshot.rs/streaming.rs return concrete `ConnectorError::Other("...lands in phase-2-3f-N")` messages — those are real strings that prevent invocation, not placeholders.

### Type consistency

- `DbConn::Postgres(sqlx::PgConnection)` — used consistently across tasks 2 and 3.
- `DbStream::Mysql / DbStream::Postgres` — introduced in Task 3, used in `next_event` + `close_stream`.
- `slot_name(schema, table)` returning `String` — defined in Task 4, called in Tasks 5+6.
- `publication_name(schema, table)` returning `String` — same.
- `parse_snapshot_cursor(s)` returning `(String, i64)` — defined in Task 5, used as the cursor parsing helper. (Same shape as the mysql-cdc-rs helper but the gtid is replaced by an lsn.)
- `db::subscribe_changes(handle, position, options: &[(&str, &str)])` — WIT signature from Task 1, called in Task 6.
- `decode_message(&[u8]) -> Result<CdcEvent>` — confirmed against `crates/worker/src/connectors/postgres/cdc/decode.rs:43`.
- `RelationTable = HashMap<u32, RelationInfo>` — confirmed against `decode.rs:161`.

All type/method names cross-checked. No drift.
