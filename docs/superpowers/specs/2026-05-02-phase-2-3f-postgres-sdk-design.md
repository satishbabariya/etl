# Phase II.3.f — Postgres SDK Port — Design Spec

> **Status:** Draft 2026-05-02. Approved by agent (user delegated all design calls). Predecessor: `2026-05-02-phase-2-3e-cdc-sdk-design.md` (MySQL SDK lift, merged at 41a55ca).

## Goal

Add Postgres host backing for the same `db.*` WIT interface that II.3.e introduced for MySQL. Close the explicit `db-error::unsupported` gate at `crates/worker/src/wasm_runtime/db_host.rs:79` and ship a reference WASM connector `examples/postgres-cdc-rs` that snapshots and streams a Postgres table via the SDK.

## Non-goals

- **Multi-table CDC.** One table per connector instance, mirroring MySQL parity. Multi-table is Phase II.3.g.
- **Streaming replication protocol (`COPY BOTH`).** The native CDC polls via `pg_logical_slot_get_binary_changes`; we mirror that. tokio-postgres-0.7's lack of streaming-protocol support drove the native choice and stays in force here.
- **Heartbeat / keepalive frames.** The native CDC doesn't emit them; we don't either.
- **Authoring in TypeScript.** Pgoutput decode requires byte-level work; Rust only for v1.

---

## Architecture overview

```
guest (postgres-cdc-rs.wasm)              host (worker)
─────────────────────────────              ────────────
discover → schema bytes
read-batch
  ├─ db.open(postgres://...)         ──▶  sqlx::PgConnection ↦ DbConn::Postgres
  ├─ db.query("SELECT pg_current_wal_lsn()", []) ──▶ pin LSN
  ├─ db.query("CREATE PUBLICATION...", [])      ──▶ idempotent setup
  ├─ db.query("SELECT pg_create_logical_replication_slot(...)", []) ──▶ slot
  ├─ db.query("SELECT id,name FROM... WHERE id>$1 LIMIT $2", [...]) ──▶ snapshot chunk
  └─ when snapshot is_final, return cursor-kind=lsn

next batch:
  ├─ db.open(...)
  ├─ db.subscribe-changes(h, "<lsn>", [("slot_name","..."), ("publication_names","...")])
  └─ db.next-event() x N
        host: SELECT lsn::text, data FROM pg_logical_slot_get_binary_changes(...)
              decode pgoutput bytes (reuse native decoder)
              push ChangeEvent rows into pending VecDeque
              return one ChangeEvent at a time
```

Postgres uses the same `WasmCdcPipelineWorkflow` and the same `wasm-cdc:` CLI prefix as MySQL. Workflow code is unchanged.

---

## WIT changes

`crates/connector-sdk/wit/db.wit` — extend `subscribe-changes` with an `options` parameter:

```wit
/// Subscribe to change events from `position`. The `options` list is
/// a free-form key-value bag passed to the host — Postgres uses it
/// for slot_name / publication_names / proto_version; MySQL ignores
/// unknown keys (server_id is recognized but currently overridden by
/// host-side allocation).
subscribe-changes: func(
    h: db-handle,
    position: string,
    options: list<tuple<string, string>>,
) -> result<change-stream, db-error>;
```

This is a breaking source-level change to one existing connector (`examples/mysql-cdc-rs`), which gets one extra `&[]` argument. The bindgen regeneration is symmetric with II.3.e.

No `cursor-kind` changes — we already have `lsn`, `snapshot-pk`. No new variants.

No new WIT verbs. No `db.execute` / `db.ensure-slot` — guests issue DDL via `db.query` exactly as native code does.

---

## Host implementation

### `DbConn` enum extension

`crates/worker/src/wasm_runtime/db_host.rs`:

```rust
pub(super) enum DbConn {
    Mysql(mysql_async::Conn),
    Postgres(sqlx::PgConnection),
    Consumed, // mysql-only marker; Postgres never sets this
}
```

### `db.open` URL routing

```rust
if url.starts_with("mysql://") {
    Conn::from_url(&url).await ↦ DbConn::Mysql
} else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
    PgConnection::connect(&url).await ↦ DbConn::Postgres
} else {
    DbError::InvalidConfig
}
```

### `db.query` dispatch

The existing MySQL path stays. Add a Postgres branch using `sqlx::query_with` (positional `$1`/`$2` params bound from the WIT `list<string>`). Each cell rendered to `Option<String>`:
- NULL → None
- Otherwise stringify via the column's text-output `Display`. Postgres returns text by default for `query`; we keep the contract "host treats results as text".

Approach: `sqlx::query(&sql).bind(&p1).bind(&p2)...fetch_all(&mut conn)` then iterate `PgRow` with `try_get::<Option<String>, _>(idx)`.

### `db.subscribe-changes` for Postgres

The connector pre-creates the slot+publication via `db.query`. `subscribe-changes` itself just records the bookkeeping the host needs for `next-event`. Steps:

1. Take the conn out of the handle map (we keep using it inside the subscription).
2. Parse `options` for required keys: `slot_name`, `publication_names` (comma-separated). `proto_version` defaults to `"1"`.
3. Allocate a stream id, store a `PgSubscription` (new struct in `db_pg_subscribe.rs`).
4. Postgres does NOT mark the handle as `Consumed` — the connection lives inside the subscription, so the WIT-level invariant ("don't `query` after `subscribe-changes`") is preserved by removal from the conns map. (Same effect as `Consumed`, different mechanism.)

### `PgSubscription` and `next-event`

New file `crates/worker/src/wasm_runtime/db_pg_subscribe.rs`:

```rust
pub struct PgSubscription {
    pub(super) conn: sqlx::PgConnection,
    pub(super) slot_name: String,
    pub(super) publication_names: String,
    pub(super) proto_version: String,         // default "1"
    pub(super) pending: VecDeque<ChangeEvent>,
    pub(super) relations: RelationTable,      // reused from native decoder
    pub(super) current_position: String,
    pub(super) idle_timeout: Duration,        // 5s default
    pub(super) max_per_poll: usize,           // 1000 default
}
```

`PgSubscription::next` drains `pending` first; on empty, runs:

```sql
SELECT lsn::text, data
FROM pg_logical_slot_get_binary_changes($1, NULL, $2,
    'proto_version', '1',
    'publication_names', $3)
```

…with `$1 = slot_name`, `$2 = max_per_poll`, `$3 = publication_names`. For each row:

1. Decode `data` (binary pgoutput) by calling into the existing `crates/worker/src/connectors/postgres/cdc/decode.rs` — specifically the `decode_message(&[u8], &mut RelationTable) -> Option<CdcEvent>` helper.
2. Filter to `Insert`/`Update`/`Delete` (skip `Begin`/`Commit`/`Truncate`/`Origin`; `Relation` updates the cache and is consumed silently).
3. Translate `CdcEvent` → `ChangeEvent`:
   - `op` = `'i'` / `'u'` / `'d'`
   - `position` = the row's `lsn::text` from the SQL result
   - `commit_ts` = 0 (Begin's commit_ts is in the surrounding transaction; for v1 we accept "0 unless source provides")
   - `txid` = 0
   - `table` = `format!("{}.{}", relation.namespace, relation.name)`
   - `row_json` = `serde_json::Value::Object` with `before` and/or `after` keyed to positional `Vec<Option<String>>` (matches MySQL shape)
4. Push into `pending`.

If the SQL returns 0 rows, return `Ok(None)` — same idle semantic as MySQL.

### `db.close-stream` for Postgres

Drop the `PgSubscription`. The slot is *not* dropped — slots are persistent and the next workflow run (or a hot restart) reuses them. The connector or its operator owns slot cleanup as a separate concern. (This matches native CDC behavior.)

### `db.close` for non-MySQL

`PgConnection` doesn't need an explicit close call (sqlx handles it on drop). Just remove from map.

### Reusing the native pgoutput decoder

`crates/worker/src/wasm_runtime/db_pg_subscribe.rs` imports:

```rust
use crate::connectors::postgres::cdc::decode::{
    decode_message, CdcEvent, RelationTable, RelationInfo,
};
```

These types/functions already exist and are public. No native code changes needed.

---

## Example connector: `examples/postgres-cdc-rs`

Mirror of `examples/mysql-cdc-rs` with three semantic differences:

1. **Initial LSN pin**: `SELECT pg_current_wal_lsn()::text` instead of `SELECT @@gtid_executed`.
2. **Slot+publication setup on the *initial* call only** (cursor = `None`). Idempotent SQL:
   ```sql
   -- in transaction:
   SELECT pg_create_logical_replication_slot($slot, 'pgoutput')
     WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $slot);
   CREATE PUBLICATION $pub FOR TABLE $schema.$table; -- swallow "already exists"
   ```
   Slot name: `etl_pgrs_<connector_ref_hash>`. Publication name: `etl_pgrs_pub_<connector_ref_hash>`.
3. **Streaming**: `db::subscribe_changes(h, &lsn, &[("slot_name", &slot), ("publication_names", &pub)])`.

Cursor flow:
- `None` → run setup SQL + pin LSN + first snapshot chunk → cursor `snapshot-pk` with `<lsn>|<last_pk>`.
- `snapshot-pk` → next chunk → either advance `<lsn>|<new_last_pk>` or transition to `lsn` value `<lsn>` when chunk is short.
- `lsn` → streaming: open + subscribe + drain.

Schema (hardcoded for the demo, same as mysql-cdc-rs): `id BIGINT, name TEXT NULL`. Arrow output schema identical: `id`, `name`, `_cdc.op`, `_cdc.position`.

---

## Cursor lifecycle table

| State | cursor-kind | value | host action |
|-------|-------------|-------|-------------|
| Initial | None | — | guest pins LSN, ensures slot+publication, returns first snapshot chunk |
| Snapshotting | `snapshot-pk` | `<lsn>\|<last_pk>` | guest fetches next chunk; same LSN reused (slot retention guarantees we can rewind) |
| Snapshot done | `lsn` | `<lsn>` | transition; first streaming call enters next branch |
| Streaming | `lsn` | `<lsn>` | guest opens db.subscribe-changes; host drains slot |

The catalog persists this via the existing `stream_state.cursor` JSON; no migrations needed.

---

## Workflow + CLI

No changes. `WasmCdcPipelineWorkflow` already handles long-lived sleep-on-empty loops and routes via `wasm-cdc:` prefix. The Postgres connector lives under the same prefix as `wasm-cdc:postgres-cdc-rs@0.1.0`.

---

## Testing strategy

### Unit (host)

- `db_host`: postgres URL routing test (asserting `DbConn::Postgres` is created).
- `db_pg_subscribe`: `decode_event_to_change_event` translation tests using fixture pgoutput bytes (steal from native CDC test fixtures if present; otherwise hand-craft 2-3 representative `Insert`/`Update`/`Delete` byte sequences).
- `db_pg_subscribe`: idle-empty returns `Ok(None)` test using a mock connection.

### Unit (example connector)

- `parse_snapshot_cursor` (already in `mysql-cdc-rs/snapshot.rs`; copy to `postgres-cdc-rs/snapshot.rs`).
- `slot_name_from_ref`: hash-based name generation is deterministic.

### Integration (e2e, `#[ignore]`)

`tests/integration/tests/postgres_cdc_wasm_e2e.rs`:

- Postgres testcontainer with `wal_level=logical`, `max_wal_senders=4`, `max_replication_slots=4`.
- Pre-seed 3 rows in `items` (snapshot fodder).
- Run pipeline with `connector_ref="wasm-cdc:postgres-cdc-rs@0.1.0"`.
- After 5s, INSERT/UPDATE/DELETE.
- Assert parquet has ≥3 rows with `_cdc.op="s"`, plus at least one each of `i`/`u`/`d`.

Mirrors `mysql_cdc_wasm_e2e.rs` byte-for-byte except testcontainer + connector ref.

---

## Build sequence (eight tasks)

1. **WIT extension** — add `options` parameter to `subscribe-changes`. Update mysql-cdc-rs (1 line) and host signatures.
2. **Host db.open + db.query for Postgres** — `DbConn::Postgres` variant, sqlx-backed query path, URL routing.
3. **Host db.subscribe-changes + next-event for Postgres** — `db_pg_subscribe.rs` module, native-decoder reuse, pending buffer.
4. **examples/postgres-cdc-rs scaffold** — Cargo.toml, src/{lib,arrow_io,snapshot,streaming}.rs, slot+publication setup SQL.
5. **postgres-cdc-rs snapshot + LSN pinning** — pg_current_wal_lsn, snapshot chunk SELECT.
6. **postgres-cdc-rs streaming** — subscribe-changes call with options, JSON row decode (mirror MySQL).
7. **e2e test** — `postgres_cdc_wasm_e2e.rs` (`#[ignore]`).
8. **README + final verification** — update Currently line, lib test sweep.

Each task ends with a commit `phase-2-3f-N: <description>` matching the II.3.e cadence.

---

## Open concerns

1. **`pg_logical_slot_get_binary_changes` advances the slot on every successful poll.** That means once we drain N events, we *cannot* re-read them. If `next-event` decodes successfully but pushing into pending fails (e.g. the activity gets terminated mid-loop), those events are lost. The existing native CDC has the same property and accepts it; downstream Temporal at-least-once semantics catch most of this. Acceptable for v1; flag for II.3.x revisit.

2. **Slot ownership across connector reinstalls.** If the operator destroys and recreates a pipeline with the same connector_ref, the slot persists and accumulates WAL. Phase II.3.x or II.4 should add a `platform pipeline destroy` cleanup hook. Out of scope for II.3.f.

3. **Pgoutput proto_version=1 vs 2.** v2 adds streaming-of-large-transactions support; v1 is what the native code uses. We pin v1. If the user has ≥10MB transactions, the slot blocks until commit — same constraint as native.

4. **`PgConnection` ownership inside `PgSubscription`**. sqlx::PgConnection is `Send` but not `Clone`. The subscription owns it, so concurrent calls would panic at the Rust borrow level if we ever introduced parallel `next-event` calls per stream. The host's WIT contract is single-threaded per stream — we rely on that.

5. **Decoder reuse across module boundaries.** `crates/worker/src/connectors/postgres/cdc/decode.rs` is currently visible to siblings via `pub`. Importing it from `wasm_runtime/db_pg_subscribe.rs` crosses module trees but stays inside the worker crate — a clean `use crate::connectors::postgres::cdc::decode::*`. If the worker crate ever splits, both modules go to the same sub-crate.

6. **Snapshot/streaming overlap window.** `pg_create_logical_replication_slot` (the SQL function, not the streaming-protocol form with `USE_SNAPSHOT`) returns the slot's `restart_lsn` at creation time, which lags `pg_current_wal_lsn`. Snapshot SELECTs run after that without an LSN-bounded transaction. Net effect: rows changed *during* snapshot may appear both in the snapshot batch and again in the early streaming events with their original `restart_lsn`-onward LSN. The downstream loader's `(id, lsn)` keying makes this idempotent for correctness but inflates row count. The native CDC has the same property and the team has accepted it; we accept it here too.

---

## Acceptance criteria

- Worker library tests: ≥5 new unit tests covering Postgres `db_host` routing + `db_pg_subscribe` event translation.
- `cargo build -p worker --lib` clean.
- `examples/postgres-cdc-rs` compiles to `wasm32-wasip2`.
- `tests/integration/tests/postgres_cdc_wasm_e2e.rs` compiles cleanly under `cargo build -p integration-tests --tests` (`#[ignore]` so manual run is the functional gate).
- README "Currently:" line updated.
