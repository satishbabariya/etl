# Phase 2.4.a: Postgres Destination Loader — Design

**Status:** Shipped 2026-05-21 (branch `phase-2-4a-postgres-loader`).
**Plan:** `docs/superpowers/plans/2026-05-21-postgres-destination-loader.md`
**RFC:** RFC-0009 (Destination Loaders).

## 1. Scope

**In:**
- Second loader after `LocalParquetLoader`. Lives at `crates/worker/src/loaders/postgres.rs`.
- Two delivery patterns:
  - Append (`pk_columns: []` ⇒ plain `INSERT`).
  - Upsert (`pk_columns: [...]` ⇒ `INSERT ... ON CONFLICT (pk) DO UPDATE`, or `DO NOTHING` if every column is part of the PK).
- Per-call transaction: ensure log → idempotency check → ensure target table → INSERT loop → log row → commit.
- Auto-`CREATE TABLE IF NOT EXISTS` on first non-empty batch.
- Type mapping covering the typed Arrow columns the existing CDC sources emit.
- Activity dispatch in `load_batch` routes `DestinationSpec::Postgres(_)` to the new loader.

**Out (deferred):**
- CDC `_cdc.op`-aware DELETE / UPDATE (RFC-9 Pattern 3 / "Apply Change Stream").
- Mid-run schema evolution (`ALTER TABLE`); only first-load `CREATE TABLE`.
- Soft delete / tombstone columns.
- Dead-letter routing for the Postgres destination (rejected rows are logged-and-dropped by the activity; LocalParquet dead-letter path is unchanged).
- `COPY FROM STDIN` perf path. INSERT-per-row is fine for MVP.
- RFC-11 secret-ref connection URLs. MVP takes an inline `postgres://...` string.
- Multi-table per spec — MVP is one target table per pipeline (matches the platform's current single-table assumption everywhere else).

## 2. Trait fit

The existing `loader_sdk::DestinationLoader` trait is `validate` + `load`. RFC-9's sketch is richer (`prepare_run` / `commit_run` / `abort_run` / `capabilities`). We **do not** extend the trait yet:

- The MVP delivery patterns (append + upsert) fit inside a single `load(...)` call because:
  - The transaction boundary is per-batch, not per-run. Each batch is atomic on its own; the destination converges as batches arrive.
  - Idempotency is per-batch via the log table — same `LoadId` twice = no-op, regardless of where in the run it lands.
- Pattern 3 (Apply Change Stream) and the runs-with-staging-table workflow will need `prepare_run`/`commit_run`. That's a follow-up.

## 3. Idempotency strategy

Per RFC-9 §"Destinations without idempotency primitives".

Log table per-schema:

```sql
CREATE TABLE IF NOT EXISTS "<schema>"."_etl_loaded_batches" (
    tenant_id UUID NOT NULL,
    pipeline_id UUID NOT NULL,
    run_id UUID NOT NULL,
    stream_name TEXT NOT NULL,
    batch_seq BIGINT NOT NULL,
    rows_loaded BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, pipeline_id, run_id, stream_name, batch_seq)
)
```

Each `load()` call opens a transaction, queries the log, and either short-circuits (`rows_loaded = 0` returned, no destination writes) or inserts the rows + the log entry + commits atomically. Crash before commit ⇒ retry sees no log entry and re-runs. Crash after commit ⇒ retry sees the log entry and no-ops.

Live with `_etl_loaded_batches` in the same schema as the user table — keeps cleanup simple (`DROP SCHEMA CASCADE` works for tests; teardown is a single decision for ops).

## 4. Type mapping

| Arrow                                            | Postgres            |
|--------------------------------------------------|---------------------|
| `Int64`                                          | `BIGINT`            |
| `Int32`                                          | `INTEGER`           |
| `Int16`                                          | `SMALLINT`          |
| `Utf8`, `LargeUtf8`                              | `TEXT`              |
| `Boolean`                                        | `BOOLEAN`           |
| `Float64`                                        | `DOUBLE PRECISION`  |
| `Float32`                                        | `REAL`              |
| `Timestamp(Microsecond, Some(_))`                | `TIMESTAMPTZ`       |
| `Timestamp(Microsecond, None)`                   | `TIMESTAMP`         |
| `Date32`                                         | `DATE`              |
| `Binary`, `LargeBinary`                          | `BYTEA`             |
| `Time64(*)`, `Time32(*)`                         | `TIME`              |
| anything else                                    | error: `unsupported`|

Covers every column the existing PG CDC and MySQL CDC sources emit today. Adding decimals / numeric is a small follow-up when a source needs it.

## 5. Connection / secret handling

`PostgresDestinationSpec.connection_url: String` — inline `postgres://user:pass@host:port/db`. Matches the dev pattern used by the existing PG source connector via `ConnectionConfig::from_url`. RFC-11 secret-ref resolution is a separate piece of work that will land for source and destination loaders together.

## 6. Dispatch in the activity

`crates/worker/src/activities/sync/mod.rs::load_batch` now matches on `DestinationSpec`:

```rust
let res = match &input.destination {
    DestinationSpec::LocalParquet(_) => LocalParquetLoader.load(...).await,
    DestinationSpec::Postgres(_)     => PostgresLoader.load(...).await,
}.map_err(to_retryable)?;
```

Dead-letter path is guarded — only LocalParquet writes rejected rows to disk; Postgres logs-and-drops with a `tracing::warn!` so it's visible in operator dashboards.

## 7. Tests

- 12 unit tests in `crates/worker/src/loaders/postgres.rs` cover type mapping, DDL, SQL builder, row extraction, log DDL.
- 3 integration tests in `tests/integration/tests/postgres_loader.rs` (each creates and drops a unique schema) cover append, idempotent retry, and PK upsert against the docker-compose `postgres:16` service.
- 2 unit tests in `activities::sync::dispatch_tests` cover the activity match arm.

Integration tests skip with a clear message when the database is unreachable; honor `ETL_INTEGRATION_PG_URL` for non-default targets.

## 8. Next steps

In rough priority order:

1. **CDC op-aware writes** — read `_cdc.op` per row and translate `i`/`u`/`d` into per-batch INSERT / UPDATE / DELETE. Required to use this loader as a CDC destination.
2. **Schema evolution mid-run** — apply additive changes via `ALTER TABLE`; pause on destructive.
3. **Multi-table per spec** — wire `stream_name → table` like LocalParquetLoader's path routing.
4. **Dead-letter routing** — write rejected rows to a `<table>__dead_letter` table in the same schema.
5. **`COPY FROM STDIN` fast path** — switch from row-at-a-time INSERT to binary COPY once a batch is large enough to amortize the per-statement overhead. Materially improves throughput on big batches.
6. **Secret-ref URLs** — share the resolution path with the source connectors once RFC-11 wiring lands on the loader side.
