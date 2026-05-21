# Phase 2.4.b: Postgres Loader — CDC op-aware writes

**Status:** Shipped 2026-05-21 (branch `phase-2-4b-postgres-loader-cdc`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-4b-postgres-loader-cdc.md`
**Builds on:** Phase 2.4.a (`docs/superpowers/specs/2026-05-21-phase-2-4a-postgres-loader-design.md`).
**RFC:** RFC-0009 §"Pattern 3: Apply Change Stream".

## What this adds

The `PostgresLoader` now honors `_cdc.op` on incoming batches. PG-source CDC pipelines can land directly in a PG destination with correct row state.

## Detection

CDC mode is data-driven: any batch whose schema contains `_cdc.op` is routed through the CDC path. There's no spec-level mode flag — the data carries the signal. The same `PostgresDestinationSpec` can serve a snapshot phase (`s`) and a streaming phase (`i`/`u`/`d`) without reconfiguration.

## Per-row routing

| `_cdc.op` | Action                                                      |
|-----------|-------------------------------------------------------------|
| `i`       | `INSERT ... ON CONFLICT (pk) DO UPDATE`                     |
| `u`       | `INSERT ... ON CONFLICT (pk) DO UPDATE`                     |
| `s`       | `INSERT ... ON CONFLICT (pk) DO UPDATE` (snapshot)          |
| `d`       | `DELETE FROM target WHERE pk = ...`                         |
| `c`       | skip + `tracing::warn!` (schema-evolution follow-up)        |
| `t`       | skip + `tracing::warn!` (destructive ops not auto-applied)  |
| other     | error                                                       |

All rows in a batch execute in the single per-call transaction, in batch order.

## Schema stripping

`_cdc.op`, `_cdc.lsn`, `_cdc.commit_ts`, `_cdc.txid` are dropped from the destination DDL and from upsert row values. The destination table looks like the source table — no platform metadata leaks. Audit-log shape (keep `_cdc.*` columns) is a separate spec/mode for later.

## Idempotency

Unchanged from phase 2.4.a. The transaction-level idempotency log in `<schema>._etl_loaded_batches` short-circuits a duplicated `LoadId` regardless of CDC vs plain mode.

## Limitations (known, deferred)

- **Mixed-mode pipelines on the same destination** — if the first batch is non-CDC and a later batch is CDC, the destination table will already exist with whatever shape the first batch produced. `cdc_apply` uses `CREATE TABLE IF NOT EXISTS`, so it won't drop and recreate. Plain → CDC mode switch on the same target requires a manual recreate (or a future schema-reconciliation pass).
- **PK changes without delete-of-old-key** — an update whose PK column value differs from the source's prior value will land as an insert/update on the new key and leave the old-key row behind. Sources that emit a `d` of the old key followed by an `i` of the new key (which is what PG logical replication does with replica identity FULL) behave correctly.
- **Schema-change events (`c`) and truncate events (`t`)** are skipped with a warning. Wiring real schema evolution is the next loader-facing work item.

## Tests

- Unit: 13 new tests in `crates/worker/src/loaders/postgres.rs::tests` (CDC detection, schema stripping, op extraction, DELETE SQL, value extraction, pk-empty validation).
- Integration: 7 new tests in `tests/integration/tests/postgres_loader_cdc.rs` covering insert/update/delete/mixed/snapshot-handoff/retry-idempotency/no-metadata-cols against docker `postgres:16`.
- Phase 2.4.a's 3 plain-mode integration tests still green (no regressions on the non-CDC path).

## Follow-ups (priority order)

1. Schema evolution (apply additive `ALTER TABLE` on `c` events; pause on destructive).
2. Multi-table per spec (`stream_name → table` routing).
3. Dead-letter routing for CDC failures.
4. `COPY FROM STDIN` perf path for snapshot-heavy batches.
5. Secret-ref URLs (RFC-11 wiring on the loader side).
6. Audit-log destination mode option.
