# Phase 2.3.k: Postgres CDC `AdvanceSlot` Activity

**Status:** Shipped 2026-05-22 (branch `phase-2-3k-pg-cdc-advance-slot`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-3k-pg-cdc-advance-slot.md`
**Builds on:** Phase 2.3.j.1 (`docs/superpowers/specs/2026-05-03-phase-2-3j-1-pg-commit-advance-design.md`).
**RFC:** RFC-0008 §"Slot lifecycle" — names `AdvanceSlotActivity` as required. May-13 + May-21 audits flagged this as MISSING.

## What this adds

A Temporal activity `CdcActivities::advance_slot` that calls `pg_replication_slot_advance(slot_name, target_lsn)` to release WAL the destination has durably persisted. Without it, Postgres retains all WAL since slot creation, exhausting disk on long-running CDC pipelines.

## Two complementary paths

Phase 2.3.j.1 already added commit-LSN advancement inside the streaming runtime (`PgSubscription::finalize` in `db_pg_subscribe.rs`). That path is **opportunistic**: cheap, no Temporal involvement, fires per WASM connector read-batch. It works while the connector is running.

Phase 2.3.k adds the **durable** path: a proper Temporal activity called from the workflow's load-then-cursor-commit loop. This ensures WAL is explicitly released even when the connector restarts, the worker crashes, or the workflow does `continue-as-new`. The two paths are complementary — the streaming-runtime path is an optimization; the activity is the durability guarantee.

## Call site

`WasmCdcPipelineWorkflow` calls `advance_slot` after each successful `commit_cursor`, but only when the new cursor's `kind` is `CursorKind::Lsn`:

```rust
if let Some(ref cv) = read_out.new_cursor {
    if cv.kind == common_types::cursor::CursorKind::Lsn {
        let slot_name = format!("etl_{}", input.pipeline_id.as_simple());
        let advance_result = ctx.start_activity(
            CdcActivities::advance_slot,
            AdvanceSlotInput { …, slot_name, target_lsn: cv.value.clone() },
            opts_short(),
        ).await;
        if let Err(e) = advance_result {
            tracing::warn!("advance_slot failed (non-fatal, will retry on next batch)");
        }
    }
}
```

Failures are **non-fatal**: log and continue. The slot will advance on the next successful batch. If the LSN can't be advanced ever (e.g., misconfigured slot), the existing `cdc_monitor` will surface it via lag metrics.

## Slot name derivation

Same formula as `ensure_slot`: `format!("etl_{}", pipeline_id.as_simple())`. `Uuid::as_simple()` returns 32 lowercase hex chars without hyphens. This avoids adding a new field to `WasmCdcPipelineInput` — the workflow derives it on demand.

## Idempotency

`pg_replication_slot_advance` is idempotent in PG 11+ when `target_lsn ≥ confirmed_flush_lsn`. Two notes from integration testing:

1. **Identical advance** (same LSN twice): returns the same `confirmed_flush_lsn`, no error.
2. **Backward advance** (LSN < current `confirmed_flush_lsn`): PG errors with `"cannot advance replication slot to X, minimum is Y"`. This is **safer than silent no-op** — slot is protected from regression. The workflow swallows this error (non-fatal); Temporal won't retry endlessly because the next batch's LSN will be higher.

## Catalog update

After successful advance, the activity updates `catalog.cdc_update_confirmed_flush(ctx, pid, &confirmed)` so the `cdc_monitor` and future restarts see an accurate confirmed-flush position. This mirrors the existing pattern in `read_window` (`activities/cdc/mod.rs:315`).

## Tests

- **Unit** (`crates/worker/src/activities/cdc/inputs.rs::tests`): 2 serde-roundtrip tests for `AdvanceSlotInput` / `AdvanceSlotOutput`.
- **Integration** (`tests/integration/tests/pg_cdc_advance_slot.rs`): 4 tests against docker `postgres:16`:
  - `advance_slot_moves_confirmed_flush_lsn` — happy path
  - `advance_slot_is_idempotent` — same LSN twice returns same result
  - `advance_slot_with_older_lsn_errors_and_does_not_regress` — PG protects from backward advance
  - `advance_slot_errors_on_nonexistent_slot` — missing slot surfaces clear error
- 184 worker unit tests + 23 PG-related integration tests all green (4 advance_slot + 19 prior PG loader).

## Limitations (deferred)

- **Per-tenant rate limiting** — every batch triggers an advance call; on extremely high batch frequency this could be optimized to "every N batches."
- **Multi-slot pipelines** — MVP is one slot per pipeline.
- **MySQL GTID advance** — RFC-8 covers it separately; MySQL binlog retention is configured server-side, not per-consumer.
- **`CdcPipelineWorkflow` (non-WASM path)** — only the WASM CDC pipeline path was wired in this phase; follow-up to mirror in the native path.

## Follow-ups

1. Mirror in the non-WASM CDC pipeline workflow.
2. Surface slot lag in operator dashboards (RFC-8 §"Slot monitoring") — `slot_lag_bytes` helper already exists, just needs a periodic metric exporter.
3. Adaptive rate limiting if measurement shows per-batch advance is costly under load.
