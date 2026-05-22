# Phase 2.6.a — Metering Foundation Design Memo

**Status:** Shipped 2026-05-22 (branch `phase-2-6a-metering-foundation`).
**Plan:** `docs/superpowers/plans/2026-05-21-phase-2-6a-metering-foundation.md`
**RFC:** RFC-0017 (Quotas, Billing Metering, and Backpressure).

## What this adds

The minimum viable metering pipeline. Closes the long-standing audit finding that RFC-17 was ~5% implemented.

- New `crates/metering/` crate (parallel to `crates/audit/`): `MeteringEvent`, `BillableMetric`, `MeteringSource`, `MeteringSink` trait, `CatalogMeteringSink` (direct Postgres INSERT), `BufferedSink` (in-memory, for tests).
- Catalog migration `0017_metering_events.sql` — append-only table with tenant FK, time-DESC index, pipeline index, RLS mirroring the audit_log pattern.
- Emission hooks at the two boundaries that produce >90% of meterable activity:
  - `read_batch` emits `RowsRead` + `BytesRead` (best-effort).
  - `load_batch` emits `RowsWritten` + `BytesWritten` (best-effort).
- `SyncActivities` gains a `pub metering: Arc<dyn MeteringSink>` field. Production binary wires `CatalogMeteringSink::new(catalog.pool().clone())`.

## Architecture decisions

### New crate vs. module-in-worker
Standalone `crates/metering` parallel to `crates/audit`. Eventually consumed by control-api, CLI, scheduler — keeps the dependency graph clean. Matches the `audit` precedent.

### Direct insert vs. local durable queue
RFC-17 specifies a "local durable queue per worker → regional Kafka → aggregation service" pipeline. MVP is direct-insert to the catalog DB. Rationale:
- Today metering is not wired to billing; lost events cost no customer money.
- The `MeteringSink` trait lets phase-2-6b swap to a queued sink without touching activity code.
- Operational complexity of a per-worker queue is unjustified before the aggregation service exists.

### Best-effort emission
Activities never fail due to a metering write failure. On error: `tracing::warn!` and continue. Correctness before durability hardening.

### `value: i64` vs. separate unit column
`MeteringEvent.value` is a plain `i64`. The `metric` enum encodes the unit implicitly (RowsRead ⇒ rows, BytesRead ⇒ bytes). A `Unit` column is a non-breaking migration when quota enforcement needs it.

### BytesRead estimation
`read_batch` has no pre-IPC byte count for raw wire data. MVP uses `b64.len()` (base64-encoded Arrow IPC stream length) as a proxy. Over-estimates by ~33% due to base64 overhead. Acceptable for MVP; an exact pre-encode count is a follow-up.

### pipeline_id missing from ReadBatchInput
`ReadBatchInput` does not carry `pipeline_id` today — the workflow assigns the load destination after read completes. MVP emits `RowsRead`/`BytesRead` with `pipeline_id = None`. Fixing this requires a backward-compatible `#[serde(default)] pub pipeline_id: Uuid` addition to `ReadBatchInput`. Deferred.

## Schema

```sql
CREATE TABLE metering_events (
    event_id    UUID        PRIMARY KEY,   -- v7, time-ordered
    tenant_id   UUID        NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id UUID        NULL,
    run_id      UUID        NULL,
    metric      TEXT        NOT NULL,      -- snake_case BillableMetric name
    value       BIGINT      NOT NULL,
    source      TEXT        NOT NULL,      -- "read" | "load" | "transform"
    emitted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**RLS:** Same template as `audit_log` (migration 0013). Policy: `tenant_id = app_tenant_id() OR app_tenant_id() IS NULL`. Admin role (`etl`) bypasses; app role (`etl_app`) is scoped per `SET LOCAL app.tenant_id`.

**Indices:** `(tenant_id, emitted_at DESC)` for billing aggregation; partial `(pipeline_id) WHERE NOT NULL` for cost attribution.

**Idempotency:** PRIMARY KEY on `event_id` (UUIDv7); `CatalogMeteringSink::emit` uses `ON CONFLICT (event_id) DO NOTHING`. Duplicate emissions are silently coalesced.

## Emission points

| Activity | Events | Fields populated |
|---|---|---|
| `read_batch` | `RowsRead`, `BytesRead` | `tenant_id`, `timestamp` (no `pipeline_id` — see above) |
| `load_batch` | `RowsWritten`, `BytesWritten` | full attribution: `tenant_id`, `pipeline_id`, `run_id`, `timestamp` |

## Tests

- **Unit** (`cargo test -p metering`): 8 tests
  - BillableMetric / MeteringSource serde roundtrip
  - snake_case wire format verification
  - MeteringEvent roundtrip with all fields populated
  - Optional `pipeline_id` / `run_id` round-trip as null
  - BufferedSink capture, drain semantics, sum-by-metric
- **Integration** (`tests/integration/tests/metering_events.rs`): 5 tests
  - `emit_writes_a_row_to_metering_events`
  - `multiple_emits_sum_to_correct_total`
  - `rls_tenant_cannot_see_other_tenants_events` — uses `etl_app` role, confirms RLS isolation
  - `duplicate_event_id_is_silently_ignored` — ON CONFLICT DO NOTHING
  - `buffered_sink_captures_and_drains`

## Limitations (deferred)

- **Kafka / durable queue transport** (RFC-17 §"Local durable queue").
- **Billing aggregation pipeline** (daily roll-ups, hourly streaming counts).
- **Quota enforcement** (`QuotaConfig`, soft/hard caps, burst budgets, backpressure signals).
- **ComputeMs / WasmFuelUsed emission** (types exist; no caller emits them yet).
- **`StorageBytesHours`, `CdcSlotHeld`, `SeatsMonthly`, `ApiRequests`, `EgressBytes`** metrics.
- **Cost observability API** (RFC-17 §13).
- **`pipeline_id` on `ReadBatchInput`** — RowsRead events emit with `pipeline_id = None` until this is fixed.
- **Exact pre-encode byte count** — BytesRead uses base64 IPC length as a proxy.
- **Idempotent event deduplication** beyond `event_id` PK (e.g., stable hash from source operation).
- **Reconciliation jobs** and late-arriving event handling.

## Follow-ups (priority order)

1. Add `pipeline_id` to `ReadBatchInput` so RowsRead events are fully attributed.
2. Exact pre-encode byte count for BytesRead.
3. `ComputeMs` emission from activity start/end timestamps (lowest-effort next metric).
4. Durable local queue sink as a `MeteringSink` impl (replaces `CatalogMeteringSink` default).
5. Kafka forwarding from local queue → billing aggregation service.
6. Daily roll-up aggregation job.
7. Quota enforcement once aggregation produces tenant-period totals.
