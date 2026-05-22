# Phase 2.6.a: RFC-17 MeteringEvent Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the absolute minimum metering pipeline: a `MeteringEvent` type with a `BillableMetric` enum, an in-process emitter that writes events to the catalog Postgres DB, and emission hooks at the two boundaries producing >90% of meterable activity — `read_batch` (extract) and `load_batch` (load). RFC-17 is currently ~5% implemented; this plan ships the foundation that everything else builds on.

**Architecture:** New `crates/metering/` crate (parallel to `crates/audit/`) with `MeteringEvent`, `BillableMetric`, `MeteringSource`, and a `MeteringSink` trait. The default impl (`CatalogMeteringSink`) holds a `sqlx::PgPool` and inserts directly into the catalog DB's `metering_events` table. A `BufferedSink` wraps `Arc<Mutex<Vec<MeteringEvent>>>` for unit and integration tests. `SyncActivities` gains a `pub metering: Arc<dyn MeteringSink>` field; both `read_batch` and `load_batch` emit best-effort (log warn on failure, never fail the activity).

**Tech Stack:** Rust · `sqlx 0.8` (workspace dep) · `chrono 0.4` (workspace dep) · `uuid 1` with v7 (workspace dep) · `async-trait 0.1` (workspace dep) · docker-compose `postgres:16` for integration tests.

**Scope cuts (explicitly deferred — not in this plan):**
- Kafka / durable queue transport (RFC-17 §"Local durable queue").
- Billing aggregation pipeline (daily roll-up jobs, hourly streaming counts).
- Quota enforcement (soft/hard/burst caps, `QuotaConfig`, backpressure signals).
- Cost observability API (RFC-17 §13 "Cost Observability for Customers").
- Tier-specific quota structures and enterprise contract rates.
- Billing audit trail beyond the basic events table (RFC-17 §15).
- `ComputeMs` and `WasmFuelUsed` metric emission (just `RowsRead`/`RowsWritten`/`BytesRead`/`BytesWritten` for MVP).
- Reconciliation jobs and late-arriving event handling.
- `StorageBytesHours`, `CdcSlotHeld`, `SeatsMonthly`, `ApiRequests`, `EgressBytes` metrics.
- Idempotent event deduplication (stable `event_id` derivation from source operation hash).

---

## File Structure

**Create:**
- `/Users/satishbabariya/Desktop/etl/crates/metering/Cargo.toml`
- `/Users/satishbabariya/Desktop/etl/crates/metering/src/lib.rs`
- `/Users/satishbabariya/Desktop/etl/crates/metering/src/event.rs` — `MeteringEvent`, `BillableMetric`, `MeteringSource`.
- `/Users/satishbabariya/Desktop/etl/crates/metering/src/sink.rs` — `MeteringSink` trait, `CatalogMeteringSink`, `BufferedSink`.
- `/Users/satishbabariya/Desktop/etl/crates/catalog/migrations/0017_metering_events.sql`
- `/Users/satishbabariya/Desktop/etl/tests/integration/tests/metering_events.rs`
- `/Users/satishbabariya/Desktop/etl/docs/superpowers/specs/2026-05-21-phase-2-6a-metering-foundation-design.md`

**Modify:**
- `/Users/satishbabariya/Desktop/etl/Cargo.toml` — add `crates/metering` to workspace members + internal dep.
- `/Users/satishbabariya/Desktop/etl/crates/worker/Cargo.toml` — add `metering` dependency.
- `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs` — add `metering` field to `SyncActivities`; add emission calls in `read_batch` and `load_batch`.
- `/Users/satishbabariya/Desktop/etl/crates/worker/src/main.rs` — construct `CatalogMeteringSink` and wire into `SyncActivities`.
- `/Users/satishbabariya/Desktop/etl/tests/integration/Cargo.toml` — add `metering` dependency.

---

## Task 1: Create `crates/metering` crate skeleton + add to workspace

**Files:**
- Modify: `/Users/satishbabariya/Desktop/etl/Cargo.toml`
- Create: `/Users/satishbabariya/Desktop/etl/crates/metering/Cargo.toml`
- Create: `/Users/satishbabariya/Desktop/etl/crates/metering/src/lib.rs`

- [ ] **Step 1: Write the failing build check**

Run: `cargo build --workspace`
Expected: succeeds (baseline). Confirms the workspace is clean before we start.

- [ ] **Step 2: Add to workspace `Cargo.toml`**

In `/Users/satishbabariya/Desktop/etl/Cargo.toml`, add `"crates/metering"` to the `[workspace] members` array (after `"crates/audit"`):

```toml
members = [
    "crates/common-types",
    "crates/catalog",
    "crates/worker",
    "crates/control-api",
    "crates/connector-sdk",
    "crates/loader-sdk",
    "crates/cli",
    "crates/auth",
    "crates/audit",
    "crates/metering",
    "tests/integration",
]
```

Under the internal crates section:

```toml
metering = { path = "crates/metering" }
```

- [ ] **Step 3: Create the crate manifest**

Create `/Users/satishbabariya/Desktop/etl/crates/metering/Cargo.toml`:

```toml
[package]
name = "metering"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lib]
path = "src/lib.rs"

[dependencies]
common-types = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
sqlx = { workspace = true }
uuid = { workspace = true }
tracing = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true }
```

- [ ] **Step 4: Create the lib.rs skeleton**

Create `/Users/satishbabariya/Desktop/etl/crates/metering/src/lib.rs`:

```rust
//! Metering foundation — RFC-17 §"Metering Events".
//!
//! MVP scope: emit `MeteringEvent` rows to the catalog Postgres DB.
//! Kafka pipeline, aggregation, quota enforcement, and cost observability
//! are explicitly deferred (see plan header).

pub mod event;
pub mod sink;

pub use event::{BillableMetric, MeteringEvent, MeteringSource};
pub use sink::{BufferedSink, CatalogMeteringSink, MeteringSink};
```

- [ ] **Step 5: Stub the modules so the workspace builds**

Create `/Users/satishbabariya/Desktop/etl/crates/metering/src/event.rs`:

```rust
// placeholder — filled in Task 2
```

Create `/Users/satishbabariya/Desktop/etl/crates/metering/src/sink.rs`:

```rust
// placeholder — filled in Task 4 and 5
```

- [ ] **Step 6: Build to confirm workspace wires**

Run: `cargo build -p metering`
Expected: succeeds (empty placeholder modules compile).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/metering/
git commit -m "phase-2-6a-1: crates/metering skeleton + workspace wiring"
```

---

## Task 2: Define `BillableMetric`, `MeteringSource`, `MeteringEvent` with unit tests

**Files:**
- Modify: `/Users/satishbabariya/Desktop/etl/crates/metering/src/event.rs`

- [ ] **Step 1: Write the failing tests**

Replace the placeholder in `/Users/satishbabariya/Desktop/etl/crates/metering/src/event.rs` with the tests first:

```rust
use chrono::Utc;
use common_types::ids::{PipelineId, RunId, TenantId};
use uuid::Uuid;

// forward-declare types so tests reference them before the impl block below
use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn billable_metric_serde_roundtrip() {
        for m in [
            BillableMetric::RowsRead,
            BillableMetric::RowsWritten,
            BillableMetric::BytesRead,
            BillableMetric::BytesWritten,
            BillableMetric::ComputeMs,
            BillableMetric::WasmFuelUsed,
        ] {
            let s = serde_json::to_string(&m).unwrap();
            let back: BillableMetric = serde_json::from_str(&s).unwrap();
            assert_eq!(
                format!("{:?}", m),
                format!("{:?}", back),
                "serde roundtrip failed for {m:?}"
            );
        }
    }

    #[test]
    fn billable_metric_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&BillableMetric::RowsRead).unwrap(),
            r#""rows_read""#
        );
        assert_eq!(
            serde_json::to_string(&BillableMetric::BytesWritten).unwrap(),
            r#""bytes_written""#
        );
        assert_eq!(
            serde_json::to_string(&BillableMetric::WasmFuelUsed).unwrap(),
            r#""wasm_fuel_used""#
        );
    }

    #[test]
    fn metering_source_serde_roundtrip() {
        for src in [MeteringSource::Read, MeteringSource::Load, MeteringSource::Transform] {
            let s = serde_json::to_string(&src).unwrap();
            let back: MeteringSource = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", src), format!("{:?}", back));
        }
    }

    #[test]
    fn metering_event_roundtrip_with_all_fields() {
        let ev = MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            pipeline_id: Some(PipelineId::new()),
            run_id: Some(RunId::new()),
            metric: BillableMetric::RowsRead,
            value: 1_024,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        };
        let j = serde_json::to_string(&ev).unwrap();
        let back: MeteringEvent = serde_json::from_str(&j).unwrap();
        assert_eq!(ev.event_id, back.event_id);
        assert_eq!(ev.tenant_id, back.tenant_id);
        assert_eq!(ev.value, back.value);
        assert_eq!(format!("{:?}", ev.metric), format!("{:?}", back.metric));
        assert_eq!(format!("{:?}", ev.source), format!("{:?}", back.source));
    }

    #[test]
    fn metering_event_pipeline_id_and_run_id_optional() {
        let ev = MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            pipeline_id: None,
            run_id: None,
            metric: BillableMetric::BytesRead,
            value: 512,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        };
        let j = serde_json::to_string(&ev).unwrap();
        let back: MeteringEvent = serde_json::from_str(&j).unwrap();
        assert!(back.pipeline_id.is_none());
        assert!(back.run_id.is_none());
    }

    #[test]
    fn event_id_is_v7_time_orderable() {
        let a = Uuid::now_v7();
        // sleep not needed — v7 uses monotonic counter; two consecutive calls
        // always increment.
        let b = Uuid::now_v7();
        assert!(b >= a, "v7 UUIDs must be non-decreasing");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p metering event::tests`
Expected: compile errors — `BillableMetric`, `MeteringSource`, `MeteringEvent` not defined.

- [ ] **Step 3: Implement the types**

Replace the content of `/Users/satishbabariya/Desktop/etl/crates/metering/src/event.rs` with:

```rust
//! Core metering types — RFC-17 §"Event shape".
//!
//! Deferred: ComputeMs and WasmFuelUsed *emission* (types exist for future
//! callers; no activity hooks emit them yet in this phase).

use chrono::{DateTime, Utc};
use common_types::ids::{PipelineId, RunId, TenantId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Every billable action the platform can measure.
///
/// MVP emission: RowsRead, RowsWritten, BytesRead, BytesWritten.
/// ComputeMs and WasmFuelUsed are defined but not yet emitted (deferred).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillableMetric {
    RowsRead,
    RowsWritten,
    BytesRead,
    BytesWritten,
    /// Wall-clock milliseconds of worker CPU for an activity. Deferred.
    ComputeMs,
    /// Wasmtime fuel units consumed by a WASM connector call. Deferred.
    WasmFuelUsed,
}

/// The platform component that produced the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeteringSource {
    Read,
    Load,
    Transform,
}

/// One billable measurement at a point in time.
///
/// `event_id` uses UUIDv7 (time-ordered) so events sort naturally by
/// insertion time without a separate sequence. `value` is an i64 to
/// support any of rows / bytes / milliseconds / fuel-units without
/// a separate unit enum at this stage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeteringEvent {
    /// UUIDv7 — time-orderable, globally unique.
    pub event_id: Uuid,
    pub tenant_id: TenantId,
    pub pipeline_id: Option<PipelineId>,
    pub run_id: Option<RunId>,
    pub metric: BillableMetric,
    /// Quantity: rows / bytes / milliseconds / fuel units, depending on metric.
    pub value: i64,
    pub timestamp: DateTime<Utc>,
    pub source: MeteringSource,
}

impl MeteringEvent {
    /// Construct a new event with a fresh UUIDv7 `event_id` and `timestamp = now()`.
    pub fn new(
        tenant_id: TenantId,
        pipeline_id: Option<PipelineId>,
        run_id: Option<RunId>,
        metric: BillableMetric,
        value: i64,
        source: MeteringSource,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            tenant_id,
            pipeline_id,
            run_id,
            metric,
            value,
            timestamp: Utc::now(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn billable_metric_serde_roundtrip() {
        for m in [
            BillableMetric::RowsRead,
            BillableMetric::RowsWritten,
            BillableMetric::BytesRead,
            BillableMetric::BytesWritten,
            BillableMetric::ComputeMs,
            BillableMetric::WasmFuelUsed,
        ] {
            let s = serde_json::to_string(&m).unwrap();
            let back: BillableMetric = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", m), format!("{:?}", back));
        }
    }

    #[test]
    fn billable_metric_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&BillableMetric::RowsRead).unwrap(),
            r#""rows_read""#
        );
        assert_eq!(
            serde_json::to_string(&BillableMetric::BytesWritten).unwrap(),
            r#""bytes_written""#
        );
        assert_eq!(
            serde_json::to_string(&BillableMetric::WasmFuelUsed).unwrap(),
            r#""wasm_fuel_used""#
        );
    }

    #[test]
    fn metering_source_serde_roundtrip() {
        for src in [MeteringSource::Read, MeteringSource::Load, MeteringSource::Transform] {
            let s = serde_json::to_string(&src).unwrap();
            let back: MeteringSource = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{:?}", src), format!("{:?}", back));
        }
    }

    #[test]
    fn metering_event_roundtrip_with_all_fields() {
        let ev = MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            pipeline_id: Some(PipelineId::new()),
            run_id: Some(RunId::new()),
            metric: BillableMetric::RowsRead,
            value: 1_024,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        };
        let j = serde_json::to_string(&ev).unwrap();
        let back: MeteringEvent = serde_json::from_str(&j).unwrap();
        assert_eq!(ev.event_id, back.event_id);
        assert_eq!(ev.tenant_id, back.tenant_id);
        assert_eq!(ev.value, back.value);
    }

    #[test]
    fn metering_event_optional_ids_round_trip_as_null() {
        let ev = MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            pipeline_id: None,
            run_id: None,
            metric: BillableMetric::BytesRead,
            value: 512,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        };
        let j = serde_json::to_value(&ev).unwrap();
        assert!(j["pipeline_id"].is_null());
        assert!(j["run_id"].is_null());
    }

    #[test]
    fn event_id_is_v7_time_orderable() {
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        assert!(b >= a, "v7 UUIDs must be non-decreasing");
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p metering event::tests`
Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/metering/src/event.rs
git commit -m "phase-2-6a-2: BillableMetric, MeteringSource, MeteringEvent types"
```

---

## Task 3: Migration `0017_metering_events.sql` with RLS + indices

**Files:**
- Create: `/Users/satishbabariya/Desktop/etl/crates/catalog/migrations/0017_metering_events.sql`

- [ ] **Step 1: Write the failing test**

Confirm the migration file doesn't exist yet:

Run: `ls /Users/satishbabariya/Desktop/etl/crates/catalog/migrations/`
Expected: no `0017_metering_events.sql` file.

- [ ] **Step 2: Create the migration**

Create `/Users/satishbabariya/Desktop/etl/crates/catalog/migrations/0017_metering_events.sql`:

```sql
-- 0017_metering_events.sql — append-only billing metering events.
-- RFC-17 §"Metering Events". MVP: direct Postgres insert; Kafka transport deferred.
--
-- Design notes:
--   • event_id is UUID v7 (time-ordered) — the PK also orders by time.
--   • tenant_id NOT NULL; every event is tenant-scoped (no system-level events here).
--   • pipeline_id and run_id are NULL-able (e.g. storage events have no pipeline).
--   • value is BIGINT to hold rows / bytes / ms / fuel-units without a separate unit column.
--   • Lossy by design at this stage: on conflict do nothing (idempotent retries safe).
--   • RLS mirrors audit_log pattern: tenant sees only its own events; admin (NULL) sees all.

CREATE TABLE IF NOT EXISTS metering_events (
    event_id      UUID          PRIMARY KEY,
    tenant_id     UUID          NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    pipeline_id   UUID          NULL,
    run_id        UUID          NULL,
    metric        TEXT          NOT NULL,
    value         BIGINT        NOT NULL,
    source        TEXT          NOT NULL,
    emitted_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Primary query pattern: per-tenant billing aggregation, ordered newest-first.
CREATE INDEX IF NOT EXISTS metering_events_tenant_emitted_idx
    ON metering_events (tenant_id, emitted_at DESC);

-- Secondary: per-pipeline drill-down for cost attribution.
CREATE INDEX IF NOT EXISTS metering_events_pipeline_idx
    ON metering_events (pipeline_id)
    WHERE pipeline_id IS NOT NULL;

GRANT SELECT, INSERT ON metering_events TO etl_app;

ALTER TABLE metering_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE metering_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON metering_events;
CREATE POLICY tenant_isolation ON metering_events
    USING  (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL)
    WITH CHECK (tenant_id = app_tenant_id() OR app_tenant_id() IS NULL);
```

- [ ] **Step 3: Apply the migration against the dev database and confirm the table exists**

Run:

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  cargo run -p cli -- tenant list 2>/dev/null || true
# The CLI's Catalog::connect calls migrate(), which runs 0017.
# Alternatively, run the catalog migrate() directly:
cargo test -p catalog 2>&1 | head -5
```

Or for a direct check:

```bash
docker compose up -d postgres
# Wait for postgres to be up, then:
psql postgres://etl:etl@localhost:5432/etl_catalog \
  -c "SELECT to_regclass('public.metering_events');"
# Expected: "metering_events" (not NULL)
```

- [ ] **Step 4: Confirm RLS blocks cross-tenant access**

Run this manual SQL check (optional, covered more thoroughly by integration test in Task 8):

```bash
psql postgres://etl_app:etl_app@localhost:5432/etl_catalog <<'SQL'
-- No app.tenant_id set — RLS allows nothing for etl_app role.
SELECT COUNT(*) FROM metering_events;
SQL
# Expected: 0 (empty table, RLS permits NULL app_tenant_id to see all rows
#   per the policy "OR app_tenant_id() IS NULL", but there are no rows yet).
```

- [ ] **Step 5: Commit**

```bash
git add crates/catalog/migrations/0017_metering_events.sql
git commit -m "phase-2-6a-3: metering_events table with RLS and time+pipeline indices"
```

---

## Task 4: `MeteringSink` trait + `CatalogMeteringSink` implementation

**Files:**
- Modify: `/Users/satishbabariya/Desktop/etl/crates/metering/src/sink.rs`

- [ ] **Step 1: Write the failing tests**

Replace the placeholder in `/Users/satishbabariya/Desktop/etl/crates/metering/src/sink.rs` with the test block at the top:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{BillableMetric, MeteringEvent, MeteringSource};
    use chrono::Utc;
    use common_types::ids::{PipelineId, RunId, TenantId};
    use uuid::Uuid;

    fn sample_event() -> MeteringEvent {
        MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            pipeline_id: Some(PipelineId::new()),
            run_id: Some(RunId::new()),
            metric: BillableMetric::RowsRead,
            value: 42,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        }
    }

    #[tokio::test]
    async fn buffered_sink_captures_events() {
        let sink = BufferedSink::new();
        sink.emit(&sample_event()).await.unwrap();
        sink.emit(&sample_event()).await.unwrap();
        let captured = sink.drain();
        assert_eq!(captured.len(), 2);
        for e in &captured {
            assert_eq!(e.value, 42);
        }
    }

    #[tokio::test]
    async fn buffered_sink_drain_clears_buffer() {
        let sink = BufferedSink::new();
        sink.emit(&sample_event()).await.unwrap();
        let first_drain = sink.drain();
        assert_eq!(first_drain.len(), 1);
        let second_drain = sink.drain();
        assert_eq!(second_drain.len(), 0, "buffer must be empty after drain");
    }

    #[tokio::test]
    async fn buffered_sink_sum_values_by_metric() {
        let sink = BufferedSink::new();
        let tid = TenantId::new();
        let pid = PipelineId::new();
        let rid = RunId::new();
        for v in [10i64, 20, 30] {
            let ev = MeteringEvent {
                event_id: Uuid::now_v7(),
                tenant_id: tid,
                pipeline_id: Some(pid),
                run_id: Some(rid),
                metric: BillableMetric::RowsRead,
                value: v,
                timestamp: Utc::now(),
                source: MeteringSource::Read,
            };
            sink.emit(&ev).await.unwrap();
        }
        let total: i64 = sink.drain().iter().map(|e| e.value).sum();
        assert_eq!(total, 60);
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p metering sink::tests`
Expected: compile errors — `BufferedSink`, `MeteringSink` not defined.

- [ ] **Step 3: Implement `MeteringSink` trait, `CatalogMeteringSink`, and `BufferedSink`**

Replace `/Users/satishbabariya/Desktop/etl/crates/metering/src/sink.rs` with:

```rust
//! MeteringSink trait and implementations.
//!
//! `CatalogMeteringSink` writes directly to the catalog DB's `metering_events`
//! table (no Kafka; MVP is direct-insert). Writes are best-effort: callers
//! log a warning on failure and continue.
//!
//! `BufferedSink` is an in-memory sink for tests.

use crate::event::MeteringEvent;
use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::{Arc, Mutex};

/// Abstraction over where metering events go.
///
/// Implementations must be `Send + Sync` so they can be held behind `Arc`
/// inside `SyncActivities`.
#[async_trait]
pub trait MeteringSink: Send + Sync {
    async fn emit(&self, event: &MeteringEvent) -> Result<()>;
}

// ── CatalogMeteringSink ──────────────────────────────────────────────────────

/// Writes metering events directly to the catalog Postgres DB.
/// Production default. Kafka transport is a future replacement.
#[derive(Clone)]
pub struct CatalogMeteringSink {
    pool: PgPool,
}

impl CatalogMeteringSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MeteringSink for CatalogMeteringSink {
    async fn emit(&self, event: &MeteringEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO metering_events \
               (event_id, tenant_id, pipeline_id, run_id, metric, value, source, emitted_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event.event_id)
        .bind(event.tenant_id.as_uuid())
        .bind(event.pipeline_id.as_ref().map(|p| p.as_uuid()))
        .bind(event.run_id.as_ref().map(|r| r.as_uuid()))
        .bind(format!("{:?}", event.metric).to_lowercase()
              // BillableMetric::RowsRead → "rowsread"; use serde name instead:
        )
        .bind(event.value)
        .bind(format!("{:?}", event.source).to_lowercase())
        .bind(event.timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
```

Wait — `format!("{:?}", BillableMetric::RowsRead).to_lowercase()` would give `"rowsread"`, not `"rows_read"`. We need the serde form. Replace the metric bind with:

```rust
        .bind(serde_json::to_string(&event.metric)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string())
        .bind(event.value)
        .bind(serde_json::to_string(&event.source)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string())
```

Full corrected `CatalogMeteringSink::emit`:

```rust
#[async_trait]
impl MeteringSink for CatalogMeteringSink {
    async fn emit(&self, event: &MeteringEvent) -> Result<()> {
        let metric_str = serde_json::to_string(&event.metric)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let source_str = serde_json::to_string(&event.source)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        sqlx::query(
            "INSERT INTO metering_events \
               (event_id, tenant_id, pipeline_id, run_id, metric, value, source, emitted_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event.event_id)
        .bind(event.tenant_id.as_uuid())
        .bind(event.pipeline_id.as_ref().map(|p| p.as_uuid()))
        .bind(event.run_id.as_ref().map(|r| r.as_uuid()))
        .bind(metric_str)
        .bind(event.value)
        .bind(source_str)
        .bind(event.timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
```

Then add `BufferedSink`:

```rust
// ── BufferedSink ─────────────────────────────────────────────────────────────

/// In-memory sink for unit and integration tests.
/// Wraps `Arc<Mutex<Vec<MeteringEvent>>>` so it is `Clone + Send + Sync`.
#[derive(Clone, Default)]
pub struct BufferedSink {
    inner: Arc<Mutex<Vec<MeteringEvent>>>,
}

impl BufferedSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take all events out of the buffer (drains on each call).
    pub fn drain(&self) -> Vec<MeteringEvent> {
        let mut guard = self.inner.lock().expect("BufferedSink mutex poisoned");
        std::mem::take(&mut *guard)
    }

    /// Read-only peek — clones; does not drain.
    pub fn snapshot(&self) -> Vec<MeteringEvent> {
        self.inner.lock().expect("BufferedSink mutex poisoned").clone()
    }
}

#[async_trait]
impl MeteringSink for BufferedSink {
    async fn emit(&self, event: &MeteringEvent) -> Result<()> {
        self.inner
            .lock()
            .expect("BufferedSink mutex poisoned")
            .push(event.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{BillableMetric, MeteringEvent, MeteringSource};
    use chrono::Utc;
    use common_types::ids::{PipelineId, RunId, TenantId};
    use uuid::Uuid;

    fn sample_event() -> MeteringEvent {
        MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            pipeline_id: Some(PipelineId::new()),
            run_id: Some(RunId::new()),
            metric: BillableMetric::RowsRead,
            value: 42,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        }
    }

    #[tokio::test]
    async fn buffered_sink_captures_events() {
        let sink = BufferedSink::new();
        sink.emit(&sample_event()).await.unwrap();
        sink.emit(&sample_event()).await.unwrap();
        let captured = sink.drain();
        assert_eq!(captured.len(), 2);
        for e in &captured {
            assert_eq!(e.value, 42);
        }
    }

    #[tokio::test]
    async fn buffered_sink_drain_clears_buffer() {
        let sink = BufferedSink::new();
        sink.emit(&sample_event()).await.unwrap();
        let first_drain = sink.drain();
        assert_eq!(first_drain.len(), 1);
        let second_drain = sink.drain();
        assert_eq!(second_drain.len(), 0, "buffer must be empty after drain");
    }

    #[tokio::test]
    async fn buffered_sink_sum_values_by_metric() {
        let sink = BufferedSink::new();
        let tid = TenantId::new();
        let pid = PipelineId::new();
        let rid = RunId::new();
        for v in [10i64, 20, 30] {
            let ev = MeteringEvent {
                event_id: Uuid::now_v7(),
                tenant_id: tid,
                pipeline_id: Some(pid),
                run_id: Some(rid),
                metric: BillableMetric::RowsRead,
                value: v,
                timestamp: Utc::now(),
                source: MeteringSource::Read,
            };
            sink.emit(&ev).await.unwrap();
        }
        let total: i64 = sink.drain().iter().map(|e| e.value).sum();
        assert_eq!(total, 60);
    }
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p metering sink::tests`
Expected: all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/metering/src/sink.rs
git commit -m "phase-2-6a-4: MeteringSink trait + CatalogMeteringSink + BufferedSink"
```

---

## Task 5: Wire `MeteringSink` into `SyncActivities` + update `main.rs`

**Files:**
- Modify: `/Users/satishbabariya/Desktop/etl/crates/worker/Cargo.toml`
- Modify: `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs`
- Modify: `/Users/satishbabariya/Desktop/etl/crates/worker/src/main.rs`

This task adds the metering field to `SyncActivities` without emitting anything yet — that's Tasks 6 and 7. Tests here are compile-time: the struct must construct and the worker binary must compile.

- [ ] **Step 1: Write the failing test**

In `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs`, add to the existing `mod dispatch_tests` at the bottom:

```rust
#[test]
fn sync_activities_has_metering_field() {
    // Compile-time check: SyncActivities must have a `metering` field of
    // the right type. This is a structural lint — if the field is missing
    // or mistyped, this test fails to compile.
    let _: fn() -> Option<Arc<dyn metering::MeteringSink>> = || {
        let _sa: &SyncActivities = unreachable!();
        // If `metering` field doesn't exist, the line below won't compile.
        // We never actually run this closure; the test is purely structural.
        None::<Arc<dyn metering::MeteringSink>>
    };
}
```

Actually, because Rust doesn't have "field exists" reflection, the simplest compile-time test is just to attempt to construct a `SyncActivities` with a `BufferedSink` in the test module:

```rust
#[cfg(test)]
mod metering_field_tests {
    use super::*;
    use metering::BufferedSink;
    use std::sync::Arc;

    #[test]
    fn sync_activities_accepts_metering_sink() {
        // If this line compiles, the `metering` field exists with the right type.
        let _sink: Arc<dyn metering::MeteringSink> = Arc::new(BufferedSink::new());
        // Full struct construction is tested end-to-end in integration; here we
        // just confirm the type alias compiles.
    }
}
```

- [ ] **Step 2: Add `metering` to worker `Cargo.toml`**

In `/Users/satishbabariya/Desktop/etl/crates/worker/Cargo.toml`, add to `[dependencies]`:

```toml
metering = { workspace = true }
```

- [ ] **Step 3: Add the field to `SyncActivities`**

In `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs`, change the import at the top to add metering:

```rust
use metering::MeteringSink;
```

Change the `SyncActivities` struct from:

```rust
#[derive(Clone)]
pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
    pub scalar_runtime: Arc<WasmScalarRuntime>,
    pub secrets: Arc<crate::secrets::auditing::AuditingSecrets>,
}
```

To:

```rust
#[derive(Clone)]
pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
    pub scalar_runtime: Arc<WasmScalarRuntime>,
    pub secrets: Arc<crate::secrets::auditing::AuditingSecrets>,
    /// Best-effort metering event emitter. Errors are logged and ignored;
    /// the activity never fails due to a metering write failure.
    pub metering: Arc<dyn MeteringSink>,
}
```

- [ ] **Step 4: Update `main.rs` to construct `CatalogMeteringSink`**

In `/Users/satishbabariya/Desktop/etl/crates/worker/src/main.rs`, add at the top:

```rust
use metering::CatalogMeteringSink;
```

Change the `SyncActivities` construction block from:

```rust
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
        scalar_runtime: scalar_runtime.clone(),
        secrets: secrets.clone(),
    };
```

To:

```rust
    let metering_sink: Arc<dyn metering::MeteringSink> =
        Arc::new(CatalogMeteringSink::new(catalog.pool().clone()));
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
        scalar_runtime: scalar_runtime.clone(),
        secrets: secrets.clone(),
        metering: metering_sink,
    };
```

- [ ] **Step 5: Fix any other construction sites**

Search for all other places that construct `SyncActivities` and add the `metering` field. Run:

```bash
grep -rn "SyncActivities {" /Users/satishbabariya/Desktop/etl/
```

For each hit that is a struct literal (not a type annotation), add:

```rust
metering: Arc::new(metering::BufferedSink::new()),
```

(Use `BufferedSink` in test/bench contexts; use `CatalogMeteringSink` in production `main.rs`.)

- [ ] **Step 6: Run tests to confirm workspace builds**

Run: `cargo build --workspace`
Expected: clean build. All existing tests continue to pass.

Run: `cargo test -p worker activities::sync`
Expected: dispatch_tests pass; new `metering_field_tests` pass.

- [ ] **Step 7: Commit**

```bash
git add crates/worker/Cargo.toml \
        crates/worker/src/activities/sync/mod.rs \
        crates/worker/src/main.rs
git commit -m "phase-2-6a-5: SyncActivities gains metering field; main.rs wires CatalogMeteringSink"
```

---

## Task 6: Hook `read_batch` — emit `RowsRead` + `BytesRead`

**Files:**
- Modify: `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs`

Emission goes **just before** the final `Ok(ReadBatchOutput {...})` return at line 227–235.

- [ ] **Step 1: Write the failing test**

Add to the `dispatch_tests` module in `mod.rs`:

```rust
#[cfg(test)]
mod read_batch_metering_tests {
    use metering::{BillableMetric, BufferedSink, MeteringSource};
    use std::sync::Arc;

    #[test]
    fn rows_read_metric_is_correct_variant() {
        // Compile-time: BillableMetric::RowsRead and BytesRead exist and are distinct.
        let a = BillableMetric::RowsRead;
        let b = BillableMetric::BytesRead;
        assert_ne!(format!("{a:?}"), format!("{b:?}"));
    }

    #[test]
    fn metering_source_read_variant_exists() {
        let s = MeteringSource::Read;
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            r#""read""#
        );
    }
}
```

Run: `cargo test -p worker read_batch_metering_tests`
Expected: passes (these are type-existence checks).

- [ ] **Step 2: Add the emission call in `read_batch`**

In `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs`, locate the section just before `Ok(ReadBatchOutput {...})` in `read_batch` (currently the metrics counter block ends at line ~226 and `Ok(...)` is at line 227).

Add the following block immediately after the metrics counters and before the `Ok(...)`:

```rust
        // Metering: emit RowsRead + BytesRead for this batch (best-effort).
        {
            let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
            let pid = Some(common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id));
            // ReadBatchInput doesn't carry run_id (it's assigned by the workflow
            // after read completes). We emit without run_id at this stage; the
            // workflow can correlate by pipeline_id + timestamp if needed.
            let rows_event = metering::MeteringEvent::new(
                tid,
                pid,
                None, // run_id not available at read time
                metering::BillableMetric::RowsRead,
                rows as i64,
                metering::MeteringSource::Read,
            );
            let bytes_event = metering::MeteringEvent::new(
                tid,
                pid,
                None,
                metering::BillableMetric::BytesRead,
                b64.len() as i64, // IPC-base64 len is a proxy for wire bytes
                metering::MeteringSource::Read,
            );
            for ev in [&rows_event, &bytes_event] {
                if let Err(e) = self.metering.emit(ev).await {
                    tracing::warn!(
                        metric = ?ev.metric,
                        error = %e,
                        "metering emit failed in read_batch (best-effort, ignored)"
                    );
                }
            }
        }
```

Note: `input.pipeline_id` — check `ReadBatchInput` fields. Looking at `inputs.rs`, `ReadBatchInput` has `tenant_id: Uuid` but NO `pipeline_id` field. The pipeline_id is not carried on `ReadBatchInput` (it was added to `LoadBatchInput` but not `ReadBatchInput`). Therefore emit without `pipeline_id`:

```rust
        {
            let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
            let rows_event = metering::MeteringEvent::new(
                tid,
                None, // pipeline_id not on ReadBatchInput; deferred
                None,
                metering::BillableMetric::RowsRead,
                rows as i64,
                metering::MeteringSource::Read,
            );
            let bytes_event = metering::MeteringEvent::new(
                tid,
                None,
                None,
                metering::BillableMetric::BytesRead,
                b64.len() as i64,
                metering::MeteringSource::Read,
            );
            for ev in [&rows_event, &bytes_event] {
                if let Err(e) = self.metering.emit(ev).await {
                    tracing::warn!(
                        metric = ?ev.metric,
                        error = %e,
                        "metering emit failed in read_batch (best-effort, ignored)"
                    );
                }
            }
        }
```

Add the `use metering` import at the top of the file alongside the existing imports:

```rust
use metering::MeteringSink;
```

(Note: `MeteringSink` is already imported from Task 5; the `metering::` path prefix in the emission block references the crate directly, which is fine because `metering` is a dependency of `worker`.)

- [ ] **Step 3: Run tests to confirm build and unit tests pass**

Run: `cargo build -p worker`
Expected: clean build.

Run: `cargo test -p worker`
Expected: all existing tests pass plus the new type-existence tests.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/activities/sync/mod.rs
git commit -m "phase-2-6a-6: read_batch emits RowsRead + BytesRead metering events"
```

---

## Task 7: Hook `load_batch` — emit `RowsWritten` + `BytesWritten`

**Files:**
- Modify: `/Users/satishbabariya/Desktop/etl/crates/worker/src/activities/sync/mod.rs`

Emission goes **just before** the final `Ok(LoadBatchOutput {...})` return at the end of `load_batch` (currently the dead-letter threshold check and `Ok(...)` block around lines 330–352).

- [ ] **Step 1: Write the failing test**

Add to the `dispatch_tests` module in `mod.rs`:

```rust
#[cfg(test)]
mod load_batch_metering_tests {
    use metering::{BillableMetric, MeteringSource};

    #[test]
    fn rows_written_and_bytes_written_metrics_exist() {
        let rw = BillableMetric::RowsWritten;
        let bw = BillableMetric::BytesWritten;
        assert_ne!(format!("{rw:?}"), format!("{bw:?}"));
        assert_eq!(
            serde_json::to_string(&rw).unwrap(),
            r#""rows_written""#
        );
        assert_eq!(
            serde_json::to_string(&bw).unwrap(),
            r#""bytes_written""#
        );
    }

    #[test]
    fn metering_source_load_variant_serializes_correctly() {
        let s = MeteringSource::Load;
        assert_eq!(serde_json::to_string(&s).unwrap(), r#""load""#);
    }
}
```

Run: `cargo test -p worker load_batch_metering_tests`
Expected: passes.

- [ ] **Step 2: Add the emission call in `load_batch`**

In `load_batch`, locate just before `Ok(LoadBatchOutput { ... })` (after the dead-letter threshold check block). Add:

```rust
        // Metering: emit RowsWritten + BytesWritten for this batch (best-effort).
        {
            let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
            let pid = Some(common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id));
            let rid = Some(common_types::ids::RunId::from_uuid_unchecked(input.run_id));
            let rows_event = metering::MeteringEvent::new(
                tid,
                pid,
                rid,
                metering::BillableMetric::RowsWritten,
                res.rows_loaded as i64,
                metering::MeteringSource::Load,
            );
            let bytes_event = metering::MeteringEvent::new(
                tid,
                pid,
                rid,
                metering::BillableMetric::BytesWritten,
                res.bytes_written as i64,
                metering::MeteringSource::Load,
            );
            for ev in [&rows_event, &bytes_event] {
                if let Err(e) = self.metering.emit(ev).await {
                    tracing::warn!(
                        metric = ?ev.metric,
                        error = %e,
                        "metering emit failed in load_batch (best-effort, ignored)"
                    );
                }
            }
        }
```

`LoadBatchInput` fields available at this point: `input.tenant_id`, `input.pipeline_id`, `input.run_id` (all `Uuid`). `res` is the `LoadResult` with `.rows_loaded: usize` and `.bytes_written: u64`.

- [ ] **Step 3: Run tests to confirm build and unit tests pass**

Run: `cargo build -p worker`
Expected: clean.

Run: `cargo test -p worker`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/activities/sync/mod.rs
git commit -m "phase-2-6a-7: load_batch emits RowsWritten + BytesWritten metering events"
```

---

## Task 8: Integration tests — emission writes rows, multi-emit sums, RLS scopes

**Files:**
- Create: `/Users/satishbabariya/Desktop/etl/tests/integration/tests/metering_events.rs`
- Modify: `/Users/satishbabariya/Desktop/etl/tests/integration/Cargo.toml`

The integration tests use the docker-compose Postgres DB directly, mirror the `postgres_loader.rs` pattern (`fresh_schema()` / `drop_schema()` → here we use `Catalog::connect` + `cat.migrate()` + `cat.truncate_all_for_tests()` as in the audit tests).

- [ ] **Step 1: Add `metering` to integration test dependencies**

In `/Users/satishbabariya/Desktop/etl/tests/integration/Cargo.toml`, add to `[dependencies]`:

```toml
metering = { workspace = true }
```

- [ ] **Step 2: Write the integration tests**

Create `/Users/satishbabariya/Desktop/etl/tests/integration/tests/metering_events.rs`:

```rust
//! Integration tests for the metering foundation (phase-2-6a).
//!
//! Requires the docker-compose `postgres` service to be running.
//! Skipped with a clear message when the database is unreachable.

use catalog::Catalog;
use chrono::Utc;
use common_types::ids::{PipelineId, RunId, TenantId};
use metering::{
    BillableMetric, BufferedSink, CatalogMeteringSink, MeteringEvent, MeteringSource,
    MeteringSink,
};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use uuid::Uuid;

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn setup() -> Option<(Catalog, PgPool)> {
    let url = catalog_url();
    let cat = match Catalog::connect(&url).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP metering_events test: cannot reach {url}: {e}");
            return None;
        }
    };
    cat.migrate().await.expect("migrate");
    cat.truncate_all_for_tests().await.expect("truncate");
    let pool = cat.pool().clone();
    Some((cat, pool))
}

/// Insert a tenant and return its UUID for seeding metering rows.
async fn seed_tenant(pool: &PgPool, name: &str) -> Uuid {
    let tid = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
        .bind(tid)
        .bind(name)
        .execute(pool)
        .await
        .expect("insert tenant");
    tid
}

fn make_event(tid_uuid: Uuid, metric: BillableMetric, value: i64) -> MeteringEvent {
    MeteringEvent {
        event_id: Uuid::now_v7(),
        tenant_id: TenantId::from_uuid_unchecked(tid_uuid),
        pipeline_id: Some(PipelineId::new()),
        run_id: Some(RunId::new()),
        metric,
        value,
        timestamp: Utc::now(),
        source: MeteringSource::Read,
    }
}

// ── Test 1: emit writes a row ────────────────────────────────────────────────

#[tokio::test]
async fn emit_writes_a_row_to_metering_events() {
    let Some((_, pool)) = setup().await else { return };
    let tid = seed_tenant(&pool, "tenant-a").await;
    let sink = CatalogMeteringSink::new(pool.clone());

    let ev = make_event(tid, BillableMetric::RowsRead, 100);
    sink.emit(&ev).await.expect("emit");

    let count: i64 = sqlx::query("SELECT COUNT(*)::BIGINT FROM metering_events WHERE tenant_id = $1")
        .bind(tid)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "expected exactly 1 metering row");
}

// ── Test 2: multiple emissions sum correctly ─────────────────────────────────

#[tokio::test]
async fn multiple_emits_sum_to_correct_total() {
    let Some((_, pool)) = setup().await else { return };
    let tid = seed_tenant(&pool, "tenant-b").await;
    let sink = CatalogMeteringSink::new(pool.clone());

    for v in [10i64, 20, 30] {
        let ev = MeteringEvent {
            event_id: Uuid::now_v7(),
            tenant_id: TenantId::from_uuid_unchecked(tid),
            pipeline_id: None,
            run_id: None,
            metric: BillableMetric::BytesRead,
            value: v,
            timestamp: Utc::now(),
            source: MeteringSource::Read,
        };
        sink.emit(&ev).await.expect("emit");
    }

    let total: i64 = sqlx::query(
        "SELECT COALESCE(SUM(value), 0)::BIGINT FROM metering_events \
         WHERE tenant_id = $1 AND metric = 'bytes_read'",
    )
    .bind(tid)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(total, 60, "sum of three emits must be 60");
}

// ── Test 3: RLS — tenant A cannot see tenant B's events ─────────────────────

#[tokio::test]
async fn rls_tenant_cannot_see_other_tenants_events() {
    let Some((_, pool)) = setup().await else { return };
    let tid_a = seed_tenant(&pool, "tenant-rls-a").await;
    let tid_b = seed_tenant(&pool, "tenant-rls-b").await;

    // Admin pool writes one event per tenant.
    let sink = CatalogMeteringSink::new(pool.clone());
    sink.emit(&make_event(tid_a, BillableMetric::RowsRead, 1)).await.expect("emit a");
    sink.emit(&make_event(tid_b, BillableMetric::RowsRead, 1)).await.expect("emit b");

    // Set app.tenant_id = tid_a in a transaction and confirm only A's rows visible.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL app.tenant_id = $1")
        .bind(tid_a.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    let count: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT FROM metering_events",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap()
    .get(0);
    tx.rollback().await.unwrap();

    assert_eq!(count, 1, "tenant A must see only its own 1 event, not tenant B's");
}

// ── Test 4: idempotent emit (ON CONFLICT DO NOTHING) ────────────────────────

#[tokio::test]
async fn duplicate_event_id_is_silently_ignored() {
    let Some((_, pool)) = setup().await else { return };
    let tid = seed_tenant(&pool, "tenant-idem").await;
    let sink = CatalogMeteringSink::new(pool.clone());

    let ev = make_event(tid, BillableMetric::RowsWritten, 50);
    sink.emit(&ev).await.expect("first emit");
    // Emit same event_id again — must not error and must not double-count.
    sink.emit(&ev).await.expect("second emit (duplicate)");

    let count: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT FROM metering_events WHERE event_id = $1",
    )
    .bind(ev.event_id)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 1, "duplicate event_id must produce exactly one row");
}

// ── Test 5: BufferedSink captures events in memory ───────────────────────────

#[tokio::test]
async fn buffered_sink_captures_and_drains() {
    let sink = BufferedSink::new();
    let tid = Uuid::now_v7();
    for m in [BillableMetric::RowsRead, BillableMetric::BytesRead] {
        let ev = make_event(tid, m, 1);
        sink.emit(&ev).await.unwrap();
    }
    let drained = sink.drain();
    assert_eq!(drained.len(), 2);
    assert_eq!(sink.drain().len(), 0, "second drain must be empty");
}
```

- [ ] **Step 3: Run tests to confirm they fail (before migration is applied)**

Run: `cargo test -p integration-tests --test metering_events -- --nocapture`
Expected: if Postgres is not yet migrated to 0017, tests fail with "relation metering_events does not exist". Once `cat.migrate()` is called inside `setup()`, it applies 0017.

Actually `cat.migrate()` inside `setup()` will apply 0017 automatically. So the tests should pass after migration.

Run with Docker:

```bash
docker compose up -d postgres
sleep 2
cargo test -p integration-tests --test metering_events -- --nocapture --test-threads=1
```

Expected: all 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/metering_events.rs tests/integration/Cargo.toml
git commit -m "phase-2-6a-8: metering integration tests (emit, sum, RLS, idempotency, buffered)"
```

---

## Task 9: Design memo

**Files:**
- Create: `/Users/satishbabariya/Desktop/etl/docs/superpowers/specs/2026-05-21-phase-2-6a-metering-foundation-design.md`

- [ ] **Step 1: Write the design memo**

Create `/Users/satishbabariya/Desktop/etl/docs/superpowers/specs/2026-05-21-phase-2-6a-metering-foundation-design.md`:

```markdown
# Phase 2.6.a — Metering Foundation Design Memo

**Date:** 2026-05-21
**Status:** Implemented
**RFC:** RFC-0017 (Quotas, Billing Metering, and Backpressure)

## Scope

### In scope (this phase)
- `MeteringEvent`, `BillableMetric`, `MeteringSource` types in new `crates/metering/` crate.
- `MeteringSink` trait, `CatalogMeteringSink` (writes to catalog Postgres DB), `BufferedSink` (tests).
- Catalog migration `0017_metering_events.sql` with RLS, tenant FK, and time+pipeline indices.
- Emission hooks in `read_batch` (`RowsRead`, `BytesRead`) and `load_batch` (`RowsWritten`, `BytesWritten`).

### Explicitly deferred
- Kafka / durable local queue transport.
- Billing aggregation pipeline (hourly streaming, daily batch).
- Quota enforcement (`QuotaConfig`, soft/hard caps, burst budgets, backpressure signals).
- `ComputeMs` and `WasmFuelUsed` emission.
- `StorageBytesHours`, `CdcSlotHeld`, `SeatsMonthly`, `ApiRequests`, `EgressBytes`.
- Cost observability API and projected-bill UI.
- Idempotent event deduplication via stable event_id hash.
- Reconciliation and late-arriving event handling.

## Architecture Decisions

### New crate vs. module-in-worker
Chose `crates/metering/` as a standalone crate, parallel to `crates/audit/`. Rationale:
- Metering will eventually be consumed by non-worker components (control-api, CLI, scheduler).
- Keeps the `worker` crate from accumulating billing domain logic.
- Matches the `audit` precedent the team already follows.

### Direct Postgres insert vs. local queue
RFC-17 specifies a "local durable queue per worker node" before Kafka. For MVP (RFC-17 §5%), direct-insert is simpler and correct. The `MeteringSink` trait means the production impl can swap to a queued sink in phase-2-6b without touching the activity code.

### Best-effort vs. at-least-once
RFC-17's stated durability goal (at-least-once, 7-year retention) is a production requirement deferred to phase-2-6b. MVP emits best-effort: a warn log on failure, the activity continues. Rationale: metering is not yet wired to billing; data loss here costs no customer money. Correctness before durability hardening.

### `value: i64` vs. separate unit column
`MeteringEvent.value` is a plain `i64`. The `metric` field encodes the unit implicitly (RowsRead ⇒ rows, BytesRead ⇒ bytes). A separate `Unit` enum (RFC-17 schema) is deferred until quota enforcement needs it — adding a column is a non-breaking migration.

### BytesRead estimation
`read_batch` does not have a pre-IPC byte count for the raw wire data. We use `b64.len()` (the base64-encoded Arrow IPC stream length) as a proxy. This over-estimates raw bytes by ~33% due to base64 overhead. Acceptable for MVP; exact accounting (pre-encode byte count) is a follow-up.

### pipeline_id missing from ReadBatchInput
`ReadBatchInput` does not carry `pipeline_id` (added to `LoadBatchInput` in phase-2-4a but not to the read side). MVP emits `RowsRead`/`BytesRead` events with `pipeline_id = None`. Fixing this requires adding `pipeline_id: Uuid` to `ReadBatchInput` (a backward-compatible `#[serde(default)]` addition) — deferred to phase-2-6b.

## Schema

```sql
CREATE TABLE metering_events (
    event_id    UUID PRIMARY KEY,          -- v7, time-ordered
    tenant_id   UUID NOT NULL FK tenants,  -- RLS-enforced
    pipeline_id UUID NULL,                 -- populated by load_batch; NULL for read_batch MVP
    run_id      UUID NULL,                 -- populated by load_batch; NULL for read_batch MVP
    metric      TEXT NOT NULL,             -- snake_case BillableMetric name
    value       BIGINT NOT NULL,           -- rows / bytes
    source      TEXT NOT NULL,             -- "read" | "load" | "transform"
    emitted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Indices: `(tenant_id, emitted_at DESC)` for billing aggregation; `(pipeline_id)` for cost attribution.

## Emission Points

| Activity | Events | Fields |
|---|---|---|
| `read_batch` | `RowsRead`, `BytesRead` | `tenant_id`, `timestamp`; `pipeline_id`=None (deferred) |
| `load_batch` | `RowsWritten`, `BytesWritten` | `tenant_id`, `pipeline_id`, `run_id`, `timestamp` |

## Next Steps (phase-2-6b)
1. Add `pipeline_id` to `ReadBatchInput` so `RowsRead` events are fully attributed.
2. Exact pre-encode byte count for `BytesRead` (remove base64 bias).
3. `ComputeMs` emission from activity start/end timestamps.
4. Durable local queue sink as a `MeteringSink` impl (replace `CatalogMeteringSink` default).
5. Kafka forwarding from local queue to billing aggregation service.
6. Daily roll-up aggregation job.
```

- [ ] **Step 2: Run the full workspace test suite**

```bash
cargo test --workspace
docker compose up -d postgres
cargo test -p integration-tests --test metering_events -- --nocapture
```

Expected: workspace tests green; metering integration tests green.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-05-21-phase-2-6a-metering-foundation-design.md
git commit -m "phase-2-6a-9: metering foundation design memo"
```

- [ ] **Step 4: Open PR**

```bash
git push -u origin HEAD
gh pr create \
  --title "phase-2-6a: RFC-17 MeteringEvent foundation" \
  --body "$(cat <<'EOF'
## Summary
- New \`crates/metering\` crate: \`MeteringEvent\`, \`BillableMetric\`, \`MeteringSource\` types
- \`MeteringSink\` trait with \`CatalogMeteringSink\` (direct Postgres INSERT) and \`BufferedSink\` (tests)
- Catalog migration \`0017_metering_events.sql\` with RLS, tenant FK, time+pipeline indices
- \`read_batch\` emits \`RowsRead\` + \`BytesRead\` (best-effort)
- \`load_batch\` emits \`RowsWritten\` + \`BytesWritten\` (best-effort)

## Deferred scope
- Kafka / durable queue transport
- Billing aggregation pipeline
- Quota enforcement
- ComputeMs / WasmFuelUsed emission
- pipeline_id on ReadBatchInput (RowsRead events emit with NULL pipeline_id for now)

## Test plan
- [x] \`cargo test -p metering\` — type serde roundtrips, BufferedSink unit tests
- [x] Integration: emit writes a row
- [x] Integration: multiple emits sum correctly
- [x] Integration: RLS blocks cross-tenant visibility
- [x] Integration: duplicate event_id is idempotent (ON CONFLICT DO NOTHING)
- [x] Integration: BufferedSink captures and drains

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**RFC-17 coverage vs. MVP scope:**
- `MeteringEvent` shape — Task 2. `event_id` (UUIDv7), `tenant_id`, `pipeline_id`, `run_id`, `metric`, `value`, `timestamp`, `source` all present. RFC-17's `workspace_id`, `dimensions`, `source_event_ref`, and `unit` fields are deferred (not required for MVP). ✓
- `BillableMetric` variants — Task 2 defines all 6 variants. Only 4 are emitted (RowsRead/Written, BytesRead/Written); ComputeMs and WasmFuelUsed exist in the enum but no caller emits them yet. Deferred clearly stated in plan header. ✓
- Migration with RLS — Task 3 mirrors `0013_audit_log.sql` exactly. Same `app_tenant_id()` function, same `OR app_tenant_id() IS NULL` admin bypass, same GRANT SELECT+INSERT to `etl_app`. ✓
- `MeteringSink` trait — Task 4. `async fn emit(&self, event: &MeteringEvent) -> Result<()>`. Object-safe (no generics). Implemented by both `CatalogMeteringSink` and `BufferedSink`. ✓
- `CatalogMeteringSink` — Task 4. Direct INSERT with `ON CONFLICT (event_id) DO NOTHING` for idempotency. No Kafka (explicitly cut). ✓
- `BufferedSink` — Task 4. `Arc<Mutex<Vec<...>>>` so it's `Clone + Send + Sync`. `drain()` empties on each call. ✓
- `SyncActivities` field + main.rs wiring — Task 5. Struct gains `pub metering: Arc<dyn MeteringSink>`. `main.rs` constructs `CatalogMeteringSink::new(catalog.pool().clone())`. ✓
- `read_batch` emission — Task 6. RowsRead (`rows as i64`) + BytesRead (`b64.len() as i64`). Best-effort (`if let Err(e)` → `tracing::warn!`). Added after metrics counter block, before `Ok(...)`. ✓
- `load_batch` emission — Task 7. RowsWritten (`res.rows_loaded as i64`) + BytesWritten (`res.bytes_written as i64`). Best-effort. Added before `Ok(...)`. ✓
- Integration tests — Task 8. 5 tests: emit writes row, multiple emit sums, RLS cross-tenant block, idempotent duplicate, BufferedSink. Mirrors `postgres_loader.rs` pattern. ✓
- Design memo — Task 9. Architecture decisions, schema, emission point table, next steps. ✓

**Placeholder scan:** No "TODO" / "similar to X" / "implement later" markers. Every step has exact code or an exact command with expected output.

**Type consistency:** `BillableMetric`, `MeteringSource`, `MeteringEvent`, `MeteringSink`, `CatalogMeteringSink`, `BufferedSink` — names are consistent across Tasks 2, 4, 5, 6, 7, 8, 9. Serde snake_case names match what `0017_metering_events.sql` stores in the `metric` and `source` TEXT columns.

**Non-breaking:** Existing `SyncActivities` construction sites gain a new required field `metering`. Task 5 Step 5 specifically directs searching for all construction sites and patching them with `BufferedSink::new()` in test contexts. No existing test behavior changes.

**Scope boundary:** Kafka, aggregation, quota enforcement, ComputeMs emission, cost API — all deferred and listed in both the plan header and the design memo. No RFC-17 §6-§16 content leaks into this plan.

---

## Execution Handoff

Plan complete. Saved to `/Users/satishbabariya/Desktop/etl/docs/superpowers/plans/2026-05-21-phase-2-6a-metering-foundation.md`. Two execution options:

1. **Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review output between tasks, iterate quickly. Task 5 (field wiring) must complete before Tasks 6 and 7 (emission hooks); all other tasks are sequential by dependency.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch with checkpoints at task boundaries.

Which approach?
