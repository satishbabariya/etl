//! MeteringSink trait and implementations.
//!
//! `CatalogMeteringSink` writes directly to the catalog DB's `metering_events`
//! table. Best-effort: callers log a warning on failure and continue.
//!
//! `BufferedSink` is an in-memory sink for tests.

use crate::event::MeteringEvent;
use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use std::sync::{Arc, Mutex};

/// Abstraction over where metering events go.
#[async_trait]
pub trait MeteringSink: Send + Sync {
    async fn emit(&self, event: &MeteringEvent) -> Result<()>;
}

// ── CatalogMeteringSink ──────────────────────────────────────────────────────

/// Writes metering events directly to the catalog Postgres DB.
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

// ── BufferedSink ─────────────────────────────────────────────────────────────

/// In-memory sink for unit and integration tests.
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
        assert_eq!(sink.drain().len(), 2);
    }

    #[tokio::test]
    async fn buffered_sink_drain_clears_buffer() {
        let sink = BufferedSink::new();
        sink.emit(&sample_event()).await.unwrap();
        assert_eq!(sink.drain().len(), 1);
        assert_eq!(sink.drain().len(), 0, "buffer must be empty after drain");
    }

    #[tokio::test]
    async fn buffered_sink_sum_values_by_metric() {
        let sink = BufferedSink::new();
        let tid = TenantId::new();
        for v in [10i64, 20, 30] {
            let ev = MeteringEvent::new(
                tid.clone(),
                None,
                None,
                BillableMetric::RowsRead,
                v,
                MeteringSource::Read,
            );
            sink.emit(&ev).await.unwrap();
        }
        let total: i64 = sink.drain().iter().map(|e| e.value).sum();
        assert_eq!(total, 60);
    }
}
