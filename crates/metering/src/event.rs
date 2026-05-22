//! Core metering types — RFC-17 §"Event shape".

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
        assert_eq!(serde_json::to_string(&BillableMetric::RowsRead).unwrap(), r#""rows_read""#);
        assert_eq!(serde_json::to_string(&BillableMetric::BytesWritten).unwrap(), r#""bytes_written""#);
        assert_eq!(serde_json::to_string(&BillableMetric::WasmFuelUsed).unwrap(), r#""wasm_fuel_used""#);
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
}
