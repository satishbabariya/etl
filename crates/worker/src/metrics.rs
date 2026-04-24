//! Metric name constants + registry setup. Single source of truth
//! so the /metrics endpoint and the emitting call sites can't drift.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;

// Counters
pub const RUN_STARTED: &str = "etl_runs_started_total";
pub const RUN_COMPLETED: &str = "etl_runs_completed_total";
pub const RUN_FAILED: &str = "etl_runs_failed_total";
pub const ROWS_READ: &str = "etl_rows_read_total";
pub const ROWS_LOADED: &str = "etl_rows_loaded_total";
pub const ROWS_REJECTED: &str = "etl_rows_rejected_total";
pub const CDC_EVENTS: &str = "etl_cdc_events_total";

// Gauges
pub const CDC_SLOT_LAG_BYTES: &str = "etl_cdc_slot_lag_bytes";

// Histograms (seconds)
pub const ACTIVITY_DURATION: &str = "etl_activity_duration_seconds";

/// Build the Prometheus recorder and install it globally. Returns a
/// handle the `/metrics` route renders on demand.
pub fn init_recorder(_bind: SocketAddr) -> anyhow::Result<PrometheusHandle> {
    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("install prometheus recorder: {e}"))?;
    Ok(handle)
}
