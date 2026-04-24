//! `/metrics` HTTP endpoint. Spawns a tokio task; errors are logged
//! but don't bring down the worker.

use axum::{routing::get, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;

pub fn spawn_metrics_endpoint(handle: PrometheusHandle, bind: SocketAddr) {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let h = handle.clone();
            async move { h.render() }
        }),
    );
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(bind).await {
            Ok(listener) => {
                tracing::info!(%bind, "metrics endpoint listening");
                if let Err(e) = axum::serve(listener, app).await {
                    tracing::error!(error = %e, "metrics endpoint exited");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, %bind, "metrics endpoint bind failed");
            }
        }
    });
}
