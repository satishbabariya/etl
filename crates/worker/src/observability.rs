//! HTTP endpoints on the metrics port: `/metrics`, `/healthz`, `/readyz`.

use axum::{extract::State, http::StatusCode, routing::get, Router};
use catalog::Catalog;
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
struct ObsState {
    metrics: PrometheusHandle,
    catalog: Arc<Catalog>,
}

pub fn spawn_metrics_endpoint(handle: PrometheusHandle, bind: SocketAddr, catalog: Arc<Catalog>) {
    let state = ObsState {
        metrics: handle,
        catalog,
    };
    let app = Router::new()
        .route(
            "/metrics",
            get(|State(s): State<ObsState>| async move { s.metrics.render() }),
        )
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(readyz))
        .with_state(state);
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

async fn readyz(State(s): State<ObsState>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(s.catalog.pool()).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
