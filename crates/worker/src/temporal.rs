use anyhow::Context;
use std::str::FromStr;
use std::sync::Arc;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

pub struct TemporalConfig {
    pub address: String,
    pub namespace: String,
    pub task_queue: String,
}

impl TemporalConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            address: std::env::var("TEMPORAL_ADDRESS")
                .unwrap_or_else(|_| "127.0.0.1:7233".into()),
            namespace: std::env::var("TEMPORAL_NAMESPACE")
                .unwrap_or_else(|_| "default".into()),
            task_queue: std::env::var("TEMPORAL_TASK_QUEUE")
                .unwrap_or_else(|_| "pipeline-default".into()),
        })
    }

    pub fn url(&self) -> anyhow::Result<Url> {
        let s = if self.address.starts_with("http") {
            self.address.clone()
        } else {
            format!("http://{}", self.address)
        };
        Url::from_str(&s).context("parsing TEMPORAL_ADDRESS as URL")
    }
}

pub fn make_runtime() -> anyhow::Result<Arc<CoreRuntime>> {
    let telemetry_options = TelemetryOptions::builder().build();
    let runtime_options = RuntimeOptions::builder()
        .telemetry_options(telemetry_options)
        .build()
        .map_err(|e| anyhow::anyhow!("building RuntimeOptions: {e}"))?;
    Ok(Arc::new(CoreRuntime::new_assume_tokio(runtime_options)?))
}

pub async fn make_client(cfg: &TemporalConfig) -> anyhow::Result<Client> {
    let conn_options = ConnectionOptions::new(cfg.url()?).build();
    let connection = Connection::connect(conn_options)
        .await
        .context("connecting to Temporal")?;
    let client_options = ClientOptions::new(cfg.namespace.clone()).build();
    Client::new(connection, client_options).context("building Client")
}
