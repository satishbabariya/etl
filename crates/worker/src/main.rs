use anyhow::Context;
use catalog::Catalog;
use std::sync::Arc;
use temporalio_common::worker::{
    WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes,
};
use temporalio_sdk::{Worker, WorkerOptions};
use worker::{
    activities::cdc::CdcActivities,
    activities::run_lifecycle::RunLifecycleActivities,
    activities::sync::SyncActivities,
    temporal::{make_client, make_runtime, TemporalConfig},
    wasm_runtime::{WasmScalarRuntime, WasmSourceRuntime},
    workflows::{CdcPipelineWorkflow, PipelineRunWorkflow},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let metrics_bind: std::net::SocketAddr = std::env::var("ETL_METRICS_BIND")
        .unwrap_or_else(|_| "0.0.0.0:9898".into())
        .parse()
        .context("ETL_METRICS_BIND must be host:port")?;
    let prom_handle = worker::metrics::init_recorder(metrics_bind)?;
    worker::observability::spawn_metrics_endpoint(prom_handle, metrics_bind);

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let app_url = std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| db_url.replace("etl:etl@", "etl_app:etl_app@"));

    // Migrations run as superuser; app paths use etl_app for RLS enforcement.
    {
        let admin = Catalog::connect(&db_url).await?;
        admin.migrate().await?;
    }
    let catalog = Arc::new(Catalog::connect_app(&app_url).await?);

    let cfg = TemporalConfig::from_env()?;
    tracing::info!(
        address = %cfg.address,
        namespace = %cfg.namespace,
        task_queue = %cfg.task_queue,
        "worker booting",
    );

    let runtime = make_runtime()?;
    let client = make_client(&cfg).await?;

    let wasm_base = std::env::var("ETL_CONNECTORS_DIR").unwrap_or_else(|_| "./connectors".into());
    let wasm_runtime = WasmSourceRuntime::new(&wasm_base)?;
    let scalar_runtime = WasmScalarRuntime::new(&wasm_base)?;

    let lifecycle = RunLifecycleActivities {
        catalog: catalog.clone(),
    };
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
        scalar_runtime: scalar_runtime.clone(),
    };
    let cdc = CdcActivities { catalog: catalog.clone() };

    // Slot-lag poller: resolves each active slot's source URL via the
    // catalog and publishes etl_cdc_slot_lag_bytes every 15s.
    let cat_for_resolver = catalog.clone();
    let source_url_resolver = move |pid: uuid::Uuid| -> Option<String> {
        let cat = cat_for_resolver.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let row: Option<(serde_json::Value,)> = sqlx::query_as(
                    "SELECT c.config FROM pipelines p \
                     JOIN connections c ON c.connection_id = p.source_conn_id \
                     WHERE p.pipeline_id = $1",
                )
                .bind(pid)
                .fetch_optional(cat.pool())
                .await
                .ok()
                .flatten();
                row.and_then(|(cfg,)| {
                    cfg.get("url").and_then(|v| v.as_str()).map(|s| s.to_string())
                })
            })
        })
    };
    worker::cdc_monitor::spawn_slot_lag_poller(
        catalog.clone(),
        source_url_resolver,
        std::time::Duration::from_secs(15),
    );

    let worker_options = WorkerOptions::new(cfg.task_queue.clone())
        .task_types(WorkerTaskTypes::all())
        .deployment_options(WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "etl".to_owned(),
                build_id: "etl-worker-0.2".to_owned(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        })
        .register_activities(lifecycle)
        .register_activities(sync)
        .register_activities(cdc)
        .register_workflow::<PipelineRunWorkflow>()
        .register_workflow::<CdcPipelineWorkflow>()
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("Worker::new: {e}"))?;
    tracing::info!("worker polling");
    worker
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Worker::run: {e}"))?;
    Ok(())
}
