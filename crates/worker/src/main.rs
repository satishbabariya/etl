use anyhow::Context;
use catalog::Catalog;
use std::sync::Arc;
use temporalio_common::worker::{
    WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes,
};
use temporalio_sdk::{Worker, WorkerOptions};
use worker::{
    activities::run_lifecycle::RunLifecycleActivities,
    activities::sync::SyncActivities,
    temporal::{make_client, make_runtime, TemporalConfig},
    wasm_runtime::WasmSourceRuntime,
    workflows::PipelineRunWorkflow,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Arc::new(Catalog::connect(&db_url).await?);
    catalog.migrate().await?;

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

    let lifecycle = RunLifecycleActivities {
        catalog: catalog.clone(),
    };
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
    };

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
        .register_workflow::<PipelineRunWorkflow>()
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
