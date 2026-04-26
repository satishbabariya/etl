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

    let raw_secrets: Arc<dyn worker::secrets::Secrets> =
        Arc::new(worker::secrets::DispatchSecrets {
            env: worker::secrets::env::EnvSecrets,
            file: worker::secrets::file::FileSecrets::new(),
            vault: worker::secrets::vault::VaultSecrets::from_env()?,
        });
    let secrets = Arc::new(worker::secrets::auditing::AuditingSecrets::new(
        raw_secrets,
        catalog.clone(),
    ));

    let lifecycle = RunLifecycleActivities {
        catalog: catalog.clone(),
    };
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
        scalar_runtime: scalar_runtime.clone(),
        secrets: secrets.clone(),
    };
    let cdc = CdcActivities {
        catalog: catalog.clone(),
        secrets: secrets.clone(),
    };

    // Slot-lag poller: resolves each active slot's source URL via the
    // catalog and publishes etl_cdc_slot_lag_bytes every 15s.
    let cat_for_resolver = catalog.clone();
    let secrets_for_resolver = secrets.clone();
    let source_url_resolver = move |pid: uuid::Uuid| -> Option<String> {
        let cat = cat_for_resolver.clone();
        let secrets = secrets_for_resolver.clone();
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
                let cfg = row?.0;
                let conn: common_types::connection_config::ConnectionConfig =
                    serde_json::from_value(cfg).ok()?;
                let resolved =
                    worker::secrets::resolve_connection(secrets.as_ref(), &conn).await.ok()?;
                Some(resolved.expect_url().to_string())
            })
        })
    };
    worker::cdc_monitor::spawn_slot_lag_poller(
        catalog.clone(),
        source_url_resolver,
        std::time::Duration::from_secs(15),
    );

    drop(client); // we'll create per-namespace clients below

    // One Temporal worker per known tenant + a `default` backstop for legacy
    // workflows. Phase II.4: tenant_watcher spawns workers for new tenants
    // without restart.
    let admin = Catalog::connect(&db_url).await?;
    let tenants = admin.list_tenants().await?;
    drop(admin);

    let task_queue = cfg.task_queue.clone();
    let local = tokio::task::LocalSet::new();

    // Build a closure that spawns one Temporal worker for a tenant.
    // Worker is !Send so we use spawn_local on a LocalSet.
    let runtime_clone = runtime.clone();
    let cfg_clone = cfg.clone();
    let lifecycle_clone = lifecycle.clone();
    let sync_clone = sync.clone();
    let cdc_clone = cdc.clone();
    let task_queue_clone = task_queue.clone();

    let spawn_one = move |tenant_id: common_types::ids::TenantId| -> tokio::task::JoinHandle<()> {
        let ns = if tenant_id.as_uuid().is_nil() {
            cfg_clone.namespace.clone()
        } else {
            format!("etl-{}", tenant_id.as_uuid().simple())
        };
        let mut ns_cfg = cfg_clone.clone();
        ns_cfg.namespace = ns.clone();
        let task_queue = task_queue_clone.clone();
        let runtime = runtime_clone.clone();
        let lifecycle = lifecycle_clone.clone();
        let sync = sync_clone.clone();
        let cdc = cdc_clone.clone();
        tokio::task::spawn_local(async move {
            let ns_client = match make_client(&ns_cfg).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(%ns, error = %e, "skipping namespace — connect failed");
                    return;
                }
            };
            let opts = WorkerOptions::new(task_queue)
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
            let mut w = match Worker::new(&runtime, ns_client, opts) {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!(%ns, error = %e, "skipping namespace — Worker::new failed");
                    return;
                }
            };
            tracing::info!(%ns, "worker polling namespace");
            if let Err(e) = w.run().await {
                tracing::error!(%ns, error = %e, "worker exited");
            }
        })
    };

    let mut initial_ids: Vec<common_types::ids::TenantId> =
        tenants.iter().map(|t| t.tenant_id).collect();
    let nil_tenant = common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::nil());

    local.run_until(async move {
        for t in &tenants {
            spawn_one(t.tenant_id);
        }
        spawn_one(nil_tenant);
        initial_ids.push(nil_tenant);

        tokio::task::spawn_local(worker::tenant_watcher::run(
            catalog.clone(),
            initial_ids,
            Box::new(spawn_one),
        ));

        // Park: workers run on the LocalSet; this future never completes.
        futures::future::pending::<()>().await
    }).await;
    Ok(())
}
