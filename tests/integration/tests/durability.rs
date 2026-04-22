//! Durability integration test: kill the worker while PipelineRunWorkflow is
//! mid-sleep, restart it, assert the workflow completes. Validates the
//! Phase I.1 exit criterion.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use common_types::ids::RunId;
use serde_json::json;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!(
        "{}/../../target/debug/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    )
}

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", db_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

#[tokio::test]
#[ignore = "requires dockerized Postgres and Temporal; run with --ignored"]
async fn workflow_survives_worker_restart() -> anyhow::Result<()> {
    // Build binaries first so cargo_bin() paths exist.
    let build = Command::new("cargo")
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(build.success(), "workspace build failed");

    let cat = Catalog::connect(&db_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let conn = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await?;
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "demo".into(),
            source_conn_id: conn,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    // --- Phase 1: spawn worker #1 and submit a run ---
    let mut w1 = spawn_worker().await?;

    let submit = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", db_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info")
        .output()
        .await?;
    assert!(
        submit.status.success(),
        "cli submit failed: {}",
        String::from_utf8_lossy(&submit.stderr)
    );

    // --- Phase 2: wait for the run to enter 'running' (start_run activity
    // fired) so we know the workflow is past its first step and currently
    // waiting in the 30s timer ---
    let run_id = wait_for_status(&cat, RunStatus::Running, Duration::from_secs(30))
        .await
        .context("waiting for status=running")?;

    // --- Phase 3: kill worker #1 while workflow is in the timer ---
    w1.kill().await?;
    w1.wait().await?;

    // --- Phase 4: restart worker #2. Temporal should drive the workflow to
    // completion once a worker is available again ---
    let mut w2 = spawn_worker().await?;

    // --- Phase 5: wait for completion (allow up to 90s: 30s timer +
    // reschedule overhead) ---
    let completed = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let run = cat
                .get_run(run_id)
                .await?
                .expect("run row disappeared");
            if run.status == RunStatus::Completed {
                return Ok::<_, anyhow::Error>(());
            }
            if run.status == RunStatus::Failed {
                anyhow::bail!("run transitioned to Failed: {:?}", run.error);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    w2.kill().await?;
    w2.wait().await?;

    completed.context("timed out waiting for completion")??;
    Ok(())
}

async fn wait_for_status(
    cat: &Catalog,
    target: RunStatus,
    timeout: Duration,
) -> anyhow::Result<RunId> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let row: Option<(uuid::Uuid, String)> = sqlx::query_as(
            "SELECT run_id, status FROM runs ORDER BY started_at DESC LIMIT 1",
        )
        .fetch_optional(cat.pool())
        .await?;
        if let Some((rid, status)) = row {
            if RunStatus::parse(&status) == Some(target) {
                return Ok(RunId::from_uuid_unchecked(rid));
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("did not reach status {:?} within {:?}", target, timeout);
}
