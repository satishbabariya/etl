//! Phase II.1.c: end-to-end tenant lifecycle.
//!
//! 1. `platform tenant create acme` → catalog row + Temporal namespace
//! 2. Seed a pipeline owned by acme
//! 3. Run the pipeline → Parquet at <tmp>/<acme_uuid>/<pipeline_uuid>/
//! 4. `platform tenant terminate acme` → catalog rows + storage gone

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let c = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", admin_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("DATABASE_URL_APP", app_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(c)
}

#[tokio::test]
#[ignore = "requires docker stack + source demo + tenant CLI"]
async fn tenant_lifecycle_provisions_runs_terminates() -> anyhow::Result<()> {
    let st = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(st.success());

    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    drop(admin);

    let tmp = tempfile::tempdir()?;

    // 1. Create tenant via CLI (registers Temporal namespace too).
    let out = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "lifecycle-test"])
        .env("DATABASE_URL", admin_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "tenant create failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 2. Resolve tenant id; seed a pipeline under it via the app catalog.
    let admin = Catalog::connect(&admin_url()).await?;
    let tenant = admin
        .get_tenant_by_name("lifecycle-test")
        .await?
        .expect("tenant created")
        .tenant_id;
    drop(admin);

    let cat = Catalog::connect_app(&app_url()).await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": "postgres://etl:etl@localhost:5432/etl_source_demo" }),
        })
        .await?;
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "lifecycle-pipe".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec: json!({
                "source": { "type":"postgres","schema":"public","table":"customers",
                            "cursor_column":"updated_at","cursor_kind":"timestamp_tz","pk_columns":["id"] },
                "destination": { "type":"local_parquet","base_path": tmp.path().to_string_lossy() },
                "batch_size": 4,
                "evolution_policy": "propagate_additive",
            }),
        })
        .await?;

    let mut tenant_dir = tmp.path().to_path_buf();
    tenant_dir.push(tenant.as_uuid().to_string());
    assert!(!tenant_dir.exists(), "tenant dir should not yet exist");

    // 3. Worker + run.
    let mut w = spawn_worker().await?;
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", admin_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("DATABASE_URL_APP", app_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "pipeline run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for ≥1 parquet under the new prefix.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if walkdir::WalkDir::new(&tenant_dir)
            .into_iter()
            .flatten()
            .any(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        tenant_dir.exists(),
        "parquet not landed at {}",
        tenant_dir.display()
    );

    w.kill().await?;
    w.wait().await?;

    // 4. Terminate via CLI.
    let out = Command::new(cargo_bin("platform"))
        .args(["tenant", "terminate", "lifecycle-test"])
        .env("DATABASE_URL", admin_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("ETL_DATA_DIR", tmp.path().to_string_lossy().into_owned())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "tenant terminate failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Catalog row gone + storage subtree removed.
    let admin = Catalog::connect(&admin_url()).await?;
    assert!(admin.get_tenant_by_name("lifecycle-test").await?.is_none());
    assert!(
        !tenant_dir.exists(),
        "tenant dir not removed: {}",
        tenant_dir.display()
    );
    Ok(())
}
