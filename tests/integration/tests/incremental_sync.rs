//! Phase I.2 end-to-end sync test: fresh sync then incremental resync.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn source_url() -> String {
    std::env::var("SOURCE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn reseed_source_10_rows() -> anyhow::Result<()> {
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success(), "seed script failed");
    Ok(())
}

async fn add_5_more_rows() -> anyhow::Result<()> {
    let sql = r#"
INSERT INTO customers (id, name, email, created_at, updated_at) VALUES
  (11,'Kim',   'kim@example.com',   '2026-04-23 09:00:00+00', '2026-04-23 09:00:00+00'),
  (12,'Leo',   'leo@example.com',   '2026-04-23 10:00:00+00', '2026-04-23 10:00:00+00'),
  (13,'Mia',   NULL,                '2026-04-23 11:00:00+00', '2026-04-23 11:00:00+00'),
  (14,'Ned',   'ned@example.com',   '2026-04-23 12:00:00+00', '2026-04-23 12:00:00+00'),
  (15,'Olga',  'olga@example.com',  '2026-04-23 13:00:00+00', '2026-04-23 13:00:00+00');
"#;
    let mut child = Command::new("docker")
        .args([
            "exec", "-i", "etl-postgres", "psql", "-U", "etl", "-d", "etl_source_demo",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()?;
    if let Some(mut s) = child.stdin.take() {
        s.write_all(sql.as_bytes()).await?;
        s.shutdown().await?;
    }
    let status = child.wait().await?;
    assert!(status.success(), "add_5_more_rows failed");
    Ok(())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

async fn seed_catalog(data_base: &Path) -> anyhow::Result<common_types::ids::PipelineId> {
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "source-demo".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": source_url() }),
        })
        .await?;

    let spec = json!({
        "source": {
            "type": "postgres",
            "schema": "public",
            "table": "customers",
            "cursor_column": "updated_at",
            "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"]
        },
        "destination": {
            "type": "local_parquet",
            "base_path": data_base.to_string_lossy()
        },
        "batch_size": 4
    });

    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "customers-sync".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;
    Ok(pipe)
}

async fn run_cli(pipe: common_types::ids::PipelineId) -> anyhow::Result<()> {
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn wait_for_last_run_status(
    cat: &Catalog,
    want: RunStatus,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool())
                .await?;
        if let Some((s,)) = row {
            if RunStatus::parse(&s) == Some(want) {
                return Ok(());
            }
            if RunStatus::parse(&s) == Some(RunStatus::Failed) {
                anyhow::bail!("run failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("timeout waiting for {:?}", want);
}

fn count_rows_in_dir(dir: &Path) -> usize {
    let mut total = 0usize;
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                total += batch.unwrap().num_rows();
            }
        }
    }
    total
}

#[tokio::test]
#[ignore = "requires docker stack + source-demo seeded"]
async fn incremental_sync_picks_up_only_new_rows() -> anyhow::Result<()> {
    let build = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(build.success());

    reseed_source_10_rows().await?;

    let tmp = tempfile::tempdir()?;
    let data = tmp.path().to_owned();

    let pipe = seed_catalog(&data).await?;
    let mut w = spawn_worker().await?;

    run_cli(pipe).await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    wait_for_last_run_status(&cat, RunStatus::Completed, Duration::from_secs(60)).await?;
    let run1_total = count_rows_in_dir(&data);
    assert_eq!(run1_total, 10, "run 1 should load 10 rows");

    let tenant = cat.get_tenant_by_name("dev").await?.unwrap().tenant_id;
    let ctx = catalog::TenantContext::new(tenant);
    let state = cat.get_stream_state(ctx, pipe, "customers").await?.unwrap();
    assert!(
        state.cursor.as_ref().unwrap().value.starts_with("2026-04-22T11:00"),
        "cursor: {:?}",
        state.cursor
    );

    add_5_more_rows().await?;

    run_cli(pipe).await?;
    wait_for_last_run_status(&cat, RunStatus::Completed, Duration::from_secs(60)).await?;
    let run2_total = count_rows_in_dir(&data);
    assert_eq!(run2_total, 15, "after run 2 total should be 15 (10 + 5 new)");

    let state2 = cat.get_stream_state(ctx, pipe, "customers").await?.unwrap();
    assert!(
        state2.cursor.as_ref().unwrap().value.starts_with("2026-04-23T13:00"),
        "cursor: {:?}",
        state2.cursor
    );

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
