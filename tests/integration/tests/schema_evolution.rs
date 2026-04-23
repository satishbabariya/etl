//! Postgres schema evolution end-to-end.

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

async fn reseed_source() -> anyhow::Result<()> {
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());
    Ok(())
}

async fn run_sql(db: &str, sql: &str) -> anyhow::Result<()> {
    let mut child = Command::new("docker")
        .args(["exec", "-i", "etl-postgres", "psql", "-U", "etl", "-d", db])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()?;
    if let Some(mut s) = child.stdin.take() {
        s.write_all(sql.as_bytes()).await?;
        s.shutdown().await?;
    }
    let status = child.wait().await?;
    assert!(status.success(), "psql: {sql}");
    Ok(())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

async fn run_cli(pipe: common_types::ids::PipelineId) -> anyhow::Result<()> {
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "cli: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn wait_for_last_run_completed(cat: &Catalog, timeout: Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool())
                .await?;
        if let Some((s,)) = row {
            if RunStatus::parse(&s) == Some(RunStatus::Completed) {
                return Ok(());
            }
            if RunStatus::parse(&s) == Some(RunStatus::Failed) {
                anyhow::bail!("run failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("timeout waiting for completion");
}

fn parquet_column_names(dir: &Path) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let b = batch.unwrap();
                for field in b.schema().fields() {
                    out.insert(field.name().clone());
                }
            }
        }
    }
    out
}

#[tokio::test]
#[ignore = "requires docker stack + source demo; adds then drops column"]
async fn schema_evolution_adds_column_on_second_run() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    reseed_source().await?;
    // Drop nickname if leftover from a previous aborted run.
    let _ = run_sql(
        "etl_source_demo",
        "ALTER TABLE customers DROP COLUMN IF EXISTS nickname;",
    )
    .await;

    let tmp = tempfile::tempdir()?;
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
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 4,
        "evolution_policy": "propagate_additive",
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

    let mut w = spawn_worker().await?;

    // Run 1 — baseline → Schema v1.
    run_cli(pipe).await?;
    wait_for_last_run_completed(&cat, Duration::from_secs(60)).await?;

    let stream = cat
        .get_stream_by_name(pipe, "customers")
        .await?
        .expect("stream auto-created");
    let v1 = cat
        .get_latest_schema(stream.stream_id)
        .await?
        .expect("schema v1 recorded");
    assert_eq!(v1.version, 1);
    assert!(v1.parent_schema_id.is_none());
    assert!(v1.change_summary.is_empty());

    // Add nickname column (nullable) + touch rows so cursor advances.
    run_sql(
        "etl_source_demo",
        "ALTER TABLE customers ADD COLUMN nickname TEXT;",
    )
    .await?;
    run_sql(
        "etl_source_demo",
        "UPDATE customers SET updated_at = updated_at + interval '1 day';",
    )
    .await?;

    // Run 2 → Schema v2 with AddColumn(nickname).
    run_cli(pipe).await?;
    wait_for_last_run_completed(&cat, Duration::from_secs(60)).await?;

    let v2 = cat
        .get_latest_schema(stream.stream_id)
        .await?
        .expect("schema v2 recorded");
    assert_eq!(v2.version, 2);
    assert_eq!(v2.parent_schema_id, Some(v1.schema_id));
    assert!(
        v2.change_summary.iter().any(|c| matches!(
            c,
            common_types::evolution::ChangeKind::AddColumn { name, nullable: true, .. } if name == "nickname"
        )),
        "expected AddColumn(nickname, nullable=true) in change_summary, got {:?}",
        v2.change_summary
    );

    let cols = parquet_column_names(tmp.path());
    assert!(
        cols.contains("nickname"),
        "parquet missing 'nickname'; got {cols:?}"
    );

    let _ = run_sql(
        "etl_source_demo",
        "ALTER TABLE customers DROP COLUMN IF EXISTS nickname;",
    )
    .await;

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
