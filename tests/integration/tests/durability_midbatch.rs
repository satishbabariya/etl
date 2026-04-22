//! Phase I.2 exit-criterion test: kill worker mid-sync, restart, verify
//! final total row count is exactly the source count (no loss, no duplication
//! at the row level — load is idempotent per LoadId).

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
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

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
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

fn count_parquet_rows(dir: &Path) -> usize {
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
#[ignore = "requires docker stack + big source seed; takes ~2–3 minutes"]
async fn sync_survives_worker_kill_midbatch() -> anyhow::Result<()> {
    let build = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(build.success());

    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo-big.sh")
        .status()
        .await?;
    assert!(status.success());

    let tmp = tempfile::tempdir()?;
    let data = tmp.path().to_owned();

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
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
            "base_path": data.to_string_lossy()
        },
        "batch_size": 10
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "big-sync".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    // Worker #1.
    let mut w1 = spawn_worker().await?;
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success());

    // Let it work for a bit, then kill.
    tokio::time::sleep(Duration::from_secs(5)).await;
    w1.kill().await?;
    w1.wait().await?;

    // Worker #2 resumes.
    let mut w2 = spawn_worker().await?;

    // Wait up to 180s for completion.
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        if std::time::Instant::now() > deadline {
            anyhow::bail!("timed out waiting for completion");
        }
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool())
                .await?;
        if let Some((s,)) = row {
            if s == "completed" {
                break;
            }
            if s == "failed" {
                anyhow::bail!("run failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    w2.kill().await?;
    w2.wait().await?;

    let total = count_parquet_rows(&data);
    assert_eq!(total, 100, "final Parquet row count must equal source rows");

    let state = cat.get_stream_state(pipe, "customers").await?.unwrap();
    // Row id=100 → 2026-04-20 00:00 + 100 minutes = 2026-04-20 01:40 UTC.
    assert!(
        state.cursor.as_ref().unwrap().value.starts_with("2026-04-20T01:40"),
        "cursor: {:?}",
        state.cursor
    );

    Ok(())
}
