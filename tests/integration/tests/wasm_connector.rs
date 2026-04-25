//! End-to-end: CSV-content WASM connector → PipelineRunWorkflow →
//! LocalParquetLoader. Validates the full Phase I.3 path.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn build_all() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success(), "workspace build failed");
    Ok(())
}

async fn build_csv_connector() -> anyhow::Result<()> {
    let status = Command::new(cargo_bin("platform"))
        .current_dir(workspace_root())
        .args(["connector", "build", "examples/csv-source"])
        .status()
        .await?;
    assert!(status.success(), "connector build failed");
    Ok(())
}

async fn spawn_worker(connectors_dir: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .env("ETL_CONNECTORS_DIR", connectors_dir)
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
#[ignore = "requires docker stack; builds WASM guest; ~60s"]
async fn csv_wasm_connector_end_to_end() -> anyhow::Result<()> {
    build_all().await?;
    build_csv_connector().await?;

    let tmp_data = tempfile::tempdir()?;
    let connectors = workspace_root().join("connectors");

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "csv-inline".into(),
            connector_ref: "wasm:csv-source@0.1.0".into(),
            config: json!({ "url": "" }),
        })
        .await?;
    let csv_text = "id,name\n1,Alice\n2,Bob\n3,Carol\n4,Dave\n5,Eve\n";
    let spec = json!({
        "source": {
            "type": "wasm",
            "config": {
                "csv_text": csv_text,
                "has_header": true
            }
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 2
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "csv-sync".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker(&connectors).await?;
    let start = Instant::now();

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CONNECTORS_DIR", &connectors)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if Instant::now() > deadline {
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
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let elapsed = start.elapsed();

    w.kill().await?;
    w.wait().await?;

    let total = count_parquet_rows(tmp_data.path());
    assert_eq!(total, 5, "CSV had 5 data rows; Parquet total must match");

    let ctx = catalog::TenantContext::new(tenant);
    let state = cat.get_stream_state(ctx, pipe, "csv-source").await?.unwrap();
    assert_eq!(state.cursor.as_ref().unwrap().value.parse::<i64>().unwrap(), 5);

    assert!(
        elapsed < Duration::from_secs(30),
        "end-to-end elapsed = {:?}, over 30s budget",
        elapsed
    );

    Ok(())
}
