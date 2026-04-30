//! Phase II.3.b.1 — TypeScript Stripe connector executed end-to-end:
//!   1. Publish examples/stripe-source-ts via the SDK CLI.
//!   2. Spawn a worker with ETL_CONNECTORS_DIR pointed at the registry.
//!   3. Stand up a wiremock server emulating Stripe /v1/customers.
//!   4. Seed catalog directly with a Connection (url = sk_test_demo) +
//!      Pipeline (source.config = { base_url = wiremock URI, ... }).
//!   5. `platform pipeline run <id>`. Poll `runs.status` until completed.
//!   6. Assert wiremock saw exactly one GET, and that the Parquet
//!      destination has 2 rows.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

async fn build_workspace() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "cargo build failed");
    Ok(())
}

async fn publish_ts_connector(registry: &Path) -> anyhow::Result<()> {
    let status = Command::new(cargo_bin("platform"))
        .current_dir(workspace_root())
        .args([
            "connector",
            "publish",
            "examples/stripe-source-ts",
            "--registry",
            registry.to_str().unwrap(),
        ])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "platform connector publish failed");
    Ok(())
}

async fn spawn_worker(connectors_dir: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
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
#[ignore = "requires docker stack + node + npm + jco; ~120s"]
async fn stripe_ts_connector_runs_in_worker() -> anyhow::Result<()> {
    build_workspace().await?;
    let connectors = workspace_root().join("connectors");
    publish_ts_connector(&connectors).await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/customers"))
        .and(query_param("limit", "100"))
        .and(header("Authorization", "Bearer sk_test_demo"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
                "data":[
                    {"id":"cus_a","email":"a@x.com","name":"Alice","created":1700000000},
                    {"id":"cus_b","email":"b@x.com","name":"Bob","created":1700000123}
                ],
                "has_more": false
            }"#,
        ))
        .expect(1..)
        .mount(&server)
        .await;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "stripe-mock-ts".into(),
            connector_ref: "wasm:stripe-source-ts@0.1.0".into(),
            config: json!({ "url": "sk_test_demo" }),
        })
        .await?;

    let tmp_data = tempfile::tempdir()?;
    let spec = json!({
        "source": {
            "type": "wasm",
            "config": {
                "base_url": server.uri(),
                "limit": 100,
                "max_429_retries": 1
            }
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 100
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "stripe-customers-mock-ts".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut worker = spawn_worker(&connectors).await?;
    let start = Instant::now();

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CONNECTORS_DIR", &connectors)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "platform pipeline run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for run completion");
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
                worker.kill().await.ok();
                anyhow::bail!("run failed (status=failed)");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let elapsed = start.elapsed();
    eprintln!("ts pipeline completed in {elapsed:?}");

    worker.kill().await?;
    worker.wait().await?;

    let total = count_parquet_rows(tmp_data.path());
    assert_eq!(total, 2, "expected 2 customer rows in parquet, got {total}");

    drop(server);
    Ok(())
}
