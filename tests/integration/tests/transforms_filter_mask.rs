//! Phase I.5: end-to-end transform pipeline.
//!
//! Postgres → filter(email IS NOT NULL) → mask(email, hash) → Parquet.
//! Expects 8 of 10 source rows (two have NULL email) with email replaced by a
//! 64-char blake3 hex hash.

use anyhow::Context;
use arrow::array::{Array, StringArray};
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
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

async fn reseed_source() -> anyhow::Result<()> {
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());
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

fn read_all_rows(dir: &Path) -> (Vec<String>, Vec<String>) {
    let mut names = Vec::new();
    let mut emails = Vec::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        // Skip dead-letter output.
        if entry.path().to_string_lossy().contains("dead-letter") {
            continue;
        }
        let f = std::fs::File::open(entry.path()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let b = batch.unwrap();
            let schema = b.schema();
            let name_idx = schema.index_of("name").unwrap();
            let email_idx = schema.index_of("email").unwrap();
            let na = b
                .column(name_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ea = b
                .column(email_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                names.push(na.value(i).to_string());
                emails.push(if ea.is_null(i) {
                    "<NULL>".into()
                } else {
                    ea.value(i).to_string()
                });
            }
        }
    }
    (names, emails)
}

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn filter_then_mask_end_to_end() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    reseed_source().await?;

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
        "transform": {
            "operators": [
                {"type": "filter", "predicate": "email IS NOT NULL"},
                {"type": "mask",   "column": "email", "strategy": {"kind": "hash"}}
            ],
            "dead_letter_threshold": 0.0
        }
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "customers-filter-mask".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;

    run_cli(pipe).await?;
    wait_for_last_run_completed(&cat, Duration::from_secs(60)).await?;

    let (names, emails) = read_all_rows(tmp.path());
    assert_eq!(names.len(), 8, "expected 8 rows after filter; got {names:?}");
    assert!(
        !names.contains(&"Bob".into()) && !names.contains(&"Frank".into()),
        "filter leaked null-email rows: {names:?}"
    );
    for e in &emails {
        assert_eq!(
            e.len(),
            64,
            "expected 64-hex blake3 digest for masked email, got {:?}",
            e
        );
        assert!(e.chars().all(|c| c.is_ascii_hexdigit()), "not hex: {e:?}");
    }

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
