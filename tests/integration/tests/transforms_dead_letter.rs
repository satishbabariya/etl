//! Phase I.5: validate operator + dead-letter routing.
//!
//! Scenario A — under threshold: seed a NULL id, validate id NOT NULL,
//! threshold 0.5 → run completes, rejected row lands in dead-letter path,
//! kept rows in the normal output.
//!
//! Scenario B — over threshold: threshold 0.05, same seed → load_batch
//! fails NonRetryable, run recorded as Failed.

use anyhow::Context;
use arrow::array::{Array, Int64Array};
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

/// Seeds a custom `people` table where `id` is nullable and one row has NULL id.
async fn seed_people_with_null_id() -> anyhow::Result<()> {
    run_sql(
        "etl_source_demo",
        "DROP TABLE IF EXISTS people;
         CREATE TABLE people (
             pk         BIGINT PRIMARY KEY,
             id         BIGINT,
             name       TEXT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL
         );
         INSERT INTO people (pk, id, name, updated_at) VALUES
           (1, 10,   'Alice', '2026-04-20 10:00:00+00'),
           (2, NULL, 'Bob',   '2026-04-20 11:00:00+00'),
           (3, 30,   'Carol', '2026-04-20 12:00:00+00'),
           (4, 40,   'Dave',  '2026-04-20 13:00:00+00');",
    )
    .await
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
            if let Some(got) = RunStatus::parse(&s) {
                if got == want {
                    return Ok(());
                }
                if got == RunStatus::Failed && want == RunStatus::Completed {
                    anyhow::bail!("run failed");
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("timeout waiting for {want:?}");
}

fn read_ids_under(dir: &Path) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let f = std::fs::File::open(entry.path()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let b = batch.unwrap();
            let idx = b.schema().index_of("id").unwrap();
            let arr = b
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                });
            }
        }
    }
    out
}

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn validate_dead_letter_under_threshold_completes() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    seed_people_with_null_id().await?;

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
            "table": "people",
            "cursor_column": "updated_at",
            "cursor_kind": "timestamp_tz",
            "pk_columns": ["pk"]
        },
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 10,
        "evolution_policy": "propagate_additive",
        "transform": {
            "operators": [
                {"type": "validate", "rules": [{"column":"id","rule":"not_null"}]}
            ],
            "dead_letter_threshold": 0.5
        }
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "people-validate".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;

    run_cli(pipe).await?;
    wait_for_last_run_status(&cat, RunStatus::Completed, Duration::from_secs(60)).await?;

    // Kept rows: three with non-null id.
    let mut happy = tmp.path().to_path_buf();
    happy.push(pipe.as_uuid().to_string());
    // Exclude dead-letter subtree.
    let kept: Vec<Option<i64>> = walkdir::WalkDir::new(&happy)
        .into_iter()
        .flatten()
        .filter(|e| {
            e.path().extension().and_then(|x| x.to_str()) == Some("parquet")
                && !e.path().to_string_lossy().contains("dead-letter")
        })
        .flat_map(|e| {
            let f = std::fs::File::open(e.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap();
            let mut v = Vec::new();
            for batch in reader {
                let b = batch.unwrap();
                let idx = b.schema().index_of("id").unwrap();
                let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    v.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                }
            }
            v
        })
        .collect();
    assert_eq!(kept.len(), 3, "kept rows = {kept:?}");
    assert!(kept.iter().all(|v| v.is_some()), "kept had null id: {kept:?}");

    // Dead-letter rows: one with null id.
    let mut dl = tmp.path().to_path_buf();
    dl.push(pipe.as_uuid().to_string());
    dl.push("dead-letter");
    assert!(dl.exists(), "dead-letter dir missing at {}", dl.display());
    let rejected = read_ids_under(&dl);
    assert_eq!(rejected.len(), 1, "rejected rows = {rejected:?}");
    assert_eq!(rejected[0], None, "expected NULL id in dead-letter");

    let _ = run_sql("etl_source_demo", "DROP TABLE IF EXISTS people;").await;

    w.kill().await?;
    w.wait().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn validate_dead_letter_over_threshold_fails_run() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    seed_people_with_null_id().await?;

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
            "table": "people",
            "cursor_column": "updated_at",
            "cursor_kind": "timestamp_tz",
            "pk_columns": ["pk"]
        },
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 10,
        "evolution_policy": "propagate_additive",
        "transform": {
            "operators": [
                {"type": "validate", "rules": [{"column":"id","rule":"not_null"}]}
            ],
            "dead_letter_threshold": 0.05
        }
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "people-validate-strict".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;

    // CLI call starts the workflow; run status transitions to Failed
    // because the activity raises NonRetryable. We don't assert CLI exit
    // code (start_workflow returns as soon as Temporal accepts it).
    let _ = run_cli(pipe).await;
    wait_for_last_run_status(&cat, RunStatus::Failed, Duration::from_secs(60)).await?;

    let _ = run_sql("etl_source_demo", "DROP TABLE IF EXISTS people;").await;

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
