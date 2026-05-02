//! Phase II.3.f — Postgres WASM CDC end-to-end:
//!   1. Build the example postgres-cdc-rs WASM connector + precompile.
//!   2. Spawn postgres:16 testcontainer with wal_level=logical.
//!   3. Pre-seed `items` with three rows (snapshot fodder).
//!   4. Spawn worker; seed catalog with a Wasm connection +
//!      Wasm pipeline using connector_ref="wasm-cdc:postgres-cdc-rs@0.1.0".
//!   5. `platform pipeline run` — workflow dispatches to
//!      WasmCdcPipelineWorkflow; the guest snapshots, then streams.
//!   6. After a short delay, INSERT/UPDATE/DELETE.
//!   7. Poll parquet for snapshot 's' rows + 'i'/'u'/'d' streaming rows.

use anyhow::Context;
use arrow::array::Array;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use sqlx::Connection;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres as PgContainer;
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

async fn build_workspace() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "workspace build failed");
    Ok(())
}

async fn build_wasm_connector() -> anyhow::Result<()> {
    let status = Command::new(cargo_bin("platform"))
        .current_dir(workspace_root())
        .args(["connector", "build", "examples/postgres-cdc-rs"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "connector build failed");
    Ok(())
}

async fn spawn_worker(connectors_dir: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env(
            "RUST_LOG",
            "info,sqlx=warn,worker::wasm_runtime=debug,worker::workflows=debug",
        )
        .env("ETL_CONNECTORS_DIR", connectors_dir)
        .current_dir(workspace_root())
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

fn read_parquet_ops(dir: &Path) -> Vec<String> {
    let mut ops: Vec<String> = Vec::new();
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(dir)
        .into_iter()
        .flatten()
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        .map(|e| e.into_path())
        .collect();
    files.sort();
    for path in files {
        let f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let reader = match ParquetRecordBatchReaderBuilder::try_new(f) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let reader = match reader.build() {
            Ok(r) => r,
            Err(_) => continue,
        };
        for batch in reader.flatten() {
            if let Ok(idx) = batch.schema().index_of("_cdc.op") {
                if let Some(arr) = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    for i in 0..arr.len() {
                        ops.push(arr.value(i).to_string());
                    }
                }
            }
        }
    }
    ops
}

async fn start_pg_container() -> anyhow::Result<(ContainerAsync<PgContainer>, String)> {
    let container = PgContainer::default()
        .with_cmd(vec![
            "-c".to_string(),
            "wal_level=logical".to_string(),
            "-c".to_string(),
            "max_wal_senders=4".to_string(),
            "-c".to_string(),
            "max_replication_slots=4".to_string(),
        ])
        .start()
        .await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    Ok((container, url))
}

async fn seed_table_and_rows(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query(
        "CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            name TEXT,
            active BOOLEAN,
            created TIMESTAMP NOT NULL DEFAULT '2026-01-01 00:00:00'
         )",
    )
    .execute(&mut conn)
    .await?;
    sqlx::query(
        "INSERT INTO items (id, name, active, created) VALUES \
         (1, 'one', true, '2026-01-01 00:00:00'), \
         (2, 'two', false, '2026-01-01 00:00:01'), \
         (3, 'three', true, '2026-01-01 00:00:02')",
    )
    .execute(&mut conn)
    .await?;
    conn.close().await?;
    Ok(())
}

async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query("INSERT INTO items (id, name, active, created) VALUES (4, 'four', true, '2026-01-02 00:00:00')")
        .execute(&mut conn)
        .await?;
    sqlx::query("UPDATE items SET name='TWO' WHERE id=2")
        .execute(&mut conn)
        .await?;
    sqlx::query("DELETE FROM items WHERE id=1")
        .execute(&mut conn)
        .await?;
    conn.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker + temporal stack; builds wasm guest; ~120s"]
async fn postgres_cdc_wasm_e2e() -> anyhow::Result<()> {
    build_workspace().await?;
    build_wasm_connector().await?;

    let (_container, pg_url) = start_pg_container().await?;
    seed_table_and_rows(&pg_url).await?;

    let tmp_data = tempfile::tempdir()?;
    let connectors = workspace_root().join("connectors");

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "pg-wasm-cdc".into(),
            connector_ref: "wasm-cdc:postgres-cdc-rs@0.1.0".into(),
            config: json!({ "url": pg_url }),
        })
        .await?;

    let spec = json!({
        "source": {
            "type": "wasm",
            "config": {
                "schema": "public",
                "table": "items"
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
            name: "pg-cdc-wasm-items".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut worker = spawn_worker(&connectors).await?;

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CONNECTORS_DIR", &connectors)
        .env("ETL_CDC_MAX_WINDOWS", "12")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "pipeline run kickoff failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    tokio::time::sleep(Duration::from_secs(5)).await;
    perform_iud(&pg_url).await?;

    let deadline = Instant::now() + Duration::from_secs(150);
    let mut last_ops: Vec<String> = Vec::new();
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for ops; saw: {last_ops:?}");
        }
        last_ops = read_parquet_ops(tmp_data.path());
        let snap_count = last_ops.iter().filter(|o| *o == "s").count();
        let i_count = last_ops.iter().filter(|o| *o == "i").count();
        let u_count = last_ops.iter().filter(|o| *o == "u").count();
        let d_count = last_ops.iter().filter(|o| *o == "d").count();
        if snap_count >= 3 && i_count >= 1 && u_count >= 1 && d_count >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    worker.kill().await?;
    worker.wait().await?;

    eprintln!("ops captured: {last_ops:?}");
    assert!(
        last_ops.iter().filter(|o| *o == "s").count() >= 3,
        "expected >=3 snapshot rows; got {last_ops:?}"
    );
    assert!(
        last_ops.iter().any(|o| o == "i"),
        "missing INSERT in {last_ops:?}"
    );
    assert!(
        last_ops.iter().any(|o| o == "u"),
        "missing UPDATE in {last_ops:?}"
    );
    assert!(
        last_ops.iter().any(|o| o == "d"),
        "missing DELETE in {last_ops:?}"
    );

    Ok(())
}
