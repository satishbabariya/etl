//! Phase II.3.d — MySQL CDC streaming-only e2e:
//!   1. Spawn mysql:8.0 testcontainer with gtid_mode=ON, binlog_format=ROW.
//!   2. Create test table `customers`.
//!   3. Build the workspace; spawn worker; seed catalog with a
//!      Connection (mysql url) + Pipeline (MysqlCdc spec).
//!   4. `platform pipeline run` — workflow captures start GTID and
//!      enters the streaming loop.
//!   5. Execute INSERT/UPDATE/DELETE on the test table.
//!   6. Poll until parquet has 3 rows with _cdc.op = ['i', 'u', 'd'].

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use mysql_async::prelude::*;
use arrow::array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;
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
    anyhow::ensure!(status.success(), "cargo build failed");
    Ok(())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env(
            "RUST_LOG",
            "info,sqlx=warn,worker::connectors::mysql=debug,worker::activities::mysql_cdc=debug",
        )
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

async fn start_mysql_container() -> anyhow::Result<(ContainerAsync<Mysql>, String)> {
    let container = Mysql::default()
        .with_cmd(vec![
            "--gtid-mode=ON".to_string(),
            "--enforce-gtid-consistency=ON".to_string(),
            "--binlog-format=ROW".to_string(),
            "--binlog-row-image=FULL".to_string(),
            "--server-id=1".to_string(),
            "--log-bin=mysql-bin".to_string(),
        ])
        .start()
        .await?;
    let port = container.get_host_port_ipv4(3306).await?;
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    Ok((container, url))
}

async fn seed_table(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "CREATE TABLE customers (
            id BIGINT PRIMARY KEY,
            email VARCHAR(255),
            name VARCHAR(255),
            created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
         )",
    )
    .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "INSERT INTO customers (id, email, name, created) \
         VALUES (1, 'a@x.com', 'Alice', '2026-01-01 00:00:00')",
    )
    .await?;
    conn.query_drop("UPDATE customers SET email='alice@x.com' WHERE id=1")
        .await?;
    conn.query_drop("DELETE FROM customers WHERE id=1").await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker + temporal stack; ~120s"]
async fn mysql_cdc_streaming_only_e2e() -> anyhow::Result<()> {
    build_workspace().await?;

    let (_container, mysql_url) = start_mysql_container().await?;
    seed_table(&mysql_url).await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "mysql-test".into(),
            connector_ref: "mysql_cdc@0.1.0".into(),
            config: json!({ "url": mysql_url }),
        })
        .await?;

    let tmp_data = tempfile::tempdir()?;
    let spec = json!({
        "source": {
            "type": "mysql_cdc",
            "schema": "test",
            "table": "customers",
            "server_id": 4242,
            "heartbeat_secs": 0
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
            name: "mysql-customers".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut worker = spawn_worker().await?;

    // Cap the streaming loop so the workflow actually completes; 8 windows
    // x 5s idle timeout = up to 40s, enough to capture 3 events.
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CDC_MAX_WINDOWS", "8")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "pipeline run kickoff failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Give the workflow time to capture start GTID and enter the loop.
    tokio::time::sleep(Duration::from_secs(3)).await;
    perform_iud(&mysql_url).await?;

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut last_ops: Vec<String> = Vec::new();
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for 3 ops; saw: {last_ops:?}");
        }
        last_ops = read_parquet_ops(tmp_data.path());
        if last_ops.iter().filter(|o| *o == "i").count() >= 1
            && last_ops.iter().filter(|o| *o == "u").count() >= 1
            && last_ops.iter().filter(|o| *o == "d").count() >= 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let row: Option<(String,)> =
        sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
            .fetch_optional(cat.pool())
            .await?;
    let final_status = row.map(|t| t.0).unwrap_or_default();

    worker.kill().await?;
    worker.wait().await?;

    eprintln!("ops: {last_ops:?}; final run status: {final_status}");
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
