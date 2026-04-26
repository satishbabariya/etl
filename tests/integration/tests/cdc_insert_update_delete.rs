//! Phase I.6: CDC round-trip.
//!
//! Seed orders, start worker + CDC workflow, INSERT/UPDATE/DELETE a row,
//! assert the resulting Parquet log contains events with the correct
//! `_cdc.op` values.

use anyhow::Context;
use arrow::array::StringArray;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn source_url() -> String {
    "postgres://etl:etl@localhost:5432/cdc_source_demo".into()
}
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn run_sql(db: &str, sql: &str) -> anyhow::Result<()> {
    let mut c = Command::new("docker")
        .args(["exec", "-i", "etl-postgres", "psql", "-U", "etl", "-d", db])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()?;
    if let Some(mut s) = c.stdin.take() {
        s.write_all(sql.as_bytes()).await?;
        s.shutdown().await?;
    }
    assert!(c.wait().await?.success(), "psql: {sql}");
    Ok(())
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let c = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
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

async fn run_cli(p: common_types::ids::PipelineId) -> anyhow::Result<()> {
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &p.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CDC_MAX_WINDOWS", "15")
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

fn read_ops(dir: &std::path::Path) -> Vec<String> {
    let mut ops = Vec::new();
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let f = std::fs::File::open(entry.path()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            let b = b.unwrap();
            let idx = match b.schema().index_of("_cdc.op") {
                Ok(i) => i,
                Err(_) => continue,
            };
            let a = b
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                ops.push(a.value(i).to_string());
            }
        }
    }
    ops
}

async fn reset_orders() -> anyhow::Result<()> {
    // Drop the table first so REPLICA IDENTITY is re-set cleanly.
    run_sql(
        "cdc_source_demo",
        "DROP TABLE IF EXISTS orders CASCADE; \
         CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT NOT NULL, amount TEXT NOT NULL); \
         ALTER TABLE orders REPLICA IDENTITY FULL; \
         INSERT INTO orders(id,customer,amount) VALUES (1,'Alice','100');",
    )
    .await?;
    // Best-effort: drop any pre-existing slot/publication from prior runs.
    let _ = run_sql(
        "cdc_source_demo",
        "SELECT pg_drop_replication_slot(slot_name) \
         FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';",
    )
    .await;
    let _ = run_sql(
        "cdc_source_demo",
        "DO $$ DECLARE r record; BEGIN \
           FOR r IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'etl_%' LOOP \
             EXECUTE format('DROP PUBLICATION %I', r.pubname); \
           END LOOP; END $$;",
    )
    .await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack + cdc_source_demo"]
async fn cdc_round_trip_insert_update_delete() -> anyhow::Result<()> {
    let st = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(st.success());

    reset_orders().await?;

    let tmp = tempfile::tempdir()?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "cdc-src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": source_url() }),
        })
        .await?;
    let spec = json!({
        "source": {
            "type":"postgres","schema":"public","table":"orders",
            "cursor_column":"id","cursor_kind":"int64","pk_columns":["id"],
            "sync_mode":"cdc"
        },
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 100,
        "evolution_policy":"propagate_additive"
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "orders-cdc".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;
    run_cli(pipe).await?;

    // Let snapshot + initial streaming window run.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Drive DML after streaming is live.
    run_sql(
        "cdc_source_demo",
        "INSERT INTO orders(id,customer,amount) VALUES (2,'Bob','200');",
    )
    .await?;
    run_sql(
        "cdc_source_demo",
        "UPDATE orders SET amount='250' WHERE id=2;",
    )
    .await?;
    run_sql("cdc_source_demo", "DELETE FROM orders WHERE id=2;").await?;

    // Poll for all four op kinds.
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    loop {
        let ops = read_ops(tmp.path());
        let s = ops.iter().filter(|o| o == &"s").count();
        let i = ops.iter().filter(|o| o == &"i").count();
        let u = ops.iter().filter(|o| o == &"u").count();
        let d = ops.iter().filter(|o| o == &"d").count();
        if s >= 1 && i >= 1 && u >= 1 && d >= 1 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("missing ops; got s={s} i={i} u={u} d={d}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
