//! Phase I.6: CDC snapshot → streaming handoff.
//!
//! Start with 3 pre-existing rows (visible to snapshot); after worker
//! is running + snapshot has completed, INSERT a 4th row. Expect at
//! least 3 snapshot rows (`_cdc.op = "s"`) + 1 stream insert (`_cdc.op = "i"`).

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
    let o = Command::new(cargo_bin("platform"))
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
        o.status.success(),
        "cli: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    Ok(())
}

fn read_ops(dir: &std::path::Path) -> Vec<String> {
    let mut ops = Vec::new();
    for e in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if e.path().extension().and_then(|x| x.to_str()) != Some("parquet") {
            continue;
        }
        let f = std::fs::File::open(e.path()).unwrap();
        let r = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
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
            for row in 0..b.num_rows() {
                ops.push(a.value(row).to_string());
            }
        }
    }
    ops
}

#[tokio::test]
#[ignore = "requires docker stack + cdc_source_demo"]
async fn snapshot_then_streaming_handoff() -> anyhow::Result<()> {
    let st = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(st.success());

    run_sql(
        "cdc_source_demo",
        "DROP TABLE IF EXISTS orders CASCADE; \
         CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT NOT NULL, amount TEXT NOT NULL); \
         ALTER TABLE orders REPLICA IDENTITY FULL; \
         INSERT INTO orders(id,customer,amount) VALUES (1,'Alice','100'),(2,'Bob','200'),(3,'Carol','300');",
    )
    .await?;
    let _ = run_sql(
        "cdc_source_demo",
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';",
    )
    .await;
    let _ = run_sql(
        "cdc_source_demo",
        "DO $$ DECLARE r record; BEGIN FOR r IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'etl_%' \
           LOOP EXECUTE format('DROP PUBLICATION %I', r.pubname); END LOOP; END $$;",
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
            name: "orders-cdc-handoff".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;
    run_cli(pipe).await?;

    // Let snapshot + initial streaming window settle.
    tokio::time::sleep(Duration::from_secs(5)).await;
    run_sql(
        "cdc_source_demo",
        "INSERT INTO orders(id,customer,amount) VALUES (4,'Dave','400');",
    )
    .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    loop {
        let ops = read_ops(tmp.path());
        let s = ops.iter().filter(|o| o == &"s").count();
        let i = ops.iter().filter(|o| o == &"i").count();
        if s >= 3 && i >= 1 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("handoff incomplete: s={s} i={i}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify the snapshot Parquet batch carries typed columns and
    // includes every data column (not just the PK).
    let snapshot_schema =
        read_snapshot_parquet_schema(tmp.path()).expect("at least one snapshot parquet file");
    let names: Vec<String> = snapshot_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert!(names.contains(&"id".to_string()), "id missing: {names:?}");
    assert!(
        names.contains(&"customer".to_string()),
        "customer missing: {names:?}"
    );
    assert!(
        names.contains(&"amount".to_string()),
        "amount missing: {names:?}"
    );
    let id_field = snapshot_schema.field_with_name("id").unwrap();
    assert_eq!(
        id_field.data_type(),
        &arrow::datatypes::DataType::Int64,
        "snapshot id should be Int64, got {:?}",
        id_field.data_type()
    );
    let customer_field = snapshot_schema.field_with_name("customer").unwrap();
    assert_eq!(
        customer_field.data_type(),
        &arrow::datatypes::DataType::Utf8,
        "snapshot customer should be Utf8, got {:?}",
        customer_field.data_type()
    );

    // === Resume verification ===
    // Re-run the same pipeline. Snapshot completed in run #1 → second run
    // should skip the snapshot loop entirely (no new 's' rows).
    let s_after_first_run = read_ops(tmp.path()).iter().filter(|o| *o == "s").count();
    run_cli(pipe).await?;
    // Give the second run's streaming loop a moment to settle (snapshot
    // should be skipped immediately).
    tokio::time::sleep(Duration::from_secs(10)).await;
    let s_after_second_run = read_ops(tmp.path()).iter().filter(|o| *o == "s").count();
    assert_eq!(
        s_after_second_run, s_after_first_run,
        "second run added {} new 's' rows; expected 0 (snapshot should be skipped)",
        s_after_second_run as i64 - s_after_first_run as i64
    );

    w.kill().await?;
    w.wait().await?;
    Ok(())
}

fn read_snapshot_parquet_schema(dir: &std::path::Path) -> Option<arrow::datatypes::Schema> {
    use arrow::array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut files: Vec<std::path::PathBuf> = walkdir::WalkDir::new(dir)
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
        let builder = match ParquetRecordBatchReaderBuilder::try_new(f) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let schema = builder.schema().as_ref().clone();
        let reader = match builder.build() {
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
                        if arr.value(i) == "s" {
                            return Some(schema);
                        }
                    }
                }
            }
        }
    }
    None
}
