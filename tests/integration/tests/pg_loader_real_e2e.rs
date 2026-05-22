//! Real-world end-to-end test for the Postgres destination loader.
//!
//! WHY THIS EXISTS
//! ---------------
//! Phase-2-4a/b/c/d shipped the PG loader but every test until now drove
//! `PostgresLoader.load(...)` directly. That tests the loader API surface,
//! NOT the full workflow → activity → dispatch → loader chain. This test
//! proves the full stack actually works end-to-end via Temporal.
//!
//! WHAT IT VERIFIES
//! ----------------
//! 1. Source PG → Destination PG via WASM CDC connector + WasmCdcPipelineWorkflow
//! 2. Snapshot rows (`_cdc.op = "s"`) become upserts in destination
//! 3. Streaming inserts (`i`) land as new rows
//! 4. Streaming updates (`u`) overwrite existing rows
//! 5. Streaming deletes (`d`) remove rows
//! 6. _cdc.* metadata columns are STRIPPED from the destination table
//! 7. Metering events (RowsRead/Written, BytesRead/Written) land in catalog
//!
//! Requires: docker-compose stack (postgres+temporal+vault), wasm32-wasip2
//! toolchain. Mirrors postgres_cdc_wasm_e2e.rs structure.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use serde_json::json;
use sqlx::Connection;
use sqlx::Row;
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
            "info,sqlx=warn,worker::activities=debug,worker::loaders::postgres=debug,worker::workflows=debug,worker::wasm_runtime=debug",
        )
        .env("ETL_CONNECTORS_DIR", connectors_dir)
        .current_dir(workspace_root())
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
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

async fn seed_source(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query(
        "CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            name TEXT,
            active BOOLEAN
         )",
    )
    .execute(&mut conn)
    .await?;
    sqlx::query(
        "INSERT INTO items (id, name, active) VALUES \
         (1, 'one', true), (2, 'two', false), (3, 'three', true)",
    )
    .execute(&mut conn)
    .await?;
    conn.close().await?;
    Ok(())
}

async fn perform_iud(url: &str) -> anyhow::Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    sqlx::query("INSERT INTO items (id, name, active) VALUES (4, 'four', true)")
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

async fn rows_in_dest(url: &str, schema: &str) -> anyhow::Result<Vec<(i64, Option<String>, Option<bool>)>> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    let rows = sqlx::query(&format!(
        "SELECT id, name, active FROM \"{schema}\".\"items\" ORDER BY id"
    ))
    .fetch_all(&mut conn)
    .await?;
    conn.close().await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.get::<i64, _>(0),
                r.try_get::<String, _>(1).ok(),
                r.try_get::<bool, _>(2).ok(),
            )
        })
        .collect())
}

async fn dest_columns(url: &str, schema: &str) -> anyhow::Result<Vec<String>> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    let rows = sqlx::query(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'items' \
         ORDER BY ordinal_position",
    )
    .bind(schema)
    .fetch_all(&mut conn)
    .await?;
    conn.close().await?;
    Ok(rows.into_iter().map(|r| r.get::<String, _>(0)).collect())
}

async fn count_metering(catalog: &str, tenant_id: uuid::Uuid) -> anyhow::Result<(i64, i64)> {
    let mut conn = sqlx::PgConnection::connect(catalog).await?;
    let row_reads: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT FROM metering_events \
         WHERE tenant_id = $1 AND metric IN ('rows_read', 'bytes_read')",
    )
    .bind(tenant_id)
    .fetch_one(&mut conn)
    .await?
    .get(0);
    let row_writes: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT FROM metering_events \
         WHERE tenant_id = $1 AND metric IN ('rows_written', 'bytes_written')",
    )
    .bind(tenant_id)
    .fetch_one(&mut conn)
    .await?
    .get(0);
    conn.close().await?;
    Ok((row_reads, row_writes))
}

/// END-TO-END: WASM PG CDC source → PostgresLoader destination via Temporal
///
/// This is the test that should have existed BEFORE shipping phase-2-4a/b/c/d.
/// It exercises every layer:
///   - WasmCdcPipelineWorkflow scheduling read_batch → load_batch
///   - load_batch dispatch matching `DestinationSpec::Postgres(_)`
///   - PostgresLoader::load detecting CDC mode + routing i/u/d
///   - `_cdc.*` columns stripped from destination DDL
///   - Metering hooks emitting on each batch
#[tokio::test]
#[ignore = "requires docker + temporal; builds wasm; ~150s"]
async fn pg_loader_full_stack_cdc_e2e() -> anyhow::Result<()> {
    build_workspace().await?;
    build_wasm_connector().await?;

    // Two PG containers: one for source, one for destination. (Catalog uses
    // the docker-compose postgres on :5432 — different from both.)
    let (_src_container, source_url) = start_pg_container().await?;
    let (_dst_container, dest_url) = start_pg_container().await?;
    seed_source(&source_url).await?;

    // Pre-create the destination schema so the PG loader has a place to write.
    let dest_schema = "etl_dest";
    {
        let mut conn = sqlx::PgConnection::connect(&dest_url).await?;
        sqlx::query(&format!("CREATE SCHEMA \"{dest_schema}\""))
            .execute(&mut conn)
            .await?;
        conn.close().await?;
    }

    let connectors = workspace_root().join("connectors");

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "pg-wasm-cdc-src".into(),
            connector_ref: "wasm-cdc:postgres-cdc-rs@0.1.0".into(),
            config: json!({ "url": source_url }),
        })
        .await?;

    // THIS IS THE KEY DIFFERENCE: destination is Postgres, not LocalParquet.
    let spec = json!({
        "source": {
            "type": "wasm",
            "config": { "schema": "public", "table": "items" }
        },
        "destination": {
            "type": "postgres",
            "connection_url": dest_url,
            "schema": dest_schema,
            "table": "items",
            "pk_columns": ["id"]
        },
        "batch_size": 2
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "pg-cdc-to-pg".into(),
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
    perform_iud(&source_url).await?;

    // Poll destination until final state stable.
    // Expected after snapshot + IUD: rows = [(2, 'TWO', false), (3, 'three', true), (4, 'four', true)]
    // (row 1 was inserted in snapshot then deleted in streaming; row 2 was inserted
    //  in snapshot then updated to 'TWO'; row 4 is a streaming insert.)
    let deadline = Instant::now() + Duration::from_secs(150);
    let mut last_rows: Vec<(i64, Option<String>, Option<bool>)> = Vec::new();
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for destination convergence; saw: {last_rows:?}");
        }
        last_rows = rows_in_dest(&dest_url, dest_schema).await.unwrap_or_default();
        // Convergence: 3 rows total (1 deleted), row 2 updated to 'TWO', row 4 present.
        let has_2_updated = last_rows
            .iter()
            .any(|(id, n, _)| *id == 2 && n.as_deref() == Some("TWO"));
        let has_4 = last_rows.iter().any(|(id, _, _)| *id == 4);
        let no_1 = !last_rows.iter().any(|(id, _, _)| *id == 1);
        if last_rows.len() == 3 && has_2_updated && has_4 && no_1 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    worker.kill().await?;
    worker.wait().await?;

    eprintln!("destination rows: {last_rows:?}");

    // ── Functional assertions ─────────────────────────────────────────────
    assert_eq!(last_rows.len(), 3, "expected 3 rows after IUD; got {last_rows:?}");
    assert_eq!(
        last_rows[0],
        (2, Some("TWO".into()), Some(false)),
        "row 2 must reflect UPDATE → 'TWO'"
    );
    assert_eq!(
        last_rows[1],
        (3, Some("three".into()), Some(true)),
        "row 3 must survive untouched"
    );
    assert_eq!(
        last_rows[2],
        (4, Some("four".into()), Some(true)),
        "row 4 must be present from streaming INSERT"
    );

    // ── Schema assertion: _cdc.* columns must NOT leak into destination ──
    let cols = dest_columns(&dest_url, dest_schema).await?;
    eprintln!("destination columns: {cols:?}");
    assert_eq!(
        cols,
        vec!["id".to_string(), "name".into(), "active".into()],
        "destination table must contain only source columns; got {cols:?}"
    );
    for col in &cols {
        assert!(!col.starts_with("_cdc"), "leaked metadata column: {col}");
    }

    // ── Metering assertion: events landed in catalog ──────────────────────
    let (reads, writes) = count_metering(&catalog_url(), tenant.as_uuid()).await?;
    eprintln!("metering: reads={reads}, writes={writes}");
    assert!(reads > 0, "expected at least one rows_read/bytes_read event");
    assert!(writes > 0, "expected at least one rows_written/bytes_written event");

    Ok(())
}
