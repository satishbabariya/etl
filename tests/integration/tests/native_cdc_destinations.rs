//! Native CDC (CdcPipelineWorkflow / MysqlCdcPipelineWorkflow) writes its
//! snapshot chunks and streaming windows through `write_cdc_batch`. These
//! tests feed it batches in the native shape (`snapshot::cdc_schema_for`:
//! data columns + `_cdc.op/lsn/commit_ts/txid`) for a Postgres destination
//! and check the table converges, including on a retried window.
//!
//! Requires the docker-compose `postgres` service to be running.

use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, LocalParquetSpec, PostgresDestinationSpec};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use worker::connectors::postgres::cdc::snapshot::cdc_schema_for;
use worker::loaders::write_cdc_batch;

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn fresh_schema() -> Option<(sqlx::PgPool, String)> {
    let url = test_url();
    let pool = match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SKIP native_cdc_destinations test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_native_cdc_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create schema");
    Some((pool, schema))
}

/// (id, name, op) rows in the native CDC batch shape.
fn native_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    let schema = cdc_schema_for(&[("id", DataType::Int64), ("name", DataType::Utf8)]);
    let n = rows.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["0/16B3748"; n])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(0i64); n])),
            Arc::new(Int64Array::from(vec![Some(7i64); n])),
        ],
    )
    .unwrap()
}

async fn rows(pool: &sqlx::PgPool, schema: &str) -> Vec<(i64, Option<String>)> {
    sqlx::query_as(&format!(
        "SELECT id, name FROM \"{schema}\".\"customers\" ORDER BY id"
    ))
    .fetch_all(pool)
    .await
    .expect("select")
}

#[tokio::test]
async fn native_cdc_snapshot_then_stream_lands_in_postgres() {
    let Some((pool, schema)) = fresh_schema().await else {
        return;
    };
    let dest = DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: test_url(),
        schema: schema.clone(),
        table: "customers".into(),
        pk_columns: vec!["id".into()],
    });
    let (tenant, pipeline, run) = (
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
    );

    // Snapshot chunk, then a streaming window (batch_seq keeps counting).
    let snapshot = native_batch(&[(1, Some("a"), "s"), (2, Some("b"), "s")]);
    write_cdc_batch(&dest, tenant, pipeline, run, 0, &snapshot)
        .await
        .unwrap();
    let window = native_batch(&[(1, Some("a2"), "u"), (2, None, "d"), (3, Some("c"), "i")]);
    write_cdc_batch(&dest, tenant, pipeline, run, 1, &window)
        .await
        .unwrap();
    // A retried window (at-least-once read) must not change anything.
    write_cdc_batch(&dest, tenant, pipeline, run, 1, &window)
        .await
        .unwrap();

    let got = rows(&pool, &schema).await;
    let cols: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'customers' ORDER BY ordinal_position",
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .unwrap();
    let _ = sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    assert_eq!(
        got,
        vec![(1, Some("a2".to_string())), (3, Some("c".to_string()))]
    );
    assert_eq!(
        cols,
        vec!["id", "name"],
        "_cdc.* columns leaked into the table"
    );
}

#[tokio::test]
async fn native_cdc_local_parquet_keeps_change_log_layout() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DestinationSpec::LocalParquet(LocalParquetSpec {
        base_path: dir.path().to_string_lossy().into_owned(),
    });
    let (tenant, pipeline, run) = (
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
    );
    write_cdc_batch(
        &dest,
        tenant,
        pipeline,
        run,
        3,
        &native_batch(&[(1, Some("a"), "i")]),
    )
    .await
    .unwrap();
    let file = dir
        .path()
        .join(tenant.to_string())
        .join(pipeline.to_string())
        .join("cdc")
        .join(run.to_string())
        .join("batch-00003.parquet");
    assert!(file.exists(), "missing {}", file.display());
}
