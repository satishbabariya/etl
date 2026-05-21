//! CDC-mode integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service to be running.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use common_types::cdc;
use common_types::ids::{PipelineId, RunId, TenantId};
use common_types::pipeline_spec::{DestinationSpec, PostgresDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::sync::Arc;
use worker::loaders::postgres::PostgresLoader;

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn fresh_schema() -> Option<(sqlx::PgPool, String)> {
    let url = test_url();
    let pool = match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SKIP postgres_loader_cdc test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_cdc_loader_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create schema");
    Some((pool, schema))
}

async fn drop_schema(pool: &sqlx::PgPool, schema: &str) {
    let _ = sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(pool)
        .await;
}

fn cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn batch_of(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(|s| s.to_string())).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.2.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len())
        .map(|i| 1_779_667_200_000_000 + i as i64)
        .collect();

    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

fn spec(connection_url: &str, schema: &str) -> DestinationSpec {
    DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: connection_url.into(),
        schema: schema.into(),
        table: "events".into(),
        pk_columns: vec!["id".into()],
    })
}

fn load_id(seq: u32) -> LoadId {
    LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: seq,
        stream_name: String::new(),
    }
}

async fn rows_in(pool: &sqlx::PgPool, schema: &str) -> Vec<(i64, Option<String>)> {
    sqlx::query(&format!(
        "SELECT id, name FROM \"{schema}\".\"events\" ORDER BY id"
    ))
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.get::<i64, _>(0), r.try_get::<String, _>(1).ok()))
    .collect()
}

#[tokio::test]
async fn cdc_inserts_create_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);
    let batch = batch_of(&[(1, Some("a"), "i"), (2, Some("b"), "i")]);
    PostgresLoader.load(&s, load_id(0), batch).await.expect("load");

    let rows = rows_in(&pool, &schema).await;
    assert_eq!(rows, vec![(1, Some("a".into())), (2, Some("b".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_updates_overwrite_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(&s, load_id(0), batch_of(&[(1, Some("old"), "i")]))
        .await
        .unwrap();
    PostgresLoader
        .load(&s, load_id(1), batch_of(&[(1, Some("new"), "u")]))
        .await
        .unwrap();

    assert_eq!(rows_in(&pool, &schema).await, vec![(1, Some("new".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_deletes_remove_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(
            &s,
            load_id(0),
            batch_of(&[(1, Some("a"), "i"), (2, Some("b"), "i")]),
        )
        .await
        .unwrap();
    PostgresLoader
        .load(&s, load_id(1), batch_of(&[(1, None, "d")]))
        .await
        .unwrap();

    assert_eq!(rows_in(&pool, &schema).await, vec![(2, Some("b".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_mixed_batch_applies_in_order() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Seed.
    PostgresLoader
        .load(
            &s,
            load_id(0),
            batch_of(&[(1, Some("a"), "i"), (2, Some("b"), "i")]),
        )
        .await
        .unwrap();
    // Mixed batch: update 1, delete 2, insert 3.
    PostgresLoader
        .load(
            &s,
            load_id(1),
            batch_of(&[
                (1, Some("a-prime"), "u"),
                (2, None, "d"),
                (3, Some("c"), "i"),
            ]),
        )
        .await
        .unwrap();

    assert_eq!(
        rows_in(&pool, &schema).await,
        vec![(1, Some("a-prime".into())), (3, Some("c".into()))]
    );
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_snapshot_then_streaming_converges() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    // Snapshot phase — `s` rows treated like upserts.
    PostgresLoader
        .load(
            &s,
            load_id(0),
            batch_of(&[(1, Some("snap-1"), "s"), (2, Some("snap-2"), "s")]),
        )
        .await
        .unwrap();
    // Streaming phase: update 1, insert 3.
    PostgresLoader
        .load(
            &s,
            load_id(1),
            batch_of(&[(1, Some("stream-1"), "u"), (3, Some("stream-3"), "i")]),
        )
        .await
        .unwrap();

    assert_eq!(
        rows_in(&pool, &schema).await,
        vec![
            (1, Some("stream-1".into())),
            (2, Some("snap-2".into())),
            (3, Some("stream-3".into())),
        ]
    );
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_retry_with_same_load_id_is_noop() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);
    let lid = load_id(0);

    let r1 = PostgresLoader
        .load(&s, lid.clone(), batch_of(&[(1, Some("a"), "i")]))
        .await
        .unwrap();
    let r2 = PostgresLoader
        .load(&s, lid, batch_of(&[(1, Some("a"), "i")]))
        .await
        .unwrap();
    assert_eq!(r1.rows_loaded, 1);
    assert_eq!(r2.rows_loaded, 0, "retry must short-circuit");

    assert_eq!(rows_in(&pool, &schema).await, vec![(1, Some("a".into()))]);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn cdc_destination_table_has_no_metadata_columns() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);
    PostgresLoader
        .load(&s, load_id(0), batch_of(&[(1, Some("a"), "i")]))
        .await
        .unwrap();

    let cols: Vec<String> = sqlx::query(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'events' \
         ORDER BY ordinal_position",
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.get::<String, _>(0))
    .collect();
    assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
    drop_schema(&pool, &schema).await;
}
