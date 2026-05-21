//! Multi-table routing integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service.

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
            eprintln!("SKIP postgres_loader_multi_table test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_multi_loader_test_{}", uuid::Uuid::new_v4().simple());
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

fn plain_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn plain_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(|s| s.to_string())).collect();
    RecordBatch::try_new(
        plain_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
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

fn cdc_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
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

fn spec(connection_url: &str, schema: &str, pk: Vec<String>) -> DestinationSpec {
    DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: connection_url.into(),
        schema: schema.into(),
        // table is the fallback when stream_name is empty; multi-table
        // tests always set stream_name, so this value should never be used.
        table: "unused_fallback".into(),
        pk_columns: pk,
    })
}

fn lid(stream: &str, seq: u32) -> LoadId {
    LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: seq,
        stream_name: stream.into(),
    }
}

async fn count(pool: &sqlx::PgPool, schema: &str, table: &str) -> i64 {
    sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"{table}\""
    ))
    .fetch_one(pool)
    .await
    .unwrap()
    .get(0)
}

#[tokio::test]
async fn multi_table_append_lands_in_distinct_tables() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    PostgresLoader
        .load(&s, lid("users", 0), plain_batch(&[(1, Some("alice"))]))
        .await
        .unwrap();
    PostgresLoader
        .load(
            &s,
            lid("orders", 0),
            plain_batch(&[(100, Some("o-1")), (101, Some("o-2"))]),
        )
        .await
        .unwrap();

    assert_eq!(count(&pool, &schema, "users").await, 1);
    assert_eq!(count(&pool, &schema, "orders").await, 2);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_cdc_routes_per_stream() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec!["id".into()]);

    // Two streams, each with a mixed CDC batch.
    PostgresLoader
        .load(
            &s,
            lid("users", 0),
            cdc_batch(&[(1, Some("alice"), "i"), (2, Some("bob"), "i")]),
        )
        .await
        .unwrap();
    PostgresLoader
        .load(
            &s,
            lid("orders", 0),
            cdc_batch(&[(10, Some("o-10"), "i"), (11, Some("o-11"), "i")]),
        )
        .await
        .unwrap();
    // Streaming batch on users only.
    PostgresLoader
        .load(
            &s,
            lid("users", 1),
            cdc_batch(&[(1, Some("alice-v2"), "u"), (2, None, "d")]),
        )
        .await
        .unwrap();

    let users: Vec<(i64, Option<String>)> = sqlx::query(&format!(
        "SELECT id, name FROM \"{schema}\".\"users\" ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (r.get::<i64, _>(0), r.try_get::<String, _>(1).ok()))
    .collect();
    assert_eq!(users, vec![(1, Some("alice-v2".into()))]);
    assert_eq!(count(&pool, &schema, "orders").await, 2);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_idempotency_keys_include_stream_name() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    // Same (run, batch_seq) but different stream_name — must NOT collide
    // in the _etl_loaded_batches log (stream_name is part of the PK).
    let tid = TenantId::new();
    let pid = PipelineId::new();
    let rid = RunId::new();
    let mk = |stream: &str| LoadId {
        tenant_id: tid.clone(),
        pipeline_id: pid.clone(),
        run_id: rid.clone(),
        batch_seq: 0,
        stream_name: stream.into(),
    };

    PostgresLoader
        .load(&s, mk("users"), plain_batch(&[(1, Some("a"))]))
        .await
        .unwrap();
    PostgresLoader
        .load(&s, mk("orders"), plain_batch(&[(10, Some("o"))]))
        .await
        .unwrap();

    assert_eq!(count(&pool, &schema, "users").await, 1);
    assert_eq!(count(&pool, &schema, "orders").await, 1);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_stream_with_dot_in_name() {
    // The connector convention is `<src_schema>.<table>` (e.g. "public.users").
    // The PG loader should accept the literal dot as part of the table name.
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    PostgresLoader
        .load(
            &s,
            lid("public.users", 0),
            plain_batch(&[(1, Some("a"))]),
        )
        .await
        .unwrap();

    assert_eq!(count(&pool, &schema, "public.users").await, 1);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn multi_table_rejects_quote_char_in_stream_name() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);

    let err = PostgresLoader
        .load(
            &s,
            lid("bad\"name", 0),
            plain_batch(&[(1, Some("a"))]),
        )
        .await
        .unwrap_err();
    assert!(format!("{err}").to_lowercase().contains("illegal"));
    drop_schema(&pool, &schema).await;
}
