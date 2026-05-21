//! Integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service to be running.
//! Skipped (with a clear message) when the database is unreachable.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
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
            eprintln!("SKIP postgres_loader test: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_loader_test_{}", uuid::Uuid::new_v4().simple());
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

fn tiny_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap()
}

fn spec(connection_url: &str, schema: &str, pk: Vec<String>) -> DestinationSpec {
    DestinationSpec::Postgres(PostgresDestinationSpec {
        connection_url: connection_url.into(),
        schema: schema.into(),
        table: "customers".into(),
        pk_columns: pk,
    })
}

#[tokio::test]
async fn append_only_load_writes_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);
    let load_id = LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: 0,
        stream_name: String::new(),
    };
    PostgresLoader.load(&s, load_id, tiny_batch()).await.expect("load");

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"customers\""
    ))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 3);
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn second_load_with_same_load_id_is_noop() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec![]);
    let load_id = LoadId {
        tenant_id: TenantId::new(),
        pipeline_id: PipelineId::new(),
        run_id: RunId::new(),
        batch_seq: 7,
        stream_name: String::new(),
    };
    let r1 = PostgresLoader.load(&s, load_id.clone(), tiny_batch()).await.unwrap();
    let r2 = PostgresLoader.load(&s, load_id, tiny_batch()).await.unwrap();
    assert_eq!(r1.rows_loaded, 3);
    assert_eq!(r2.rows_loaded, 0);

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"customers\""
    ))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 3, "row count must not double on retry");
    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn upsert_overwrites_on_pk_conflict() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema, vec!["id".into()]);

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("old-a"), Some("old-b")])),
        ],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("new-a"),
                Some("new-b"),
                Some("new-c"),
            ])),
        ],
    )
    .unwrap();

    let pid = PipelineId::new();
    let tid = TenantId::new();
    let rid = RunId::new();

    PostgresLoader
        .load(
            &s,
            LoadId {
                tenant_id: tid.clone(),
                pipeline_id: pid.clone(),
                run_id: rid.clone(),
                batch_seq: 0,
                stream_name: String::new(),
            },
            batch1,
        )
        .await
        .unwrap();
    PostgresLoader
        .load(
            &s,
            LoadId {
                tenant_id: tid,
                pipeline_id: pid,
                run_id: rid,
                batch_seq: 1,
                stream_name: String::new(),
            },
            batch2,
        )
        .await
        .unwrap();

    let rows = sqlx::query(&format!(
        "SELECT id, name FROM \"{schema}\".\"customers\" ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 3);
    let names: Vec<String> = rows.iter().map(|r| r.get::<String, _>(1)).collect();
    assert_eq!(names, vec!["new-a", "new-b", "new-c"]);
    drop_schema(&pool, &schema).await;
}
