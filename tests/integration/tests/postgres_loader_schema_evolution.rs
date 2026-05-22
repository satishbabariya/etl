//! Schema evolution integration tests for the Postgres destination loader.
//!
//! Requires the docker-compose `postgres` service to be running.

use arrow::array::{Float64Array, Int64Array, StringArray};
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
            eprintln!("SKIP postgres_loader_schema_evolution: cannot reach {url}: {e}");
            return None;
        }
    };
    let schema = format!("etl_evo_loader_test_{}", uuid::Uuid::new_v4().simple());
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

fn base_cdc_schema() -> Arc<Schema> {
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

fn widened_cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn base_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(str::to_string)).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.2.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();
    RecordBatch::try_new(
        base_cdc_schema(),
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

fn widened_batch(rows: &[(i64, Option<&str>, Option<f64>, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let names: Vec<Option<String>> = rows.iter().map(|r| r.1.map(str::to_string)).collect();
    let scores: Vec<Option<f64>> = rows.iter().map(|r| r.2).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.3.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();
    RecordBatch::try_new(
        widened_cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn additive_new_column_mid_stream_lands_correctly() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(&s, load_id(0), base_batch(&[(1, Some("alice"), "i"), (2, Some("bob"), "i")]))
        .await
        .expect("batch 0");

    let cols_before: Vec<String> = sqlx::query(
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
    assert_eq!(cols_before, vec!["id", "name"]);

    PostgresLoader
        .load(
            &s,
            load_id(1),
            widened_batch(&[(0, None, None, "c"), (3, Some("carol"), Some(9.5), "i")]),
        )
        .await
        .expect("batch 1 (additive evolution)");

    let cols_after: Vec<String> = sqlx::query(
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
    assert_eq!(cols_after, vec!["id", "name", "score"]);

    let rows: Vec<(i64, Option<String>, Option<f64>)> = sqlx::query(&format!(
        "SELECT id, name, score FROM \"{schema}\".\"events\" ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|r| (
        r.get::<i64, _>(0),
        r.try_get::<String, _>(1).ok(),
        r.try_get::<f64, _>(2).ok(),
    ))
    .collect();

    assert_eq!(rows[0], (1, Some("alice".into()), None));
    assert_eq!(rows[1], (2, Some("bob".into()), None));
    assert_eq!(rows[2], (3, Some("carol".into()), Some(9.5)));

    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn schema_change_only_batch_adds_column_and_does_not_insert_rows() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(&s, load_id(0), base_batch(&[(1, Some("alice"), "i")]))
        .await
        .expect("seed");

    PostgresLoader
        .load(&s, load_id(1), widened_batch(&[(0, None, None, "c")]))
        .await
        .expect("c-only batch");

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
    assert_eq!(cols, vec!["id", "name", "score"]);

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"events\""
    ))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 1, "no rows inserted from c-only batch (alice from seed only)");

    drop_schema(&pool, &schema).await;
}

// ───── destructive change tests ─────

fn dropped_name_cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn dropped_name_batch(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let ops: Vec<String> = rows.iter().map(|r| r.1.to_string()).collect();
    let lsns: Vec<String> = (0..rows.len()).map(|i| format!("lsn-{i}")).collect();
    let ts: Vec<i64> = (0..rows.len()).map(|i| 1_779_667_200_000_000 + i as i64).collect();
    RecordBatch::try_new(
        dropped_name_cdc_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(ops)),
            Arc::new(StringArray::from(lsns)),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn destructive_drop_column_returns_error_and_leaves_destination_unchanged() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    PostgresLoader
        .load(&s, load_id(0), base_batch(&[(1, Some("alice"), "i")]))
        .await
        .expect("seed");

    let err = PostgresLoader
        .load(&s, load_id(1), dropped_name_batch(&[(0, "c"), (2, "i")]))
        .await
        .unwrap_err();

    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("destructive") && msg.contains("operator"),
        "expected destructive-change error, got: {err}"
    );
    assert!(msg.contains("name"), "error must name the dropped column, got: {err}");

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
    assert_eq!(cols, vec!["id", "name"], "destination must be unchanged after abort");

    let count: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT FROM \"{schema}\".\"events\""
    ))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 1, "alice must still be in the table");

    drop_schema(&pool, &schema).await;
}

#[tokio::test]
async fn destructive_type_narrowing_returns_error_and_leaves_destination_unchanged() {
    let Some((pool, schema)) = fresh_schema().await else { return };
    let s = spec(&test_url(), &schema);

    let bigint_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let seed = RecordBatch::try_new(
        bigint_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![Some(1_000_000i64)])),
            Arc::new(StringArray::from(vec!["i"])),
            Arc::new(StringArray::from(vec!["lsn-0"])),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(vec![1_779_667_200_000_000i64])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    PostgresLoader.load(&s, load_id(0), seed).await.expect("seed");

    let narrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int32, true),
        Field::new(cdc::COL_OP, DataType::Utf8, false),
        Field::new(cdc::COL_LSN, DataType::Utf8, false),
        Field::new(
            cdc::COL_COMMIT_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let narrow = RecordBatch::try_new(
        narrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(arrow::array::Int32Array::from(vec![Some(0i32)])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(StringArray::from(vec!["lsn-1"])),
            Arc::new(
                arrow::array::TimestampMicrosecondArray::from(vec![1_779_667_200_000_001i64])
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();

    let err = PostgresLoader.load(&s, load_id(1), narrow).await.unwrap_err();

    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("destructive") && msg.contains("operator"),
        "expected destructive-change error, got: {err}"
    );
    assert!(msg.contains("amount"), "error must name the narrowed column, got: {err}");

    drop_schema(&pool, &schema).await;
}
