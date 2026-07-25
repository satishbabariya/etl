//! SnowflakeLoader against a wiremock fake of the SQL API v2.
//! Proves the loader's statement sequence without a Snowflake account:
//! DDL → idempotency check → BEGIN/DML/log/COMMIT multi-statement.
//! Real-account coverage: snowflake_loader.rs (env-gated).
//!
//! Run with --test-threads=1; the ETL_SNOWFLAKE_BASE_URL and key env
//! vars are process-global seams.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use worker::loaders::snowflake::SnowflakeLoader;

fn test_pem() -> String {
    use rsa::pkcs8::EncodePrivateKey;
    let mut rng = rand::thread_rng();
    let key = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
    key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string()
}

fn spec(pk: Vec<String>) -> DestinationSpec {
    DestinationSpec::Snowflake(SnowflakeDestinationSpec {
        account: "xy12345".into(),
        user: "loader".into(),
        private_key_env: "SNOWFLAKE_MOCK_KEY".into(),
        warehouse: "W".into(),
        database: "DB".into(),
        schema: "RAW".into(),
        table: "items".into(),
        role: None,
        pk_columns: pk,
    })
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap()
}

fn load_id(seq: u32) -> LoadId {
    LoadId {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::nil()),
        pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(uuid::Uuid::nil()),
        run_id: common_types::ids::RunId::from_uuid_unchecked(uuid::Uuid::nil()),
        batch_seq: seq,
        stream_name: "public.items".into(),
    }
}

fn mock_env(server: &MockServer) {
    std::env::set_var("SNOWFLAKE_MOCK_KEY", test_pem());
    std::env::set_var("ETL_SNOWFLAKE_BASE_URL", server.uri());
}

/// Not-yet-loaded check returns an empty rowset.
fn empty_ok() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []}))
}

#[tokio::test]
async fn append_flow_issues_insert_in_transaction() {
    let server = MockServer::start().await;
    mock_env(&server);
    // catch-all 200 for DDL + check (empty rowset)
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server)
        .await;

    let res = SnowflakeLoader
        .load(&spec(vec![]), load_id(0), batch())
        .await
        .unwrap();
    assert_eq!(res.rows_loaded, 2);

    // request log: last POST must contain BEGIN + INSERT INTO target +
    // log insert + COMMIT (quotes are JSON-escaped in the raw body)
    let reqs = server.received_requests().await.unwrap();
    let last = String::from_utf8_lossy(&reqs.last().unwrap().body).to_string();
    assert!(last.contains("BEGIN"), "got {last}");
    assert!(
        last.contains("INSERT INTO \\\"DB\\\".\\\"RAW\\\".\\\"items\\\""),
        "got {last}"
    );
    assert!(last.contains("_ETL_LOADED_BATCHES"), "got {last}");
    assert!(last.contains("COMMIT"), "got {last}");
}

#[tokio::test]
async fn upsert_flow_issues_merge() {
    let server = MockServer::start().await;
    mock_env(&server);
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server)
        .await;

    let res = SnowflakeLoader
        .load(&spec(vec!["id".into()]), load_id(1), batch())
        .await
        .unwrap();
    assert_eq!(res.rows_loaded, 2);
    let reqs = server.received_requests().await.unwrap();
    let all: String = reqs
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    assert!(all.contains("MERGE INTO"), "got {all}");
    assert!(all.contains("WHEN NOT MATCHED THEN INSERT"), "got {all}");
}

#[tokio::test]
async fn replay_short_circuits_without_dml() {
    let server = MockServer::start().await;
    mock_env(&server);
    // check-loaded returns one row ⇒ already loaded
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("SELECT ROWS_LOADED"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": [["2"]]})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server)
        .await;

    let res = SnowflakeLoader
        .load(&spec(vec![]), load_id(2), batch())
        .await
        .unwrap();
    assert_eq!(res.rows_loaded, 0, "replay must be a no-op");
    let reqs = server.received_requests().await.unwrap();
    for r in &reqs {
        let b = String::from_utf8_lossy(&r.body);
        assert!(
            !b.contains("BEGIN"),
            "no transactional DML on replay; got {b}"
        );
    }
}
