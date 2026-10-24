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
        // Empty ⇒ single-table: the batch lands in spec.table.
        stream_name: String::new(),
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

fn cdc_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("_cdc.op", DataType::Utf8, false),
        Field::new("_cdc.lsn", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            Arc::new(StringArray::from(vec!["i", "i", "d"])),
            Arc::new(StringArray::from(vec!["0/1", "0/2", "0/3"])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn cdc_flow_deletes_and_merges_in_one_transaction() {
    let server = MockServer::start().await;
    mock_env(&server);
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server)
        .await;

    let res = SnowflakeLoader
        .load(&spec(vec!["id".into()]), load_id(2), cdc_batch())
        .await
        .unwrap();
    assert_eq!(res.rows_loaded, 3);

    let reqs = server.received_requests().await.unwrap();
    let bodies: Vec<String> = reqs
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    // Target DDL carries data columns only.
    let ddl = bodies
        .iter()
        .find(|b| b.contains("CREATE TABLE IF NOT EXISTS \\\"DB\\\".\\\"RAW\\\".\\\"items\\\""))
        .expect("target DDL");
    assert!(!ddl.contains("_cdc"), "got {ddl}");
    // Key 2 (i then d) is deleted; key 1 is merged; one transaction.
    let last = bodies.last().unwrap();
    assert!(last.contains("BEGIN"), "got {last}");
    assert!(
        last.contains("DELETE FROM \\\"DB\\\".\\\"RAW\\\".\\\"items\\\" WHERE (\\\"id\\\" = 2)"),
        "got {last}"
    );
    assert!(last.contains("MERGE INTO"), "got {last}");
    assert!(!last.contains("_cdc"), "got {last}");
    assert!(last.contains("_ETL_LOADED_BATCHES"), "got {last}");
    assert!(last.contains("COMMIT"), "got {last}");
}

#[tokio::test]
async fn cdc_batch_without_pk_is_rejected() {
    let server = MockServer::start().await;
    mock_env(&server);
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server)
        .await;

    let err = SnowflakeLoader
        .load(&spec(vec![]), load_id(3), cdc_batch())
        .await
        .err()
        .expect("CDC without pk must fail");
    assert!(format!("{err:#}").contains("pk_columns"), "got {err:#}");
}

#[tokio::test]
async fn stream_name_routes_each_batch_to_its_own_table() {
    let server = MockServer::start().await;
    mock_env(&server);
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(&server)
        .await;

    for (seq, stream) in [(20, "public.users"), (21, "public.orders")] {
        let id = LoadId {
            stream_name: stream.into(),
            ..load_id(seq)
        };
        let res = SnowflakeLoader
            .load(&spec(vec![]), id, batch())
            .await
            .unwrap();
        assert_eq!(res.path, format!("snowflake://xy12345/DB/RAW/{stream}"));
    }

    let reqs = server.received_requests().await.unwrap();
    let all: String = reqs
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    for t in ["public.users", "public.orders"] {
        assert!(
            all.contains(&format!("INSERT INTO \\\"DB\\\".\\\"RAW\\\".\\\"{t}\\\"")),
            "no insert into {t}: {all}"
        );
    }
    assert!(
        !all.contains("\\\"items\\\""),
        "spec.table used despite stream_name"
    );
}

#[tokio::test]
async fn stream_name_with_quote_is_rejected() {
    let server = MockServer::start().await;
    mock_env(&server);
    let id = LoadId {
        stream_name: "evil\"name".into(),
        ..load_id(22)
    };
    assert!(SnowflakeLoader
        .load(&spec(vec![]), id, batch())
        .await
        .is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
}

/// CDC batch (id, name, email) whose first row is a `c` schema-change event.
fn evolved_cdc_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("_cdc.op", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![None, Some("a")])),
            Arc::new(StringArray::from(vec![None, Some("a@x")])),
            Arc::new(StringArray::from(vec!["c", "i"])),
        ],
    )
    .unwrap()
}

async fn mount_dest_columns(server: &MockServer, cols: serde_json::Value) {
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("INFORMATION_SCHEMA.COLUMNS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": cols})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(empty_ok())
        .mount(server)
        .await;
}

#[tokio::test]
async fn schema_change_adds_column_before_the_transaction() {
    let server = MockServer::start().await;
    mock_env(&server);
    mount_dest_columns(
        &server,
        serde_json::json!([["id", "NUMBER"], ["name", "TEXT"]]),
    )
    .await;

    let res = SnowflakeLoader
        .load(&spec(vec!["id".into()]), load_id(30), evolved_cdc_batch())
        .await
        .unwrap();
    assert_eq!(res.rows_loaded, 1);

    let bodies: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    let alter = bodies
        .iter()
        .position(|b| b.contains("ADD COLUMN IF NOT EXISTS \\\"email\\\" TEXT"))
        .expect("ALTER TABLE ADD COLUMN email");
    let txn = bodies
        .iter()
        .position(|b| b.contains("BEGIN"))
        .expect("BEGIN");
    assert!(alter < txn, "DDL must run before the data transaction");
    assert!(
        !bodies[alter].contains("BEGIN"),
        "DDL inside the transaction"
    );
    assert!(
        bodies[txn].contains("\\\"email\\\""),
        "MERGE missing new column"
    );
}

#[tokio::test]
async fn destructive_schema_change_pauses_without_writes() {
    let server = MockServer::start().await;
    mock_env(&server);
    // Destination still has `legacy`, which the batch no longer carries.
    mount_dest_columns(
        &server,
        serde_json::json!([
            ["id", "NUMBER"],
            ["name", "TEXT"],
            ["email", "TEXT"],
            ["legacy", "TEXT"]
        ]),
    )
    .await;

    let err = SnowflakeLoader
        .load(&spec(vec!["id".into()]), load_id(31), evolved_cdc_batch())
        .await
        .err()
        .expect("dropped column must pause the pipeline");
    assert!(format!("{err:#}").contains("destructive"), "got {err:#}");
    let bodies: Vec<String> = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    assert!(!bodies.iter().any(|b| b.contains("ALTER TABLE")), "DDL ran");
    assert!(!bodies.iter().any(|b| b.contains("BEGIN")), "data written");
}
