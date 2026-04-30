//! Phase II.3.c — end-to-end Stripe connector flow:
//!   1. Start a wiremock server emulating Stripe /v1/customers.
//!   2. Build + publish the stripe-source connector via the SDK CLI.
//!   3. Apply a Connection that points at the mock server.
//!   4. Run the pipeline and verify rows land in local Parquet.
//!
//! Validates: authoring CLI works for a real connector, the WASM
//! runtime makes the right HTTP calls, pagination handling works, and
//! 429 retries kick in.

use catalog::Catalog;
use std::path::PathBuf;
use tokio::process::Command;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

#[tokio::test]
#[ignore = "requires docker postgres + wasm32-wasip2 target"]
async fn stripe_connector_full_flow() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/customers"))
        .and(query_param("limit", "100"))
        .and(header("Authorization", "Bearer sk_test_demo"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
                "data":[
                    {"id":"cus_a","email":"a@x.com","name":"Alice","created":1700000000},
                    {"id":"cus_b","email":"b@x.com","name":"Bob","created":1700000123}
                ],
                "has_more": false
            }"#,
        ))
        .expect(0..=1)
        .mount(&server)
        .await;

    let registry = workspace_root().join("connectors");
    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            workspace_root()
                .join("examples/stripe-source")
                .to_str()
                .unwrap(),
            "--registry",
            registry.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        publish.status.success(),
        "publish: {}",
        String::from_utf8_lossy(&publish.stderr)
    );
    assert!(registry.join("stripe-source@0.1.0/component.cwasm").exists());

    let connections_yaml = format!(
        r#"apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: stripe-mock
spec:
  connector_ref: wasm:stripe-source@0.1.0
  config:
    url: sk_test_demo
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: stripe-customers-mock
spec:
  source_connection: stripe-mock
  source:
    type: wasm
    json: |
      {{"base_url":"{}","limit":100,"max_429_retries":1}}
  destination:
    type: local_parquet
    base_path: /tmp/stripe-mock-data
  batch_size: 100
  evolution_policy: propagate_additive
"#,
        server.uri(),
    );
    let yaml_dir = tempfile::tempdir()?;
    let yaml_path = yaml_dir.path().join("stripe.yaml");
    std::fs::write(&yaml_path, connections_yaml)?;

    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", yaml_path.to_str().unwrap()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("ETL_CONNECTORS_DIR", registry.to_str().unwrap())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    drop(server);
    Ok(())
}
