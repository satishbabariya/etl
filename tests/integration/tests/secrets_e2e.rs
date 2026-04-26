//! Phase II.2.a end-to-end check: a connection applied with `url_secret`
//! lands in the catalog with NO plaintext URL — only a SecretRef pointer.

use anyhow::Context;
use catalog::Catalog;
use std::path::PathBuf;
use tokio::process::Command;

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
#[ignore = "requires docker postgres"]
async fn apply_with_url_secret_writes_no_plaintext_to_catalog() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // 1) Stage the plaintext in an isolated file backend + register the
    //    catalog SecretRef row in one shot.
    let secrets_dir = tempfile::tempdir()?;
    let secrets_file = secrets_dir.path().join(".etl-secrets.json");
    let plaintext = "postgres://etl:etl@localhost:5432/etl_source_demo";

    let put = Command::new(cargo_bin("platform"))
        .args(["secret", "put", "pg-source-url", plaintext, "--register"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("ETL_SECRETS_FILE", &secrets_file)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        put.status.success(),
        "secret put failed: {}",
        String::from_utf8_lossy(&put.stderr)
    );

    // 2) Apply a Connection that references the secret by name.
    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync-secret.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("ETL_SECRETS_FILE", &secrets_file)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply failed: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    // 3) Read the connection row directly and assert the JSON has no
    //    plaintext URL substring AND `url_secret` is an object (resolved).
    let row: (serde_json::Value,) = sqlx::query_as(
        "SELECT config FROM connections WHERE name = 'source-demo-secret'",
    )
    .fetch_one(cat.pool())
    .await
    .context("loading connection row")?;
    let config = row.0;

    let raw = serde_json::to_string(&config)?;
    assert!(
        !raw.contains(plaintext),
        "catalog config still contains plaintext URL: {raw}"
    );
    assert!(
        !raw.contains("postgres://"),
        "catalog config still contains a postgres:// URL: {raw}"
    );

    let url_secret = config
        .get("url_secret")
        .expect("config should have url_secret field");
    assert!(
        url_secret.is_object(),
        "url_secret should be a resolved SecretRef object, got {url_secret}"
    );
    assert_eq!(
        url_secret.get("name").and_then(|v| v.as_str()),
        Some("pg-source-url")
    );
    assert_eq!(
        url_secret.get("backend").and_then(|v| v.as_str()),
        Some("file")
    );

    // url field should be absent (we never set it for url_secret pipelines).
    assert!(config.get("url").is_none(), "legacy url field should be absent");

    Ok(())
}
