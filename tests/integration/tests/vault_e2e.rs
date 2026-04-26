//! Phase II.2.b — Vault-backed SecretRef resolves end-to-end through
//! the platform CLI. Gated on VAULT_ADDR; skipped silently otherwise.

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
#[ignore = "requires docker postgres + vault"]
async fn vault_backed_secret_resolves_to_plaintext() -> anyhow::Result<()> {
    let vault_addr = match std::env::var("VAULT_ADDR") {
        Ok(a) => a,
        Err(_) => {
            eprintln!("VAULT_ADDR not set — skipping vault_e2e");
            return Ok(());
        }
    };
    let vault_token = std::env::var("VAULT_TOKEN").unwrap_or_else(|_| "etl-dev-token".into());

    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    let plaintext = "postgres://etl:etl@localhost:5432/etl_source_demo";
    let body = serde_json::json!({"data": {"value": plaintext}}).to_string();
    let put = Command::new("curl")
        .args([
            "-sf",
            "-XPOST",
            "-H",
            &format!("X-Vault-Token: {vault_token}"),
            "-d",
            &body,
            &format!("{vault_addr}/v1/secret/data/etl/pg-url-vault"),
        ])
        .output()
        .await?;
    assert!(
        put.status.success(),
        "vault write failed: {}",
        String::from_utf8_lossy(&put.stderr)
    );

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let create = Command::new(cargo_bin("platform"))
        .args([
            "secret",
            "create",
            "pg-url-vault",
            "--backend",
            "vault",
            "--key",
            "etl/pg-url-vault",
        ])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        create.status.success(),
        "secret create: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync-vault.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    let row: (serde_json::Value,) =
        sqlx::query_as("SELECT config FROM connections WHERE name='source-vault'")
            .fetch_one(cat.pool())
            .await?;
    let raw = serde_json::to_string(&row.0)?;
    assert!(!raw.contains("postgres://"), "plaintext leaked: {raw}");
    assert_eq!(
        row.0
            .get("url_secret")
            .and_then(|v| v.get("backend"))
            .and_then(|v| v.as_str()),
        Some("vault")
    );
    Ok(())
}
