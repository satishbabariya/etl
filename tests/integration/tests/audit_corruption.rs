//! Phase II.2.d — corrupt a payload via SQL → verify-chain flags it.

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
async fn corrupting_payload_fails_verify_chain() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "corrupt-test"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let id: (i64,) = sqlx::query_as(
        "SELECT audit_id FROM audit_log ORDER BY audit_id DESC OFFSET 1 LIMIT 1",
    )
    .fetch_one(cat.pool())
    .await?;
    sqlx::query(
        "UPDATE audit_log SET payload = '{\"tampered\": true}'::jsonb WHERE audit_id = $1",
    )
    .bind(id.0)
    .execute(cat.pool())
    .await?;

    let verify = Command::new(cargo_bin("platform"))
        .args(["audit", "verify-chain"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(!verify.status.success(), "expected verify-chain to fail");
    let stderr = String::from_utf8_lossy(&verify.stderr);
    assert!(
        stderr.contains("MISMATCH"),
        "expected MISMATCH in stderr: {stderr}"
    );
    Ok(())
}
