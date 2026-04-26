//! Phase II.4 — `platform audit prune` removes old rows; the chain
//! continues to verify thanks to the checkpoint table.

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
async fn prune_then_verify_succeeds() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ret-tenant"])
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

    let before: (i64,) = sqlx::query_as("SELECT count(*) FROM audit_log")
        .fetch_one(cat.pool())
        .await?;

    // Rows are at occurred_at = now; cutoff must be strictly after them.
    // Sleep a moment so `--older-than-days 0` (cutoff = now) catches them.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let prune = Command::new(cargo_bin("platform"))
        .args(["audit", "prune", "--older-than-days", "0"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        prune.status.success(),
        "prune: {}",
        String::from_utf8_lossy(&prune.stderr)
    );

    let after: (i64,) = sqlx::query_as("SELECT count(*) FROM audit_log")
        .fetch_one(cat.pool())
        .await?;
    assert!(
        after.0 < before.0,
        "expected pruned rows, before={} after={}",
        before.0,
        after.0
    );

    let verify = Command::new(cargo_bin("platform"))
        .args(["audit", "verify-chain"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        verify.status.success(),
        "verify after prune: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let stdout = String::from_utf8_lossy(&verify.stdout);
    assert!(stdout.contains("OK"), "expected OK after prune: {stdout}");
    Ok(())
}
