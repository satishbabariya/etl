//! `platform tenant create` refuses a name that already exists: tenant
//! names resolve `--tenant <name>` everywhere, so duplicates are ambiguous.

use catalog::Catalog;
use std::path::{Path, PathBuf};
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

/// `platform` with HOME in a temp dir, so the real credential cache is
/// never touched. Temporal is not running; namespace registration warns.
fn platform(home: &Path) -> Command {
    let mut cmd = Command::new(cargo_bin("platform"));
    cmd.env("HOME", home)
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:1")
        .current_dir(workspace_root());
    cmd
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn duplicate_tenant_name_is_rejected() -> anyhow::Result<()> {
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;
    let home = tempfile::tempdir()?;

    let first = platform(home.path())
        .args(["tenant", "create", "dupco"])
        .output()
        .await?;
    assert!(
        first.status.success(),
        "first create failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );

    let second = platform(home.path())
        .args(["tenant", "create", "dupco"])
        .output()
        .await?;
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(!second.status.success(), "duplicate create succeeded");
    assert!(
        stderr.contains("already exists"),
        "unexpected error: {stderr}"
    );

    let n = cat
        .list_tenants()
        .await?
        .into_iter()
        .filter(|t| t.name == "dupco")
        .count();
    assert_eq!(n, 1);
    Ok(())
}
