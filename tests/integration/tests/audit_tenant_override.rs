//! Phase II.2.d — admin login + --tenant override emits two TENANT_OVERRIDE
//! rows (one in admin's home tenant, one in target).

use catalog::Catalog;
use std::path::{Path, PathBuf};
use std::time::Duration;
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

/// `platform` invocation with HOME pointed at a per-test temp dir, so
/// the CLI's credential cache (`$HOME/.etl/credentials.json`) never
/// touches the real user's file.
fn platform(home: &Path) -> Command {
    let mut cmd = Command::new(cargo_bin("platform"));
    cmd.env("HOME", home)
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root());
    cmd
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn admin_tenant_override_audits_both_tenants() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let keys = tempfile::tempdir()?;
    let cli_home = tempfile::tempdir()?;
    let home = cli_home.path();
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(), "init-issuer"])
        .status()
        .await?;
    let mut server = Command::new(cargo_bin("etl-auth"))
        .args([
            "--keys-dir",
            keys.path().to_str().unwrap(),
            "serve",
            "--bind",
            "127.0.0.1:18460",
            "--issuer-url",
            "http://127.0.0.1:18460",
            "--audience",
            "etl-platform",
            "--database-url",
            &catalog_url(),
        ])
        .kill_on_drop(true)
        .spawn()?;
    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18460/.well-known/jwks.json")
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    platform(home)
        .args(["tenant", "create", "home"])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;
    platform(home)
        .args(["tenant", "create", "other"])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;
    platform(home)
        .args([
            "auth",
            "create-principal",
            "--tenant",
            "home",
            "root",
            "--password",
            "pw",
            "--role",
            "admin",
        ])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;

    platform(home)
        .args(["auth", "login", "root", "--password", "pw"])
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18460")
        .output()
        .await?;

    let out = platform(home)
        .args(["--tenant", "other", "audit", "tail", "--limit", "1"])
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18460")
        .output()
        .await?;
    assert!(
        out.status.success(),
        "audit tail with --tenant: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let home_rows: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log al \
         JOIN tenants t ON t.tenant_id = al.tenant_id \
         WHERE t.name = 'home' AND al.action = 'TENANT_OVERRIDE'",
    )
    .fetch_one(cat.pool())
    .await?;
    let other: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log al \
         JOIN tenants t ON t.tenant_id = al.tenant_id \
         WHERE t.name = 'other' AND al.action = 'TENANT_OVERRIDE'",
    )
    .fetch_one(cat.pool())
    .await?;
    assert_eq!(home_rows.0, 1, "expected 1 home TENANT_OVERRIDE row");
    assert_eq!(other.0, 1, "expected 1 target TENANT_OVERRIDE row");

    let _ = server.start_kill();
    Ok(())
}
