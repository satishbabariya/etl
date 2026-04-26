//! Phase II.2.b — auth login/whoami + wrong-password rejection.

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
async fn login_then_whoami_returns_principal() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ack"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args([
            "auth",
            "create-principal",
            "--tenant",
            "ack",
            "alice",
            "--password",
            "pw",
            "--role",
            "operator",
        ])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let creds_path = dirs::home_dir().unwrap().join(".etl/credentials.json");
    let _ = std::fs::remove_file(&creds_path);

    let login = Command::new(cargo_bin("platform"))
        .args(["auth", "login", "alice", "--password", "pw"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        login.status.success(),
        "login failed: {}",
        String::from_utf8_lossy(&login.stderr)
    );

    let whoami = Command::new(cargo_bin("platform"))
        .args(["auth", "whoami"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    let stdout = String::from_utf8_lossy(&whoami.stdout);
    assert!(whoami.status.success());
    assert!(stdout.contains("Operator"), "expected role: {stdout}");

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn login_with_wrong_password_rejects() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ack2"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args([
            "auth",
            "create-principal",
            "--tenant",
            "ack2",
            "bob",
            "--password",
            "right",
            "--role",
            "viewer",
        ])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let login = Command::new(cargo_bin("platform"))
        .args(["auth", "login", "bob", "--password", "wrong"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(!login.status.success());
    let stderr = String::from_utf8_lossy(&login.stderr);
    assert!(
        stderr.contains("invalid password"),
        "expected invalid-password error: {stderr}"
    );

    Ok(())
}
