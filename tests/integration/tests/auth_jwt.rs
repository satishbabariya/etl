//! Phase II.2.b/c — auth login/whoami + wrong-password rejection.
//! II.2.c moves login to the etl-auth issuer; the test spawns one.

use catalog::Catalog;
use std::path::PathBuf;
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

async fn spawn_issuer(port: u16) -> anyhow::Result<(tokio::process::Child, tempfile::TempDir, String)> {
    let keys = tempfile::tempdir()?;
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(), "init-issuer"])
        .status()
        .await?;
    let bind = format!("127.0.0.1:{port}");
    let issuer = format!("http://{bind}");
    let child = Command::new(cargo_bin("etl-auth"))
        .args([
            "--keys-dir",
            keys.path().to_str().unwrap(),
            "serve",
            "--bind",
            &bind,
            "--issuer-url",
            &issuer,
            "--audience",
            "etl-platform",
            "--database-url",
            &catalog_url(),
        ])
        .kill_on_drop(true)
        .spawn()?;
    for _ in 0..30 {
        if reqwest::get(format!("{issuer}/.well-known/jwks.json"))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok((child, keys, issuer))
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

    let (mut server, _keys, issuer) = spawn_issuer(18403).await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ack"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args([
            "auth", "create-principal", "--tenant", "ack",
            "alice", "--password", "pw", "--role", "operator",
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
        .env("ETL_AUTH_ISSUER", &issuer)
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

    let _ = server.start_kill();
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

    let (mut server, _keys, issuer) = spawn_issuer(18404).await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "ack2"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args([
            "auth", "create-principal", "--tenant", "ack2",
            "bob", "--password", "right", "--role", "viewer",
        ])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let login = Command::new(cargo_bin("platform"))
        .args(["auth", "login", "bob", "--password", "wrong"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_ISSUER", &issuer)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(!login.status.success(), "expected wrong-password rejection");

    let _ = server.start_kill();
    Ok(())
}
