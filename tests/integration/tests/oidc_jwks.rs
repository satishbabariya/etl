//! Phase II.2.c — login via etl-auth issuer, decode + verify the JWT
//! end-to-end through the JWKS endpoint.

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
async fn issuer_login_jwks_round_trip() -> anyhow::Result<()> {
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
            "127.0.0.1:18400",
            "--issuer-url",
            "http://127.0.0.1:18400",
            "--audience",
            "etl-platform",
            "--database-url",
            &catalog_url(),
        ])
        .kill_on_drop(true)
        .spawn()?;

    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18400/.well-known/jwks.json")
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    platform(home)
        .args(["tenant", "create", "oidc-tenant"])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;
    platform(home)
        .args([
            "auth",
            "create-principal",
            "--tenant",
            "oidc-tenant",
            "alice",
            "--password",
            "pw",
            "--role",
            "operator",
        ])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;

    let creds_path = home.join(".etl/credentials.json");
    let login = platform(home)
        .args(["auth", "login", "alice", "--password", "pw"])
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18400")
        .output()
        .await?;
    assert!(
        login.status.success(),
        "login: {}",
        String::from_utf8_lossy(&login.stderr)
    );

    let resp = reqwest::get("http://127.0.0.1:18400/.well-known/jwks.json").await?;
    let set: auth::jwks::JwkSet = resp.json().await?;
    let cached: serde_json::Value = serde_json::from_slice(&std::fs::read(&creds_path)?)?;
    let access = cached["access_token"].as_str().unwrap();

    let v = auth::jwt::JwtVerifier::jwks_inline(set)
        .with_issuer("http://127.0.0.1:18400")
        .with_audience("etl-platform");
    let p = v.verify(access).await?;
    assert_eq!(p.role, common_types::auth::Role::Operator);

    let _ = server.start_kill();
    Ok(())
}
