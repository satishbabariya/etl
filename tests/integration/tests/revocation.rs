//! Phase II.2.c — revoking an access token's jti blocks subsequent
//! calls when ETL_AUTH_REVOCATION_CHECK=1 is set.

use base64::Engine;
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
async fn revoking_jti_blocks_subsequent_calls() -> anyhow::Result<()> {
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
            "127.0.0.1:18402",
            "--issuer-url",
            "http://127.0.0.1:18402",
            "--audience",
            "etl-platform",
            "--database-url",
            &catalog_url(),
        ])
        .kill_on_drop(true)
        .spawn()?;
    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18402/.well-known/jwks.json")
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    platform(home)
        .args(["tenant", "create", "rev-tenant"])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;
    platform(home)
        .args([
            "auth",
            "create-principal",
            "--tenant",
            "rev-tenant",
            "carol",
            "--password",
            "pw",
            "--role",
            "operator",
        ])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;

    let creds_path = home.join(".etl/credentials.json");

    platform(home)
        .args(["auth", "login", "carol", "--password", "pw"])
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .output()
        .await?;

    let cached: serde_json::Value = serde_json::from_slice(&std::fs::read(&creds_path)?)?;
    let access = cached["access_token"].as_str().unwrap();
    let parts: Vec<&str> = access.split('.').collect();
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1])?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    let jti = claims["jti"].as_str().unwrap().to_string();

    let apply = platform(home)
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("ETL_AUTH_REVOCATION_CHECK", "1")
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply pre-revoke: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    // No --keys-dir here, so etl-auth would resolve $HOME/.etl/auth-keys;
    // keep it off the real home directory too.
    Command::new(cargo_bin("etl-auth"))
        .env("HOME", home)
        .args([
            "revoke",
            &jti,
            "--tenant",
            "rev-tenant",
            "--database-url",
            &catalog_url(),
        ])
        .status()
        .await?;

    let apply2 = platform(home)
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("ETL_AUTH_REVOCATION_CHECK", "1")
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .output()
        .await?;
    assert!(!apply2.status.success(), "expected revoked-token rejection");
    let stderr = String::from_utf8_lossy(&apply2.stderr);
    assert!(
        stderr.contains("revoked"),
        "expected 'revoked' in stderr: {stderr}"
    );

    let _ = server.start_kill();
    Ok(())
}
