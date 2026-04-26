//! Phase II.2.c — refresh-token rotate-on-use: a refresh can be used
//! once; re-use must be rejected.

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

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn refresh_can_be_used_once_replay_rejects() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let keys = tempfile::tempdir()?;
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
            "127.0.0.1:18401",
            "--issuer-url",
            "http://127.0.0.1:18401",
            "--audience",
            "etl-platform",
            "--database-url",
            &catalog_url(),
        ])
        .kill_on_drop(true)
        .spawn()?;
    for _ in 0..30 {
        if reqwest::get("http://127.0.0.1:18401/.well-known/jwks.json")
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "rot-tenant"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args([
            "auth", "create-principal", "--tenant", "rot-tenant",
            "bob", "--password", "pw", "--role", "viewer",
        ])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let login: serde_json::Value = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/login")
        .json(&serde_json::json!({"name": "bob", "password": "pw"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let r1 = login["refresh_token"].as_str().unwrap().to_string();

    let refresh1 = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/refresh")
        .json(&serde_json::json!({"refresh_token": r1}))
        .send()
        .await?;
    assert!(
        refresh1.status().is_success(),
        "first refresh failed: {}",
        refresh1.text().await?
    );
    let body1: serde_json::Value = refresh1.json().await?;
    let r2 = body1["refresh_token"].as_str().unwrap().to_string();
    assert_ne!(r2, r1, "refresh should rotate");

    let replay = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/refresh")
        .json(&serde_json::json!({"refresh_token": r1}))
        .send()
        .await?;
    assert!(!replay.status().is_success(), "replay should be rejected");

    let refresh3 = reqwest::Client::new()
        .post("http://127.0.0.1:18401/auth/refresh")
        .json(&serde_json::json!({"refresh_token": r2}))
        .send()
        .await?;
    assert!(refresh3.status().is_success());

    let _ = server.start_kill();
    Ok(())
}
