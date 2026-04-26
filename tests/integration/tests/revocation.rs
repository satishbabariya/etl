//! Phase II.2.c — revoking an access token's jti blocks subsequent
//! calls when ETL_AUTH_REVOCATION_CHECK=1 is set.

use base64::Engine;
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

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "rev-tenant"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    Command::new(cargo_bin("platform"))
        .args([
            "auth", "create-principal", "--tenant", "rev-tenant",
            "carol", "--password", "pw", "--role", "operator",
        ])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let creds_path = dirs::home_dir().unwrap().join(".etl/credentials.json");
    let _ = std::fs::remove_file(&creds_path);

    Command::new(cargo_bin("platform"))
        .args(["auth", "login", "carol", "--password", "pw"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .current_dir(workspace_root())
        .output()
        .await?;

    let cached: serde_json::Value = serde_json::from_slice(&std::fs::read(&creds_path)?)?;
    let access = cached["access_token"].as_str().unwrap();
    let parts: Vec<&str> = access.split('.').collect();
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1])?;
    let claims: serde_json::Value = serde_json::from_slice(&payload)?;
    let jti = claims["jti"].as_str().unwrap().to_string();

    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_REVOCATION_CHECK", "1")
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply pre-revoke: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    Command::new(cargo_bin("etl-auth"))
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

    let apply2 = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_REVOCATION_CHECK", "1")
        .env("ETL_AUTH_ISSUER", "http://127.0.0.1:18402")
        .current_dir(workspace_root())
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
