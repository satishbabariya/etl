//! Phase II.4 — etl-auth init-issuer + ETL_MASTER_KEY → private.enc.
//! seal-keys upgrades a legacy keystore in place.

use std::path::PathBuf;
use tokio::process::Command;

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

#[tokio::test]
#[ignore = "requires built etl-auth binary"]
async fn init_with_master_key_writes_enc() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "-p", "auth"])
        .status()
        .await?;
    let dir = tempfile::tempdir()?;
    let out = Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", dir.path().to_str().unwrap(), "init-issuer"])
        .env("ETL_MASTER_KEY", "0".repeat(64))
        .output()
        .await?;
    assert!(
        out.status.success(),
        "init: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let kid = std::fs::read_to_string(dir.path().join("active.txt"))?;
    let kid = kid.trim();
    assert!(dir.path().join(kid).join("private.enc").exists());
    assert!(!dir.path().join(kid).join("private.pem").exists());
    Ok(())
}

#[tokio::test]
#[ignore = "requires built etl-auth binary"]
async fn seal_keys_upgrades_legacy_keystore() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "-p", "auth"])
        .status()
        .await?;
    let dir = tempfile::tempdir()?;
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", dir.path().to_str().unwrap(), "init-issuer"])
        .env_remove("ETL_MASTER_KEY")
        .output()
        .await?;
    let kid = std::fs::read_to_string(dir.path().join("active.txt"))?;
    let kid = kid.trim();
    assert!(dir.path().join(kid).join("private.pem").exists());
    let out = Command::new(cargo_bin("etl-auth"))
        .args([
            "--keys-dir",
            dir.path().to_str().unwrap(),
            "seal-keys",
            "--confirm",
        ])
        .env("ETL_MASTER_KEY", "1".repeat(64))
        .output()
        .await?;
    assert!(
        out.status.success(),
        "seal: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(dir.path().join(kid).join("private.enc").exists());
    assert!(!dir.path().join(kid).join("private.pem").exists());
    Ok(())
}
