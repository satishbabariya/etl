//! Phase II.3.a — end-to-end: create a connector, build it, publish it.

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
#[ignore = "requires wasm32-wasip2 target installed (rustup target add wasm32-wasip2)"]
async fn create_test_publish_round_trip() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "-p", "cli"])
        .status()
        .await?;

    let workdir = tempfile::tempdir()?;
    let connector_root = workdir.path().join("acme-source");

    let create = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "create",
            "acme-source",
            "--out",
            workdir.path().to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        create.status.success(),
        "create: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert!(connector_root.join("Cargo.toml").exists());
    assert!(connector_root.join("src/lib.rs").exists());
    assert!(connector_root.join("wit/source-connector.wit").exists());

    let test = Command::new(cargo_bin("platform"))
        .args(["connector", "test", connector_root.to_str().unwrap()])
        .output()
        .await?;
    assert!(
        test.status.success(),
        "test: {}",
        String::from_utf8_lossy(&test.stderr)
    );

    let registry = workdir.path().join("registry");
    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            connector_root.to_str().unwrap(),
            "--registry",
            registry.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        publish.status.success(),
        "publish: {}",
        String::from_utf8_lossy(&publish.stderr)
    );

    let artifact = registry.join("acme-source@0.1.0/component.cwasm");
    let manifest = registry.join("acme-source@0.1.0/manifest.yaml");
    assert!(artifact.exists(), "missing {}", artifact.display());
    assert!(manifest.exists(), "missing {}", manifest.display());
    let manifest_yaml = std::fs::read_to_string(manifest)?;
    assert!(manifest_yaml.contains("name: acme-source"));
    assert!(manifest_yaml.contains("version: 0.1.0"));
    assert!(manifest_yaml.contains("kind: source"));
    assert!(manifest_yaml.contains("sha256:"));
    Ok(())
}
