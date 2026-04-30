//! Phase II.3.b — TypeScript connector lifecycle:
//!   1. `platform connector create <name> --lang typescript`
//!   2. `platform connector test <name>` (npm install + vitest + jco componentize)
//!   3. `platform connector publish <name> --registry <dir>`
//! Asserts the produced artifact + manifest.

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
#[ignore = "requires node + npm + network for npm install + jco"]
async fn typescript_connector_lifecycle() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "-p", "cli"])
        .status()
        .await?;
    assert!(status.success());

    let scratch = tempfile::tempdir()?;
    let connector_name = "lifecycle-demo-ts";
    let connector_dir = scratch.path().join(connector_name);
    let registry_dir = scratch.path().join("connectors");

    let create = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "create",
            connector_name,
            "--lang",
            "typescript",
            "--out",
            scratch.path().to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        create.status.success(),
        "create: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert!(connector_dir.join("package.json").exists());
    assert!(connector_dir.join("src/connector.ts").exists());
    assert!(connector_dir.join("wit/source-connector.wit").exists());

    let test = Command::new(cargo_bin("platform"))
        .args(["connector", "test", connector_dir.to_str().unwrap()])
        .output()
        .await?;
    assert!(
        test.status.success(),
        "test: stdout={} stderr={}",
        String::from_utf8_lossy(&test.stdout),
        String::from_utf8_lossy(&test.stderr)
    );
    assert!(connector_dir.join("dist/connector.wasm").exists());

    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            connector_dir.to_str().unwrap(),
            "--registry",
            registry_dir.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        publish.status.success(),
        "publish: {}",
        String::from_utf8_lossy(&publish.stderr)
    );

    let cwasm = registry_dir
        .join(format!("{connector_name}@0.1.0"))
        .join("component.cwasm");
    let manifest = registry_dir
        .join(format!("{connector_name}@0.1.0"))
        .join("manifest.yaml");
    assert!(cwasm.exists(), "missing {}", cwasm.display());
    assert!(manifest.exists(), "missing {}", manifest.display());

    let manifest_text = std::fs::read_to_string(&manifest)?;
    assert!(manifest_text.contains(&format!("name: {connector_name}")));
    assert!(manifest_text.contains("version: 0.1.0"));
    assert!(manifest_text.contains("kind: source"));
    assert!(manifest_text.contains("sha256:"));

    Ok(())
}
