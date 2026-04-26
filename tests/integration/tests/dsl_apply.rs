use anyhow::Context;
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
async fn apply_is_idempotent() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let out1 = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out1.status.success(),
        "apply 1 failed: {}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let s1 = String::from_utf8_lossy(&out1.stdout);
    assert!(
        s1.contains("1 created"),
        "expected 1 created on first apply:\n{s1}"
    );

    let out2 = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out2.status.success());
    let s2 = String::from_utf8_lossy(&out2.stdout);
    assert!(
        s2.contains("0 created") && s2.contains("1 unchanged"),
        "expected all-unchanged on second apply:\n{s2}"
    );

    let out3 = Command::new(cargo_bin("platform"))
        .args(["diff", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out3.status.success());
    let s3 = String::from_utf8_lossy(&out3.stdout);
    // Filter to diff-output lines (start with +/~/=); ignore any stray
    // tracing lines that leak onto stdout.
    let diff_lines: Vec<&str> = s3
        .lines()
        .filter(|l| l.starts_with('+') || l.starts_with('~') || l.starts_with('='))
        .collect();
    assert!(
        !diff_lines.is_empty() && diff_lines.iter().all(|l| l.starts_with('=')),
        "expected all =Unchanged lines:\n{s3}"
    );

    Ok(())
}
