//! Phase II.2.b: tenants.status='suspended' blocks pipeline runs.

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
async fn suspended_tenant_cannot_run_pipeline() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", "examples/dsl/customers-sync.yaml"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply failed: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    let suspend = Command::new(cargo_bin("platform"))
        .args(["tenant", "suspend", "dev"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        suspend.status.success(),
        "suspend failed: {}",
        String::from_utf8_lossy(&suspend.stderr)
    );

    let row: (uuid::Uuid,) =
        sqlx::query_as("SELECT pipeline_id FROM pipelines WHERE name='customers-sync'")
            .fetch_one(cat.pool())
            .await?;
    let pid = row.0.to_string();

    let run = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pid])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(!run.status.success(), "expected pipeline run to fail");
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        stderr.contains("suspended"),
        "expected 'suspended' in error, got: {stderr}"
    );

    let resume = Command::new(cargo_bin("platform"))
        .args(["tenant", "resume", "dev"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        resume.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );

    Ok(())
}
