//! Phase II.2.d — manually insert a SECRET_READ audit row and verify
//! the chain still validates. Exercises the row-shape + canonical-bytes
//! path that worker activities use; running the worker end-to-end is
//! covered by the existing secrets_e2e suite.

use audit::{AuditEvent, AuditRow};
use catalog::Catalog;
use chrono::Utc;
use common_types::ids::TenantId;
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
async fn secret_read_row_chains_correctly() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "sread-tenant"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;

    let tid: (uuid::Uuid,) =
        sqlx::query_as("SELECT tenant_id FROM tenants WHERE name='sread-tenant'")
            .fetch_one(cat.pool())
            .await?;
    let row = AuditRow {
        tenant_id: Some(TenantId::from_uuid_unchecked(tid.0)),
        principal_id: None,
        jti: None,
        event: AuditEvent::SecretRead,
        target: Some("pg-source-url".into()),
        occurred_at: Utc::now(),
        payload: serde_json::json!({"backend": "file", "key": "pg-source-url"}),
    };
    cat.audit_write(&row).await?;

    let count: (i64,) =
        sqlx::query_as("SELECT count(*) FROM audit_log WHERE action = 'SECRET_READ'")
            .fetch_one(cat.pool())
            .await?;
    assert!(count.0 >= 1, "expected ≥1 SECRET_READ row");

    let verify = Command::new(cargo_bin("platform"))
        .args(["--tenant", "sread-tenant", "audit", "verify-chain"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        verify.status.success(),
        "verify failed: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    Ok(())
}
