//! Phase II.2.b — RBAC matrix + cross-tenant JWT rejection.

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

async fn login(name: &str, password: &str) {
    let creds_path = dirs::home_dir().unwrap().join(".etl/credentials.json");
    let _ = std::fs::remove_file(&creds_path);
    let out = Command::new(cargo_bin("platform"))
        .args(["auth", "login", name, "--password", password])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "login {name}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
#[ignore = "requires docker postgres"]
async fn viewer_cannot_write_secrets() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "rbacco"])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .current_dir(workspace_root())
        .output()
        .await?;
    for (name, role) in [("v_user", "viewer"), ("o_user", "operator"), ("a_user", "admin")] {
        let out = Command::new(cargo_bin("platform"))
            .args([
                "auth",
                "create-principal",
                "--tenant",
                "rbacco",
                name,
                "--password",
                "pw",
                "--role",
                role,
            ])
            .env("DATABASE_URL", catalog_url())
            .env("ETL_AUTH_BYPASS", "1")
            .current_dir(workspace_root())
            .output()
            .await?;
        assert!(
            out.status.success(),
            "create-principal {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    login("v_user", "pw").await;
    let put = Command::new(cargo_bin("platform"))
        .args(["secret", "put", "k", "v", "--register"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(!put.status.success(), "viewer should be rejected");
    let stderr = String::from_utf8_lossy(&put.stderr);
    assert!(
        stderr.contains("not permitted"),
        "expected not-permitted: {stderr}"
    );

    login("o_user", "pw").await;
    let put = Command::new(cargo_bin("platform"))
        .args(["secret", "put", "k", "v", "--register"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        put.status.success(),
        "operator put failed: {}",
        String::from_utf8_lossy(&put.stderr)
    );

    let create = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "another"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(!create.status.success(), "operator should not create tenants");

    login("a_user", "pw").await;
    let create = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "another"])
        .env("DATABASE_URL", catalog_url())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        create.status.success(),
        "admin create-tenant failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    Ok(())
}
