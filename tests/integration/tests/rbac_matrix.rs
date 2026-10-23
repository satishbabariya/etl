//! Phase II.2.b — RBAC matrix + cross-tenant JWT rejection.

use catalog::Catalog;
use std::path::{Path, PathBuf};
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

async fn spawn_issuer(port: u16) -> (tokio::process::Child, tempfile::TempDir, String) {
    let keys = tempfile::tempdir().unwrap();
    Command::new(cargo_bin("etl-auth"))
        .args(["--keys-dir", keys.path().to_str().unwrap(), "init-issuer"])
        .status()
        .await
        .unwrap();
    let bind = format!("127.0.0.1:{port}");
    let issuer = format!("http://{bind}");
    let child = Command::new(cargo_bin("etl-auth"))
        .args([
            "--keys-dir",
            keys.path().to_str().unwrap(),
            "serve",
            "--bind",
            &bind,
            "--issuer-url",
            &issuer,
            "--audience",
            "etl-platform",
            "--database-url",
            &catalog_url(),
        ])
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    for _ in 0..30 {
        if reqwest::get(format!("{issuer}/.well-known/jwks.json"))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    (child, keys, issuer)
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

async fn login(home: &Path, name: &str, password: &str, issuer: &str) {
    let _ = std::fs::remove_file(home.join(".etl/credentials.json"));
    let out = platform(home)
        .args(["auth", "login", name, "--password", password])
        .env("ETL_AUTH_ISSUER", issuer)
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

    let (mut server, _keys, issuer) = spawn_issuer(18405).await;
    let cli_home = tempfile::tempdir()?;
    let home = cli_home.path();

    let out = platform(home)
        .args(["tenant", "create", "rbacco"])
        .env("ETL_AUTH_BYPASS", "1")
        .output()
        .await?;
    assert!(
        out.status.success(),
        "bootstrap create-tenant failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    for (name, role) in [
        ("v_user", "viewer"),
        ("o_user", "operator"),
        ("a_user", "admin"),
    ] {
        let out = platform(home)
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
            .env("ETL_AUTH_BYPASS", "1")
            .output()
            .await?;
        assert!(
            out.status.success(),
            "create-principal {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    login(home, "v_user", "pw", &issuer).await;
    let put = platform(home)
        .args(["secret", "put", "k", "v", "--register"])
        .output()
        .await?;
    assert!(!put.status.success(), "viewer should be rejected");
    let stderr = String::from_utf8_lossy(&put.stderr);
    assert!(
        stderr.contains("not permitted"),
        "expected not-permitted: {stderr}"
    );

    login(home, "o_user", "pw", &issuer).await;
    let put = platform(home)
        .args(["secret", "put", "k", "v", "--register"])
        .output()
        .await?;
    assert!(
        put.status.success(),
        "operator put failed: {}",
        String::from_utf8_lossy(&put.stderr)
    );

    let create = platform(home)
        .args(["tenant", "create", "another"])
        .output()
        .await?;
    assert!(
        !create.status.success(),
        "operator should not create tenants"
    );
    let stderr = String::from_utf8_lossy(&create.stderr);
    assert!(
        stderr.contains("not permitted"),
        "expected not-permitted for operator tenant create: {stderr}"
    );

    login(home, "a_user", "pw", &issuer).await;
    let create = platform(home)
        .args(["tenant", "create", "another"])
        .output()
        .await?;
    assert!(
        create.status.success(),
        "admin create-tenant failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let _ = server.start_kill();
    Ok(())
}
