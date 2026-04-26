//! Secret CLI: catalog rows + file-backend writes.
//!
//! `create` registers a SecretRef row pointing at a (backend, key) pair.
//! `put` writes the plaintext to the file backend (and optionally also
//! creates the catalog row in one shot via `--register`).
//! `list` and `delete` operate on catalog rows; plaintexts in the file
//! backend persist independently of catalog rows.

use anyhow::{Context, Result};
use catalog::{Catalog, NewSecret};
use common_types::ids::TenantContext;
use common_types::secrets::SecretBackendKind;
use worker::secrets::file::FileSecrets;

async fn open_admin() -> Result<(Catalog, TenantContext)> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    cat.migrate().await?;
    crate::auth::ensure_bypass_tenant(&cat).await?;
    let p = crate::auth::current_principal()?;
    crate::auth::assert_not_revoked(&cat, &p).await?;
    let ctx = crate::auth::resolve_context(&cat, None).await?;
    Ok((cat, ctx))
}

fn require(action: common_types::auth::Action) -> Result<()> {
    let p = crate::auth::current_principal()?;
    crate::auth::require_role(&p, action)
}

fn parse_backend(s: &str) -> Result<SecretBackendKind> {
    match s.to_ascii_lowercase().as_str() {
        "env" => Ok(SecretBackendKind::Env),
        "file" => Ok(SecretBackendKind::File),
        "vault" => Ok(SecretBackendKind::Vault),
        other => anyhow::bail!("unknown backend '{other}' (expected env|file)"),
    }
}

pub async fn create(name: String, backend: String, key: String) -> Result<()> {
    require(common_types::auth::Action::Write)?;
    let (cat, ctx) = open_admin().await?;
    let backend_kind = parse_backend(&backend)?;
    let id = cat
        .secret_create(
            ctx.clone(),
            NewSecret {
                tenant_id: ctx.tenant_id,
                name: name.clone(),
                backend: backend_kind,
                key,
            },
        )
        .await
        .context("inserting secret row")?;
    let p = crate::auth::current_principal()?;
    let (pid, jti) = crate::auditlog::principal_into(&p);
    crate::auditlog::record(
        &cat,
        Some(ctx.tenant_id),
        pid,
        jti,
        audit::AuditEvent::SecretCreate,
        Some(name.clone()),
        serde_json::json!({"backend": backend, "secret_id": id.to_string()}),
    )
    .await;
    println!("created secret {} ({}) backend={}", name, id, backend);
    Ok(())
}

pub async fn put(name: String, value: String, register: bool) -> Result<()> {
    require(common_types::auth::Action::Write)?;
    let file = FileSecrets::new();
    file.put(&name, &value)?;
    println!("wrote secret '{}' to file backend", name);

    if register {
        let (cat, ctx) = open_admin().await?;
        if cat.secret_get_by_name(ctx.clone(), &name).await?.is_none() {
            cat.secret_create(
                ctx.clone(),
                NewSecret {
                    tenant_id: ctx.tenant_id,
                    name: name.clone(),
                    backend: SecretBackendKind::File,
                    key: name.clone(),
                },
            )
            .await
            .context("inserting secret row")?;
            let p = crate::auth::current_principal()?;
            let (pid, jti) = crate::auditlog::principal_into(&p);
            crate::auditlog::record(
                &cat,
                Some(ctx.tenant_id),
                pid,
                jti,
                audit::AuditEvent::SecretCreate,
                Some(name.clone()),
                serde_json::json!({"backend": "file", "via": "put --register"}),
            )
            .await;
            println!("registered catalog row for '{}' (backend=file, key={})", name, name);
        } else {
            println!("catalog row for '{}' already exists — skipped", name);
        }
    }
    Ok(())
}

pub async fn list() -> Result<()> {
    require(common_types::auth::Action::Read)?;
    let (cat, ctx) = open_admin().await?;
    let rows = cat.secret_list(ctx).await?;
    if rows.is_empty() {
        println!("(no secrets registered)");
        return Ok(());
    }
    for s in rows {
        let backend = match s.backend {
            SecretBackendKind::Env => "env",
            SecretBackendKind::File => "file",
            SecretBackendKind::Vault => "vault",
        };
        println!("{}\t{}\t{}\t{}", s.name, backend, s.key, s.created_at);
    }
    Ok(())
}

pub async fn delete(name: String) -> Result<()> {
    require(common_types::auth::Action::Write)?;
    let (cat, ctx) = open_admin().await?;
    let row = cat
        .secret_get_by_name(ctx.clone(), &name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("secret '{}' not found", name))?;
    let secret_id = row.secret_id;
    cat.secret_delete(ctx.clone(), secret_id).await?;
    let p = crate::auth::current_principal()?;
    let (pid, jti) = crate::auditlog::principal_into(&p);
    crate::auditlog::record(
        &cat,
        Some(ctx.tenant_id),
        pid,
        jti,
        audit::AuditEvent::SecretDelete,
        Some(name.clone()),
        serde_json::json!({"secret_id": secret_id.to_string()}),
    )
    .await;
    println!("deleted secret '{}' ({})", name, secret_id);
    Ok(())
}
