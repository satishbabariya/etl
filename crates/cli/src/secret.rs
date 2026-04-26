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
    let tid = crate::ensure_dev_tenant(&cat).await?;
    Ok((cat, TenantContext::new(tid)))
}

fn parse_backend(s: &str) -> Result<SecretBackendKind> {
    match s.to_ascii_lowercase().as_str() {
        "env" => Ok(SecretBackendKind::Env),
        "file" => Ok(SecretBackendKind::File),
        other => anyhow::bail!("unknown backend '{other}' (expected env|file)"),
    }
}

pub async fn create(name: String, backend: String, key: String) -> Result<()> {
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
    println!("created secret {} ({}) backend={}", name, id, backend);
    Ok(())
}

pub async fn put(name: String, value: String, register: bool) -> Result<()> {
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
            println!("registered catalog row for '{}' (backend=file, key={})", name, name);
        } else {
            println!("catalog row for '{}' already exists — skipped", name);
        }
    }
    Ok(())
}

pub async fn list() -> Result<()> {
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
        };
        println!("{}\t{}\t{}\t{}", s.name, backend, s.key, s.created_at);
    }
    Ok(())
}

pub async fn delete(name: String) -> Result<()> {
    let (cat, ctx) = open_admin().await?;
    let row = cat
        .secret_get_by_name(ctx.clone(), &name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("secret '{}' not found", name))?;
    cat.secret_delete(ctx, row.secret_id).await?;
    println!("deleted secret '{}' ({})", name, row.secret_id);
    Ok(())
}
