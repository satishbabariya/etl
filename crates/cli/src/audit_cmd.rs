use anyhow::{Context, Result};
use catalog::Catalog;

pub async fn tail(tenant_override: Option<String>, limit: i64) -> Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    cat.migrate().await?;
    crate::auth::ensure_bypass_tenant(&cat).await?;
    let p = crate::auth::current_principal()?;
    crate::auth::require_role(&p, common_types::auth::Action::Read)?;
    let ctx = crate::auth::resolve_context(&cat, tenant_override.as_deref()).await?;
    let rows = cat.audit_tail(ctx.tenant_id, limit).await?;
    for (id, action, principal_id, target, ts, payload) in rows {
        let pid = principal_id
            .map(|u| u.to_string())
            .unwrap_or_else(|| "-".into());
        let target = target.unwrap_or_else(|| "-".into());
        println!(
            "{:<8} {:<20} {:<20} {:<40} {:<40} {}",
            id,
            ts.format("%Y-%m-%dT%H:%M:%S"),
            action,
            pid,
            target,
            payload,
        );
    }
    Ok(())
}

pub async fn verify_chain(tenant_override: Option<String>) -> Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let cat = Catalog::connect(&url).await?;
    cat.migrate().await?;
    crate::auth::ensure_bypass_tenant(&cat).await?;
    let p = crate::auth::current_principal()?;
    crate::auth::require_role(&p, common_types::auth::Action::Admin)?;
    let ctx = crate::auth::resolve_context(&cat, tenant_override.as_deref()).await?;
    match cat.audit_verify_chain(Some(ctx.tenant_id)).await? {
        ::audit::verify::VerifyResult::Ok { rows_checked } => {
            println!("OK — {} rows verified", rows_checked);
            Ok(())
        }
        ::audit::verify::VerifyResult::Mismatch { audit_id } => {
            anyhow::bail!("chain MISMATCH at audit_id={audit_id}")
        }
    }
}
