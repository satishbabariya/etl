//! Tenant lifecycle: create / list / suspend / terminate.
//!
//! create  → catalog row + Temporal namespace etl-<simple>
//! list    → tabular dump (UUID, name, created_at)
//! suspend → name-prefix hack ("suspended:<name>") so resolver misses;
//!           Phase II.2 will add a proper status column
//! terminate → catalog cascade (FKs ON DELETE CASCADE) + remove
//!             ./data/<tenant_id>/ subtree

use anyhow::Context;
use catalog::Catalog;
use common_types::ids::TenantId;

pub async fn create(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let id = admin.create_tenant(&name).await?;
    println!("created tenant {} ({})", name, id);
    register_temporal_namespace(&id).await?;
    Ok(())
}

pub async fn list() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let tenants = admin.list_tenants().await?;
    for t in tenants {
        println!("{}\t{}\t{}", t.tenant_id, t.name, t.created_at);
    }
    Ok(())
}

pub async fn suspend(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let result = sqlx::query(
        "UPDATE tenants SET name = 'suspended:' || name \
         WHERE name = $1 AND name NOT LIKE 'suspended:%'",
    )
    .bind(&name)
    .execute(admin.pool())
    .await?;
    if result.rows_affected() == 0 {
        println!("no active tenant named {} (already suspended or missing)", name);
    } else {
        println!("suspended tenant {} (renamed to suspended:{})", name, name);
    }
    Ok(())
}

pub async fn terminate(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin
        .get_tenant_by_name(&name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", name))?;
    admin.delete_tenant(t.tenant_id).await?;
    println!("terminated tenant {} ({}) — catalog rows cascaded", name, t.tenant_id);

    let base = std::env::var("ETL_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let path = std::path::PathBuf::from(&base).join(t.tenant_id.as_uuid().to_string());
    if path.exists() {
        std::fs::remove_dir_all(&path)
            .with_context(|| format!("removing {}", path.display()))?;
        println!("removed {}", path.display());
    }
    println!(
        "note: Temporal namespace etl-{} deprecated — `tctl namespace delete` for full cleanup",
        t.tenant_id.as_uuid().simple()
    );
    Ok(())
}

async fn register_temporal_namespace(id: &TenantId) -> anyhow::Result<()> {
    use temporalio_client::WorkflowService;
    use temporalio_common::protos::temporal::api::workflowservice::v1::RegisterNamespaceRequest;

    let cfg = worker::temporal::TemporalConfig::from_env()?;
    let client = worker::temporal::make_client(&cfg).await?;
    let ns = format!("etl-{}", id.as_uuid().simple());
    let req = RegisterNamespaceRequest {
        namespace: ns.clone(),
        description: format!("Per-tenant namespace for {id}"),
        workflow_execution_retention_period: Some(prost_wkt_types::Duration {
            seconds: 7 * 24 * 3600,
            nanos: 0,
        }),
        ..Default::default()
    };
    let mut svc = client.workflow_service();
    match svc.register_namespace(req).await {
        Ok(_) => println!("registered Temporal namespace {ns}"),
        Err(s) => {
            let msg = format!("{s}");
            if msg.to_lowercase().contains("alreadyexists")
                || msg.to_lowercase().contains("already exists")
            {
                println!("Temporal namespace {ns} already exists");
            } else {
                return Err(anyhow::anyhow!("register_namespace: {s}"));
            }
        }
    }
    Ok(())
}
