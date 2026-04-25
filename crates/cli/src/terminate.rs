use anyhow::Context;
use catalog::Catalog;
use temporalio_client::{UntypedWorkflow, WorkflowTerminateOptions};
use worker::temporal::{make_client, TemporalConfig};

pub async fn terminate(workflow_id: String, reason: Option<String>) -> anyhow::Result<()> {
    let cfg = TemporalConfig::from_env()?;
    let client = make_client(&cfg).await?;
    let handle = client.get_workflow_handle::<UntypedWorkflow>(workflow_id.clone());
    let mut opts = WorkflowTerminateOptions::default();
    opts.reason = reason.clone().unwrap_or_else(|| "platform-cli-terminate".into());
    handle
        .terminate(opts)
        .await
        .context("terminate_workflow_execution")?;
    println!("terminated workflow {}", workflow_id);

    // Best-effort catalog update.
    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    let row: Option<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
        "SELECT run_id, tenant_id FROM runs WHERE temporal_workflow_id = $1 ORDER BY started_at DESC LIMIT 1",
    )
    .bind(&workflow_id)
    .fetch_optional(catalog.pool())
    .await?;
    if let Some((rid, tid)) = row {
        let rid = common_types::ids::RunId::from_uuid_unchecked(rid);
        let ctx = catalog::TenantContext::new(common_types::ids::TenantId::from_uuid_unchecked(tid));
        catalog
            .mark_run_failed(ctx, rid, &reason.unwrap_or_else(|| "terminated".into()))
            .await?;
        println!("marked run {} failed", rid);
    } else {
        println!("no matching runs row (workflow may be external)");
    }
    Ok(())
}
