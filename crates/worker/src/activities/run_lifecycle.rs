//! Activities that advance the `runs` row through its lifecycle.

use catalog::Catalog;
use common_types::ids::{RunId, TenantContext, TenantId};
use metrics;
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use uuid::Uuid;

#[derive(Clone)]
pub struct RunLifecycleActivities {
    pub catalog: Arc<Catalog>,
}

#[activities]
impl RunLifecycleActivities {
    /// Mark a run as running. Idempotent.
    #[activity]
    pub async fn start_run(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: StartRunInput,
    ) -> Result<(), ActivityError> {
        let rid = RunId::from_uuid_unchecked(input.run_id);
        let ctx = TenantContext::new(TenantId::from_uuid_unchecked(input.tenant_id));
        self.catalog
            .mark_run_running(ctx, rid)
            .await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_running: {e}").into()))?;
        tracing::info!(run_id = %input.run_id, "run started");
        metrics::counter!(
            crate::metrics::RUN_STARTED,
            "tenant_id" => input.tenant_id.to_string(),
        )
        .increment(1);
        Ok(())
    }

    /// Mark a run as completed. Idempotent.
    #[activity]
    pub async fn complete_run(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CompleteRunInput,
    ) -> Result<(), ActivityError> {
        let rid = RunId::from_uuid_unchecked(input.run_id);
        let ctx = TenantContext::new(TenantId::from_uuid_unchecked(input.tenant_id));
        self.catalog
            .mark_run_completed(ctx, rid)
            .await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_completed: {e}").into()))?;
        tracing::info!(run_id = %input.run_id, "run completed");
        metrics::counter!(
            crate::metrics::RUN_COMPLETED,
            "tenant_id" => input.tenant_id.to_string(),
        )
        .increment(1);
        Ok(())
    }

    /// Mark a run as failed with an error message. Idempotent.
    #[activity]
    pub async fn fail_run(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: FailRunInput,
    ) -> Result<(), ActivityError> {
        let rid = RunId::from_uuid_unchecked(input.run_id);
        let ctx = TenantContext::new(TenantId::from_uuid_unchecked(input.tenant_id));
        self.catalog
            .mark_run_failed(ctx, rid, &input.error)
            .await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_failed: {e}").into()))?;
        tracing::warn!(run_id = %input.run_id, error = %input.error, "run failed");
        metrics::counter!(
            crate::metrics::RUN_FAILED,
            "tenant_id" => input.tenant_id.to_string(),
        )
        .increment(1);
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StartRunInput {
    pub run_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CompleteRunInput {
    pub run_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FailRunInput {
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    pub error: String,
}
