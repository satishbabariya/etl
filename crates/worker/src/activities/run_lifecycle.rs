//! Activities that advance the `runs` row through its lifecycle.

use catalog::Catalog;
use common_types::ids::RunId;
use metrics;
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use uuid::Uuid;

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
        run_id: Uuid,
    ) -> Result<(), ActivityError> {
        let rid = RunId::from_uuid_unchecked(run_id);
        self.catalog
            .mark_run_running(rid)
            .await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_running: {e}").into()))?;
        tracing::info!(%run_id, "run started");
        metrics::counter!(crate::metrics::RUN_STARTED).increment(1);
        Ok(())
    }

    /// Mark a run as completed. Idempotent.
    #[activity]
    pub async fn complete_run(
        self: Arc<Self>,
        _ctx: ActivityContext,
        run_id: Uuid,
    ) -> Result<(), ActivityError> {
        let rid = RunId::from_uuid_unchecked(run_id);
        self.catalog
            .mark_run_completed(rid)
            .await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_completed: {e}").into()))?;
        tracing::info!(%run_id, "run completed");
        metrics::counter!(crate::metrics::RUN_COMPLETED).increment(1);
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
        self.catalog
            .mark_run_failed(rid, &input.error)
            .await
            .map_err(|e| ActivityError::NonRetryable(anyhow::anyhow!("mark_failed: {e}").into()))?;
        tracing::warn!(run_id = %input.run_id, error = %input.error, "run failed");
        metrics::counter!(crate::metrics::RUN_FAILED).increment(1);
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FailRunInput {
    pub run_id: Uuid,
    pub error: String,
}
