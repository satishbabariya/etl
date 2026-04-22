//! Activities that advance the `runs` row through its lifecycle.

use catalog::Catalog;
use common_types::ids::RunId;
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
        Ok(())
    }
}
