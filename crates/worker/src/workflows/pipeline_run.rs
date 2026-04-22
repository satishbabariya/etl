//! PipelineRunWorkflow: the canonical one-run workflow (RFC-4).
//!
//! Phase I.1 shape: start_run activity → 30s timer → complete_run activity.
//! The timer exists specifically to make durability testing meaningful — if
//! the worker is killed during the timer, Temporal preserves the workflow
//! state on the server, and a restarted worker resumes it without re-running
//! start_run.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::run_lifecycle::RunLifecycleActivities;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineRunInput {
    pub run_id: Uuid,
}

#[workflow]
pub struct PipelineRunWorkflow {
    run_id: Uuid,
}

#[workflow_methods]
impl PipelineRunWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: PipelineRunInput) -> Self {
        Self {
            run_id: input.run_id,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let run_id = ctx.state(|s| s.run_id);

        let opts = || ActivityOptions {
            start_to_close_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };

        ctx.start_activity(RunLifecycleActivities::start_run, run_id, opts())
            .await?;

        // The 30-second timer is where durability matters: if the worker is
        // killed here, the server retains the state, and a restarted worker
        // resumes the workflow at this point.
        ctx.timer(Duration::from_secs(30)).await;

        ctx.start_activity(RunLifecycleActivities::complete_run, run_id, opts())
            .await?;

        Ok(())
    }
}
