//! Long-lived Postgres CDC pipeline workflow (RFC-8).
//!
//! Phase I.6 MVP shape — no child workflow, no continue-as-new:
//!   start_run
//!   → ensure_slot
//!   → snapshot loop { snapshot_chunk until is_final }
//!   → streaming loop { read_window; sleep on empty }
//!
//! RFC-8's `CdcSnapshotWorkflow` (finite child) and continue-as-new
//! every 10M events are deferred: for local-dogfood scale the Temporal
//! history blow-up they defend against isn't a live concern.

use common_types::pipeline_spec::{PipelineSpec, SourceSpec};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::cdc::CdcActivities;
use crate::activities::cdc::inputs::{
    EnsureSlotInput, ReadWindowInput, ReadWindowOutput, SnapshotChunkInput, SnapshotChunkOutput,
};
use crate::activities::run_lifecycle::{FailRunInput, RunLifecycleActivities};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcPipelineInput {
    pub run_id: Uuid,
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    pub spec: PipelineSpec,
    pub source_url: String,
    /// Max number of streaming windows to process before returning (useful
    /// for tests; 0 means forever).
    #[serde(default)]
    pub max_windows: u32,
}

#[workflow]
pub struct CdcPipelineWorkflow {
    input: CdcPipelineInput,
}

fn opts_short() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}
fn opts_long() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    }
}

#[workflow_methods]
impl CdcPipelineWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: CdcPipelineInput) -> Self {
        Self { input }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let run_id = ctx.state(|s| s.input.run_id);
        match Self::run_inner(ctx).await {
            Ok(()) => Ok(()),
            Err(t) => {
                let err_str = format!("{t}");
                let _ = ctx
                    .start_activity(
                        RunLifecycleActivities::fail_run,
                        FailRunInput { run_id, error: err_str },
                        opts_short(),
                    )
                    .await;
                Err(t)
            }
        }
    }

    async fn run_inner(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let input = ctx.state(|s| s.input.clone());
        let pg = match &input.spec.source {
            SourceSpec::Postgres(p) => p.clone(),
            _ => {
                return Err(anyhow::anyhow!(
                    "CdcPipelineWorkflow requires Postgres source"
                )
                .into());
            }
        };
        let rel_name = format!("{}.{}", pg.schema, pg.table);
        let dest = input.spec.destination.clone();

        ctx.start_activity(
            RunLifecycleActivities::start_run,
            input.run_id,
            opts_short(),
        )
        .await?;

        let slot = ctx
            .start_activity(
                CdcActivities::ensure_slot,
                EnsureSlotInput {
                    pipeline_id: input.pipeline_id,
                    source_url: input.source_url.clone(),
                    schema: pg.schema.clone(),
                    table: pg.table.clone(),
                },
                opts_short(),
            )
            .await?;

        // Snapshot loop (inline; no child workflow in Phase I.6 MVP).
        let pk_col = pg
            .pk_columns
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("CDC requires at least one PK column"))?;
        let mut batch_seq: u32 = 0;
        let mut last_pk: Option<i64> = None;
        loop {
            let out: SnapshotChunkOutput = ctx
                .start_activity(
                    CdcActivities::snapshot_chunk,
                    SnapshotChunkInput {
                        pipeline_id: input.pipeline_id,
                        run_id: input.run_id,
                        batch_seq,
                        source_url: input.source_url.clone(),
                        schema: pg.schema.clone(),
                        table: pg.table.clone(),
                        pk_col: pk_col.clone(),
                        last_pk,
                        batch_size: input.spec.batch_size.max(100),
                        consistent_point: slot.consistent_point.clone(),
                        destination: dest.clone(),
                    },
                    opts_long(),
                )
                .await?;
            batch_seq += 1;
            if out.is_final {
                break;
            }
            last_pk = out.last_pk;
        }

        // Streaming loop. `max_windows > 0` caps iterations (used by tests).
        let mut window_seq: u32 = 0;
        let mut current_lsn: Option<String> = Some(slot.consistent_point.clone());
        loop {
            if input.max_windows > 0 && window_seq >= input.max_windows {
                break;
            }
            let out: ReadWindowOutput = ctx
                .start_activity(
                    CdcActivities::read_window,
                    ReadWindowInput {
                        pipeline_id: input.pipeline_id,
                        run_id: input.run_id,
                        batch_seq,
                        source_url: input.source_url.clone(),
                        slot_name: slot.slot_name.clone(),
                        publication_name: slot.publication_name.clone(),
                        start_lsn: current_lsn.clone(),
                        max_events: 1000,
                        table_rel_name: rel_name.clone(),
                        destination: dest.clone(),
                    },
                    opts_long(),
                )
                .await?;
            if let Some(lsn) = out.new_lsn {
                current_lsn = Some(lsn);
            }
            batch_seq += 1;
            window_seq += 1;
            if out.rows == 0 {
                ctx.timer(Duration::from_secs(2)).await;
            }
        }

        ctx.start_activity(
            RunLifecycleActivities::complete_run,
            input.run_id,
            opts_short(),
        )
        .await?;
        Ok(())
    }
}
