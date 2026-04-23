//! PipelineRunWorkflow: the canonical single-run workflow (RFC-4).
//!
//! Phase I.2 shape:
//!   start_run
//!   → discover_stream
//!   → loop { read_batch → load_batch → commit_cursor }
//!   → complete_run
//!
//! Cursor advances only after load_batch succeeds. Temporal replay +
//! deterministic LoadId + idempotent activities give at-least-once
//! delivery with correct resumption across worker restarts.

use common_types::connection_config::ConnectionConfig;
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::PipelineSpec;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::run_lifecycle::RunLifecycleActivities;
use crate::activities::sync::SyncActivities;
use crate::activities::sync::inputs::{
    CommitCursorInput, DiscoverInput, LoadBatchInput, ReadBatchInput,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineRunInput {
    pub run_id: Uuid,
    pub pipeline_id: Uuid,
    pub spec: PipelineSpec,
    pub source_connection: ConnectionConfig,
    pub initial_cursor: Option<CursorValue>,
    /// Name of the stream being synced. Phase I.2 uses the source table name.
    pub stream_name: String,
}

#[workflow]
pub struct PipelineRunWorkflow {
    run_id: Uuid,
    pipeline_id: Uuid,
    spec: PipelineSpec,
    source_connection: ConnectionConfig,
    cursor: Option<CursorValue>,
    stream_name: String,
}

fn opts_short() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

fn opts_long() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(300)),
        ..Default::default()
    }
}

#[workflow_methods]
impl PipelineRunWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: PipelineRunInput) -> Self {
        Self {
            run_id: input.run_id,
            pipeline_id: input.pipeline_id,
            spec: input.spec,
            source_connection: input.source_connection,
            cursor: input.initial_cursor,
            stream_name: input.stream_name,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (run_id, pipeline_id, spec, conn, stream_name) = ctx.state(|s| {
            (
                s.run_id,
                s.pipeline_id,
                s.spec.clone(),
                s.source_connection.clone(),
                s.stream_name.clone(),
            )
        });

        ctx.start_activity(RunLifecycleActivities::start_run, run_id, opts_short())
            .await?;

        ctx.start_activity(
            SyncActivities::discover_stream,
            DiscoverInput {
                source: spec.source.clone(),
                source_url: conn.url.clone(),
            },
            opts_short(),
        )
        .await?;

        let mut batch_seq: u32 = 0;
        loop {
            let cursor = ctx.state(|s| s.cursor.clone());

            let read_out = ctx
                .start_activity(
                    SyncActivities::read_batch,
                    ReadBatchInput {
                        source: spec.source.clone(),
                        source_url: conn.url.clone(),
                        cursor,
                        batch_size: spec.batch_size,
                    },
                    opts_long(),
                )
                .await?;

            if read_out.rows == 0 {
                break;
            }

            ctx.start_activity(
                SyncActivities::load_batch,
                LoadBatchInput {
                    destination: spec.destination.clone(),
                    batch_ipc_b64: read_out.batch_ipc_b64,
                    pipeline_id,
                    run_id,
                    batch_seq,
                },
                opts_long(),
            )
            .await?;

            ctx.start_activity(
                SyncActivities::commit_cursor,
                CommitCursorInput {
                    pipeline_id,
                    run_id,
                    stream_name: stream_name.clone(),
                    cursor: read_out.new_cursor.clone(),
                },
                opts_short(),
            )
            .await?;

            ctx.state_mut(|s| s.cursor = read_out.new_cursor);

            batch_seq += 1;
            if read_out.is_final {
                break;
            }
        }

        ctx.start_activity(RunLifecycleActivities::complete_run, run_id, opts_short())
            .await?;

        Ok(())
    }
}
