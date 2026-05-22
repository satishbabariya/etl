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
    pub connector_ref: String,
    pub evolution_policy: common_types::evolution::EvolutionPolicy,
    pub cursor_column: String,
    pub cursor_kind: common_types::cursor::CursorKind,
    pub pk_columns: Vec<String>,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
}

#[workflow]
pub struct PipelineRunWorkflow {
    run_id: Uuid,
    pipeline_id: Uuid,
    spec: PipelineSpec,
    source_connection: ConnectionConfig,
    cursor: Option<CursorValue>,
    stream_name: String,
    connector_ref: String,
    evolution_policy: common_types::evolution::EvolutionPolicy,
    cursor_column: String,
    cursor_kind: common_types::cursor::CursorKind,
    pk_columns: Vec<String>,
    tenant_id: Uuid,
    principal_id: Uuid,
    jti: Uuid,
    rows_total_so_far: u64,
    rows_rejected_so_far: u64,
}

/// Cap retries so failing activities don't spin forever in Temporal
/// history — a failed workflow terminates after this many tries, and
/// the next worker that starts doesn't re-pick-up stale attempts.
fn retry_policy() -> temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
    use prost_wkt_types::Duration as PbDuration;
    temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
        initial_interval: Some(PbDuration { seconds: 1, nanos: 0 }),
        backoff_coefficient: 2.0,
        maximum_interval: Some(PbDuration { seconds: 30, nanos: 0 }),
        maximum_attempts: 5,
        non_retryable_error_types: vec![],
    }
}

fn opts_short() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(30)),
        retry_policy: Some(retry_policy()),
        ..Default::default()
    }
}

fn opts_long() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(300)),
        retry_policy: Some(retry_policy()),
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
            connector_ref: input.connector_ref,
            evolution_policy: input.evolution_policy,
            cursor_column: input.cursor_column,
            cursor_kind: input.cursor_kind,
            pk_columns: input.pk_columns,
            tenant_id: input.tenant_id,
            principal_id: input.principal_id,
            jti: input.jti,
            rows_total_so_far: 0,
            rows_rejected_so_far: 0,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (run_id, tenant_id) = ctx.state(|s| (s.run_id, s.tenant_id));
        match Self::run_inner(ctx).await {
            Ok(()) => Ok(()),
            Err(termination) => {
                let err_str = format!("{termination}");
                let _ = ctx
                    .start_activity(
                        RunLifecycleActivities::fail_run,
                        crate::activities::run_lifecycle::FailRunInput {
                            run_id,
                            tenant_id,
                            error: err_str,
                        },
                        opts_short(),
                    )
                    .await;
                Err(termination)
            }
        }
    }

    async fn run_inner(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (
            run_id,
            pipeline_id,
            spec,
            conn,
            stream_name,
            connector_ref,
            evolution_policy,
            cursor_column,
            cursor_kind,
            pk_columns,
            tenant_id,
            principal_id,
            jti,
        ) = ctx.state(|s| {
            (
                s.run_id,
                s.pipeline_id,
                s.spec.clone(),
                s.source_connection.clone(),
                s.stream_name.clone(),
                s.connector_ref.clone(),
                s.evolution_policy,
                s.cursor_column.clone(),
                s.cursor_kind,
                s.pk_columns.clone(),
                s.tenant_id,
                s.principal_id,
                s.jti,
            )
        });

        ctx.start_activity(
            RunLifecycleActivities::start_run,
            crate::activities::run_lifecycle::StartRunInput { run_id, tenant_id },
            opts_short(),
        )
        .await?;

        ctx.start_activity(
            SyncActivities::discover_stream,
            DiscoverInput {
                source: spec.source.clone(),
                source_conn: conn.clone(),
                connector_ref: connector_ref.clone(),
                tenant_id,
                principal_id,
                jti,
                stream_name: stream_name.clone(),
                pipeline_id,
                cursor_column: cursor_column.clone(),
                cursor_kind,
                pk_columns: pk_columns.clone(),
                evolution_policy,
                transform: spec.transform.clone(),
            },
            opts_short(),
        )
        .await?;

        let dead_letter_threshold = spec
            .transform
            .as_ref()
            .map(|t| t.dead_letter_threshold)
            .unwrap_or(0.0);

        let mut batch_seq: u32 = 0;
        loop {
            let cursor = ctx.state(|s| s.cursor.clone());

            let read_out = ctx
                .start_activity(
                    SyncActivities::read_batch,
                    ReadBatchInput {
                        source: spec.source.clone(),
                        source_conn: conn.clone(),
                        cursor,
                        batch_size: spec.batch_size,
                        connector_ref: connector_ref.clone(),
                        tenant_id,
                        principal_id,
                        jti,
                        transform: spec.transform.clone(),
                    },
                    opts_long(),
                )
                .await?;

            let rows_this_batch = read_out.rows + read_out.rows_rejected;
            ctx.state_mut(|s| {
                s.rows_total_so_far = s.rows_total_so_far.saturating_add(rows_this_batch as u64);
                s.rows_rejected_so_far = s
                    .rows_rejected_so_far
                    .saturating_add(read_out.rows_rejected as u64);
            });
            let (rows_total_so_far, rows_rejected_so_far) =
                ctx.state(|s| (s.rows_total_so_far, s.rows_rejected_so_far));

            if read_out.rows == 0 && read_out.rows_rejected == 0 {
                break;
            }

            // See WasmCdcPipelineWorkflow for the same fix + rationale:
            // load_batch.stream_name = TARGET TABLE (empty → use spec.table);
            // commit_cursor.stream_name = CURSOR KEY (uses pipeline-level fallback).
            let load_stream = read_out.stream_name.clone().unwrap_or_default();
            let cursor_stream = read_out
                .stream_name
                .clone()
                .unwrap_or_else(|| stream_name.clone());

            ctx.start_activity(
                SyncActivities::load_batch,
                LoadBatchInput {
                    destination: spec.destination.clone(),
                    batch_ipc_b64: read_out.batch_ipc_b64,
                    pipeline_id,
                    tenant_id,
                    run_id,
                    batch_seq,
                    rejected_ipc_b64: read_out.rejected_ipc_b64,
                    dead_letter_threshold,
                    rows_rejected_so_far: rows_rejected_so_far as usize,
                    rows_total_so_far: rows_total_so_far as usize,
                    stream_name: load_stream,
                },
                opts_long(),
            )
            .await?;

            ctx.start_activity(
                SyncActivities::commit_cursor,
                CommitCursorInput {
                    pipeline_id,
                    tenant_id,
                    run_id,
                    stream_name: cursor_stream,
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

        ctx.start_activity(
            RunLifecycleActivities::complete_run,
            crate::activities::run_lifecycle::CompleteRunInput { run_id, tenant_id },
            opts_short(),
        )
        .await?;

        Ok(())
    }
}
