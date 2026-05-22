//! Long-lived WASM-CDC pipeline workflow (Phase II.3.e).
//!
//! Mirrors `PipelineRunWorkflow` but defers stop semantics to the
//! guest connector: the WASM CDC guest signals snapshot phase via
//! cursor-kind=snapshot-pk, then transitions to streaming
//! cursor-kind=gtid (or lsn) once snapshot is_final. The workflow
//! itself doesn't model the two phases — it just runs read-batch /
//! load-batch / commit-cursor in a loop until either:
//!   - the configured `max_windows` cap is hit (tests),
//!   - or an empty streaming window arrives, in which case we sleep
//!     before retrying so we don't busy-loop the activity worker.
//!
//! `is_final` is treated as advisory: it tells us "snapshot phase is
//! done"; we don't break, because the guest is now in streaming mode
//! and will keep emitting events.

use common_types::connection_config::ConnectionConfig;
use common_types::cursor::{CursorKind, CursorValue};
use common_types::pipeline_spec::PipelineSpec;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::run_lifecycle::{FailRunInput, RunLifecycleActivities};
use crate::activities::sync::SyncActivities;
use crate::activities::sync::inputs::{
    CommitCursorInput, DiscoverInput, LoadBatchInput, ReadBatchInput,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmCdcPipelineInput {
    pub run_id: Uuid,
    pub pipeline_id: Uuid,
    pub spec: PipelineSpec,
    pub source_connection: ConnectionConfig,
    pub initial_cursor: Option<CursorValue>,
    pub stream_name: String,
    pub connector_ref: String,
    pub evolution_policy: common_types::evolution::EvolutionPolicy,
    pub cursor_column: String,
    pub cursor_kind: CursorKind,
    pub pk_columns: Vec<String>,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    /// Max read-batch iterations before the workflow returns. 0 = run
    /// forever (production); tests pass a small number to bound runtime.
    #[serde(default)]
    pub max_windows: u32,
    /// Sleep applied after an empty streaming window before re-polling.
    /// Defaults to 2s when zero.
    #[serde(default)]
    pub idle_sleep_secs: u32,
}

#[workflow]
pub struct WasmCdcPipelineWorkflow {
    input: WasmCdcPipelineInput,
}

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
        start_to_close_timeout: Some(Duration::from_secs(60)),
        retry_policy: Some(retry_policy()),
        ..Default::default()
    }
}

fn opts_long() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(600)),
        retry_policy: Some(retry_policy()),
        ..Default::default()
    }
}

#[workflow_methods]
impl WasmCdcPipelineWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: WasmCdcPipelineInput) -> Self {
        Self { input }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (run_id, tenant_id) = ctx.state(|s| (s.input.run_id, s.input.tenant_id));
        match Self::run_inner(ctx).await {
            Ok(()) => Ok(()),
            Err(t) => {
                let err_str = format!("{t}");
                let _ = ctx
                    .start_activity(
                        RunLifecycleActivities::fail_run,
                        FailRunInput { run_id, tenant_id, error: err_str },
                        opts_short(),
                    )
                    .await;
                Err(t)
            }
        }
    }

    async fn run_inner(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let input = ctx.state(|s| s.input.clone());

        ctx.start_activity(
            RunLifecycleActivities::start_run,
            crate::activities::run_lifecycle::StartRunInput {
                run_id: input.run_id,
                tenant_id: input.tenant_id,
            },
            opts_short(),
        )
        .await?;

        ctx.start_activity(
            SyncActivities::discover_stream,
            DiscoverInput {
                source: input.spec.source.clone(),
                source_conn: input.source_connection.clone(),
                connector_ref: input.connector_ref.clone(),
                tenant_id: input.tenant_id,
                principal_id: input.principal_id,
                jti: input.jti,
                stream_name: input.stream_name.clone(),
                pipeline_id: input.pipeline_id,
                cursor_column: input.cursor_column.clone(),
                cursor_kind: input.cursor_kind,
                pk_columns: input.pk_columns.clone(),
                evolution_policy: input.evolution_policy,
                transform: input.spec.transform.clone(),
            },
            opts_short(),
        )
        .await?;

        let dead_letter_threshold = input
            .spec
            .transform
            .as_ref()
            .map(|t| t.dead_letter_threshold)
            .unwrap_or(0.0);

        let idle_sleep = Duration::from_secs(if input.idle_sleep_secs == 0 {
            2
        } else {
            input.idle_sleep_secs as u64
        });

        let mut cursor: Option<CursorValue> = input.initial_cursor.clone();
        let mut batch_seq: u32 = 0;
        let mut window_seq: u32 = 0;
        let mut rows_total_so_far: u64 = 0;
        let mut rows_rejected_so_far: u64 = 0;

        loop {
            if input.max_windows > 0 && window_seq >= input.max_windows {
                break;
            }
            let read_out = ctx
                .start_activity(
                    SyncActivities::read_batch,
                    ReadBatchInput {
                        source: input.spec.source.clone(),
                        source_conn: input.source_connection.clone(),
                        cursor: cursor.clone(),
                        batch_size: input.spec.batch_size,
                        connector_ref: input.connector_ref.clone(),
                        tenant_id: input.tenant_id,
                        principal_id: input.principal_id,
                        jti: input.jti,
                        transform: input.spec.transform.clone(),
                    },
                    opts_long(),
                )
                .await?;

            let rows_this_batch = read_out.rows + read_out.rows_rejected;
            rows_total_so_far = rows_total_so_far.saturating_add(rows_this_batch as u64);
            rows_rejected_so_far =
                rows_rejected_so_far.saturating_add(read_out.rows_rejected as u64);

            if read_out.rows == 0 && read_out.rows_rejected == 0 {
                // Streaming idle window. Advance cursor anyway in case
                // the guest moved its position via a heartbeat, then
                // sleep so we don't hammer the source.
                if read_out.new_cursor.is_some() {
                    cursor = read_out.new_cursor;
                }
                ctx.timer(idle_sleep).await;
                window_seq += 1;
                continue;
            }

            // CRITICAL: load_batch.stream_name and commit_cursor.stream_name
            // are SEMANTICALLY DIFFERENT despite sharing a name:
            //   - For the LOADER (phase-2-4c), stream_name picks the per-batch
            //     target TABLE inside spec.schema. Empty = use spec.table.
            //   - For the CATALOG, stream_name is a CURSOR TRACKING KEY scoped
            //     to (pipeline, stream).
            // Until phase-2-4c, both were the same. For WASM CDC, input.stream_name
            // is the connector identifier ("postgres-cdc-rs") — a meaningful cursor
            // key but a NONSENSICAL table name. Falling back to it for the loader
            // caused single-table WASM CDC pipelines to write to a table named
            // after the connector, not spec.table. Discovered via pg_loader_real_e2e.
            //
            // Fix: only set the loader's stream_name when the connector
            // EXPLICITLY emits one (multi-table CDC case). Cursor key keeps
            // the existing semantics.
            let load_stream = read_out.stream_name.clone().unwrap_or_default();
            let cursor_stream = read_out
                .stream_name
                .clone()
                .unwrap_or_else(|| input.stream_name.clone());

            ctx.start_activity(
                SyncActivities::load_batch,
                LoadBatchInput {
                    destination: input.spec.destination.clone(),
                    batch_ipc_b64: read_out.batch_ipc_b64,
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                    run_id: input.run_id,
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
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                    run_id: input.run_id,
                    stream_name: cursor_stream,
                    cursor: read_out.new_cursor.clone(),
                },
                opts_short(),
            )
            .await?;

            // NB: Workflow-driven advance_slot intentionally omitted here.
            // The WASM CDC connector owns its slot name (etl_pgrs_<sha256>
            // per examples/postgres-cdc-rs/src/lib.rs::slot_name), which
            // the workflow has no visibility into — calling advance_slot
            // with the etl_<pipeline_id> formula would error every batch.
            // WAL release for this path happens opportunistically inside
            // PgSubscription::finalize (db_pg_subscribe.rs, phase-2-3j-1).
            // The advance_slot activity (phase-2-3k) remains available
            // for the native PG CDC workflow when wired.

            cursor = read_out.new_cursor;
            batch_seq += 1;
            window_seq += 1;
        }

        ctx.start_activity(
            RunLifecycleActivities::complete_run,
            crate::activities::run_lifecycle::CompleteRunInput {
                run_id: input.run_id,
                tenant_id: input.tenant_id,
            },
            opts_short(),
        )
        .await?;
        Ok(())
    }
}
