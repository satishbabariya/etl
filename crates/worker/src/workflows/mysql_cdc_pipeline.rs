//! Streaming-only MySQL CDC pipeline (Phase II.3.d).
//!
//! Per RFC-0008 §"Skip-snapshot mode": no snapshot, capture current GTID,
//! stream forward. Single workflow, no child, no continue-as-new — full
//! CDC topology is deferred to a follow-up phase.

use common_types::pipeline_spec::{PipelineSpec, SourceSpec};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::mysql_cdc::inputs::*;
use crate::activities::mysql_cdc::MysqlCdcActivities;
use crate::activities::run_lifecycle::{
    CompleteRunInput, FailRunInput, RunLifecycleActivities, StartRunInput,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlCdcPipelineInput {
    pub run_id: Uuid,
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub spec: PipelineSpec,
    pub source_conn: common_types::connection_config::ConnectionConfig,
    /// 0 = forever (production); >0 caps streaming windows for tests.
    #[serde(default)]
    pub max_windows: u32,
    /// Windows per run before continuing-as-new; 0 = the default.
    #[serde(default)]
    pub continue_as_new_after: u32,
    /// Set when this run is a continuation of an earlier one.
    #[serde(default)]
    pub resume: Option<super::StreamResume>,
}

#[workflow]
pub struct MysqlCdcPipelineWorkflow {
    input: MysqlCdcPipelineInput,
}

fn retry_policy() -> temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
    use prost_wkt_types::Duration as PbDuration;
    temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
        initial_interval: Some(PbDuration {
            seconds: 1,
            nanos: 0,
        }),
        backoff_coefficient: 2.0,
        maximum_interval: Some(PbDuration {
            seconds: 30,
            nanos: 0,
        }),
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
impl MysqlCdcPipelineWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: MysqlCdcPipelineInput) -> Self {
        Self { input }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (run_id, tenant_id) = ctx.state(|s| (s.input.run_id, s.input.tenant_id));
        match Self::run_inner(ctx).await {
            Ok(()) => Ok(()),
            Err(t) if super::is_continue_as_new(&t) => Err(t),
            Err(t) => {
                let err_str = format!("{t}");
                let _ = ctx
                    .start_activity(
                        RunLifecycleActivities::fail_run,
                        FailRunInput {
                            run_id,
                            tenant_id,
                            error: err_str,
                        },
                        opts_short(),
                    )
                    .await;
                Err(t)
            }
        }
    }

    async fn run_inner(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let input = ctx.state(|s| s.input.clone());
        let my = match &input.spec.source {
            SourceSpec::MysqlCdc(m) => m.clone(),
            _ => {
                return Err(
                    anyhow::anyhow!("MysqlCdcPipelineWorkflow requires MysqlCdc source").into(),
                )
            }
        };
        let dest = input.spec.destination.clone();

        ctx.start_activity(
            RunLifecycleActivities::start_run,
            StartRunInput {
                run_id: input.run_id,
                tenant_id: input.tenant_id,
            },
            opts_short(),
        )
        .await?;

        ctx.start_activity(
            MysqlCdcActivities::verify_mysql_config,
            VerifyMysqlConfigInput {
                tenant_id: input.tenant_id,
                principal_id: input.principal_id,
                jti: input.jti,
                source_conn: input.source_conn.clone(),
                schema: my.schema.clone(),
                table: my.table.clone(),
            },
            opts_short(),
        )
        .await?;

        // Snapshot resume check: if a prior run captured a GTID and
        // partway-completed snapshot, we MUST reuse that GTID. Re-
        // capturing now would skip past writes during the failure
        // window and lose data.
        let snap_state = ctx
            .start_activity(
                MysqlCdcActivities::mysql_snapshot_state_get,
                MysqlSnapshotStateGetInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                },
                opts_short(),
            )
            .await?;
        let captured_gtid = if !snap_state.captured_gtid.is_empty() {
            snap_state.captured_gtid.clone()
        } else {
            ctx.start_activity(
                MysqlCdcActivities::capture_start_gtid,
                CaptureStartGtidInput {
                    pipeline_id: input.pipeline_id,
                    run_id: input.run_id,
                    tenant_id: input.tenant_id,
                    principal_id: input.principal_id,
                    jti: input.jti,
                    source_conn: input.source_conn.clone(),
                },
                opts_short(),
            )
            .await?
            .gtid_set
        };

        let schema_out = ctx
            .start_activity(
                MysqlCdcActivities::discover_mysql_schema,
                DiscoverMysqlSchemaInput {
                    pipeline_id: input.pipeline_id,
                    run_id: input.run_id,
                    tenant_id: input.tenant_id,
                    principal_id: input.principal_id,
                    jti: input.jti,
                    source_conn: input.source_conn.clone(),
                    schema: my.schema.clone(),
                    table: my.table.clone(),
                },
                opts_short(),
            )
            .await?;

        // Snapshot phase: when initial_sync == SnapshotThenStreaming,
        // chunked SELECT against the source until is_final. The captured
        // GTID was already recorded above (capture_start_gtid runs before
        // discover_schema), so streaming will resume from that point and
        // overlap is reconciled at the destination via PK merge.
        if matches!(
            my.initial_sync,
            common_types::pipeline_spec::MysqlInitialSync::SnapshotThenStreaming
        ) && !snap_state.completed
        {
            let pk_col = my.pk_column.clone().ok_or_else(|| {
                anyhow::anyhow!("MysqlCdcSourceSpec.pk_column required for snapshot mode")
            })?;
            let mut snap_seq: u32 = 0;
            let mut last_pk: Option<i64> = snap_state.last_pk;
            loop {
                let snap_out = ctx
                    .start_activity(
                        MysqlCdcActivities::mysql_snapshot_chunk,
                        MysqlSnapshotChunkInput {
                            pipeline_id: input.pipeline_id,
                            run_id: input.run_id,
                            tenant_id: input.tenant_id,
                            principal_id: input.principal_id,
                            jti: input.jti,
                            batch_seq: snap_seq,
                            source_conn: input.source_conn.clone(),
                            schema: my.schema.clone(),
                            table: my.table.clone(),
                            pk_column: pk_col.clone(),
                            last_pk,
                            batch_size: input.spec.batch_size.max(100) as u32,
                            schema_json: schema_out.schema_json.clone(),
                            captured_gtid: captured_gtid.clone(),
                            destination: dest.clone(),
                        },
                        opts_long(),
                    )
                    .await?;
                last_pk = snap_out.last_pk;
                snap_seq += 1;
                if snap_out.is_final {
                    break;
                }
            }
            // Mark snapshot complete so future re-runs skip the loop.
            ctx.start_activity(
                MysqlCdcActivities::mysql_snapshot_mark_completed,
                MysqlSnapshotMarkCompletedInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                },
                opts_short(),
            )
            .await?;
        }

        let mut current_gtid = captured_gtid;
        let mut window_seq: u32 = 0;
        let mut batch_seq: u32 = if matches!(
            my.initial_sync,
            common_types::pipeline_spec::MysqlInitialSync::SnapshotThenStreaming
        ) {
            // Streaming files start after the highest snapshot batch_seq.
            // Use a high offset so snapshot+streaming don't share filenames.
            10_000
        } else {
            0
        };
        // A continuation resumes the GTID and batch numbering of the run it
        // replaced; the GTID is otherwise only held in workflow memory.
        if let Some(r) = &input.resume {
            batch_seq = r.batch_seq;
            if let Some(gtid) = &r.position {
                current_gtid = gtid.clone();
            }
        }
        loop {
            if input.max_windows > 0 && window_seq >= input.max_windows {
                break;
            }
            if super::should_continue_as_new(ctx, window_seq, input.continue_as_new_after) {
                let next = MysqlCdcPipelineInput {
                    max_windows: super::remaining_windows(input.max_windows, window_seq),
                    resume: Some(super::StreamResume {
                        batch_seq,
                        position: Some(current_gtid.clone()),
                        ..Default::default()
                    }),
                    ..input.clone()
                };
                return Err(super::continue_as_new(
                    MysqlCdcPipelineWorkflow::name(),
                    &next,
                ));
            }
            let out = ctx
                .start_activity(
                    MysqlCdcActivities::read_window,
                    MysqlReadWindowInput {
                        pipeline_id: input.pipeline_id,
                        run_id: input.run_id,
                        tenant_id: input.tenant_id,
                        principal_id: input.principal_id,
                        jti: input.jti,
                        batch_seq,
                        source_conn: input.source_conn.clone(),
                        server_id: my.server_id,
                        schema: my.schema.clone(),
                        table: my.table.clone(),
                        start_gtid: current_gtid.clone(),
                        max_events: input.spec.batch_size.max(100) as u32,
                        schema_json: schema_out.schema_json.clone(),
                        heartbeat_secs: my.heartbeat_secs,
                        destination: dest.clone(),
                    },
                    opts_long(),
                )
                .await?;
            current_gtid = out.new_gtid;
            batch_seq += 1;
            window_seq += 1;
            if out.rows == 0 {
                ctx.timer(Duration::from_secs(2)).await;
            }
        }

        ctx.start_activity(
            RunLifecycleActivities::complete_run,
            CompleteRunInput {
                run_id: input.run_id,
                tenant_id: input.tenant_id,
            },
            opts_short(),
        )
        .await?;
        Ok(())
    }
}
