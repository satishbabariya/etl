//! Workflows: orchestration logic (RFC-4).
pub mod cdc_pipeline;
pub mod mysql_cdc_pipeline;
pub mod pipeline_run;
pub mod wasm_cdc_pipeline;

pub use cdc_pipeline::{CdcPipelineInput, CdcPipelineWorkflow};
pub use mysql_cdc_pipeline::{MysqlCdcPipelineInput, MysqlCdcPipelineWorkflow};
pub use pipeline_run::{PipelineRunInput, PipelineRunWorkflow};
pub use wasm_cdc_pipeline::{WasmCdcPipelineInput, WasmCdcPipelineWorkflow};

use serde::{Deserialize, Serialize};
use temporalio_common::protos::coresdk::workflow_commands::ContinueAsNewWorkflowExecution;
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_sdk::{WorkflowContext, WorkflowTermination};

/// Streaming windows a CDC workflow runs before it continues-as-new, so
/// its event history stays bounded. Temporal may suggest continuing
/// sooner (history size/length); that is honoured too.
pub const CONTINUE_AS_NEW_AFTER_WINDOWS: u32 = 500;

/// Patch marker: workflows started before continue-as-new existed replay
/// their original command sequence.
const CONTINUE_AS_NEW_PATCH: &str = "cdc-continue-as-new";

/// Streaming state carried across continue-as-new. `batch_seq` must keep
/// counting: loaders key idempotency on (run_id, batch_seq), so restarting
/// it under the same run_id would skip new batches as already loaded.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamResume {
    pub batch_seq: u32,
    /// Source position to resume from (Postgres LSN / MySQL GTID set);
    /// WASM CDC carries its cursor in `initial_cursor` instead.
    pub position: Option<String>,
    pub rows_total: u64,
    pub rows_rejected: u64,
}

/// True when the streaming loop should hand off to a fresh run.
/// `after == 0` means [`CONTINUE_AS_NEW_AFTER_WINDOWS`].
pub(crate) fn should_continue_as_new<W>(
    ctx: &WorkflowContext<W>,
    window_seq: u32,
    after: u32,
) -> bool {
    let after = if after == 0 {
        CONTINUE_AS_NEW_AFTER_WINDOWS
    } else {
        after
    };
    (window_seq >= after || ctx.continue_as_new_suggested()) && ctx.patched(CONTINUE_AS_NEW_PATCH)
}

/// `max_windows` for the next run, so a test cap spans the whole chain
/// (0 stays 0 = forever).
pub(crate) fn remaining_windows(max_windows: u32, window_seq: u32) -> u32 {
    if max_windows == 0 {
        0
    } else {
        max_windows.saturating_sub(window_seq).max(1)
    }
}

pub(crate) fn continue_as_new<I: Serialize>(workflow_type: &str, input: &I) -> WorkflowTermination {
    match input.as_json_payload() {
        Ok(payload) => WorkflowTermination::continue_as_new(ContinueAsNewWorkflowExecution {
            workflow_type: workflow_type.to_string(),
            arguments: vec![payload],
            ..Default::default()
        }),
        Err(e) => WorkflowTermination::failed(e),
    }
}

/// Continue-as-new ends a run without the pipeline run failing.
pub(crate) fn is_continue_as_new(t: &WorkflowTermination) -> bool {
    matches!(t, WorkflowTermination::ContinueAsNew(_))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_windows_spans_the_chain() {
        assert_eq!(remaining_windows(0, 500), 0, "forever stays forever");
        assert_eq!(remaining_windows(10, 4), 6);
        // Never hands a run a cap of 0, which would mean forever.
        assert_eq!(remaining_windows(4, 4), 1);
    }

    #[test]
    fn continue_as_new_carries_the_input() {
        let resume = StreamResume {
            batch_seq: 42,
            position: Some("0/16B3748".into()),
            rows_total: 7,
            rows_rejected: 1,
        };
        let t = continue_as_new("CdcPipelineWorkflow", &resume);
        assert!(is_continue_as_new(&t));
        let WorkflowTermination::ContinueAsNew(can) = t else {
            unreachable!()
        };
        assert_eq!(can.workflow_type, "CdcPipelineWorkflow");
        let back: StreamResume = serde_json::from_slice(&can.arguments[0].data).unwrap();
        assert_eq!(back, resume);
    }
}
