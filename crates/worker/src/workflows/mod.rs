//! Workflows: orchestration logic (RFC-4).
pub mod cdc_pipeline;
pub mod mysql_cdc_pipeline;
pub mod pipeline_run;
pub mod wasm_cdc_pipeline;

pub use cdc_pipeline::{CdcPipelineInput, CdcPipelineWorkflow};
pub use mysql_cdc_pipeline::{MysqlCdcPipelineInput, MysqlCdcPipelineWorkflow};
pub use pipeline_run::{PipelineRunInput, PipelineRunWorkflow};
pub use wasm_cdc_pipeline::{WasmCdcPipelineInput, WasmCdcPipelineWorkflow};
