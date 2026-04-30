//! Workflows: orchestration logic (RFC-4).
pub mod cdc_pipeline;
pub mod mysql_cdc_pipeline;
pub mod pipeline_run;

pub use cdc_pipeline::{CdcPipelineInput, CdcPipelineWorkflow};
pub use mysql_cdc_pipeline::{MysqlCdcPipelineInput, MysqlCdcPipelineWorkflow};
pub use pipeline_run::{PipelineRunInput, PipelineRunWorkflow};
