//! Placeholder — real implementation lands in Task 8.

use common_types::connection_config::ConnectionConfig;
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::PostgresSourceSpec;
use connector_sdk::ReadOutcome;

#[allow(dead_code)]
pub async fn run(
    _conn: &ConnectionConfig,
    _spec: &PostgresSourceSpec,
    _cursor: Option<CursorValue>,
    _batch_size: usize,
) -> anyhow::Result<ReadOutcome> {
    anyhow::bail!("read::run not yet implemented (Task 8)")
}
