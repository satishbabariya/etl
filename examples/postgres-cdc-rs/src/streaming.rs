//! Streaming phase. Filled in Task 6.

use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn next_window(
    _url: &str,
    _cfg: &SourceCfg,
    _start_lsn: &str,
    _batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    Err(ConnectorError::Other(
        "postgres-cdc-rs streaming::next_window not yet implemented (lands in phase-2-3f-6)".into(),
    ))
}
