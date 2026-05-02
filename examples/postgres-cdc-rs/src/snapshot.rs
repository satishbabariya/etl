//! Snapshot phase. Filled in Task 5.

use crate::platform::connector::db;
use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn initial(_url: &str, _cfg: &SourceCfg, _batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    Err(ConnectorError::Other(
        "postgres-cdc-rs snapshot::initial not yet implemented (lands in phase-2-3f-5)".into(),
    ))
}

pub fn next_chunk(
    _url: &str,
    _cfg: &SourceCfg,
    _cursor_value: &str,
    _batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    Err(ConnectorError::Other(
        "postgres-cdc-rs snapshot::next_chunk not yet implemented (lands in phase-2-3f-5)".into(),
    ))
}

pub(crate) fn db_err_to_connector_err(e: db::DbError) -> ConnectorError {
    match e {
        db::DbError::InvalidConfig(s) => ConnectorError::InvalidConfig(s),
        db::DbError::ConnectFailed(s) | db::DbError::PositionLost(s) => {
            ConnectorError::SourceUnavailable(s)
        }
        db::DbError::QueryFailed(s) => ConnectorError::Other(s),
        db::DbError::Unsupported(s) => ConnectorError::Other(format!("unsupported: {s}")),
    }
}
