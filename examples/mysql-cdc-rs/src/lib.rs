//! mysql-cdc-rs — Phase II.3.e reference WASM CDC connector.
//!
//! Demonstrates: snapshot-then-streaming MySQL CDC authored entirely
//! in the SDK using only the typed db.* host verbs (no raw TCP, no
//! per-connector binlog parser).
//!
//! ## Cursor lifecycle
//!
//! `None` (initial run)
//!   ↓ pin GTID via `SELECT @@gtid_executed`, return one snapshot chunk
//! `snapshot-pk` value="<gtid>|<last_pk>"
//!   ↓ snapshot loop: fetch next chunk WHERE id > last_pk
//!   ↓ when chunk_size < batch_size, transition cursor to `gtid`
//! `gtid` value="<gtid_set>"
//!   ↓ streaming loop forever: db.subscribe-changes + db.next-event
//!
//! ## Source config JSON
//!
//! ```json
//! { "schema": "test", "table": "items" }
//! ```
//!
//! Schema (hardcoded for the demo): `id BIGINT PRIMARY KEY, name TEXT`.

mod arrow_io;
mod discover;
mod snapshot;
mod streaming;

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "source-connector",
});

use platform::connector::host::{log, LogLevel};
use platform::connector::types::CursorKind;

struct Component;
export!(Component);

#[derive(serde::Deserialize, Clone)]
pub(crate) struct SourceCfg {
    pub schema: String,
    pub table: String,
}

fn parse_source_cfg(json: &str) -> Result<SourceCfg, ConnectorError> {
    serde_json::from_str(json)
        .map_err(|e| ConnectorError::InvalidConfig(format!("source config: {e}")))
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        arrow_io::schema_ipc_bytes()
            .map_err(|e| ConnectorError::Other(format!("schema ipc: {e}")))
    }

    fn read_batch(
        conn: ConnectionConfig,
        source: SourceConfig,
        cursor: Option<CursorValue>,
        batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        let cfg = parse_source_cfg(&source.json)?;
        let bs = batch_size.max(1) as i64;
        log(
            LogLevel::Info,
            &format!(
                "mysql-cdc-rs: read_batch table={}.{} cursor={:?} batch_size={}",
                cfg.schema, cfg.table, cursor, bs
            ),
        );

        match cursor.as_ref().map(|c| c.kind) {
            None => snapshot::initial(&conn.url, &cfg, bs),
            Some(CursorKind::SnapshotPk) => {
                snapshot::next_chunk(&conn.url, &cfg, &cursor.unwrap().value, bs)
            }
            Some(CursorKind::Gtid) => {
                streaming::next_window(&conn.url, &cfg, &cursor.unwrap().value, bs)
            }
            Some(other) => Err(ConnectorError::InvalidConfig(format!(
                "unexpected cursor kind for mysql-cdc-rs: {other:?}"
            ))),
        }
    }
}
