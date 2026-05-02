//! postgres-cdc-rs — Phase II.3.f reference WASM CDC connector for Postgres.
//!
//! Cursor lifecycle (matches mysql-cdc-rs):
//!
//! `None` (initial run)
//!   ↓ pin LSN via `SELECT pg_current_wal_lsn()`,
//!     ensure publication+slot via idempotent SQL,
//!     return one snapshot chunk
//! `snapshot-pk` value="<lsn>|<last_pk>"
//!   ↓ snapshot loop: fetch next chunk WHERE id > last_pk
//!   ↓ when chunk_size < batch_size, transition cursor to `lsn`
//! `lsn` value="<lsn>"
//!   ↓ streaming loop forever: db.subscribe-changes + db.next-event
//!
//! Source config JSON: `{ "schema": "public", "table": "items" }`.
//! Schema (hardcoded for the demo): `id BIGINT, name TEXT NULL`.

mod arrow_io;
mod discover;
mod snapshot;
mod streaming;

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "source-connector",
});

use platform::connector::db;
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

/// Slot + publication names derive deterministically from schema+table
/// so re-runs of the same pipeline reuse the same slot. Truncated
/// SHA-256 hex; safe for Postgres identifier length limits
/// (NAMEDATALEN = 64).
pub(crate) fn slot_name(schema: &str, table: &str) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(schema.as_bytes());
    h.update(b".");
    h.update(table.as_bytes());
    let digest = h.finalize();
    let short = hex::encode(&digest[..6]);
    format!("etl_pgrs_{short}")
}

pub(crate) fn publication_name(schema: &str, table: &str) -> String {
    let s = slot_name(schema, table);
    format!("{s}_pub")
}

impl Guest for Component {
    fn discover(conn: ConnectionConfig, source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let cfg = parse_source_cfg(&source.json)?;
        let h = db::open(&conn.url).map_err(snapshot::db_err_to_connector_err)?;
        let cols = discover::query_columns(h, &cfg.schema, &cfg.table)?;
        db::close(h);
        let schema = arrow_io::build_full_schema(&discover::columns_to_fields(&cols));
        arrow_io::schema_ipc_bytes(&schema)
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
                "postgres-cdc-rs: read_batch table={}.{} cursor={:?} batch_size={}",
                cfg.schema, cfg.table, cursor, bs
            ),
        );

        match cursor.as_ref().map(|c| c.kind) {
            None => snapshot::initial(&conn.url, &cfg, bs),
            Some(CursorKind::SnapshotPk) => {
                snapshot::next_chunk(&conn.url, &cfg, &cursor.unwrap().value, bs)
            }
            Some(CursorKind::Lsn) => {
                streaming::next_window(&conn.url, &cfg, &cursor.unwrap().value, bs)
            }
            Some(other) => Err(ConnectorError::InvalidConfig(format!(
                "unexpected cursor kind for postgres-cdc-rs: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_name_deterministic() {
        assert_eq!(slot_name("public", "items"), slot_name("public", "items"));
    }

    #[test]
    fn slot_name_distinguishes_tables() {
        assert_ne!(slot_name("public", "a"), slot_name("public", "b"));
    }

    #[test]
    fn publication_name_includes_slot_prefix() {
        let p = publication_name("public", "items");
        assert!(p.starts_with("etl_pgrs_"));
        assert!(p.ends_with("_pub"));
    }
}
