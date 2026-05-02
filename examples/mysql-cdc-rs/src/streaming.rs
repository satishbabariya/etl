//! Streaming phase: db.subscribe-changes + drain via db.next-event.
//!
//! Each call to `read_batch` opens one short-lived subscription, drains
//! up to `batch_size` events, then closes it. This keeps each activity
//! invocation self-contained — handles don't survive across activations,
//! and the host's idle timeout (default 5s) bounds the per-call wait.

use crate::arrow_io::{rows_to_ipc, Row};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::snapshot::db_err_to_connector_err;
use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn next_window(
    url: &str,
    cfg: &SourceCfg,
    start_gtid: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let h = db::open(url).map_err(db_err_to_connector_err)?;
    let sub = db::subscribe_changes(h, start_gtid).map_err(db_err_to_connector_err)?;
    let qualified = format!("{}.{}", cfg.schema, cfg.table);

    let mut rows: Vec<Row> = Vec::new();
    let mut latest_position: String = start_gtid.to_string();

    while (rows.len() as i64) < batch_size {
        let evt = match db::next_event(sub).map_err(db_err_to_connector_err)? {
            Some(e) => e,
            None => break, // host idle timeout — drain done
        };
        if !evt.position.is_empty() {
            latest_position = evt.position.clone();
        }
        if evt.table != qualified {
            continue;
        }
        if let Some(row) = decode_row(&evt) {
            rows.push(row);
        }
    }
    db::close_stream(sub);

    let bytes = if rows.is_empty() {
        Vec::new()
    } else {
        rows_to_ipc(&rows).map_err(|e| ConnectorError::Other(format!("rows_to_ipc: {e}")))?
    };

    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows.len() as u32,
        new_cursor: Some(CursorValue {
            kind: CursorKind::Gtid,
            value: latest_position,
        }),
        is_final: false,
    })
}

fn decode_row(evt: &db::ChangeEvent) -> Option<Row> {
    use serde_json::Value;
    let v: Value = serde_json::from_str(&evt.row_json).ok()?;
    // `before`/`after` arrays per the host JSON shape: positional Values.
    // Inserts/updates use `after`, deletes use `before`.
    let arr = match evt.op {
        'd' => v.get("before")?.as_array()?,
        _ => v.get("after")?.as_array()?,
    };
    let id = arr.first()?.as_i64()?;
    let name: Option<String> = arr.get(1).and_then(|c| match c {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    });
    Some(Row {
        id,
        name,
        op: evt.op,
        position: evt.position.clone(),
    })
}
