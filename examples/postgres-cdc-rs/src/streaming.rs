//! Streaming phase: db.subscribe-changes + drain via db.next-event.
//!
//! Each call to `read_batch` opens one short-lived subscription, drains
//! up to `batch_size` events, then closes it. Slot and publication
//! names are passed via the WIT options bag — the host uses them as
//! parameters to pg_logical_slot_get_binary_changes.

use crate::arrow_io::{rows_to_ipc, Row};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::snapshot::db_err_to_connector_err;
use crate::{
    publication_name as pub_name_fn, slot_name as slot_name_fn, ConnectorError, ReadOutcome,
    SourceCfg,
};

pub fn next_window(
    url: &str,
    cfg: &SourceCfg,
    start_lsn: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let h = db::open(url).map_err(db_err_to_connector_err)?;
    let slot = slot_name_fn(&cfg.schema, &cfg.table);
    let pub_name = pub_name_fn(&cfg.schema, &cfg.table);
    let opts: Vec<(String, String)> = vec![
        ("slot_name".to_string(), slot.clone()),
        ("publication_names".to_string(), pub_name.clone()),
    ];
    let sub = db::subscribe_changes(h, start_lsn, &opts).map_err(db_err_to_connector_err)?;
    let qualified = format!("{}.{}", cfg.schema, cfg.table);

    let mut rows: Vec<Row> = Vec::new();
    let mut latest_position: String = start_lsn.to_string();

    while (rows.len() as i64) < batch_size {
        let evt = match db::next_event(sub).map_err(db_err_to_connector_err)? {
            Some(e) => e,
            None => break,
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
            kind: CursorKind::Lsn,
            value: latest_position,
        }),
        is_final: false,
    })
}

fn decode_row(evt: &db::ChangeEvent) -> Option<Row> {
    use serde_json::Value;
    let v: Value = serde_json::from_str(&evt.row_json).ok()?;
    let arr = match evt.op {
        'd' => v.get("before")?.as_array()?,
        _ => v.get("after")?.as_array()?,
    };
    // Pgoutput v1 returns text values, so cells arrive as JSON strings.
    let id_str = arr.first().and_then(|c| match c {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    })?;
    let id: i64 = id_str.parse().ok()?;
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
