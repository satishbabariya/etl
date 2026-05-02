//! Streaming phase: discover schema once per read_batch, drain events
//! from db.subscribe-changes, decode JSON rows positionally per
//! discovered column type.

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns};
use crate::platform::connector::db;
use crate::platform::connector::host::{log, LogLevel};
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
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let sub = db::subscribe_changes(h, start_gtid, &[]).map_err(db_err_to_connector_err)?;
    let qualified = format!("{}.{}", cfg.schema, cfg.table);
    let schema = build_full_schema(&columns_to_fields(&cols));
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    let mut latest_position = start_gtid.to_string();
    let mut rows_collected = 0i64;
    while rows_collected < batch_size {
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
        if append_event(&mut bb, &evt, cols.len()) {
            rows_collected += 1;
        }
    }
    db::close_stream(sub);
    let rows_n = bb.rows() as u32;
    let bytes = if rows_n == 0 {
        Vec::new()
    } else {
        bb.finish_to_ipc()
            .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue {
            kind: CursorKind::Gtid,
            value: latest_position,
        }),
        is_final: false,
    })
}

fn append_event(bb: &mut DynamicBatchBuilder, evt: &db::ChangeEvent, n_cols: usize) -> bool {
    use serde_json::Value;
    let v: Value = match serde_json::from_str(&evt.row_json) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let arr = match evt.op {
        'd' => v.get("before").and_then(|x| x.as_array()),
        _ => v.get("after").and_then(|x| x.as_array()),
    };
    let arr = match arr {
        Some(a) => a,
        None => return false,
    };
    if arr.len() != n_cols {
        log(
            LogLevel::Warn,
            &format!(
                "mysql-cdc-rs: row_json arity {} != discovered cols {} (op={}, table={}, json={})",
                arr.len(),
                n_cols,
                evt.op,
                evt.table,
                evt.row_json
            ),
        );
    }
    let mut owned: Vec<Option<String>> = Vec::with_capacity(n_cols);
    for i in 0..n_cols {
        owned.push(match arr.get(i) {
            Some(Value::Null) | None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            Some(other) => Some(other.to_string()),
        });
    }
    let cells: Vec<Option<&str>> = owned.iter().map(|c| c.as_deref()).collect();
    bb.append_row(&cells, evt.op, &evt.position);
    true
}
