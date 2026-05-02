//! Snapshot phase: discover schema + PK on each call, build dynamic
//! SELECT projection, decode rows through the DynamicBatchBuilder.

use std::sync::Arc;

use arrow_schema::Schema;

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns, query_pk_column, DiscoveredColumn};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::{ConnectorError, ReadOutcome, SourceCfg};

pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    run_chunk(url, cfg, batch_size, 0, None)
}

pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (gtid, last_pk) = parse_snapshot_cursor(cursor_value)?;
    run_chunk(url, cfg, batch_size, last_pk, Some(gtid))
}

fn run_chunk(
    url: &str,
    cfg: &SourceCfg,
    batch_size: i64,
    last_pk: i64,
    pinned_gtid: Option<String>,
) -> Result<ReadOutcome, ConnectorError> {
    let h = open(url)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let pk = query_pk_column(h, &cfg.schema, &cfg.table)?;
    let gtid = match pinned_gtid {
        Some(g) => g,
        None => read_gtid_executed(h)?,
    };
    let chunk = chunk_after(h, cfg, &cols, &pk, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, &gtid, last_pk, batch_size)
}

struct Chunk {
    rows: Vec<Vec<Option<String>>>,
    last_pk_in_chunk: Option<i64>,
}

fn chunk_after(
    h: db::DbHandle,
    cfg: &SourceCfg,
    cols: &[DiscoveredColumn],
    pk: &str,
    last_pk: i64,
    batch_size: i64,
) -> Result<Chunk, ConnectorError> {
    let select_list = cols
        .iter()
        .map(|c| format!("`{}`", c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select_list} FROM `{schema}`.`{table}` \
         WHERE `{pk}` > ? ORDER BY `{pk}` LIMIT {limit}",
        schema = cfg.schema,
        table = cfg.table,
        limit = batch_size,
    );
    let rows = db::query(h, &sql, &[last_pk.to_string()]).map_err(db_err_to_connector_err)?;
    let pk_idx = cols.iter().position(|c| c.name == pk).ok_or_else(|| {
        ConnectorError::Other(format!("PK column {pk} missing from discovered columns"))
    })?;
    let mut last_pk_in_chunk: Option<i64> = None;
    for r in &rows {
        if let Some(Some(s)) = r.get(pk_idx) {
            if let Ok(v) = s.parse::<i64>() {
                last_pk_in_chunk = Some(v);
            }
        }
    }
    Ok(Chunk {
        rows: rows.into_iter().collect(),
        last_pk_in_chunk,
    })
}

fn finalize(
    chunk: Chunk,
    cols: &[DiscoveredColumn],
    gtid: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let schema = build_full_schema(&columns_to_fields(cols));
    if chunk.rows.is_empty() {
        return Ok(ReadOutcome {
            batch_ipc: schema_only_bytes(&schema)?,
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Gtid,
                value: gtid.to_string(),
            }),
            is_final: true,
        });
    }
    let new_last_pk = chunk.last_pk_in_chunk.unwrap_or(last_pk_in);
    let position = format!("snapshot:{gtid}|{new_last_pk}");
    let mut bb = DynamicBatchBuilder::new(schema.clone());
    for row in &chunk.rows {
        let cells: Vec<Option<&str>> = row
            .iter()
            .take(cols.len())
            .map(|c| c.as_deref())
            .collect();
        bb.append_row(&cells, 's', &position);
    }
    let rows_n = bb.rows() as u32;
    let bytes = bb
        .finish_to_ipc()
        .map_err(|e| ConnectorError::Other(format!("finish_to_ipc: {e}")))?;
    let snapshot_done = (rows_n as i64) < batch_size;
    let (kind, value) = if snapshot_done {
        (CursorKind::Gtid, gtid.to_string())
    } else {
        (CursorKind::SnapshotPk, format!("{gtid}|{new_last_pk}"))
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue { kind, value }),
        is_final: snapshot_done,
    })
}

fn schema_only_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, ConnectorError> {
    crate::arrow_io::schema_ipc_bytes(schema)
        .map_err(|e| ConnectorError::Other(format!("schema_ipc_bytes: {e}")))
}

fn read_gtid_executed(h: db::DbHandle) -> Result<String, ConnectorError> {
    let rows = db::query(h, "SELECT @@gtid_executed", &[]).map_err(db_err_to_connector_err)?;
    let cell = rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .flatten()
        .unwrap_or_default();
    Ok(cell)
}

fn open(url: &str) -> Result<db::DbHandle, ConnectorError> {
    db::open(url).map_err(db_err_to_connector_err)
}

pub(crate) fn parse_snapshot_cursor(s: &str) -> Result<(String, i64), ConnectorError> {
    let (gtid, pk) = s.split_once('|').ok_or_else(|| {
        ConnectorError::InvalidConfig(format!("snapshot cursor missing '|': {s}"))
    })?;
    let pk: i64 = pk
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor pk not i64: {e}")))?;
    Ok((gtid.to_string(), pk))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_cursor_basic() {
        let (g, pk) = parse_snapshot_cursor("uuid:1-7|42").unwrap();
        assert_eq!(g, "uuid:1-7");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_bad() {
        assert!(parse_snapshot_cursor("nopipe").is_err());
        assert!(parse_snapshot_cursor("g|x").is_err());
    }
}
