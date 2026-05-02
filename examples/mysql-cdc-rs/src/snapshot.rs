//! Snapshot phase: chunked SELECT WHERE id > last_pk ORDER BY id LIMIT N.
//!
//! The cursor for snapshot is `snapshot-pk` with value `<gtid>|<last_pk>`.
//! We pin the GTID once on the first call (so streaming starts from the
//! point the snapshot saw a consistent view) and carry it through every
//! snapshot chunk, swapping the cursor kind to `gtid` when the chunk
//! returns fewer rows than batch_size (i.e. snapshot done).

use crate::arrow_io::{rows_to_ipc, Row};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::{ConnectorError, ReadOutcome, SourceCfg};

/// First call: pin GTID, return one chunk starting from id=0.
pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    let h = open(url)?;
    let gtid = read_gtid_executed(h)?;
    let chunk = chunk_after(h, cfg, 0, batch_size)?;
    db::close(h);
    finalize(chunk, &gtid, 0, batch_size)
}

/// Subsequent snapshot call: parse "<gtid>|<last_pk>", fetch next chunk.
pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (gtid, last_pk) = parse_snapshot_cursor(cursor_value)?;
    let h = open(url)?;
    let chunk = chunk_after(h, cfg, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &gtid, last_pk, batch_size)
}

fn finalize(
    chunk: Chunk,
    gtid: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    if chunk.rows.is_empty() {
        // No rows yet, but we still want to transition to streaming so
        // we don't loop forever in snapshot mode on an empty table.
        return Ok(ReadOutcome {
            batch_ipc: vec![],
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Gtid,
                value: gtid.to_string(),
            }),
            is_final: true,
        });
    }
    let new_last_pk = chunk.rows.last().map(|(id, _)| *id).unwrap_or(last_pk_in);
    let pos = format!("snapshot:{gtid}|{new_last_pk}");
    let arrow_rows: Vec<Row> = chunk
        .rows
        .into_iter()
        .map(|(id, name)| Row {
            id,
            name,
            op: 's',
            position: pos.clone(),
        })
        .collect();
    let rows_n = arrow_rows.len() as u32;
    let bytes = rows_to_ipc(&arrow_rows)
        .map_err(|e| ConnectorError::Other(format!("rows_to_ipc: {e}")))?;

    // Snapshot done when the chunk is short — switch the cursor kind
    // to gtid so the next read_batch call hits the streaming arm.
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

struct Chunk {
    rows: Vec<(i64, Option<String>)>,
}

fn chunk_after(
    h: db::DbHandle,
    cfg: &SourceCfg,
    last_pk: i64,
    batch_size: i64,
) -> Result<Chunk, ConnectorError> {
    // CAST AS CHAR so the host receives bytes and emits utf8 strings;
    // we keep the SDK pattern from native MySQL CDC snapshot.
    let sql = format!(
        "SELECT id, CAST(name AS CHAR) FROM `{schema}`.`{table}` \
         WHERE id > ? ORDER BY id LIMIT {limit}",
        schema = cfg.schema,
        table = cfg.table,
        limit = batch_size,
    );
    let rows = db::query(h, &sql, &[last_pk.to_string()])
        .map_err(db_err_to_connector_err)?;
    let mut out: Vec<(i64, Option<String>)> = Vec::with_capacity(rows.len());
    for r in rows {
        let id: i64 = r.first().and_then(|v| v.as_deref()).and_then(|s| s.parse().ok())
            .ok_or_else(|| ConnectorError::Other("snapshot: expected i64 id".into()))?;
        let name: Option<String> = r.get(1).and_then(|v| v.clone());
        out.push((id, name));
    }
    Ok(Chunk { rows: out })
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
    let pk: i64 = pk.parse().map_err(|e| {
        ConnectorError::InvalidConfig(format!("snapshot cursor pk not i64: {e}"))
    })?;
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
    fn parse_snapshot_cursor_accepts_basic() {
        let (g, pk) = parse_snapshot_cursor("uuid:1-7|42").unwrap();
        assert_eq!(g, "uuid:1-7");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_malformed() {
        assert!(parse_snapshot_cursor("no-pipe-here").is_err());
        assert!(parse_snapshot_cursor("uuid|notanumber").is_err());
    }
}
