//! Snapshot phase: chunked SELECT WHERE id > last_pk ORDER BY id LIMIT N.
//!
//! On the initial call we additionally:
//!   - pin the LSN via `SELECT pg_current_wal_lsn()`;
//!   - ensure the publication exists (CREATE PUBLICATION FOR TABLE; guarded
//!     by a pg_publication lookup);
//!   - ensure the replication slot exists (idempotent SELECT-WHERE-NOT-EXISTS guard
//!     around `pg_create_logical_replication_slot`).
//!
//! Cursor format during snapshot: `snapshot-pk` value=`<lsn>|<last_pk>`.
//! Transitions to `lsn` value=`<lsn>` once a short chunk arrives.

use crate::arrow_io::{rows_to_ipc, Row};
use crate::platform::connector::db;
use crate::platform::connector::types::{CursorKind, CursorValue};
use crate::{
    publication_name as pub_name_fn, slot_name as slot_name_fn, ConnectorError, ReadOutcome,
    SourceCfg,
};

pub fn initial(url: &str, cfg: &SourceCfg, batch_size: i64) -> Result<ReadOutcome, ConnectorError> {
    let h = open(url)?;
    ensure_publication(h, cfg)?;
    ensure_slot(h, cfg)?;
    let lsn = read_current_lsn(h)?;
    let chunk = chunk_after(h, cfg, 0, batch_size)?;
    db::close(h);
    finalize(chunk, &lsn, 0, batch_size)
}

pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (lsn, last_pk) = parse_snapshot_cursor(cursor_value)?;
    let h = open(url)?;
    let chunk = chunk_after(h, cfg, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &lsn, last_pk, batch_size)
}

fn finalize(
    chunk: Chunk,
    lsn: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    if chunk.rows.is_empty() {
        return Ok(ReadOutcome {
            batch_ipc: vec![],
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Lsn,
                value: lsn.to_string(),
            }),
            is_final: true,
        });
    }
    let new_last_pk = chunk.rows.last().map(|(id, _)| *id).unwrap_or(last_pk_in);
    let pos = format!("snapshot:{lsn}|{new_last_pk}");
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

    let snapshot_done = (rows_n as i64) < batch_size;
    let (kind, value) = if snapshot_done {
        (CursorKind::Lsn, lsn.to_string())
    } else {
        (CursorKind::SnapshotPk, format!("{lsn}|{new_last_pk}"))
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
    let sql = format!(
        "SELECT id, name FROM \"{schema}\".\"{table}\" \
         WHERE id > $1 ORDER BY id LIMIT {limit}",
        schema = cfg.schema,
        table = cfg.table,
        limit = batch_size,
    );
    let rows = db::query(h, &sql, &[last_pk.to_string()]).map_err(db_err_to_connector_err)?;
    let mut out: Vec<(i64, Option<String>)> = Vec::with_capacity(rows.len());
    for r in rows {
        let id: i64 = r
            .first()
            .and_then(|v| v.as_deref())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| ConnectorError::Other("snapshot: expected i64 id".into()))?;
        let name: Option<String> = r.get(1).and_then(|v| v.clone());
        out.push((id, name));
    }
    Ok(Chunk { rows: out })
}

fn read_current_lsn(h: db::DbHandle) -> Result<String, ConnectorError> {
    let rows =
        db::query(h, "SELECT pg_current_wal_lsn()::text", &[]).map_err(db_err_to_connector_err)?;
    let cell = rows
        .into_iter()
        .next()
        .and_then(|r| r.into_iter().next())
        .flatten()
        .ok_or_else(|| ConnectorError::Other("pg_current_wal_lsn returned no rows".into()))?;
    Ok(cell)
}

fn ensure_publication(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let pub_name = pub_name_fn(&cfg.schema, &cfg.table);
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_publication WHERE pubname = $1",
        &[pub_name.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        let stmt = format!(
            "CREATE PUBLICATION \"{pub_name}\" \
             FOR TABLE \"{schema}\".\"{table}\"",
            schema = cfg.schema,
            table = cfg.table,
        );
        db::query(h, &stmt, &[]).map_err(db_err_to_connector_err)?;
    }
    Ok(())
}

fn ensure_slot(h: db::DbHandle, cfg: &SourceCfg) -> Result<(), ConnectorError> {
    let slot = slot_name_fn(&cfg.schema, &cfg.table);
    let exists_rows = db::query(
        h,
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
        &[slot.clone()],
    )
    .map_err(db_err_to_connector_err)?;
    if exists_rows.is_empty() {
        db::query(
            h,
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[slot],
        )
        .map_err(db_err_to_connector_err)?;
    }
    Ok(())
}

fn open(url: &str) -> Result<db::DbHandle, ConnectorError> {
    db::open(url).map_err(db_err_to_connector_err)
}

pub(crate) fn parse_snapshot_cursor(s: &str) -> Result<(String, i64), ConnectorError> {
    let (lsn, pk) = s.split_once('|').ok_or_else(|| {
        ConnectorError::InvalidConfig(format!("snapshot cursor missing '|': {s}"))
    })?;
    let pk: i64 = pk
        .parse()
        .map_err(|e| ConnectorError::InvalidConfig(format!("snapshot cursor pk not i64: {e}")))?;
    Ok((lsn.to_string(), pk))
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
        let (lsn, pk) = parse_snapshot_cursor("0/16B3748|42").unwrap();
        assert_eq!(lsn, "0/16B3748");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_malformed() {
        assert!(parse_snapshot_cursor("no-pipe-here").is_err());
        assert!(parse_snapshot_cursor("lsn|notanumber").is_err());
    }
}
