//! Snapshot phase for Postgres: discover schema + PK on each call,
//! ensure publication + slot on the initial call, build dynamic
//! SELECT projection, decode rows through DynamicBatchBuilder.

use std::sync::Arc;

use arrow_schema::Schema;

use crate::arrow_io::{build_full_schema, DynamicBatchBuilder};
use crate::discover::{columns_to_fields, query_columns, query_pk_column, DiscoveredColumn};
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
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let pk = query_pk_column(h, &cfg.schema, &cfg.table)?;
    let chunk = chunk_after(h, cfg, &cols, &pk, 0, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, &lsn, 0, batch_size)
}

pub fn next_chunk(
    url: &str,
    cfg: &SourceCfg,
    cursor_value: &str,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let (lsn, last_pk) = parse_snapshot_cursor(cursor_value)?;
    let h = open(url)?;
    let cols = query_columns(h, &cfg.schema, &cfg.table)?;
    let pk = query_pk_column(h, &cfg.schema, &cfg.table)?;
    let chunk = chunk_after(h, cfg, &cols, &pk, last_pk, batch_size)?;
    db::close(h);
    finalize(chunk, &cols, &lsn, last_pk, batch_size)
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
    use arrow_schema::DataType;
    // Cast every column to text so the host's PgRow::try_get::<Option<String>>
    // succeeds regardless of source column type. sqlx's default binary
    // protocol doesn't auto-coerce BIGINT/TIMESTAMP/etc. to String, so
    // without this every non-text column would arrive as None.
    let select_list = cols
        .iter()
        .map(|c| format!("\"{}\"::text AS \"{}\"", c.name, c.name))
        .collect::<Vec<_>>()
        .join(", ");
    // Postgres rejects `bigint > $1` when sqlx binds $1 as text.
    // Cast the bound string to the PK's actual SQL type. Snapshot
    // requires an integer PK (we already document that) — derive
    // the right cast from the discovered Arrow DataType.
    let pk_cast = cols
        .iter()
        .find(|c| c.name == pk)
        .map(|c| match &c.data_type {
            DataType::Int16 => "smallint",
            DataType::Int32 => "integer",
            DataType::Int64 => "bigint",
            _ => "bigint",
        })
        .unwrap_or("bigint");
    let sql = format!(
        "SELECT {select_list} FROM \"{schema}\".\"{table}\" \
         WHERE \"{pk}\" > $1::{pk_cast} ORDER BY \"{pk}\" LIMIT {limit}",
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
    lsn: &str,
    last_pk_in: i64,
    batch_size: i64,
) -> Result<ReadOutcome, ConnectorError> {
    let schema = build_full_schema(&columns_to_fields(cols));
    if chunk.rows.is_empty() {
        return Ok(ReadOutcome {
            batch_ipc: schema_only_bytes(&schema)?,
            rows: 0,
            new_cursor: Some(CursorValue {
                kind: CursorKind::Lsn,
                value: lsn.to_string(),
            }),
            is_final: true,
            stream_name: None,
        });
    }
    let new_last_pk = chunk.last_pk_in_chunk.unwrap_or(last_pk_in);
    let position = format!("snapshot:{lsn}|{new_last_pk}");
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
        (CursorKind::Lsn, lsn.to_string())
    } else {
        (CursorKind::SnapshotPk, format!("{lsn}|{new_last_pk}"))
    };
    Ok(ReadOutcome {
        batch_ipc: bytes,
        rows: rows_n,
        new_cursor: Some(CursorValue { kind, value }),
        is_final: snapshot_done,
        stream_name: None,
    })
}

fn schema_only_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, ConnectorError> {
    crate::arrow_io::schema_ipc_bytes(schema)
        .map_err(|e| ConnectorError::Other(format!("schema_ipc_bytes: {e}")))
}

fn read_current_lsn(h: db::DbHandle) -> Result<String, ConnectorError> {
    let rows = db::query(h, "SELECT pg_current_wal_lsn()::text", &[])
        .map_err(db_err_to_connector_err)?;
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
    fn parse_snapshot_cursor_basic() {
        let (lsn, pk) = parse_snapshot_cursor("0/16B3748|42").unwrap();
        assert_eq!(lsn, "0/16B3748");
        assert_eq!(pk, 42);
    }

    #[test]
    fn parse_snapshot_cursor_rejects_bad() {
        assert!(parse_snapshot_cursor("nopipe").is_err());
        assert!(parse_snapshot_cursor("lsn|bad").is_err());
    }
}
