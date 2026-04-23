//! Streaming CDC via `pg_logical_slot_get_binary_changes`.
//!
//! We deliberately do not use the streaming replication protocol in
//! Phase I.6 MVP — tokio-postgres 0.7 does not expose it, and the SQL
//! function returns the same pgoutput bytes. Trade-off: a shorter
//! polling tick rather than server-push latency. Good enough for local
//! dogfood; streaming-protocol migration is a Phase II task.
//!
//! Each `read_window` call issues one SQL query that drains up to
//! `max_events` rows from the slot. The slot advances automatically:
//! `get_binary_changes` commits position on success.

use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, Row};

use super::decode::{self, CdcEvent, RelationTable};
use common_types::cursor::lsn_to_string;

pub struct WindowOutput {
    pub events: Vec<CdcEvent>,
    pub relations: RelationTable,
    pub new_position: Option<u64>,
    pub is_empty: bool,
}

pub async fn read_window(
    conn_url: &str,
    slot_name: &str,
    publication: &str,
    _start_lsn: Option<&str>, // informational only — pg_logical_slot_get advances the slot itself
    max_events: usize,
    mut relations: RelationTable,
) -> Result<WindowOutput> {
    let mut c = PgConnection::connect(conn_url).await?;
    // Drain up to max_events rows; proto_version=1 matches our decoder.
    let stmt = "SELECT lsn::text, data \
                FROM pg_logical_slot_get_binary_changes($1, NULL, $2, \
                    'proto_version', '1', 'publication_names', $3)";
    let rows = sqlx::query(stmt)
        .bind(slot_name)
        .bind(max_events as i32)
        .bind(publication)
        .fetch_all(&mut c)
        .await
        .context("pg_logical_slot_get_binary_changes")?;
    let mut events: Vec<CdcEvent> = Vec::with_capacity(rows.len());
    let mut last_lsn_str: Option<String> = None;
    for r in &rows {
        let lsn: String = r.try_get(0)?;
        let data: Vec<u8> = r.try_get(1)?;
        let ev = decode::decode_message(&data)
            .with_context(|| format!("decoding pgoutput msg @ {lsn}"))?;
        if let CdcEvent::Relation(rel) = &ev {
            relations.insert(rel.rel_id, rel.clone());
        }
        events.push(ev);
        last_lsn_str = Some(lsn);
    }
    let new_position = last_lsn_str
        .map(|s| common_types::cursor::lsn_from_string(&s))
        .transpose()
        .unwrap_or(None);
    let is_empty = events.is_empty();
    Ok(WindowOutput { events, relations, new_position, is_empty })
}

/// Convert a flush of `read_window` into a `RecordBatch`. Begin/Commit/
/// Relation are folded into per-row `_cdc.lsn` / `_cdc.commit_ts` /
/// `_cdc.txid` metadata; only data rows (i/u/d) become Arrow rows.
pub fn events_to_batch(
    events: &[CdcEvent],
    relations: &RelationTable,
    rel_id_filter: u32,
    schema: arrow::datatypes::SchemaRef,
) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    use arrow::array::{ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
    use std::sync::Arc;
    let rel = relations
        .get(&rel_id_filter)
        .ok_or_else(|| anyhow::anyhow!("no Relation seen for rel_id {rel_id_filter}"))?;
    let n_data_cols = rel.columns.len();
    let mut col_builders: Vec<StringBuilder> =
        (0..n_data_cols).map(|_| StringBuilder::new()).collect();
    let mut op_b = StringBuilder::new();
    let mut lsn_b = StringBuilder::new();
    let mut ts_b = TimestampMicrosecondBuilder::new();
    let mut tx_b = Int64Builder::new();
    let mut current_txid: Option<u32> = None;
    let mut current_commit_ts: Option<i64> = None;
    let mut current_lsn: Option<u64> = None;
    for ev in events {
        match ev {
            CdcEvent::Begin { xid, commit_ts_micros, .. } => {
                current_txid = Some(*xid);
                current_commit_ts = Some(*commit_ts_micros);
            }
            CdcEvent::Commit { end_lsn, .. } => {
                current_lsn = Some(*end_lsn);
            }
            CdcEvent::Relation(_) => {}
            CdcEvent::Insert { rel_id, row } if *rel_id == rel_id_filter => {
                for (i, v) in row.iter().enumerate() {
                    col_builders[i].append_option(v.as_deref());
                }
                op_b.append_value("i");
                lsn_b.append_value(lsn_to_string(current_lsn.unwrap_or(0)));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            CdcEvent::Update { rel_id, row } if *rel_id == rel_id_filter => {
                for (i, v) in row.iter().enumerate() {
                    col_builders[i].append_option(v.as_deref());
                }
                op_b.append_value("u");
                lsn_b.append_value(lsn_to_string(current_lsn.unwrap_or(0)));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            CdcEvent::Delete { rel_id, key } if *rel_id == rel_id_filter => {
                for (i, col) in rel.columns.iter().enumerate() {
                    if col.is_key {
                        col_builders[i].append_option(key.get(i).and_then(|v| v.as_deref()));
                    } else {
                        col_builders[i].append_null();
                    }
                }
                op_b.append_value("d");
                lsn_b.append_value(lsn_to_string(current_lsn.unwrap_or(0)));
                ts_b.append_option(current_commit_ts);
                tx_b.append_option(current_txid.map(|t| t as i64));
            }
            _ => {}
        }
    }
    let mut cols: Vec<ArrayRef> = col_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    cols.push(Arc::new(op_b.finish()));
    cols.push(Arc::new(lsn_b.finish()));
    cols.push(Arc::new(ts_b.finish()));
    cols.push(Arc::new(tx_b.finish()));
    Ok(arrow::record_batch::RecordBatch::try_new(schema, cols)?)
}
