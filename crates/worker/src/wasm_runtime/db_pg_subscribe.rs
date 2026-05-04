//! Postgres logical-replication subscription state.
//!
//! One `PgSubscription` per active `db.subscribe-changes` call.
//! Each `next-event` call drains a buffered `pending` VecDeque first;
//! when empty, it polls `pg_logical_slot_get_binary_changes` for up
//! to `max_per_poll` rows and decodes the binary pgoutput payload via
//! the existing native decoder at `connectors/postgres/cdc/decode.rs`.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use sqlx::PgConnection;
use sqlx::Row as _;

use crate::connectors::postgres::cdc::decode::{
    decode_message, CdcEvent, RelationInfo, RelationTable,
};
use common_types::cursor::lsn_to_string;

use super::bindings::platform::connector::db::{ChangeEvent, DbError};

const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 5;
const DEFAULT_MAX_PER_POLL: usize = 1000;

/// One pending entry holds the decoded ChangeEvent + the LSN of the
/// transaction's Commit message. When the guest pulls this event via
/// next_event, we use `txn_commit_lsn` (when present) as the slot-
/// advance target rather than the data event's own position. That
/// way the slot moves past the entire transaction in one step,
/// avoiding the next subscription replaying the trailing Commit
/// (which would re-emit the data events too).
pub(super) struct PendingEvent {
    pub(super) event: ChangeEvent,
    /// Set when the txn's Commit message was decoded in the same
    /// peek that produced this event. None if the peek ended mid-
    /// transaction (shouldn't happen with peek_binary_changes, which
    /// always returns whole txns, but defensive).
    pub(super) txn_commit_lsn: Option<String>,
}

pub struct PgSubscription {
    pub(super) conn: PgConnection,
    pub(super) slot_name: String,
    pub(super) publication_names: String,
    pub(super) proto_version: String,
    pub(super) pending: VecDeque<PendingEvent>,
    pub(super) relations: RelationTable,
    pub(super) current_position: String,
    pub(super) idle_timeout: Duration,
    pub(super) max_per_poll: usize,
    /// Highest LSN safe to advance the slot to. Set when next_event
    /// pulls a PendingEvent — uses `txn_commit_lsn` if present
    /// (advances past the whole transaction), falls back to the
    /// event's own position otherwise.
    pub(super) last_consumed_lsn: Option<String>,
}

impl PgSubscription {
    pub fn new(
        conn: PgConnection,
        slot_name: String,
        publication_names: String,
        proto_version: String,
        start_position: String,
    ) -> Self {
        Self {
            conn,
            slot_name,
            publication_names,
            proto_version,
            pending: VecDeque::new(),
            relations: HashMap::new(),
            current_position: start_position,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
            max_per_poll: DEFAULT_MAX_PER_POLL,
            last_consumed_lsn: None,
        }
    }

    /// Advance the slot to the highest LSN actually consumed by the
    /// guest. Called from close_stream so any events left in `pending`
    /// (drained from peek but not pulled by next_event) stay in the
    /// slot for the next subscription.
    pub async fn finalize(mut self) -> Result<(), DbError> {
        if let Some(lsn) = self.last_consumed_lsn.take() {
            let stmt = "SELECT pg_replication_slot_advance($1, $2::pg_lsn)";
            if let Err(e) = sqlx::query(stmt)
                .bind(&self.slot_name)
                .bind(&lsn)
                .execute(&mut self.conn)
                .await
            {
                return Err(DbError::QueryFailed(format!(
                    "pg_replication_slot_advance({}, {}): {e}",
                    self.slot_name, lsn
                )));
            }
        }
        Ok(())
    }

    /// Drain one event. `Ok(None)` means the slot returned 0 rows on
    /// this poll — the guest should treat that as "drain done" and
    /// return from read_batch.
    pub async fn next(&mut self) -> Result<Option<ChangeEvent>, DbError> {
        if let Some(p) = self.pending.pop_front() {
            self.last_consumed_lsn = p
                .txn_commit_lsn
                .clone()
                .or_else(|| Some(p.event.position.clone()));
            return Ok(Some(p.event));
        }
        match self.poll_and_buffer().await {
            Ok(()) => {
                let next = self.pending.pop_front();
                if let Some(ref p) = next {
                    self.last_consumed_lsn = p
                        .txn_commit_lsn
                        .clone()
                        .or_else(|| Some(p.event.position.clone()));
                }
                Ok(next.map(|p| p.event))
            }
            Err(e) => Err(e),
        }
    }

    async fn poll_and_buffer(&mut self) -> Result<(), DbError> {
        // PEEK, not GET. _peek_ leaves events in the slot until we
        // explicitly advance via finalize() in close_stream — so events
        // that get drained into `pending` but never pulled by next_event
        // (e.g. when the connector hits batch_size and stops) stay in
        // the slot for the next subscription instead of being lost.
        let stmt = "SELECT lsn::text, data \
                    FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, \
                        'proto_version', $3, \
                        'publication_names', $4)";
        let rows = match sqlx::query(stmt)
            .bind(&self.slot_name)
            .bind(self.max_per_poll as i32)
            .bind(&self.proto_version)
            .bind(&self.publication_names)
            .fetch_all(&mut self.conn)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(DbError::QueryFailed(format!(
                    "pg_logical_slot_get_binary_changes: {e}"
                )));
            }
        };
        // Index in `pending` of the first event of the in-progress
        // transaction. When we see the matching Commit, back-fill
        // each entry from this index forward with the Commit's
        // end_lsn — that's what we'll advance the slot to once the
        // guest consumes those events.
        let mut txn_start_index: Option<usize> = None;
        for r in &rows {
            let lsn: String = r.try_get(0).unwrap_or_default();
            let data: Vec<u8> = r.try_get(1).unwrap_or_default();
            let event = match decode_message(&data) {
                Ok(e) => e,
                Err(e) => {
                    return Err(DbError::QueryFailed(format!(
                        "pgoutput decode at lsn {lsn}: {e}"
                    )));
                }
            };
            match &event {
                CdcEvent::Begin { .. } => {
                    txn_start_index = Some(self.pending.len());
                }
                CdcEvent::Commit { end_lsn, .. } => {
                    // Use end_lsn (next byte after the Commit message)
                    // so pg_replication_slot_advance moves the slot
                    // PAST the entire transaction.
                    let commit_lsn_str = lsn_to_string(*end_lsn);
                    if let Some(start) = txn_start_index.take() {
                        for entry in self.pending.iter_mut().skip(start) {
                            entry.txn_commit_lsn = Some(commit_lsn_str.clone());
                        }
                    }
                }
                _ => {
                    if let Some(ce) = self.cdc_event_to_change_event(event, &lsn) {
                        self.pending.push_back(PendingEvent {
                            event: ce,
                            txn_commit_lsn: None,
                        });
                    }
                }
            }
        }
        if let Some(last) = rows.last() {
            let lsn: String = last.try_get(0).unwrap_or_default();
            if !lsn.is_empty() {
                self.current_position = lsn;
            }
        }
        Ok(())
    }

    fn cdc_event_to_change_event(
        &mut self,
        ev: CdcEvent,
        lsn: &str,
    ) -> Option<ChangeEvent> {
        match ev {
            CdcEvent::Relation(info) => {
                self.relations.insert(info.rel_id, info);
                None
            }
            CdcEvent::Insert { rel_id, row } => {
                let rel = self.relations.get(&rel_id)?;
                Some(row_event(rel, 'i', lsn, Some(row), None))
            }
            CdcEvent::Update { rel_id, row } => {
                let rel = self.relations.get(&rel_id)?;
                Some(row_event(rel, 'u', lsn, Some(row), None))
            }
            CdcEvent::Delete { rel_id, key } => {
                let rel = self.relations.get(&rel_id)?;
                Some(row_event(rel, 'd', lsn, None, Some(key)))
            }
            // Begin/Commit/Truncate/Origin produce no row-level event.
            _ => None,
        }
    }
}

fn row_event(
    rel: &RelationInfo,
    op: char,
    lsn: &str,
    after: Option<Vec<Option<String>>>,
    before: Option<Vec<Option<String>>>,
) -> ChangeEvent {
    let mut obj = serde_json::Map::new();
    if let Some(a) = after {
        obj.insert("after".into(), positional(&a));
    }
    if let Some(b) = before {
        obj.insert("before".into(), positional(&b));
    }
    ChangeEvent {
        op,
        position: lsn.to_string(),
        commit_ts: 0,
        txid: 0,
        table: format!("{}.{}", rel.namespace, rel.name),
        row_json: serde_json::Value::Object(obj).to_string(),
    }
}

fn positional(cells: &[Option<String>]) -> serde_json::Value {
    serde_json::Value::Array(
        cells
            .iter()
            .map(|c| match c {
                None => serde_json::Value::Null,
                Some(s) => serde_json::Value::String(s.clone()),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positional_handles_empty() {
        let arr = positional(&[]);
        assert_eq!(arr, serde_json::Value::Array(vec![]));
    }

    #[test]
    fn positional_preserves_order_with_null() {
        let arr = positional(&[
            Some("a".into()),
            Some("b".into()),
            None,
            Some("d".into()),
        ]);
        let items = match arr {
            serde_json::Value::Array(v) => v,
            _ => panic!(),
        };
        assert_eq!(items.len(), 4);
        assert_eq!(items[0], serde_json::Value::String("a".into()));
        assert_eq!(items[2], serde_json::Value::Null);
        assert_eq!(items[3], serde_json::Value::String("d".into()));
    }

    #[test]
    fn row_event_carries_table_and_position() {
        let rel = RelationInfo {
            rel_id: 7,
            namespace: "public".into(),
            name: "items".into(),
            columns: vec![],
        };
        let ev = row_event(
            &rel,
            'i',
            "0/16B3748",
            Some(vec![Some("1".into()), Some("alice".into())]),
            None,
        );
        assert_eq!(ev.op, 'i');
        assert_eq!(ev.table, "public.items");
        assert_eq!(ev.position, "0/16B3748");
        assert!(ev.row_json.contains("\"after\":["));
    }
}
