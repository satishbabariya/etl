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

use super::bindings::platform::connector::db::{ChangeEvent, DbError};

const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 5;
const DEFAULT_MAX_PER_POLL: usize = 1000;

pub struct PgSubscription {
    pub(super) conn: PgConnection,
    pub(super) slot_name: String,
    pub(super) publication_names: String,
    pub(super) proto_version: String,
    pub(super) pending: VecDeque<ChangeEvent>,
    pub(super) relations: RelationTable,
    pub(super) current_position: String,
    pub(super) idle_timeout: Duration,
    pub(super) max_per_poll: usize,
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
        }
    }

    /// Drain one event. `Ok(None)` means the slot returned 0 rows on
    /// this poll — the guest should treat that as "drain done" and
    /// return from read_batch.
    pub async fn next(&mut self) -> Result<Option<ChangeEvent>, DbError> {
        if let Some(ev) = self.pending.pop_front() {
            return Ok(Some(ev));
        }
        match self.poll_and_buffer().await {
            Ok(()) => Ok(self.pending.pop_front()),
            Err(e) => Err(e),
        }
    }

    async fn poll_and_buffer(&mut self) -> Result<(), DbError> {
        let stmt = "SELECT lsn::text, data \
                    FROM pg_logical_slot_get_binary_changes($1, NULL, $2, \
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
            if let Some(ce) = self.cdc_event_to_change_event(event, &lsn) {
                self.pending.push_back(ce);
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
