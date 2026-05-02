//! Per-stream MySQL binlog subscription state.
//!
//! One `MysqlSubscription` per active `db.subscribe-changes` call.
//! `db.next-event` drains buffered events first, then pulls more from
//! the underlying `BinlogStream` until either an event is ready or the
//! idle timeout elapses.
//!
//! Why a buffer? A single `RowsEvent` from the binlog may contain N
//! rows. The WIT contract delivers one row per `next-event`, so we
//! need somewhere to park the leftover rows between calls.

use anyhow::{anyhow, Context, Result};
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::{BinlogStream, Value};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use super::bindings::platform::connector::db::{ChangeEvent, DbError};

const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 5;

pub struct MysqlSubscription {
    pub(super) stream: BinlogStream,
    pub(super) table_map_cache: HashMap<u64, TableMapEvent<'static>>,
    pub(super) pending: VecDeque<ChangeEvent>,
    /// Most-recent uuid:gno from the most-recent GtidEvent. Committed
    /// into `current_gtid` on XidEvent.
    pub(super) current_uuid_gno: Option<(String, u64)>,
    /// Microseconds since unix epoch from the most-recent GtidEvent.
    pub(super) current_commit_ts: i64,
    /// Position string we surface to the guest. Updated whenever a
    /// transaction commits.
    pub(super) current_position: String,
    pub(super) idle_timeout: Duration,
}

impl MysqlSubscription {
    pub fn new(stream: BinlogStream, start_position: String) -> Self {
        Self {
            stream,
            table_map_cache: HashMap::new(),
            pending: VecDeque::new(),
            current_uuid_gno: None,
            current_commit_ts: 0,
            current_position: start_position,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
        }
    }

    /// Drain one event. `Ok(None)` means the idle timeout elapsed
    /// before any event was decoded — the guest should treat that as
    /// "stop draining for this batch".
    pub async fn next(&mut self) -> Result<Option<ChangeEvent>, DbError> {
        if let Some(ev) = self.pending.pop_front() {
            return Ok(Some(ev));
        }
        loop {
            use futures_util::StreamExt;
            let next = match tokio::time::timeout(self.idle_timeout, self.stream.next()).await {
                Ok(Some(Ok(ev))) => ev,
                Ok(Some(Err(e))) => {
                    return Err(DbError::PositionLost(format!("binlog stream error: {e}")));
                }
                Ok(None) => return Ok(None),
                Err(_) => return Ok(None),
            };

            let data = match next.read_data() {
                Ok(Some(d)) => d,
                Ok(None) => continue,
                Err(e) => {
                    return Err(DbError::QueryFailed(format!("read_data: {e}")));
                }
            };

            match data {
                EventData::GtidEvent(g) => {
                    let uuid = uuid::Uuid::from_bytes(g.sid()).hyphenated().to_string();
                    self.current_uuid_gno = Some((uuid, g.gno()));
                    let micros = g.immediate_commit_timestamp();
                    if micros != 0 {
                        self.current_commit_ts = micros as i64;
                    }
                }
                EventData::XidEvent(_) => {
                    if let Some((uuid, gno)) = self.current_uuid_gno.take() {
                        // We don't merge into a GtidSet here — the guest
                        // owns position arithmetic. We just hand back the
                        // most-recent committed gno as the position.
                        self.current_position = format!("{uuid}:{gno}");
                    }
                }
                EventData::TableMapEvent(tme) => {
                    self.table_map_cache.insert(tme.table_id(), tme.into_owned());
                }
                EventData::RowsEvent(rd) => {
                    let tid = rd.table_id();
                    let tme = match self.table_map_cache.get(&tid) {
                        Some(t) => t.clone(),
                        None => continue,
                    };
                    let table_name = format!("{}.{}", tme.database_name(), tme.table_name());
                    if let Err(e) = self.buffer_rows(&rd, &tme, &table_name) {
                        return Err(DbError::QueryFailed(format!("decode rows: {e}")));
                    }
                }
                EventData::HeartbeatEvent => {}
                _ => {}
            }

            if let Some(ev) = self.pending.pop_front() {
                return Ok(Some(ev));
            }
        }
    }

    fn buffer_rows(
        &mut self,
        rd: &RowsEventData<'_>,
        tme: &TableMapEvent<'static>,
        table_name: &str,
    ) -> Result<()> {
        match rd {
            RowsEventData::WriteRowsEvent(ev) => {
                for pair in ev.rows(tme) {
                    let (_b, after) = pair.context("decode WRITE row")?;
                    let after = after.ok_or_else(|| anyhow!("WRITE missing after-image"))?;
                    self.pending.push_back(self.row_to_event(
                        'i',
                        table_name,
                        Some(after),
                        None,
                    )?);
                }
            }
            RowsEventData::UpdateRowsEvent(ev) => {
                for pair in ev.rows(tme) {
                    let (before, after) = pair.context("decode UPDATE row")?;
                    let after = after.ok_or_else(|| anyhow!("UPDATE missing after-image"))?;
                    self.pending.push_back(self.row_to_event(
                        'u',
                        table_name,
                        Some(after),
                        before,
                    )?);
                }
            }
            RowsEventData::DeleteRowsEvent(ev) => {
                for pair in ev.rows(tme) {
                    let (before, _a) = pair.context("decode DELETE row")?;
                    let before = before.ok_or_else(|| anyhow!("DELETE missing before-image"))?;
                    self.pending.push_back(self.row_to_event(
                        'd',
                        table_name,
                        None,
                        Some(before),
                    )?);
                }
            }
            RowsEventData::WriteRowsEventV1(_)
            | RowsEventData::UpdateRowsEventV1(_)
            | RowsEventData::DeleteRowsEventV1(_) => {
                anyhow::bail!("row event v1 not supported; require MySQL 5.7+ binlog row format");
            }
            RowsEventData::PartialUpdateRowsEvent(_) => {
                anyhow::bail!(
                    "partial JSON row updates not supported; \
                     set binlog_row_value_options to its default"
                );
            }
        }
        Ok(())
    }

    fn row_to_event(
        &self,
        op: char,
        table: &str,
        after: Option<BinlogRow>,
        before: Option<BinlogRow>,
    ) -> Result<ChangeEvent> {
        let mut obj = serde_json::Map::new();
        if let Some(a) = after {
            obj.insert("after".into(), row_to_json(a)?);
        }
        if let Some(b) = before {
            obj.insert("before".into(), row_to_json(b)?);
        }
        let json = serde_json::Value::Object(obj).to_string();
        Ok(ChangeEvent {
            op,
            position: self.current_position.clone(),
            commit_ts: self.current_commit_ts,
            txid: 0,
            table: table.to_string(),
            row_json: json,
        })
    }
}

fn row_to_json(row: BinlogRow) -> Result<serde_json::Value> {
    let mut arr: Vec<serde_json::Value> = Vec::with_capacity(row.len());
    let values = row.unwrap();
    for v in values {
        arr.push(binlog_value_to_json(&v));
    }
    Ok(serde_json::Value::Array(arr))
}

fn binlog_value_to_json(v: &BinlogValue<'_>) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        BinlogValue::Value(inner) => mysql_value_to_json(inner),
        BinlogValue::Jsonb(jb) => match jb.clone().parse() {
            Ok(parsed) => parsed.into(),
            Err(_) => J::String("__jsonb_parse_error__".into()),
        },
        BinlogValue::JsonDiff(_) => J::String("__partial_json_diff__".into()),
    }
}

fn mysql_value_to_json(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::NULL => J::Null,
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => J::String(s.to_string()),
            Err(_) => J::String(format!("0x{}", hex_encode(b))),
        },
        Value::Int(i) => J::Number((*i).into()),
        Value::UInt(u) => J::Number((*u).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(J::Number)
            .unwrap_or(J::Null),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(J::Number)
            .unwrap_or(J::Null),
        Value::Date(y, mo, d, h, mi, s, us) => J::String(format!(
            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
        )),
        Value::Time(neg, days, h, mi, s, us) => {
            let total_h = (*days as u32) * 24 + *h as u32;
            J::String(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                if *neg { "-" } else { "" },
                total_h,
                mi,
                s,
                us
            ))
        }
    }
}

fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_value_null_round_trip() {
        assert_eq!(mysql_value_to_json(&Value::NULL), serde_json::Value::Null);
    }

    #[test]
    fn mysql_value_text_bytes_become_string() {
        let v = Value::Bytes(b"hello".to_vec());
        assert_eq!(
            mysql_value_to_json(&v),
            serde_json::Value::String("hello".into())
        );
    }

    #[test]
    fn mysql_value_binary_bytes_become_hex_prefixed() {
        let v = Value::Bytes(vec![0xff, 0xfe, 0xab]);
        assert_eq!(
            mysql_value_to_json(&v),
            serde_json::Value::String("0xfffeab".into())
        );
    }

    #[test]
    fn mysql_value_int_serializes_as_number() {
        assert_eq!(
            mysql_value_to_json(&Value::Int(-7)),
            serde_json::Value::Number((-7i64).into())
        );
    }

    #[test]
    fn mysql_value_date_iso8601() {
        let v = Value::Date(2026, 5, 2, 13, 14, 15, 999_999);
        let s = match mysql_value_to_json(&v) {
            serde_json::Value::String(s) => s,
            other => panic!("expected string, got {other:?}"),
        };
        assert_eq!(s, "2026-05-02 13:14:15.999999");
    }

    #[test]
    fn hex_encode_round_trip() {
        assert_eq!(hex_encode(&[0x00, 0xff, 0x10]), "00ff10");
    }
}
