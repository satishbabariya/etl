//! Host implementation of the `platform:connector/db` WIT interface.
//!
//! Backs both mysql:// (mysql_async) and postgres:// (sqlx::PgConnection)
//! URLs. Streaming-side state for either DB family lives in db_subscribe
//! (MySQL) or db_pg_subscribe (Postgres); this file is the dispatch +
//! synchronous-SQL layer.
#![allow(dead_code)]

use mysql_async::prelude::*;
use mysql_async::{Conn, Value};
use sqlx::Connection as _;
use std::collections::HashMap;

/// Per-instance db state. One of these lives inside `HostState` per
/// component activation; handles do not survive across activations.
pub struct DbHostState {
    next_id: u32,
    conns: HashMap<u32, DbConn>,
    pub(super) streams: HashMap<u32, DbStream>,
}

pub(super) enum DbStream {
    Mysql(super::db_subscribe::MysqlSubscription),
    Postgres(super::db_pg_subscribe::PgSubscription),
}

pub(super) enum DbConn {
    Mysql(Conn),
    Postgres(sqlx::PgConnection),
    /// The MySQL handle was passed to `subscribe-changes`, which
    /// consumed the underlying connection. Subsequent db.query calls
    /// on this id fail. Postgres never sets this — pg connections
    /// move into the PgSubscription on subscribe and the conn map
    /// just removes the entry.
    Consumed,
}

impl Default for DbHostState {
    fn default() -> Self {
        Self {
            next_id: 1,
            conns: HashMap::new(),
            streams: HashMap::new(),
        }
    }
}

impl DbHostState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn alloc_id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        id
    }

    /// Each MySQL replica needs a distinct server_id when it connects
    /// to a primary as a slave. We allocate from the high range so
    /// guest-driven CDC streams don't clash with server-side numbers.
    pub(super) fn alloc_server_id(&mut self) -> u32 {
        100_000 + self.alloc_id()
    }

    pub(super) fn take_conn(&mut self, id: u32) -> Option<DbConn> {
        self.conns.remove(&id)
    }

    pub(super) fn put_conn(&mut self, id: u32, c: DbConn) {
        self.conns.insert(id, c);
    }
}

use super::bindings::platform::connector::db::{
    self as wit_db, ChangeEvent, ChangeStream, DbError, DbHandle, Host as DbHost,
};

impl DbHost for super::host::HostState {
    async fn open(&mut self, url: String) -> wasmtime::Result<Result<DbHandle, DbError>> {
        if url.starts_with("mysql://") {
            return match Conn::from_url(&url).await {
                Ok(conn) => {
                    let id = self.db.alloc_id();
                    self.db.conns.insert(id, DbConn::Mysql(conn));
                    Ok(Ok(DbHandle { id }))
                }
                Err(e) => Ok(Err(DbError::ConnectFailed(e.to_string()))),
            };
        }
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return match sqlx::PgConnection::connect(&url).await {
                Ok(conn) => {
                    let id = self.db.alloc_id();
                    self.db.conns.insert(id, DbConn::Postgres(conn));
                    Ok(Ok(DbHandle { id }))
                }
                Err(e) => Ok(Err(DbError::ConnectFailed(e.to_string()))),
            };
        }
        Ok(Err(DbError::InvalidConfig(format!(
            "unsupported scheme in url: {url}"
        ))))
    }

    async fn query(
        &mut self,
        h: DbHandle,
        sql: String,
        params: Vec<String>,
    ) -> wasmtime::Result<Result<Vec<Vec<Option<String>>>, DbError>> {
        let Some(entry) = self.db.conns.get_mut(&h.id) else {
            return Ok(Err(DbError::QueryFailed(format!(
                "no such db handle: {}",
                h.id
            ))));
        };
        match entry {
            DbConn::Mysql(conn) => {
                let params_v: Vec<Value> = params
                    .into_iter()
                    .map(|s| Value::Bytes(s.into_bytes()))
                    .collect();
                let rows: Vec<mysql_async::Row> = match conn.exec(&sql, params_v).await {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(DbError::QueryFailed(e.to_string()))),
                };
                let mut out: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len());
                for row in rows {
                    let n = row.columns_ref().len();
                    let mut cells: Vec<Option<String>> = Vec::with_capacity(n);
                    for i in 0..n {
                        let v: Value = row.as_ref(i).cloned().unwrap_or(Value::NULL);
                        cells.push(value_to_string(v));
                    }
                    out.push(cells);
                }
                Ok(Ok(out))
            }
            DbConn::Postgres(conn) => {
                let mut q = sqlx::query(&sql);
                for p in &params {
                    q = q.bind(p);
                }
                let rows = match q.fetch_all(&mut *conn).await {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(DbError::QueryFailed(e.to_string()))),
                };
                let mut out: Vec<Vec<Option<String>>> = Vec::with_capacity(rows.len());
                for row in &rows {
                    use sqlx::Row as _;
                    let n = row.len();
                    let mut cells: Vec<Option<String>> = Vec::with_capacity(n);
                    for i in 0..n {
                        let v: Option<String> = row.try_get(i).unwrap_or_default();
                        cells.push(v);
                    }
                    out.push(cells);
                }
                Ok(Ok(out))
            }
            DbConn::Consumed => Ok(Err(DbError::QueryFailed(
                "this handle was consumed by subscribe-changes".into(),
            ))),
        }
    }

    async fn subscribe_changes(
        &mut self,
        h: DbHandle,
        position: String,
        options: Vec<(String, String)>,
    ) -> wasmtime::Result<Result<ChangeStream, DbError>> {
        let conn = match self.db.conns.remove(&h.id) {
            Some(c) => c,
            None => {
                return Ok(Err(DbError::QueryFailed(format!(
                    "no such db handle: {}",
                    h.id
                ))));
            }
        };

        match conn {
            DbConn::Mysql(my_conn) => {
                // Mark the slot consumed so guests get a clear error if
                // they try db.query on the same handle later.
                self.db.conns.insert(h.id, DbConn::Consumed);
                let server_id = self.db.alloc_server_id();
                let req = match build_binlog_request(server_id, &position) {
                    Ok(r) => r,
                    Err(e) => return Ok(Err(e)),
                };
                let stream = match my_conn.get_binlog_stream(req).await {
                    Ok(s) => s,
                    Err(e) => {
                        return Ok(Err(DbError::ConnectFailed(format!(
                            "get_binlog_stream: {e}"
                        ))));
                    }
                };
                let sub_id = self.db.alloc_id();
                self.db.streams.insert(
                    sub_id,
                    DbStream::Mysql(super::db_subscribe::MysqlSubscription::new(
                        stream, position,
                    )),
                );
                Ok(Ok(ChangeStream { id: sub_id }))
            }
            DbConn::Postgres(pg_conn) => {
                let mut slot_name: Option<String> = None;
                let mut publication_names: Option<String> = None;
                let mut proto_version = "1".to_string();
                for (k, v) in &options {
                    match k.as_str() {
                        "slot_name" => slot_name = Some(v.clone()),
                        "publication_names" => publication_names = Some(v.clone()),
                        "proto_version" => proto_version = v.clone(),
                        _ => {}
                    }
                }
                let slot_name = match slot_name {
                    Some(s) => s,
                    None => {
                        return Ok(Err(DbError::InvalidConfig(
                            "postgres subscribe-changes requires options[slot_name]".into(),
                        )));
                    }
                };
                let publication_names = match publication_names {
                    Some(s) => s,
                    None => {
                        return Ok(Err(DbError::InvalidConfig(
                            "postgres subscribe-changes requires options[publication_names]"
                                .into(),
                        )));
                    }
                };
                let sub_id = self.db.alloc_id();
                self.db.streams.insert(
                    sub_id,
                    DbStream::Postgres(super::db_pg_subscribe::PgSubscription::new(
                        pg_conn,
                        slot_name,
                        publication_names,
                        proto_version,
                        position,
                    )),
                );
                Ok(Ok(ChangeStream { id: sub_id }))
            }
            DbConn::Consumed => Ok(Err(DbError::QueryFailed(
                "handle already consumed by an earlier subscribe-changes".into(),
            ))),
        }
    }

    async fn next_event(
        &mut self,
        s: ChangeStream,
    ) -> wasmtime::Result<Result<Option<ChangeEvent>, DbError>> {
        let Some(sub) = self.db.streams.get_mut(&s.id) else {
            return Ok(Err(DbError::QueryFailed(format!(
                "no such change-stream: {}",
                s.id
            ))));
        };
        let res = match sub {
            DbStream::Mysql(m) => m.next().await,
            DbStream::Postgres(p) => p.next().await,
        };
        Ok(res)
    }

    async fn close(&mut self, h: DbHandle) -> wasmtime::Result<()> {
        if let Some(conn) = self.db.conns.remove(&h.id) {
            match conn {
                DbConn::Mysql(c) => {
                    let _ = c.disconnect().await;
                }
                DbConn::Postgres(c) => {
                    let _ = c.close().await;
                }
                DbConn::Consumed => {}
            }
        }
        Ok(())
    }

    async fn close_stream(&mut self, s: ChangeStream) -> wasmtime::Result<()> {
        if let Some(sub) = self.db.streams.remove(&s.id) {
            // BinlogStream::close is async and consumes self. We
            // explicitly drop without close to avoid blocking on the
            // server roundtrip — the server will detect the closed
            // socket on its end.
            drop(sub);
        }
        Ok(())
    }
}

fn build_binlog_request<'a>(
    server_id: u32,
    position: &str,
) -> Result<mysql_async::BinlogStreamRequest<'a>, DbError> {
    use mysql_async::{BinlogStreamRequest, GnoInterval, Sid};
    let req = BinlogStreamRequest::new(server_id);
    if position.is_empty() {
        return Ok(req);
    }
    let mut sids: Vec<Sid<'a>> = Vec::new();
    for segment in position.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (uuid_str, ranges) = segment.split_once(':').ok_or_else(|| {
            DbError::InvalidConfig(format!("malformed GTID segment: {segment}"))
        })?;
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|e| DbError::InvalidConfig(format!("parse uuid '{uuid_str}': {e}")))?;
        let mut intervals: Vec<GnoInterval> = Vec::new();
        for r in ranges.split(':') {
            let (lo, hi) = match r.split_once('-') {
                Some((a, b)) => {
                    let a: u64 = a.parse().map_err(|e| {
                        DbError::InvalidConfig(format!("parse gno '{a}': {e}"))
                    })?;
                    let b: u64 = b.parse().map_err(|e| {
                        DbError::InvalidConfig(format!("parse gno '{b}': {e}"))
                    })?;
                    (a, b)
                }
                None => {
                    let n: u64 = r.parse().map_err(|e| {
                        DbError::InvalidConfig(format!("parse gno '{r}': {e}"))
                    })?;
                    (n, n)
                }
            };
            intervals.push(GnoInterval::new(lo, hi.saturating_add(1)));
        }
        sids.push(Sid::new(uuid.into_bytes()).with_intervals(intervals));
    }
    Ok(req.with_gtid().with_gtid_set(sids))
}

fn value_to_string(v: Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(d) => Some(d.to_string()),
        Value::Date(y, mo, d, h, mi, s, us) => Some(format!(
            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
        )),
        Value::Time(neg, days, h, mi, s, us) => {
            let total_h = (days as u32) * 24 + h as u32;
            Some(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                if neg { "-" } else { "" },
                total_h,
                mi,
                s,
                us
            ))
        }
    }
}

pub fn add_to_linker<T: 'static + Send>(
    linker: &mut wasmtime::component::Linker<T>,
    get: fn(&mut T) -> &mut super::host::HostState,
) -> wasmtime::Result<()>
where
    super::host::HostState: DbHost,
{
    wit_db::add_to_linker::<T, wasmtime::component::HasSelf<super::host::HostState>>(
        linker, get,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_id_monotonic() {
        let mut s = DbHostState::new();
        assert_eq!(s.alloc_id(), 1);
        assert_eq!(s.alloc_id(), 2);
        assert_eq!(s.alloc_id(), 3);
    }

    #[test]
    fn alloc_server_id_above_threshold() {
        let mut s = DbHostState::new();
        assert!(s.alloc_server_id() >= 100_000);
        assert!(s.alloc_server_id() >= 100_000);
    }

    #[test]
    fn value_to_string_handles_null() {
        assert_eq!(value_to_string(Value::NULL), None);
        assert_eq!(
            value_to_string(Value::Bytes(b"hello".to_vec())),
            Some("hello".into())
        );
        assert_eq!(value_to_string(Value::Int(-42)), Some("-42".into()));
    }

    #[test]
    fn value_to_string_formats_date() {
        let v = Value::Date(2026, 5, 2, 13, 14, 15, 999_999);
        assert_eq!(
            value_to_string(v),
            Some("2026-05-02 13:14:15.999999".into())
        );
    }

    #[tokio::test]
    async fn open_rejects_unknown_scheme() {
        use crate::wasm_runtime::limits::Limits;
        let mut state = crate::wasm_runtime::host::HostState::new(Limits::default());
        let res = DbHost::open(&mut state, "redis://localhost:6379".into())
            .await
            .unwrap();
        match res {
            Err(DbError::InvalidConfig(msg)) => {
                assert!(msg.contains("unsupported scheme"), "got: {msg}");
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }
}
