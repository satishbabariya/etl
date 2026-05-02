//! Host implementation of the `platform:connector/db` WIT interface.
//!
//! Tasks 2 (this commit): open / query / close — synchronous SQL backed
//! by mysql_async. Postgres URLs are accepted at the WIT level but
//! rejected with `db-error::unsupported` for now.
//!
//! Task 3 will replace the streaming method stubs.
#![allow(dead_code)]

use mysql_async::prelude::*;
use mysql_async::{Conn, Value};
use std::collections::HashMap;

/// Per-instance db state. One of these lives inside `HostState` per
/// component activation; handles do not survive across activations.
pub struct DbHostState {
    next_id: u32,
    conns: HashMap<u32, DbConn>,
    /// Streams are populated by Task 3.
    pub(super) streams: HashMap<u32, super::db_subscribe::MysqlSubscription>,
}

pub(super) enum DbConn {
    Mysql(Conn),
    /// The handle was passed to `subscribe-changes`, which consumed the
    /// underlying connection. Subsequent db.query calls on this id fail.
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
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return Ok(Err(DbError::Unsupported(
                "postgres support not in v1; use mysql:// urls".into(),
            )));
        }
        if !url.starts_with("mysql://") {
            return Ok(Err(DbError::InvalidConfig(format!(
                "unsupported scheme in url: {url}"
            ))));
        }
        match Conn::from_url(&url).await {
            Ok(conn) => {
                let id = self.db.alloc_id();
                self.db.conns.insert(id, DbConn::Mysql(conn));
                Ok(Ok(DbHandle { id }))
            }
            Err(e) => Ok(Err(DbError::ConnectFailed(e.to_string()))),
        }
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
        let conn = match entry {
            DbConn::Mysql(c) => c,
            DbConn::Consumed => {
                return Ok(Err(DbError::QueryFailed(
                    "this handle was consumed by subscribe-changes".into(),
                )));
            }
        };

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

    async fn subscribe_changes(
        &mut self,
        _h: DbHandle,
        _position: String,
    ) -> wasmtime::Result<Result<ChangeStream, DbError>> {
        // Task 3 wires this up.
        Ok(Err(DbError::Unsupported(
            "subscribe-changes not yet implemented (lands in phase-2-3e-3)".into(),
        )))
    }

    async fn next_event(
        &mut self,
        _s: ChangeStream,
    ) -> wasmtime::Result<Result<Option<ChangeEvent>, DbError>> {
        Ok(Err(DbError::Unsupported(
            "next-event not yet implemented (lands in phase-2-3e-3)".into(),
        )))
    }

    async fn close(&mut self, h: DbHandle) -> wasmtime::Result<()> {
        if let Some(conn) = self.db.conns.remove(&h.id) {
            if let DbConn::Mysql(c) = conn {
                let _ = c.disconnect().await;
            }
        }
        Ok(())
    }

    async fn close_stream(&mut self, _s: ChangeStream) -> wasmtime::Result<()> {
        // Task 3 implements stream cleanup.
        Ok(())
    }
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
}
