//! Per-stream MySQL binlog subscription state. Populated by Task 3
//! (phase-2-3e-3); here we keep just the type definition so Task 2
//! can compile against `DbHostState::streams`.

#![allow(dead_code)]

/// One active binlog subscription.
///
/// In Task 3 this will own the `BinlogStream`, a table_map_cache
/// keyed by table_id, and a VecDeque of pending events that the
/// guest hasn't drained yet (because one binlog event can contain
/// multiple rows but `db.next-event` returns one event at a time).
pub struct MysqlSubscription {
    /// Placeholder; replaced in Task 3 with the actual binlog stream
    /// + ancillary state.
    pub _placeholder: (),
}
