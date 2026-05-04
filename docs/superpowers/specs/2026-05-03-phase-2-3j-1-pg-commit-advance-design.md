# Phase II.3.j.1 — Postgres slot-advance-to-commit — Design Spec

> **Status:** Draft 2026-05-03. Approved by agent (user delegated all design calls). Predecessor: `2026-05-02-phase-2-3i-ci-harness-design.md` and the II.3.j fix loop merged at `bdc6f11`.

## Goal

Eliminate the at-least-once duplication noise from the Postgres CDC path. The II.3.j fix uses `pg_logical_slot_peek_binary_changes` and advances the slot via `pg_replication_slot_advance(slot, last_consumed_lsn)` on `close_stream`. `last_consumed_lsn` is the LSN of the last DATA event the guest pulled — but Postgres's logical decoding emits `Begin → I/U/D → Commit` per transaction, so advancing to the data event's LSN leaves the `Commit` message in the slot. Next subscription replays the entire transaction. Test surfaces this as 9 'd' rows for a 1-DELETE underlying truth.

Fix: advance the slot to the LSN of the **transaction's `Commit` message**, not the data event itself, but **only when that transaction's events have been fully consumed by the guest**. Multi-transaction peeks must not skip transactions whose data the guest hasn't seen yet.

## Non-goals

- **Eliminate at-least-once entirely.** The contract documented in the II.3.f spec is at-least-once with `(id, lsn)` keying for idempotency. We're removing one specific source of duplication, not promising exactly-once.
- **Group-commit batching across transactions.** If the slot has multiple transactions, we still advance one at a time as the connector drains them; we don't peek and advance an unconsumed transaction.
- **Changes to the WIT contract.** `next-event` still returns one event at a time; `close-stream` still triggers slot advance. Internal mechanics only.

---

## Why per-event commit tracking is needed

Consider a peek buffer:

```
B1 (lsn=100) → I (lsn=110) → U (lsn=120) → C1 (lsn=130)
B2 (lsn=200) → D (lsn=210) → C2 (lsn=220)
```

Guest connector batch_size=2 consumes I and U; D and C2 stay in `pending` and are dropped on `close_stream`.

If we tracked only `last_commit_lsn = 220` and advanced to it, we'd skip transaction 2 entirely — D would be lost forever.

If we track `last_consumed_lsn = 120` (the U event), the slot stays at 120; next peek replays [I, U, C1, B2, D, C2] and the guest sees I and U again before getting D.

The right answer: advance to **C1 (130)** because txn 1 is fully consumed; txn 2 isn't. Next peek returns [B2, D, C2] and the guest gets D cleanly without re-seeing I or U.

## Mechanism

Each entry in `pending` carries the LSN of its transaction's `Commit` message:

```rust
struct PendingEvent {
    event: ChangeEvent,
    txn_commit_lsn: Option<String>,  // None if Commit hasn't arrived yet in this peek
}

pub(super) pending: VecDeque<PendingEvent>,
```

In `poll_and_buffer`:

1. Track `txn_start_index: Option<usize>` — pending.len() at the most recent Begin.
2. For each row decoded from the peek:
   - `CdcEvent::Begin { ... }` → `txn_start_index = Some(self.pending.len())`.
   - `CdcEvent::Insert/Update/Delete` → push `PendingEvent { event, txn_commit_lsn: None }`. Stays None until the Commit arrives.
   - `CdcEvent::Commit { ... }` → walk pending from `txn_start_index.take()` to the end, set `txn_commit_lsn = Some(this_commit_lsn)` on each.
   - `CdcEvent::Relation` → updates relation cache, no push.
   - Other → ignore.

In `next`:

```rust
if let Some(p) = self.pending.pop_front() {
    self.last_consumed_lsn = p.txn_commit_lsn.or(Some(p.event.position.clone()));
    return Ok(Some(p.event));
}
```

When `txn_commit_lsn` is Some (the txn is fully present in the peek), use it. Fallback to the event's own position if Commit hasn't arrived yet (shouldn't happen with `peek_binary_changes` returning whole txns, but defensive).

`finalize` stays the same — it calls `pg_replication_slot_advance(slot, last_consumed_lsn)`.

## File structure

| Path | Action |
|---|---|
| `crates/worker/src/wasm_runtime/db_pg_subscribe.rs` | Modify — add `PendingEvent` wrapper, update `poll_and_buffer` and `next` |
| `tests/integration/tests/postgres_cdc_wasm_e2e.rs` | Modify (light) — assert exact op counts now that duplication is gone |

No SDK / connector / migration changes. Single-file host fix.

## Acceptance

- `make e2e` (just postgres_cdc_wasm_e2e): ops sequence is `[s,s,s,i,u,u,d]` exactly (no `d` repetition). The `u,u` pair stays — that's the snapshot/streaming overlap window the II.3.f spec accepts.
- 138 worker lib tests pass. (Unit-test budget: 1 new test for the per-event commit tracking — feed a synthetic peek buffer through `poll_and_buffer` and assert pending entries carry the right `txn_commit_lsn`.)
- `mysql_cdc_wasm_e2e` and other curated tests remain green (no MySQL or non-CDC code touched).

## Open concerns

1. **CdcEvent::Begin / CdcEvent::Commit shape.** The native pgoutput decoder emits these with `final_lsn` / `commit_lsn` / `end_lsn` fields. The host needs `commit_lsn` (or `end_lsn`, whichever is the position of the Commit message itself). Inspecting `crates/worker/src/connectors/postgres/cdc/decode.rs` confirms which.

2. **Begin without Commit at end of peek buffer.** Should not happen with `peek_binary_changes` (always returns complete txns), but if it does, the trailing data events stay with `txn_commit_lsn = None` and use the fallback. Net effect: those events replay on the next subscription, same as today's behavior. Safe.

3. **Truncate events.** Treated like Insert/Update/Delete (have a transaction context) but currently filtered out by the host. We extend the same Begin/Commit tracking — irrelevant since pending pushes nothing for Truncate, but `txn_start_index` still resets correctly.
