# Phase 2.3.k: Postgres CDC `AdvanceSlot` Activity

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `advance_slot` Temporal activity that calls `pg_replication_slot_advance(slot_name, target_lsn)` to release WAL the destination has durably persisted. Without it, Postgres retains all WAL since slot creation, exhausting disk on long-running CDC pipelines. RFC-8 §"Slot lifecycle" names `AdvanceSlotActivity` as required; the May-13 and May-21 audits flag it as MISSING.

**Context (what already exists):** Phase 2.3.j.1 (merged at `40a7c16`) added commit-LSN advancement *inside* the streaming runtime (`PgSubscription::finalize` in `db_pg_subscribe.rs` calls `pg_replication_slot_advance` on `close_stream`). That path is opportunistic — cheap, no Temporal involvement, fires per WASM connector read-batch. This plan adds the *durable* path: a proper Temporal activity called from the workflow's load-then-cursor-commit loop so that Postgres WAL is explicitly released even when a connector restarts or the workflow does `continue-as-new`. The two paths are complementary, not redundant.

**Architecture:**
- New free function `advance_slot(conn_url, slot, target_lsn) -> anyhow::Result<String>` in `crates/worker/src/connectors/postgres/cdc/slot.rs` — mirrors the existing `ensure_slot`, `release_slot`, `slot_lag_bytes` pattern.
- New `AdvanceSlotInput` / `AdvanceSlotOutput` structs in `crates/worker/src/activities/cdc/inputs.rs`.
- New `#[activity] advance_slot` method on `CdcActivities` in `crates/worker/src/activities/cdc/mod.rs`.
- Workflow call site in `crates/worker/src/workflows/wasm_cdc_pipeline.rs`: after `commit_cursor` succeeds, when the new cursor has kind `CursorKind::Lsn`, call `advance_slot` with the LSN. Failure is non-fatal — log and continue.
- The slot name is derived from `pipeline_id` using the established formula `format!("etl_{}", pipeline_id.as_simple())`, the same formula used in `ensure_slot` (mod.rs:35) and the catalog. No new field on `WasmCdcPipelineInput`.

**Tech Stack:** Rust · `sqlx 0.8` (PgConnection) · `tokio-postgres` is NOT used (existing slot.rs uses sqlx) · docker-compose `postgres:16` with `wal_level=logical`.

**Scope cuts (deferred, not in this plan):**
- Per-tenant rate limiting on slot advance (every N batches).
- Multi-slot pipelines — MVP is one slot per pipeline.
- MySQL GTID advance (RFC-8 covers it separately; no WAL-retention problem at this layer).
- Slot-advance for `CdcPipelineWorkflow` (the non-WASM path) — follow-up once WASM path is proven.

---

## File Structure

**Modify:**
- `crates/worker/src/activities/cdc/inputs.rs` — add `AdvanceSlotInput` and `AdvanceSlotOutput`.
- `crates/worker/src/activities/cdc/mod.rs` — add `advance_slot` activity (stub in Task 3, filled in Task 4).
- `crates/worker/src/connectors/postgres/cdc/slot.rs` — add `advance_slot` free function (Task 2).
- `crates/worker/src/workflows/wasm_cdc_pipeline.rs` — call `advance_slot` after `commit_cursor` (Task 6).

**Create:**
- `tests/integration/tests/pg_cdc_advance_slot.rs` — integration tests against docker `postgres:16` (Task 5).
- `docs/superpowers/specs/2026-05-21-phase-2-3k-pg-cdc-advance-slot-design.md` — design memo (Task 7).

**No changes to:**
- `crates/worker/src/main.rs` — `CdcActivities` is already registered; adding an `#[activity]` method to the `#[activities]` impl block is automatically picked up by the `temporalio_macros::activities` derive.
- `docker-compose.yml` — `wal_level=logical` and `max_replication_slots=10` already set.
- MySQL, MongoDB, or other connector paths.

---

## Task 1: Define `AdvanceSlotInput` and `AdvanceSlotOutput`

**Files:**
- Modify: `crates/worker/src/activities/cdc/inputs.rs`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module at the bottom of `crates/worker/src/activities/cdc/inputs.rs` (create the module if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_slot_input_roundtrips_serde() {
        let input = AdvanceSlotInput {
            pipeline_id: uuid::Uuid::nil(),
            tenant_id: uuid::Uuid::nil(),
            principal_id: uuid::Uuid::nil(),
            jti: uuid::Uuid::nil(),
            source_conn: common_types::connection_config::ConnectionConfig::from_url(
                "postgres://localhost/test".into(),
            ),
            slot_name: "etl_abc".into(),
            target_lsn: "0/1A2B3C4".into(),
        };
        let j = serde_json::to_string(&input).unwrap();
        let back: AdvanceSlotInput = serde_json::from_str(&j).unwrap();
        assert_eq!(back.slot_name, "etl_abc");
        assert_eq!(back.target_lsn, "0/1A2B3C4");
    }

    #[test]
    fn advance_slot_output_roundtrips_serde() {
        let out = AdvanceSlotOutput {
            confirmed_flush_lsn: "0/1A2B3C4".into(),
        };
        let j = serde_json::to_string(&out).unwrap();
        let back: AdvanceSlotOutput = serde_json::from_str(&j).unwrap();
        assert_eq!(back.confirmed_flush_lsn, "0/1A2B3C4");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```
cargo test -p worker activities::cdc::inputs::tests
```

Expected: compile error — `AdvanceSlotInput` and `AdvanceSlotOutput` are not defined.

- [ ] **Step 3: Add the structs**

Append to `crates/worker/src/activities/cdc/inputs.rs` after the `CdcSnapshotMarkCompletedInput` block (currently the final struct, ending at line 107):

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvanceSlotInput {
    pub pipeline_id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    #[serde(default)]
    pub principal_id: uuid::Uuid,
    #[serde(default)]
    pub jti: uuid::Uuid,
    pub source_conn: ConnectionConfig,
    /// Logical replication slot name to advance. Callers derive this as
    /// `format!("etl_{}", pipeline_id.as_simple())` — the same formula used
    /// in `ensure_slot` (activities/cdc/mod.rs:35).
    pub slot_name: String,
    /// Target LSN in Postgres text format, e.g. `"0/1A2B3C4"`. The source
    /// side has durably persisted everything up to and including this position.
    pub target_lsn: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvanceSlotOutput {
    /// The slot's `confirmed_flush_lsn` after the advance, as reported by
    /// `pg_replication_slots`. Equals `target_lsn` on first advance; on a
    /// re-delivery (idempotent replay) it may already be ≥ `target_lsn`.
    pub confirmed_flush_lsn: String,
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```
cargo test -p worker activities::cdc::inputs::tests
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/cdc/inputs.rs
git commit -m "phase-2-3k-1: AdvanceSlotInput/AdvanceSlotOutput in CDC inputs"
```

---

## Task 2: `advance_slot` free function in `slot.rs`

**Files:**
- Modify: `crates/worker/src/connectors/postgres/cdc/slot.rs`

- [ ] **Step 1: Write the failing unit test**

Add to `crates/worker/src/connectors/postgres/cdc/slot.rs` (create the test module — none exists today):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // A live-database test lives in tests/integration/tests/pg_cdc_advance_slot.rs.
    // This module guards the pure-logic validation: confirm that passing an
    // LSN that looks wrong returns an error from the Postgres parser — but
    // that requires a live connection. Instead we verify the query string is
    // formed correctly by confirming the function signature compiles and that
    // calling it with a clearly invalid URL fails immediately on connect.
    #[tokio::test]
    async fn advance_slot_fails_fast_on_bad_url() {
        let err = advance_slot(
            "postgres://127.0.0.1:1/noexist",
            "etl_test_slot",
            "0/1A2B3C4",
        )
        .await
        .unwrap_err();
        let msg = format!("{err}");
        // Could be "connection refused" or "timeout" depending on OS.
        // The important thing is the function returns an error, not panics.
        assert!(!msg.is_empty(), "expected a non-empty error: {msg}");
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

```
cargo test -p worker connectors::postgres::cdc::slot::tests::advance_slot_fails_fast_on_bad_url
```

Expected: compile error — `advance_slot` not defined.

- [ ] **Step 3: Implement `advance_slot` in slot.rs**

Append after the `slot_lag_bytes` function (current end of file, line 88):

```rust
/// Advance the logical replication slot to `target_lsn`, releasing WAL the
/// consumer has durably committed. Returns the slot's `confirmed_flush_lsn`
/// after the advance.
///
/// Idempotent: `pg_replication_slot_advance` is a no-op when
/// `target_lsn` ≤ the current `confirmed_flush_lsn` (PG 11+).
pub async fn advance_slot(
    conn_url: &str,
    slot: &str,
    target_lsn: &str,
) -> anyhow::Result<String> {
    let mut c = PgConnection::connect(conn_url).await?;
    sqlx::query("SELECT pg_replication_slot_advance($1, $2::pg_lsn)")
        .bind(slot)
        .bind(target_lsn)
        .execute(&mut c)
        .await
        .context("pg_replication_slot_advance")?;
    let (confirmed,): (Option<String>,) = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_one(&mut c)
    .await
    .context("re-query confirmed_flush_lsn after advance")?;
    Ok(confirmed.unwrap_or_else(|| target_lsn.to_string()))
}
```

- [ ] **Step 4: Run test to confirm it passes**

```
cargo test -p worker connectors::postgres::cdc::slot::tests::advance_slot_fails_fast_on_bad_url
```

Expected: 1 test passes (connect to port 1 fails fast).

- [ ] **Step 5: Run the full worker unit tests to confirm no regressions**

```
cargo test -p worker
```

Expected: all existing tests pass; 1 new test added.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/connectors/postgres/cdc/slot.rs
git commit -m "phase-2-3k-2: advance_slot free function in slot.rs"
```

---

## Task 3: Stub `advance_slot` activity returning `unimplemented`

**Files:**
- Modify: `crates/worker/src/activities/cdc/mod.rs`

The stub makes the activity discoverable by Temporal's registration scan, lets the workflow call site in Task 6 compile, and gives a clear failure mode before Task 4 fills in the body.

- [ ] **Step 1: Write the failing test**

Add to the inline `#[cfg(test)]` module of `crates/worker/src/activities/cdc/mod.rs` (create one at the bottom if it doesn't exist):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_slot_input_type_accessible() {
        // Confirm AdvanceSlotInput is importable from crate root.
        // (Compile-only guard — if the activity stub doesn't compile
        //  because of missing imports, this test catches it.)
        let _: fn(inputs::AdvanceSlotInput) = |i| {
            let _ = i.slot_name;
            let _ = i.target_lsn;
        };
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

```
cargo test -p worker activities::cdc::tests::advance_slot_input_type_accessible
```

Expected: compile error — `AdvanceSlotInput` not re-exported from `inputs`, or activity method not present yet. (The `inputs` module is `pub mod inputs` already; the test just confirms the field names imported correctly.)

- [ ] **Step 3: Add the stub activity method**

In `crates/worker/src/activities/cdc/mod.rs`, add the following method inside the `#[activities] impl CdcActivities` block, after the `release_slot` method (currently the last method, ending at line 336), and before the closing `}`:

```rust
    #[activity]
    pub async fn advance_slot(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: AdvanceSlotInput,
    ) -> Result<AdvanceSlotOutput, ActivityError> {
        tracing::info!(
            slot = %input.slot_name,
            target_lsn = %input.target_lsn,
            "cdc: advance_slot entering (stub — not yet implemented)"
        );
        Err(anyhow::anyhow!("advance_slot: not yet implemented").into())
    }
```

Also add `AdvanceSlotInput` and `AdvanceSlotOutput` to the `use inputs::*;` line that already exists at the top of the file (line 6). That wildcard import covers them automatically — no change needed.

- [ ] **Step 4: Run test to confirm it passes**

```
cargo test -p worker activities::cdc::tests::advance_slot_input_type_accessible
```

Expected: 1 test passes.

- [ ] **Step 5: Run the full worker tests**

```
cargo test -p worker
```

Expected: all tests pass. The stub activity returns an error if called, but no test calls it yet.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/activities/cdc/mod.rs
git commit -m "phase-2-3k-3: stub advance_slot activity on CdcActivities"
```

---

## Task 4: Implement the `advance_slot` activity body

**Files:**
- Modify: `crates/worker/src/activities/cdc/mod.rs`

Replace the stub body with the real implementation.

- [ ] **Step 1: Write the failing unit test (connection-error path)**

Add to the `#[cfg(test)]` module in `crates/worker/src/activities/cdc/mod.rs`:

```rust
    // NOTE: This test is unit-only (no Temporal, no live DB). It constructs
    // a CdcActivities with a fake catalog URL and confirms the activity
    // returns a retryable error (not a panic) when the PG connection fails.
    //
    // The live-DB correctness tests live in
    // tests/integration/tests/pg_cdc_advance_slot.rs (Task 5).
    #[tokio::test]
    async fn advance_slot_returns_error_on_bad_connection() {
        use std::sync::Arc;
        // We can't easily construct CdcActivities (needs a live catalog).
        // Instead, test the inner slot::advance_slot function directly,
        // confirming the activity plumbing will propagate errors correctly.
        let result = crate::connectors::postgres::cdc::slot::advance_slot(
            "postgres://127.0.0.1:1/noexist",
            "etl_test",
            "0/1",
        )
        .await;
        assert!(result.is_err(), "expected error on bad connection");
    }
```

- [ ] **Step 2: Run test to confirm it fails**

```
cargo test -p worker activities::cdc::tests::advance_slot_returns_error_on_bad_connection
```

Expected: test already passes if `slot::advance_slot` from Task 2 is in place. (If it passes immediately, that's expected — it's confirming existing behavior before wiring it into the activity body.)

- [ ] **Step 3: Replace stub body with real implementation**

In `crates/worker/src/activities/cdc/mod.rs`, replace the stub body of `advance_slot` with:

```rust
    #[activity]
    pub async fn advance_slot(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: AdvanceSlotInput,
    ) -> Result<AdvanceSlotOutput, ActivityError> {
        tracing::info!(
            slot = %input.slot_name,
            target_lsn = %input.target_lsn,
            "cdc: advance_slot entering"
        );
        let resolve_ctx = crate::secrets::auditing::ResolveContext {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            principal_id: (!input.principal_id.is_nil())
                .then(|| common_types::ids::PrincipalId::from_uuid_unchecked(input.principal_id)),
            jti: (!input.jti.is_nil()).then_some(input.jti),
        };
        let resolved = crate::secrets::resolve_connection_audited(
            self.secrets.as_ref(),
            &input.source_conn,
            resolve_ctx,
        )
        .await
        .map_err(retryable)?;
        let confirmed = slot::advance_slot(
            resolved.expect_url(),
            &input.slot_name,
            &input.target_lsn,
        )
        .await
        .map_err(retryable)?;
        // Persist the confirmed flush LSN in the catalog so the CDC monitor
        // and future restarts have an accurate view.
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        let tid = common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id);
        let ctx = common_types::ids::TenantContext::new(tid);
        self.catalog
            .cdc_update_confirmed_flush(ctx, pid, &confirmed)
            .await
            .map_err(|e| retryable(anyhow::anyhow!(e)))?;
        tracing::info!(
            slot = %input.slot_name,
            confirmed_flush_lsn = %confirmed,
            "cdc: advance_slot complete"
        );
        Ok(AdvanceSlotOutput { confirmed_flush_lsn: confirmed })
    }
```

- [ ] **Step 4: Run full worker tests**

```
cargo test -p worker
```

Expected: all tests pass including the unit test from Step 2.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/cdc/mod.rs
git commit -m "phase-2-3k-4: implement advance_slot activity body"
```

---

## Task 5: Integration tests against docker-compose postgres

**Files:**
- Create: `tests/integration/tests/pg_cdc_advance_slot.rs`

These tests call `slot::advance_slot` (the free function from Task 2) directly — no Temporal worker needed, no `ActivityContext` mock required. The slot is created and torn down within each test.

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/tests/pg_cdc_advance_slot.rs`:

```rust
//! Integration tests for pg_replication_slot_advance via the slot.rs helper.
//!
//! Requires the docker-compose `postgres` service (postgres:16, wal_level=logical).
//! Skipped with a message if the database is unreachable.

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use worker::connectors::postgres::cdc::slot;

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

/// Connect or return None (causes the test to skip).
async fn connect() -> Option<sqlx::PgPool> {
    let url = test_url();
    match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("SKIP pg_cdc_advance_slot: cannot reach {url}: {e}");
            None
        }
    }
}

/// Unique slot name per test run to prevent cross-test interference.
fn slot_name() -> String {
    format!("etl_advance_test_{}", uuid::Uuid::new_v4().simple())
}

async fn create_slot(pool: &sqlx::PgPool, name: &str) {
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(name)
        .execute(pool)
        .await
        .expect("create test slot");
}

async fn drop_slot(pool: &sqlx::PgPool, name: &str) {
    let _ = sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(name)
        .execute(pool)
        .await;
}

async fn current_wal_lsn(pool: &sqlx::PgPool) -> String {
    let (lsn,): (String,) =
        sqlx::query_as("SELECT pg_current_wal_lsn()::text")
            .fetch_one(pool)
            .await
            .expect("pg_current_wal_lsn");
    lsn
}

async fn confirmed_flush(pool: &sqlx::PgPool, name: &str) -> Option<String> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .expect("query confirmed_flush_lsn");
    row.and_then(|(lsn,)| lsn)
}

#[tokio::test]
async fn advance_slot_moves_confirmed_flush_lsn() {
    let Some(pool) = connect().await else { return };
    let sname = slot_name();
    create_slot(&pool, &sname).await;

    // Generate WAL: insert into a scratch table.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _etl_advance_test_scratch (id bigserial primary key, v text)",
    )
    .execute(&pool)
    .await
    .expect("create scratch");
    sqlx::query("INSERT INTO _etl_advance_test_scratch (v) VALUES ('x')")
        .execute(&pool)
        .await
        .expect("insert");

    // Record a WAL position AFTER the insert so we have something to advance to.
    let target = current_wal_lsn(&pool).await;

    let url = test_url();
    let confirmed = slot::advance_slot(&url, &sname, &target)
        .await
        .expect("advance_slot should succeed");

    // confirmed_flush_lsn must be >= target after the advance.
    // We use pg_lsn comparison via Postgres itself to avoid parsing LSNs in Rust.
    let (advanced,): (bool,) = sqlx::query_as(
        "SELECT $1::pg_lsn >= $2::pg_lsn",
    )
    .bind(&confirmed)
    .bind(&target)
    .fetch_one(&pool)
    .await
    .expect("lsn compare");
    assert!(advanced, "confirmed_flush_lsn {confirmed} should be >= target {target}");

    // confirm the catalog-side view via pg_replication_slots agrees.
    let slot_confirmed = confirmed_flush(&pool, &sname).await;
    assert!(slot_confirmed.is_some(), "slot should still exist after advance");

    drop_slot(&pool, &sname).await;
}

#[tokio::test]
async fn advance_slot_is_idempotent() {
    let Some(pool) = connect().await else { return };
    let sname = slot_name();
    create_slot(&pool, &sname).await;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _etl_advance_test_scratch (id bigserial primary key, v text)",
    )
    .execute(&pool)
    .await
    .expect("create scratch");
    sqlx::query("INSERT INTO _etl_advance_test_scratch (v) VALUES ('y')")
        .execute(&pool)
        .await
        .expect("insert");

    let target = current_wal_lsn(&pool).await;
    let url = test_url();

    let first = slot::advance_slot(&url, &sname, &target)
        .await
        .expect("first advance");
    // Second call with the same target must not error (PG no-ops it).
    let second = slot::advance_slot(&url, &sname, &target)
        .await
        .expect("second advance (idempotent)");

    assert_eq!(first, second, "idempotent calls must return same lsn");
    drop_slot(&pool, &sname).await;
}

#[tokio::test]
async fn advance_slot_with_older_lsn_is_a_noop() {
    let Some(pool) = connect().await else { return };
    let sname = slot_name();
    create_slot(&pool, &sname).await;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _etl_advance_test_scratch (id bigserial primary key, v text)",
    )
    .execute(&pool)
    .await
    .expect("create scratch");
    sqlx::query("INSERT INTO _etl_advance_test_scratch (v) VALUES ('z')")
        .execute(&pool)
        .await
        .expect("insert");

    let first_target = current_wal_lsn(&pool).await;
    let url = test_url();

    // Advance to current position.
    let after_first = slot::advance_slot(&url, &sname, &first_target)
        .await
        .expect("first advance");

    // Attempt to advance backwards (to "0/1"). PG should no-op, not error.
    let after_old = slot::advance_slot(&url, &sname, "0/1")
        .await
        .expect("advance to older lsn must not error");

    // confirmed_flush_lsn should not regress.
    let (no_regression,): (bool,) = sqlx::query_as(
        "SELECT $1::pg_lsn >= $2::pg_lsn",
    )
    .bind(&after_old)
    .bind(&after_first)
    .fetch_one(&pool)
    .await
    .expect("lsn compare");
    assert!(no_regression, "lsn must not regress: after_first={after_first} after_old={after_old}");

    drop_slot(&pool, &sname).await;
}

#[tokio::test]
async fn advance_slot_errors_on_nonexistent_slot() {
    let Some(pool) = connect().await else { return };
    let _ = pool; // connection check only
    let url = test_url();
    let err = slot::advance_slot(&url, "etl_does_not_exist_xyz", "0/1")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("pg_replication_slot_advance") || msg.contains("slot") || msg.contains("exist"),
        "unexpected error message: {err}"
    );
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```
docker compose up -d postgres
cargo test -p integration-tests --test pg_cdc_advance_slot -- --nocapture --test-threads=1
```

Expected: compile error — `worker::connectors::postgres::cdc::slot` module path not re-exported, or `advance_slot` not `pub`. (We'll verify the pub path in Step 3.)

- [ ] **Step 3: Confirm the re-export path and fix if needed**

The `slot` module is declared as `pub mod slot` in `crates/worker/src/connectors/postgres/cdc/mod.rs`. Verify:

```
cargo test -p integration-tests --test pg_cdc_advance_slot -- --list
```

If the compile error is "module not found," check `crates/worker/src/connectors/postgres/cdc/mod.rs` for the `pub mod slot` declaration. It is already `pub` (the existing CDC activities import it via `use crate::connectors::postgres::cdc::{slot, snapshot, stream}`). The integration test accesses it as `worker::connectors::postgres::cdc::slot::advance_slot` — this requires that each module in the chain is `pub`. Confirm with:

```
grep -n "pub mod" crates/worker/src/connectors/postgres/cdc/mod.rs
grep -n "pub mod" crates/worker/src/connectors/postgres/mod.rs
grep -n "pub mod" crates/worker/src/connectors/mod.rs
```

If any intermediate module is not `pub`, add `pub` to the `mod` declaration (these are all internal plumbing modules — making them `pub` is safe within the worker crate).

- [ ] **Step 4: Run tests until green**

```
cargo test -p integration-tests --test pg_cdc_advance_slot -- --test-threads=1
```

Expected: 4 tests pass:
- `advance_slot_moves_confirmed_flush_lsn` — confirmed_flush_lsn advances to target.
- `advance_slot_is_idempotent` — two identical calls return the same LSN, no error.
- `advance_slot_with_older_lsn_is_a_noop` — LSN does not regress.
- `advance_slot_errors_on_nonexistent_slot` — returns an error containing a descriptive message.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/tests/pg_cdc_advance_slot.rs
git commit -m "phase-2-3k-5: integration tests for advance_slot against docker postgres"
```

---

## Task 6: Wire `advance_slot` into `WasmCdcPipelineWorkflow`

**Files:**
- Modify: `crates/worker/src/workflows/wasm_cdc_pipeline.rs`

Call `advance_slot` after `commit_cursor` succeeds, when the new cursor has kind `CursorKind::Lsn`. Failures are non-fatal: the workflow logs the error and continues. The slot will advance again on the next successful batch.

- [ ] **Step 1: Write a compile-guard unit test**

Add to the inline `#[cfg(test)]` module of `wasm_cdc_pipeline.rs` (create one at the bottom if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_slot_lsn_condition_compiles() {
        // Confirm the CursorKind::Lsn variant is accessible from this module.
        let kind = common_types::cursor::CursorKind::Lsn;
        assert_eq!(format!("{kind:?}"), "Lsn");
    }
}
```

- [ ] **Step 2: Run test to confirm it passes**

```
cargo test -p worker workflows::wasm_cdc_pipeline::tests::advance_slot_lsn_condition_compiles
```

Expected: 1 test passes. (This is a smoke test that `CursorKind::Lsn` is visible from the workflow module — it verifies the import path before we add real code that uses it.)

- [ ] **Step 3: Add the import and call site**

In `crates/worker/src/workflows/wasm_cdc_pipeline.rs`:

a) Extend the existing `use` block at the top to add the CDC activity import:

```rust
use crate::activities::cdc::CdcActivities;
use crate::activities::cdc::inputs::AdvanceSlotInput;
```

Add these two lines after the existing `use crate::activities::sync::inputs::{...};` block (currently lines 28-31).

b) Replace the `commit_cursor` call site and the lines that follow it (currently lines 237-252 — the `commit_cursor` activity call through `cursor = read_out.new_cursor`) with:

```rust
            let commit_stream = batch_stream.clone();
            ctx.start_activity(
                SyncActivities::commit_cursor,
                CommitCursorInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                    run_id: input.run_id,
                    stream_name: commit_stream,
                    cursor: read_out.new_cursor.clone(),
                },
                opts_short(),
            )
            .await?;

            // Post-commit slot advance: release WAL the destination has
            // durably persisted. Only meaningful for LSN-typed cursors
            // (Postgres logical replication). Errors here are non-fatal —
            // log and continue; the slot will advance on the next batch.
            // This is the workflow-durable complement to the opportunistic
            // advance inside PgSubscription::finalize (db_pg_subscribe.rs).
            if let Some(ref cv) = read_out.new_cursor {
                if cv.kind == common_types::cursor::CursorKind::Lsn {
                    let slot_name =
                        format!("etl_{}", input.pipeline_id.simple());
                    let advance_result = ctx
                        .start_activity(
                            CdcActivities::advance_slot,
                            AdvanceSlotInput {
                                pipeline_id: input.pipeline_id,
                                tenant_id: input.tenant_id,
                                principal_id: input.principal_id,
                                jti: input.jti,
                                source_conn: input.source_connection.clone(),
                                slot_name,
                                target_lsn: cv.value.clone(),
                            },
                            opts_short(),
                        )
                        .await;
                    if let Err(ref e) = advance_result {
                        tracing::warn!(
                            target: "workflow.wasm_cdc_pipeline",
                            pipeline_id = %input.pipeline_id,
                            error = %e,
                            "advance_slot failed (non-fatal, will retry on next batch)"
                        );
                    }
                }
            }

            cursor = read_out.new_cursor;
            batch_seq += 1;
            window_seq += 1;
```

Note: `input.pipeline_id` is a `Uuid`; calling `.simple()` on `Uuid` returns a `SimpleRef` which implements `Display` as lowercase hex without hyphens — the same format that `ensure_slot` (mod.rs:35) produces with `pipeline_id.as_simple()`. Use the appropriate method: if `input.pipeline_id` is `uuid::Uuid`, use `input.pipeline_id.as_simple()`. Check the exact type:

```
grep -n "pipeline_id" crates/worker/src/workflows/wasm_cdc_pipeline.rs | head -5
```

`WasmCdcPipelineInput::pipeline_id` is `Uuid` (line 42 of the workflow file). `Uuid::as_simple()` returns `Simple` which displays as 32 lowercase hex chars — exactly matching `ensure_slot`'s formula.

- [ ] **Step 4: Run the full worker tests**

```
cargo test -p worker
```

Expected: all tests pass. The new advance_slot call site compiles; the test added in Step 2 passes.

- [ ] **Step 5: Run an end-to-end smoke check (optional, requires Temporal)**

If the docker-compose stack (postgres + temporal) is running and a CDC pipeline test is available:

```
docker compose up -d postgres temporal
cargo test -p integration-tests --test postgres_cdc_wasm_e2e -- --nocapture
```

Expected: existing e2e tests remain green; log output should show `advance_slot entering` and `advance_slot complete` lines after each `commit_cursor`.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/workflows/wasm_cdc_pipeline.rs
git commit -m "phase-2-3k-6: call advance_slot after commit_cursor in WasmCdcPipelineWorkflow"
```

---

## Task 7: Design memo

**Files:**
- Create: `docs/superpowers/specs/2026-05-21-phase-2-3k-pg-cdc-advance-slot-design.md`

- [ ] **Step 1: Write the design memo**

Create `docs/superpowers/specs/2026-05-21-phase-2-3k-pg-cdc-advance-slot-design.md`:

```markdown
