# Phase II.3.d.6 — Snapshot Resume via Catalog Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make CDC snapshot progress survive worker crashes and workflow failures. Today both Postgres and MySQL CDC keep `last_pk` in workflow state — a failed run starts snapshot from scratch. This phase persists `last_pk` + captured position to the catalog so a re-run picks up where the previous run left off.

**Architecture:** New `cdc_snapshots` catalog table keyed by `(pipeline_id)`. Both Postgres and MySQL CDC snapshot activities upsert state after each chunk; both workflows query the table once at snapshot loop start to seed `last_pk` and skip the loop entirely if a prior run completed. The MySQL `captured_gtid` is also persisted so resume reuses the original position rather than re-capturing (which would lose data during the failure window).

**Tech Stack:** sqlx (existing), Postgres catalog DB (existing). No new deps.

---

## File Map

- **`crates/catalog/migrations/0015_cdc_snapshots.sql`** *(new)* — `cdc_snapshots` table.
- **`crates/catalog/src/cdc_snapshot.rs`** *(new)* — `CdcSnapshotState` struct + `upsert` / `get` / `mark_completed` functions. Mirrors the shape of `cdc_slots`.
- **`crates/catalog/src/lib.rs`** — Export `cdc_snapshot` module + add `Catalog::cdc_snapshot_upsert`/`get`/`mark_completed` async methods.
- **`crates/worker/src/activities/cdc/mod.rs`** — Postgres CDC `snapshot_chunk` activity persists state after each chunk write. New `cdc_snapshot_state_get` activity reads state at workflow start.
- **`crates/worker/src/workflows/cdc_pipeline.rs`** — Postgres CDC workflow seeds `last_pk` from persisted state and skips snapshot loop if completed.
- **`crates/worker/src/activities/mysql_cdc/mod.rs`** — MySQL CDC `mysql_snapshot_chunk` persists state after each chunk. New `mysql_cdc_snapshot_state_get` activity reads state at workflow start. (Or share the Postgres activity — see Task 4 for the call.)
- **`crates/worker/src/workflows/mysql_cdc_pipeline.rs`** — MySQL CDC workflow reads persisted state, uses persisted `captured_gtid` if present (skipping the `capture_start_gtid` activity on resume), seeds `last_pk`, skips loop if completed.
- **`tests/integration/tests/cdc_snapshot_streaming_handoff.rs`** — Add a "resume" assertion: re-running the same pipeline_id after first run completes shows snapshot is skipped (no new 's' rows produced on re-run).

---

## Task 1: Catalog migration + `cdc_snapshot` module

**Files:**
- Create: `crates/catalog/migrations/0015_cdc_snapshots.sql`
- Create: `crates/catalog/src/cdc_snapshot.rs`
- Modify: `crates/catalog/src/lib.rs`

- [ ] **Step 1: Add the migration**

Create `crates/catalog/migrations/0015_cdc_snapshots.sql`:

```sql
-- 0015_cdc_snapshots.sql — per-pipeline snapshot progress tracking
-- Survives worker crashes and workflow failures so re-runs of the
-- same pipeline pick up where the previous run left off.
CREATE TABLE IF NOT EXISTS cdc_snapshots (
    pipeline_id        UUID PRIMARY KEY
                       REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    tenant_id          UUID        NOT NULL
                       REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    last_pk            BIGINT,
    completed          BOOLEAN     NOT NULL DEFAULT false,
    -- Captured GTID (MySQL) or LSN (Postgres). For Postgres CDC the
    -- consistent_point is also stored in cdc_slots; this column
    -- duplicates it so workflows have a single place to read snapshot
    -- state without joining tables.
    captured_position  TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cdc_snapshots_completed_idx
    ON cdc_snapshots(completed) WHERE completed = false;
```

- [ ] **Step 2: Add the catalog module**

Create `crates/catalog/src/cdc_snapshot.rs`:

```rust
use common_types::ids::{PipelineId, TenantId};

#[derive(Debug, Clone)]
pub struct CdcSnapshotState {
    pub pipeline_id: PipelineId,
    pub tenant_id: TenantId,
    pub last_pk: Option<i64>,
    pub completed: bool,
    pub captured_position: String,
}

/// Insert or update snapshot state for a pipeline. Used after each
/// snapshot chunk to checkpoint progress and once at completion.
pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    state: &CdcSnapshotState,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO cdc_snapshots(pipeline_id, tenant_id, last_pk, completed, captured_position, updated_at) \
         VALUES ($1,$2,$3,$4,$5, now()) \
         ON CONFLICT (pipeline_id) DO UPDATE SET \
           last_pk = EXCLUDED.last_pk, \
           completed = EXCLUDED.completed, \
           captured_position = EXCLUDED.captured_position, \
           updated_at = now()",
    )
    .bind(state.pipeline_id.as_uuid())
    .bind(state.tenant_id.as_uuid())
    .bind(state.last_pk)
    .bind(state.completed)
    .bind(&state.captured_position)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Fetch snapshot state for a pipeline. Returns None if no snapshot
/// has been started for this pipeline (typical for first run).
pub async fn get(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
) -> sqlx::Result<Option<CdcSnapshotState>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, Option<i64>, bool, String)> = sqlx::query_as(
        "SELECT pipeline_id, tenant_id, last_pk, completed, captured_position \
         FROM cdc_snapshots WHERE pipeline_id = $1",
    )
    .bind(pipeline_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(pid, tid, last_pk, completed, cp)| CdcSnapshotState {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        last_pk,
        completed,
        captured_position: cp,
    }))
}

/// Mark snapshot complete; idempotent.
pub async fn mark_completed(
    conn: &mut sqlx::PgConnection,
    pipeline_id: PipelineId,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE cdc_snapshots SET completed = true, updated_at = now() \
         WHERE pipeline_id = $1",
    )
    .bind(pipeline_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}
```

- [ ] **Step 3: Wire the module + add Catalog API methods**

In `crates/catalog/src/lib.rs`, find the existing `pub mod cdc;` line and add `pub mod cdc_snapshot;` directly after it.

Find the existing `Catalog::cdc_upsert` method. Add three new methods just below it (matching the same `TenantContext`-aware pattern):

```rust
    pub async fn cdc_snapshot_upsert(
        &self,
        ctx: TenantContext,
        state: &cdc_snapshot::CdcSnapshotState,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        cdc_snapshot::upsert(&mut tx, state).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn cdc_snapshot_get(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
    ) -> sqlx::Result<Option<cdc_snapshot::CdcSnapshotState>> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        let r = cdc_snapshot::get(&mut tx, pipeline_id).await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn cdc_snapshot_mark_completed(
        &self,
        ctx: TenantContext,
        pipeline_id: PipelineId,
    ) -> sqlx::Result<()> {
        let mut tx = self.begin_with_tenant(Some(ctx)).await?;
        cdc_snapshot::mark_completed(&mut tx, pipeline_id).await?;
        tx.commit().await?;
        Ok(())
    }
```

(`PipelineId` and `TenantContext` should already be imported at the top of `lib.rs`. If not, add them.)

- [ ] **Step 4: Add `cdc_snapshots` to the truncate list for tests**

In `crates/catalog/src/lib.rs`, find `truncate_all_for_tests`. The TRUNCATE statement is one big string; insert `cdc_snapshots` into the list after `cdc_slots`:

```rust
            "TRUNCATE audit_verified_chain, audit_log, revoked_tokens, refresh_tokens, principals, secrets, cdc_snapshots, cdc_slots, runs, stream_state, schemas, streams, pipelines, connections, workspaces, tenants CASCADE",
```

- [ ] **Step 5: Verify the migration applies + library builds**

Run: `cargo build -p catalog 2>&1 | grep -E "^error" | head -5`
Expected: empty.

The migration runs on the next `cat.migrate().await?` call. Verify by spinning up the docker stack and running an e2e test in Task 5; for now we just confirm the catalog crate compiles.

- [ ] **Step 6: Commit**

```bash
git add crates/catalog/migrations/0015_cdc_snapshots.sql crates/catalog/src/cdc_snapshot.rs crates/catalog/src/lib.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-6-1: cdc_snapshots catalog table + API

New cdc_snapshots table keyed by pipeline_id, tracking last_pk +
completed flag + captured_position (GTID for MySQL, LSN for
Postgres — duplicates cdc_slots.consistent_point for the latter).
Catalog::cdc_snapshot_{upsert,get,mark_completed} methods follow
the existing cdc_slots pattern.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Postgres CDC — persist + resume from `cdc_snapshots`

**Files:**
- Modify: `crates/worker/src/activities/cdc/mod.rs`
- Modify: `crates/worker/src/workflows/cdc_pipeline.rs`

- [ ] **Step 1: Add a snapshot-state-get activity**

In `crates/worker/src/activities/cdc/mod.rs`, near the existing `snapshot_chunk` activity, add:

```rust
    #[activity]
    pub async fn cdc_snapshot_state_get(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CdcSnapshotStateGetInput,
    ) -> Result<CdcSnapshotStateGetOutput, ActivityError> {
        let ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        let state = self
            .catalog
            .cdc_snapshot_get(ctx, pid)
            .await
            .map_err(|e| retryable(anyhow::anyhow!(e)))?;
        Ok(CdcSnapshotStateGetOutput {
            last_pk: state.as_ref().and_then(|s| s.last_pk),
            completed: state.as_ref().map(|s| s.completed).unwrap_or(false),
            captured_position: state.map(|s| s.captured_position).unwrap_or_default(),
        })
    }
```

In `crates/worker/src/activities/cdc/inputs.rs`, add the input/output structs:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSnapshotStateGetInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSnapshotStateGetOutput {
    pub last_pk: Option<i64>,
    pub completed: bool,
    pub captured_position: String,
}
```

- [ ] **Step 2: Update `snapshot_chunk` to persist state after each chunk**

In `crates/worker/src/activities/cdc/mod.rs`, find the existing `snapshot_chunk` activity. After the `CdcParquetLoader.write(...)` call (which writes the Parquet batch), add a state upsert:

```rust
        // After each chunk: persist last_pk + captured_point so a
        // crashed-and-restarted workflow resumes from here. Setting
        // completed=true is the workflow's responsibility once the loop
        // breaks (see cdc_pipeline.rs); per-chunk we only update last_pk.
        let snap_state = catalog::cdc_snapshot::CdcSnapshotState {
            pipeline_id: PipelineId::from_uuid_unchecked(input.pipeline_id),
            tenant_id: TenantId::from_uuid_unchecked(input.tenant_id),
            last_pk: chunk.last_pk,
            completed: false,
            captured_position: input.consistent_point.clone(),
        };
        let snap_ctx = common_types::ids::TenantContext::new(
            TenantId::from_uuid_unchecked(input.tenant_id),
        );
        self.catalog
            .cdc_snapshot_upsert(snap_ctx, &snap_state)
            .await
            .map_err(|e| retryable(anyhow::anyhow!(e)))?;
```

This goes after the existing `CdcParquetLoader.write(...).await.map_err(retryable)?;` line and before `Ok(SnapshotChunkOutput { ... })`.

- [ ] **Step 3: Update Postgres CDC workflow to resume from persisted state**

In `crates/worker/src/workflows/cdc_pipeline.rs`, find the snapshot loop. Before the loop starts, add a call to `cdc_snapshot_state_get` and use it to seed `last_pk`:

Find:

```rust
        let pk_col = pg
            .pk_columns
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("CDC requires at least one PK column"))?;
        let mut batch_seq: u32 = 0;
        let mut last_pk: Option<i64> = None;
        loop {
```

Replace with:

```rust
        let pk_col = pg
            .pk_columns
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("CDC requires at least one PK column"))?;

        // Resume snapshot from persisted state if a prior run got partway.
        let snap_state = ctx
            .start_activity(
                CdcActivities::cdc_snapshot_state_get,
                CdcSnapshotStateGetInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                },
                opts_short(),
            )
            .await?;
        let mut batch_seq: u32 = 0;
        let mut last_pk: Option<i64> = snap_state.last_pk;
        // Skip snapshot entirely if a prior run completed it.
        if !snap_state.completed {
            loop {
```

Then find the end of the snapshot loop (where it currently breaks on `is_final`) and close the new `if !snap_state.completed` block. After the existing loop's closing `}`, before the streaming-loop start, add:

```rust
            // Loop exited (is_final == true). Mark snapshot complete in
            // catalog so future re-runs of this pipeline skip the loop.
            ctx.start_activity(
                CdcActivities::cdc_snapshot_mark_completed,
                CdcSnapshotMarkCompletedInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                },
                opts_short(),
            )
            .await?;
        }
```

(That closes the `if !snap_state.completed` introduced above.)

- [ ] **Step 4: Add `cdc_snapshot_mark_completed` activity + input**

Back in `crates/worker/src/activities/cdc/mod.rs`, add a third activity:

```rust
    #[activity]
    pub async fn cdc_snapshot_mark_completed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CdcSnapshotMarkCompletedInput,
    ) -> Result<(), ActivityError> {
        let ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        self.catalog
            .cdc_snapshot_mark_completed(ctx, pid)
            .await
            .map_err(|e| retryable(anyhow::anyhow!(e)))?;
        Ok(())
    }
```

In `crates/worker/src/activities/cdc/inputs.rs`, add:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSnapshotMarkCompletedInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
}
```

- [ ] **Step 5: Verify build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add crates/worker/src/activities/cdc crates/worker/src/workflows/cdc_pipeline.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-6-2: Postgres CDC — persist+resume snapshot via cdc_snapshots

snapshot_chunk activity now upserts cdc_snapshots state (last_pk +
captured_position) after each chunk write. Two new activities,
cdc_snapshot_state_get and cdc_snapshot_mark_completed, plumb the
read+complete sides.

CdcPipelineWorkflow reads persisted state at snapshot loop start:
- If completed=true, skips the loop entirely (re-running a pipeline
  whose snapshot already finished doesn't re-snapshot).
- Otherwise seeds last_pk from the persisted value so a crashed
  run resumes from where it left off.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: MySQL CDC — persist + resume (with captured_gtid handling)

**Files:**
- Modify: `crates/worker/src/activities/mysql_cdc/mod.rs`
- Modify: `crates/worker/src/activities/mysql_cdc/inputs.rs`
- Modify: `crates/worker/src/workflows/mysql_cdc_pipeline.rs`

MySQL CDC has an extra wrinkle: `captured_gtid` is captured fresh each run by `capture_start_gtid`. On resume, we MUST reuse the persisted `captured_gtid` — re-capturing would skip past the writes that happened during the failure window, losing data.

- [ ] **Step 1: Add the three snapshot-state activities to MySQL CDC**

In `crates/worker/src/activities/mysql_cdc/mod.rs`, add three new activities mirroring the Postgres CDC ones from Task 2:

```rust
    #[activity]
    pub async fn mysql_snapshot_state_get(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlSnapshotStateGetInput,
    ) -> Result<MysqlSnapshotStateGetOutput, ActivityError> {
        let ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        let state = self
            .catalog
            .cdc_snapshot_get(ctx, pid)
            .await
            .map_err(|e| into_activity_err(anyhow::anyhow!(e)))?;
        Ok(MysqlSnapshotStateGetOutput {
            last_pk: state.as_ref().and_then(|s| s.last_pk),
            completed: state.as_ref().map(|s| s.completed).unwrap_or(false),
            captured_gtid: state.map(|s| s.captured_position).unwrap_or_default(),
        })
    }

    #[activity]
    pub async fn mysql_snapshot_mark_completed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MysqlSnapshotMarkCompletedInput,
    ) -> Result<(), ActivityError> {
        let ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        let pid = common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id);
        self.catalog
            .cdc_snapshot_mark_completed(ctx, pid)
            .await
            .map_err(|e| into_activity_err(anyhow::anyhow!(e)))?;
        Ok(())
    }
```

In `crates/worker/src/activities/mysql_cdc/inputs.rs`, add:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotStateGetInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotStateGetOutput {
    pub last_pk: Option<i64>,
    pub completed: bool,
    pub captured_gtid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotMarkCompletedInput {
    pub pipeline_id: Uuid,
    pub tenant_id: Uuid,
}
```

- [ ] **Step 2: Update `mysql_snapshot_chunk` to persist state after each chunk**

In `crates/worker/src/activities/mysql_cdc/mod.rs`, find the existing `mysql_snapshot_chunk` activity. After the `CdcParquetLoader.write(...)` call (or after the `if let Some(batch) = chunk.batch.as_ref() { ... }` block — the persistence runs whether or not the batch was empty), add:

```rust
        // Persist state for crash-resume.
        let snap_state = catalog::cdc_snapshot::CdcSnapshotState {
            pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(input.pipeline_id),
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            last_pk: chunk.last_pk,
            completed: false,
            captured_position: input.captured_gtid.clone(),
        };
        let snap_ctx = common_types::ids::TenantContext::new(
            common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
        );
        self.catalog
            .cdc_snapshot_upsert(snap_ctx, &snap_state)
            .await
            .map_err(|e| into_activity_err(anyhow::anyhow!(e)))?;
```

Insert this before the existing `Ok(MysqlSnapshotChunkOutput { ... })`.

- [ ] **Step 3: Update MySQL CDC workflow to resume from persisted state**

In `crates/worker/src/workflows/mysql_cdc_pipeline.rs`, find:

```rust
        let gtid_out = ctx
            .start_activity(
                MysqlCdcActivities::capture_start_gtid,
```

Insert just before this block — read snapshot state first; if a prior run captured a GTID, reuse it instead of re-capturing:

```rust
        // Snapshot resume check: if a prior run captured a GTID and
        // partway-completed snapshot, we MUST reuse that GTID. Re-
        // capturing now would skip past writes during the failure
        // window and lose data.
        let snap_state = ctx
            .start_activity(
                MysqlCdcActivities::mysql_snapshot_state_get,
                MysqlSnapshotStateGetInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                },
                opts_short(),
            )
            .await?;
        let resumed_captured_gtid = if !snap_state.captured_gtid.is_empty() {
            Some(snap_state.captured_gtid.clone())
        } else {
            None
        };
```

Find the existing capture-and-discover-schema block:

```rust
        let gtid_out = ctx
            .start_activity(
                MysqlCdcActivities::capture_start_gtid,
                CaptureStartGtidInput { ... },
                opts_short(),
            )
            .await?;
```

Replace with a conditional that uses the resumed GTID when present:

```rust
        let gtid_set = if let Some(gtid) = resumed_captured_gtid {
            gtid
        } else {
            ctx.start_activity(
                MysqlCdcActivities::capture_start_gtid,
                CaptureStartGtidInput {
                    pipeline_id: input.pipeline_id,
                    run_id: input.run_id,
                    tenant_id: input.tenant_id,
                    principal_id: input.principal_id,
                    jti: input.jti,
                    source_conn: input.source_conn.clone(),
                },
                opts_short(),
            )
            .await?
            .gtid_set
        };
```

Then update every downstream reference to `gtid_out.gtid_set` (there are several — `MysqlReadWindowInput.start_gtid`, `MysqlSnapshotChunkInput.captured_gtid`, etc.) to use `gtid_set` directly.

Find the snapshot loop:

```rust
        if matches!(
            my.initial_sync,
            common_types::pipeline_spec::MysqlInitialSync::SnapshotThenStreaming
        ) {
            let pk_col = my.pk_column.clone().ok_or_else(|| { ... })?;
            let mut snap_seq: u32 = 0;
            let mut last_pk: Option<i64> = None;
            loop {
```

Wrap with completion check + seed `last_pk`:

```rust
        if matches!(
            my.initial_sync,
            common_types::pipeline_spec::MysqlInitialSync::SnapshotThenStreaming
        ) && !snap_state.completed
        {
            let pk_col = my.pk_column.clone().ok_or_else(|| {
                anyhow::anyhow!("MysqlCdcSourceSpec.pk_column required for snapshot mode")
            })?;
            let mut snap_seq: u32 = 0;
            let mut last_pk: Option<i64> = snap_state.last_pk;
            loop {
```

After the existing snapshot loop's closing brace (where `is_final` breaks), add the mark-completed activity:

```rust
            ctx.start_activity(
                MysqlCdcActivities::mysql_snapshot_mark_completed,
                MysqlSnapshotMarkCompletedInput {
                    pipeline_id: input.pipeline_id,
                    tenant_id: input.tenant_id,
                },
                opts_short(),
            )
            .await?;
        }
```

(This closes the `if matches!(...) && !snap_state.completed` branch.)

- [ ] **Step 4: Verify build**

Run: `cargo build -p worker 2>&1 | grep -E "^error" | head -5`
Expected: empty.

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/activities/mysql_cdc crates/worker/src/workflows/mysql_cdc_pipeline.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-6-3: MySQL CDC — persist+resume snapshot via cdc_snapshots

mysql_snapshot_chunk persists snapshot state after each chunk. New
mysql_snapshot_state_get + mysql_snapshot_mark_completed activities.

MysqlCdcPipelineWorkflow now reads snapshot state before
capture_start_gtid: if a prior run captured a GTID, reuses it
(re-capturing would skip past writes during the failure window).
If completed=true, skips the snapshot loop entirely.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: E2E — verify resume behavior for Postgres CDC

**Files:**
- Modify: `tests/integration/tests/cdc_snapshot_streaming_handoff.rs`

The cleanest "did it resume?" test: run the pipeline twice with the same `pipeline_id`. After the first run completes the snapshot, the second run should see `completed=true` in the catalog and skip the snapshot loop entirely.

- [ ] **Step 1: Add a resume assertion to the existing handoff test**

In `tests/integration/tests/cdc_snapshot_streaming_handoff.rs`, after the existing assertions and before `worker.kill()`, add a second pipeline run:

```rust
    // === Resume verification ===
    // Re-run the same pipeline a second time. The snapshot state is
    // marked completed in cdc_snapshots, so the second run should NOT
    // re-snapshot. We verify by counting 's' rows: the first run
    // produced N (existing rows + any inserted before stream window);
    // the second run should produce 0 additional 's' rows.
    let s_after_first_run = read_ops(tmp.path())
        .iter()
        .filter(|o| *o == &"s")
        .count();

    // Kick off a second run on the same pipeline_id.
    let out = std::process::Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipeline_id.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()?;
    assert!(
        out.status.success(),
        "second pipeline run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait a few seconds for the second run's streaming loop to start
    // (snapshot should be skipped since it's already completed).
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    let s_after_second_run = read_ops(tmp.path())
        .iter()
        .filter(|o| *o == &"s")
        .count();
    assert_eq!(
        s_after_second_run, s_after_first_run,
        "second run added {} new 's' rows; expected 0 (snapshot should be skipped)",
        s_after_second_run - s_after_first_run
    );
```

(Note: this assumes the test uses a tokio Runtime — the existing test has `#[tokio::test]` so async ops work. The `cargo_bin`, `catalog_url`, `workspace_root`, `read_ops`, `tmp`, and `pipeline_id` references match the existing test helpers; verify each before commit.)

- [ ] **Step 2: Verify the test compiles**

Run: `cargo build --workspace --tests 2>&1 | grep -E "^error" | head -5`
Expected: empty.

- [ ] **Step 3: Run the e2e (requires docker stack)**

Prerequisite:

```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
```

Then:

```bash
cargo test -p integration-tests --test cdc_snapshot_streaming_handoff -- --ignored --nocapture 2>&1 | tail -10
```

Expected: PASS. Both the original handoff assertion (≥3 's' + ≥1 'i') AND the new resume assertion (`s_after_second_run == s_after_first_run`).

If the second run produces additional 's' rows, the snapshot loop didn't see `completed=true` — re-check Task 2's workflow conditional.

If the second run produces 0 's' rows but the first produced 0 too (test failed earlier), the snapshot persistence isn't kicking in — re-check Task 2 Step 2's upsert call.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/cdc_snapshot_streaming_handoff.rs
git commit -m "$(cat <<'EOF'
phase-2-3d-6-4: e2e — verify Postgres CDC snapshot resume

The handoff test now runs the same pipeline twice. The first run
completes the snapshot and marks cdc_snapshots.completed=true; the
second run should skip the snapshot loop (no additional 's' rows
produced).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README "Currently:" line**

In `README.md`, find the existing `Currently:` line and replace with:

```markdown
Currently: **Phase II.3.d.6 — CDC snapshot resume via catalog persistence (complete)** on top of II.3.d.5. Both Postgres and MySQL CDC snapshots now persist last_pk + captured_position to a new `cdc_snapshots` catalog table after each chunk. A pipeline whose snapshot was interrupted resumes from the last persisted PK; a pipeline whose snapshot completed skips the loop on subsequent runs. MySQL CDC additionally reuses the persisted GTID on resume so writes during the failure window aren't lost. Runtime on **wasmtime 36**. Remaining II.3.x follow-ups (multi-table, lift CDC to SDK) ship next. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-test run**

Run: `cargo test --workspace --lib 2>&1 | grep "test result" | tail -5`
Expected: all green.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: README refresh for Phase II.3.d.6 — snapshot resume

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```
