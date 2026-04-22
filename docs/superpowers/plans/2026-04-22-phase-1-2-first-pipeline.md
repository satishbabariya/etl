# Phase I.2 — First Pipeline (no WASM yet) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Phase I.1 no-op `start_run → 30s timer → complete_run` workflow with a real incremental Postgres→Parquet pipeline: read batches via a cursor on the source table, write each batch as a Parquet file, advance the cursor in the catalog only after the loader succeeds, and resume cleanly across worker restarts.

**Architecture:** A single in-process Rust Postgres connector (no WASM yet) implements `SourceConnector` and is invoked inside Temporal activities. A single Rust-native `LocalParquetLoader` implements `DestinationLoader` with idempotent writes keyed by `LoadId = (pipeline_id, run_id, batch_seq)`. The workflow state holds the current cursor; each iteration calls `read_batch → load_batch → commit_cursor` as three separate activities, so a crash anywhere in that triple is recoverable via Temporal's per-activity idempotency. Cursor advances only *after* loader success, giving at-least-once delivery. A new `stream_state` catalog table persists the cursor between runs.

**Tech Stack:** Rust 1.88, `arrow` 53 (Arrow IPC for inter-activity transport), `parquet` (workspace-new), `sqlx` 0.8 with postgres feature (already present), `temporalio-sdk` 0.2 (`#[workflow]` / `#[activities]` macros established in Phase I.1), `serde` / `serde_json`.

---

## File Structure

### Modified
- `crates/common-types/Cargo.toml` — add `serde_json`
- `crates/common-types/src/lib.rs` — expose new modules
- `crates/catalog/src/lib.rs` — add `stream_state` module + methods on `Catalog`
- `crates/connector-sdk/Cargo.toml` — add `arrow`, `async-trait`, `common-types`, `anyhow`
- `crates/connector-sdk/src/lib.rs` — real `SourceConnector` trait
- `crates/loader-sdk/Cargo.toml` — add `arrow`, `async-trait`, `common-types`, `anyhow`
- `crates/loader-sdk/src/lib.rs` — real `DestinationLoader` trait
- `crates/worker/Cargo.toml` — add `connector-sdk`, `loader-sdk`, `parquet`, `async-trait`, `futures`, `base64`
- `crates/worker/src/lib.rs` — expose `connectors`, `loaders` modules
- `crates/worker/src/activities/mod.rs` — add `sync` submodule
- `crates/worker/src/workflows/pipeline_run.rs` — rewrite: real read→load→commit loop
- `crates/worker/src/main.rs` — register `SyncActivities`
- `crates/cli/src/main.rs` — build full `PipelineRunInput` from catalog (spec + source connection + cursor)
- `docker-compose.yml` — mount `db/postgres-init/` to seed `etl_source_demo` on first-start
- `README.md` — Phase I.2 instructions

### New
- `crates/catalog/migrations/0002_stream_state.sql`
- `crates/catalog/src/stream_state.rs`
- `crates/common-types/src/pipeline_spec.rs` — `PipelineSpec`, `SourceSpec`, `DestinationSpec`
- `crates/common-types/src/cursor.rs` — `CursorKind`, `CursorValue`
- `crates/common-types/src/connection_config.rs` — `ConnectionConfig { url: String }`
- `crates/worker/src/connectors/mod.rs`
- `crates/worker/src/connectors/postgres/mod.rs` — public entry: `discover`, `read_batch`
- `crates/worker/src/connectors/postgres/discover.rs` — schema introspection
- `crates/worker/src/connectors/postgres/read.rs` — cursor-based SELECT + row→Arrow
- `crates/worker/src/loaders/mod.rs`
- `crates/worker/src/loaders/parquet_local.rs` — single file, whole loader
- `crates/worker/src/activities/sync/mod.rs` — `SyncActivities` struct + `#[activities]` impl
- `crates/worker/src/activities/sync/inputs.rs` — serde types for activity inputs/outputs
- `scripts/seed-source-demo.sh` — idempotent source-db seeder
- `db/postgres-init/01-source-demo.sql` — first-start init
- `tests/integration/tests/incremental_sync.rs` — end-to-end sync test
- `tests/integration/tests/durability_midbatch.rs` — kill-restart during a sync

Seven crates stay. `connector-sdk` and `loader-sdk` get real trait definitions (they were stub docstrings in Phase I.1). The concrete Postgres/Parquet *implementations* live inside `worker` for now — Phase I.3 moves them into the SDK crates when we introduce WASM.

---

## Type Contracts (referenced throughout)

These are the load-bearing types every downstream task depends on. Defined in Tasks 1–4; repeated here for orientation.

```rust
// common-types/src/cursor.rs
pub enum CursorKind { Int64, TimestampTz }
pub struct CursorValue { pub kind: CursorKind, pub value: String }

// common-types/src/connection_config.rs
pub struct ConnectionConfig { pub url: String }

// common-types/src/pipeline_spec.rs
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    pub batch_size: usize,
}
pub enum SourceSpec { Postgres(PostgresSourceSpec) }
pub struct PostgresSourceSpec {
    pub schema: String,
    pub table: String,
    pub cursor_column: String,
    pub cursor_kind: CursorKind,
    pub pk_columns: Vec<String>,
}
pub enum DestinationSpec { LocalParquet(LocalParquetSpec) }
pub struct LocalParquetSpec { pub base_path: String }

// connector-sdk/src/lib.rs
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    async fn discover(&self, conn: &ConnectionConfig, source: &SourceSpec)
        -> anyhow::Result<arrow::datatypes::SchemaRef>;
    async fn read_batch(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
        cursor: Option<CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome>;
}
pub struct ReadOutcome {
    pub batch: arrow::record_batch::RecordBatch,
    pub new_cursor: Option<CursorValue>,
    pub is_final: bool,
}

// loader-sdk/src/lib.rs
#[async_trait::async_trait]
pub trait DestinationLoader: Send + Sync {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()>;
    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<LoadResult>;
}
pub struct LoadId {
    pub pipeline_id: common_types::ids::PipelineId,
    pub run_id: common_types::ids::RunId,
    pub batch_seq: u32,
}
pub struct LoadResult {
    pub rows_loaded: usize,
    pub bytes_written: u64,
    pub path: String,
}
```

All types derive `Clone`, `Debug`, `Serialize`, `Deserialize` unless the code below specifies otherwise.

---

## Task 1: Catalog migration 0002 — `stream_state` table

**Files:**
- Create: `crates/catalog/migrations/0002_stream_state.sql`

RFC-10 "stream" entity is Phase I.4; Phase I.2 needs just enough: a row-per-(pipeline, stream_name) with a stringified cursor.

- [ ] **Step 1: Write the migration**

Create `crates/catalog/migrations/0002_stream_state.sql`:

```sql
-- Per-stream cursor state. One row per (pipeline_id, stream_name).
-- Phase I.2 scope. Full Stream/Schema entity lands in Phase I.4 (RFC-10).

CREATE TABLE stream_state (
    pipeline_id    UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    stream_name    TEXT NOT NULL,
    cursor_kind    TEXT NOT NULL CHECK (cursor_kind IN ('int64','timestamptz')),
    cursor_value   TEXT,           -- null = never synced; strings for kind-agnostic storage
    last_run_id    UUID REFERENCES runs(run_id) ON DELETE SET NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, stream_name)
);
```

- [ ] **Step 2: Apply the migration manually to verify SQL**

Run: `docker exec -i etl-postgres psql -U etl -d etl_catalog < crates/catalog/migrations/0002_stream_state.sql`
Expected: `CREATE TABLE`.

Then roll it back so sqlx can re-apply from Rust:
Run: `docker exec -i etl-postgres psql -U etl -d etl_catalog -c "DROP TABLE stream_state CASCADE;"`

- [ ] **Step 3: Commit**

```bash
git add crates/catalog/migrations/0002_stream_state.sql
git commit -m "feat(catalog): add stream_state table for cursor persistence

Phase I.2 minimal — full Stream entity per RFC-10 is Phase I.4.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `common-types` — cursor, connection_config, pipeline_spec

**Files:**
- Create: `crates/common-types/src/cursor.rs`
- Create: `crates/common-types/src/connection_config.rs`
- Create: `crates/common-types/src/pipeline_spec.rs`
- Modify: `crates/common-types/src/lib.rs`
- Modify: `crates/common-types/Cargo.toml`

- [ ] **Step 1: Add `serde_json` to common-types**

Edit `crates/common-types/Cargo.toml`, add under `[dependencies]` (not just dev-dependencies):

```toml
serde_json = { workspace = true }
```

- [ ] **Step 2: Write failing test**

Create `crates/common-types/src/cursor.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorKind {
    Int64,
    TimestampTz,
}

/// Stringified cursor, kind-tagged. The string form is canonical and
/// survives serialization across Temporal and catalog JSONB.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorValue {
    pub kind: CursorKind,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_int() {
        let c = CursorValue { kind: CursorKind::Int64, value: "42".into() };
        let j = serde_json::to_string(&c).unwrap();
        let back: CursorValue = serde_json::from_str(&j).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn cursor_kind_serializes_snake_case() {
        let j = serde_json::to_string(&CursorKind::TimestampTz).unwrap();
        assert_eq!(j, "\"timestamp_tz\"");
    }
}
```

- [ ] **Step 3: Add `connection_config.rs`**

Create `crates/common-types/src/connection_config.rs`:

```rust
use serde::{Deserialize, Serialize};

/// Connection parameters for a connector. Phase I.2: a single URL.
/// Phase II.2 (RFC-11) splits this into a reference + resolved secret.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub url: String,
}
```

- [ ] **Step 4: Add `pipeline_spec.rs` with tests**

Create `crates/common-types/src/pipeline_spec.rs`:

```rust
use crate::cursor::CursorKind;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    /// Max rows per read_batch activity call.
    pub batch_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresSourceSpec {
    pub schema: String,
    pub table: String,
    pub cursor_column: String,
    pub cursor_kind: CursorKind,
    pub pk_columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DestinationSpec {
    LocalParquet(LocalParquetSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalParquetSpec {
    /// Directory where Parquet files will be written. Created on demand.
    pub base_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_roundtrip_pg_to_parquet() {
        let s = PipelineSpec {
            source: SourceSpec::Postgres(PostgresSourceSpec {
                schema: "public".into(),
                table: "customers".into(),
                cursor_column: "updated_at".into(),
                cursor_kind: CursorKind::TimestampTz,
                pk_columns: vec!["id".into()],
            }),
            destination: DestinationSpec::LocalParquet(LocalParquetSpec {
                base_path: "./data".into(),
            }),
            batch_size: 100,
        };
        let j = serde_json::to_string(&s).unwrap();
        // Must be a valid JSON object round-trip.
        let back: PipelineSpec = serde_json::from_str(&j).unwrap();
        // round-trip preserves fields; compare serialized forms
        assert_eq!(serde_json::to_string(&back).unwrap(), j);
    }

    #[test]
    fn source_serialized_form_is_tagged() {
        let s = SourceSpec::Postgres(PostgresSourceSpec {
            schema: "public".into(),
            table: "t".into(),
            cursor_column: "c".into(),
            cursor_kind: CursorKind::Int64,
            pk_columns: vec!["id".into()],
        });
        let j: serde_json::Value = serde_json::to_value(&s).unwrap();
        assert_eq!(j["type"], "postgres");
    }
}
```

- [ ] **Step 5: Wire modules into `lib.rs`**

Edit `crates/common-types/src/lib.rs`:

```rust
//! Shared newtype identifiers and primitive types for the platform.
pub mod connection_config;
pub mod cursor;
pub mod ids;
pub mod pipeline_spec;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p common-types`
Expected: 4 previous tests pass + 3 new tests (2 from cursor.rs, 1 from pipeline_spec.rs spec_roundtrip, 1 from pipeline_spec.rs source_serialized_form_is_tagged) = **7 passed**.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(common-types): PipelineSpec, SourceSpec, DestinationSpec, CursorValue

Adds the wire types shared across catalog (pipelines.spec JSONB),
worker (activity inputs), and CLI (workflow input construction).
SourceSpec and DestinationSpec are serde-tagged enums so new variants
are additive. Phase I.2 minimal: Postgres source, local-Parquet dest,
Int64/TimestampTz cursor kinds.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `connector-sdk` — `SourceConnector` trait

**Files:**
- Modify: `crates/connector-sdk/Cargo.toml`
- Modify: `crates/connector-sdk/src/lib.rs`

- [ ] **Step 1: Add deps**

Edit `crates/connector-sdk/Cargo.toml`:

```toml
[dependencies]
serde = { workspace = true }
arrow = { workspace = true }
async-trait = "0.1"
common-types = { workspace = true }
anyhow = { workspace = true }
```

Add `async-trait = "0.1"` to root `Cargo.toml`'s `[workspace.dependencies]` so other crates can share the version.

Edit root `Cargo.toml`, under `[workspace.dependencies]`:

```toml
async-trait = "0.1"
```

- [ ] **Step 2: Write trait**

Replace `crates/connector-sdk/src/lib.rs`:

```rust
//! Connector SDK — traits for source connectors (RFC-6).
//!
//! Phase I.2 shape: in-process Rust implementations only. Phase I.3 adds
//! WASM Component Model packaging on top of these traits.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common_types::connection_config::ConnectionConfig;
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::SourceSpec;

/// A source connector: given a connection + source config, emit Arrow batches
/// from a cursor position.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    /// Introspect the source and return its Arrow schema.
    async fn discover(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
    ) -> anyhow::Result<SchemaRef>;

    /// Read up to `batch_size` rows strictly after `cursor`.
    async fn read_batch(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
        cursor: Option<CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome>;
}

/// Result of a single `read_batch` call.
pub struct ReadOutcome {
    /// Always valid — empty batches are encoded by `rows == 0`, not a null batch.
    pub batch: RecordBatch,
    /// Cursor value of the *last* row in the batch. None if the batch is empty.
    pub new_cursor: Option<CursorValue>,
    /// True if fewer than `batch_size` rows were returned — indicates the source
    /// has no more data at this moment.
    pub is_final: bool,
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p connector-sdk`
Expected: compiles clean.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(connector-sdk): real SourceConnector trait (RFC-6)

Phase I.2 in-process shape; Phase I.3 wraps this in the WASM component
model. discover() + read_batch() with ReadOutcome carrying the batch,
new cursor, and is_final flag for bounded reads.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `loader-sdk` — `DestinationLoader` trait

**Files:**
- Modify: `crates/loader-sdk/Cargo.toml`
- Modify: `crates/loader-sdk/src/lib.rs`

- [ ] **Step 1: Add deps**

Edit `crates/loader-sdk/Cargo.toml`:

```toml
[dependencies]
serde = { workspace = true }
arrow = { workspace = true }
async-trait = { workspace = true }
common-types = { workspace = true }
anyhow = { workspace = true }
```

- [ ] **Step 2: Write trait**

Replace `crates/loader-sdk/src/lib.rs`:

```rust
//! Loader SDK — Rust-native trait for destination loaders (RFC-9).
//!
//! Phase I.2: Direct Append pattern only. Phase II.3 adds MERGE-on-commit,
//! Apply Change Stream, and Append-Only Event Log variants.

use arrow::record_batch::RecordBatch;
use common_types::ids::{PipelineId, RunId};
use common_types::pipeline_spec::DestinationSpec;
use serde::{Deserialize, Serialize};

#[async_trait::async_trait]
pub trait DestinationLoader: Send + Sync {
    /// Cheap sanity-check (paths exist, credentials valid, etc.).
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()>;

    /// Idempotent write. Same `load_id` twice MUST produce the same durable
    /// state (overwrite, or no-op if already landed). Retries are safe.
    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult>;
}

/// Deterministic identifier for a single loaded batch.
/// Same `(pipeline_id, run_id, batch_seq)` tuple ⇒ same underlying artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadId {
    pub pipeline_id: PipelineId,
    pub run_id: RunId,
    pub batch_seq: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadResult {
    pub rows_loaded: usize,
    pub bytes_written: u64,
    /// Destination-specific path/URI (for logs and manual inspection).
    pub path: String,
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p loader-sdk`
Expected: compiles clean.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(loader-sdk): real DestinationLoader trait (RFC-9)

Phase I.2: validate() + idempotent load() keyed by LoadId
(pipeline_id, run_id, batch_seq). LoadResult reports rows, bytes, and
the destination path. prepare_run / commit_run / abort_run deferred to
Phase II.3 when warehouse MERGE loaders need them.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Source demo database — docker init + seed script

**Files:**
- Create: `db/postgres-init/01-source-demo.sql`
- Create: `scripts/seed-source-demo.sh`
- Modify: `docker-compose.yml`

The init SQL seeds the source DB automatically on first-time container start. The shell script handles re-runs and reseeding after tests. Both are idempotent.

- [ ] **Step 1: Create the init SQL**

Create `db/postgres-init/01-source-demo.sql`:

```sql
-- Runs once on first container start via /docker-entrypoint-initdb.d/.
-- Subsequent runs should use scripts/seed-source-demo.sh.
CREATE DATABASE etl_source_demo;

\c etl_source_demo

CREATE TABLE customers (
    id         BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO customers (id, name, email, created_at, updated_at) VALUES
    (1, 'Alice',   'alice@example.com',   '2026-04-20 10:00:00+00', '2026-04-20 10:00:00+00'),
    (2, 'Bob',     NULL,                  '2026-04-20 11:00:00+00', '2026-04-20 11:00:00+00'),
    (3, 'Carol',   'carol@example.com',   '2026-04-20 12:00:00+00', '2026-04-20 12:00:00+00'),
    (4, 'Dave',    'dave@example.com',    '2026-04-21 09:00:00+00', '2026-04-21 09:00:00+00'),
    (5, 'Eve',     'eve@example.com',     '2026-04-21 10:00:00+00', '2026-04-21 10:00:00+00'),
    (6, 'Frank',   NULL,                  '2026-04-21 11:00:00+00', '2026-04-21 11:00:00+00'),
    (7, 'Grace',   'grace@example.com',   '2026-04-21 12:00:00+00', '2026-04-21 12:00:00+00'),
    (8, 'Heidi',   'heidi@example.com',   '2026-04-22 09:00:00+00', '2026-04-22 09:00:00+00'),
    (9, 'Ivan',    'ivan@example.com',    '2026-04-22 10:00:00+00', '2026-04-22 10:00:00+00'),
    (10,'Judy',    'judy@example.com',    '2026-04-22 11:00:00+00', '2026-04-22 11:00:00+00');
```

- [ ] **Step 2: Create the idempotent seed script**

Create `scripts/seed-source-demo.sh`:

```bash
#!/usr/bin/env bash
# Idempotent source-db seed. Safe to run against an existing container.
# Resets customers to a known 10-row baseline.
set -euo pipefail

docker exec -i etl-postgres psql -U etl -d postgres <<'SQL'
SELECT 'CREATE DATABASE etl_source_demo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_source_demo')\gexec
SQL

docker exec -i etl-postgres psql -U etl -d etl_source_demo <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id         BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
TRUNCATE customers;
INSERT INTO customers (id, name, email, created_at, updated_at) VALUES
    (1, 'Alice',   'alice@example.com',   '2026-04-20 10:00:00+00', '2026-04-20 10:00:00+00'),
    (2, 'Bob',     NULL,                  '2026-04-20 11:00:00+00', '2026-04-20 11:00:00+00'),
    (3, 'Carol',   'carol@example.com',   '2026-04-20 12:00:00+00', '2026-04-20 12:00:00+00'),
    (4, 'Dave',    'dave@example.com',    '2026-04-21 09:00:00+00', '2026-04-21 09:00:00+00'),
    (5, 'Eve',     'eve@example.com',     '2026-04-21 10:00:00+00', '2026-04-21 10:00:00+00'),
    (6, 'Frank',   NULL,                  '2026-04-21 11:00:00+00', '2026-04-21 11:00:00+00'),
    (7, 'Grace',   'grace@example.com',   '2026-04-21 12:00:00+00', '2026-04-21 12:00:00+00'),
    (8, 'Heidi',   'heidi@example.com',   '2026-04-22 09:00:00+00', '2026-04-22 09:00:00+00'),
    (9, 'Ivan',    'ivan@example.com',    '2026-04-22 10:00:00+00', '2026-04-22 10:00:00+00'),
    (10,'Judy',    'judy@example.com',    '2026-04-22 11:00:00+00', '2026-04-22 11:00:00+00');
SELECT COUNT(*) AS customer_count FROM customers;
SQL
```

Make executable: `chmod +x scripts/seed-source-demo.sh`.

- [ ] **Step 3: Wire the init dir into docker-compose**

Edit `docker-compose.yml`. Modify the `postgres` service `volumes` to add the init-mount:

```yaml
  postgres:
    image: postgres:16
    container_name: etl-postgres
    environment:
      POSTGRES_USER: etl
      POSTGRES_PASSWORD: etl
      POSTGRES_DB: etl_catalog
    ports:
      - "5432:5432"
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=10"
    volumes:
      - etl_pg:/var/lib/postgresql/data
      - ./db/postgres-init:/docker-entrypoint-initdb.d:ro
```

- [ ] **Step 4: Run the seed script**

Run: `bash scripts/seed-source-demo.sh`
Expected stdout ends with `customer_count` = `10`.

Verify the 10 rows exist:
Run: `docker exec -i etl-postgres psql -U etl -d etl_source_demo -c "SELECT id, name, updated_at FROM customers ORDER BY updated_at LIMIT 3;"`
Expected: 3 rows, ids 1-3, timestamps on 2026-04-20.

- [ ] **Step 5: Commit**

```bash
git add db/ scripts/ docker-compose.yml
git commit -m "feat: seed Postgres source-demo DB (10-row customers fixture)

etl_source_demo.customers has BIGINT id, TEXT name, nullable TEXT email,
TIMESTAMPTZ created_at/updated_at. Spans 2026-04-20 through 2026-04-22
to exercise the cursor on updated_at. Both a docker-entrypoint init
script (first-run) and an idempotent shell seed script (reruns) are
provided.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Catalog — `stream_state` CRUD

**Files:**
- Create: `crates/catalog/src/stream_state.rs`
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/catalog/tests/crud.rs`

- [ ] **Step 1: Write the failing test**

Edit `crates/catalog/tests/crud.rs`, append:

```rust
use common_types::cursor::{CursorKind, CursorValue};

#[tokio::test]
async fn stream_state_upsert_then_get() {
    let cat = test_catalog().await;
    let tenant = cat.create_tenant("acme").await.unwrap();
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await
        .unwrap();
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "demo".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await
        .unwrap();

    // Initially no state.
    assert!(cat.get_stream_state(pipe, "customers").await.unwrap().is_none());

    // Upsert.
    cat.upsert_stream_state(
        pipe,
        "customers",
        Some(CursorValue {
            kind: CursorKind::TimestampTz,
            value: "2026-04-22T11:00:00Z".into(),
        }),
        None,
    )
    .await
    .unwrap();

    let got = cat.get_stream_state(pipe, "customers").await.unwrap().unwrap();
    assert_eq!(got.cursor.as_ref().unwrap().value, "2026-04-22T11:00:00Z");
    assert_eq!(got.cursor.as_ref().unwrap().kind, CursorKind::TimestampTz);

    // Second upsert overwrites.
    cat.upsert_stream_state(
        pipe,
        "customers",
        Some(CursorValue {
            kind: CursorKind::TimestampTz,
            value: "2026-04-23T10:00:00Z".into(),
        }),
        None,
    )
    .await
    .unwrap();
    let got2 = cat.get_stream_state(pipe, "customers").await.unwrap().unwrap();
    assert_eq!(got2.cursor.as_ref().unwrap().value, "2026-04-23T10:00:00Z");
}
```

- [ ] **Step 2: Run — verify it fails**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p catalog stream_state_upsert -- --test-threads=1`
Expected: FAIL (`upsert_stream_state` method not found on `Catalog`).

- [ ] **Step 3: Implement `stream_state.rs`**

Create `crates/catalog/src/stream_state.rs`:

```rust
use chrono::{DateTime, Utc};
use common_types::cursor::{CursorKind, CursorValue};
use common_types::ids::{PipelineId, RunId};
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct StreamState {
    pub pipeline_id: PipelineId,
    pub stream_name: String,
    pub cursor: Option<CursorValue>,
    pub last_run_id: Option<RunId>,
    pub updated_at: DateTime<Utc>,
}

fn kind_str(k: CursorKind) -> &'static str {
    match k {
        CursorKind::Int64 => "int64",
        CursorKind::TimestampTz => "timestamptz",
    }
}

fn parse_kind(s: &str) -> CursorKind {
    match s {
        "int64" => CursorKind::Int64,
        "timestamptz" => CursorKind::TimestampTz,
        other => panic!("unknown cursor_kind in DB: {other}"),
    }
}

pub async fn upsert(
    pool: &PgPool,
    pipeline_id: PipelineId,
    stream_name: &str,
    cursor: Option<CursorValue>,
    last_run_id: Option<RunId>,
) -> sqlx::Result<()> {
    // On first insert we need SOME kind; default to int64 if cursor absent.
    let (kind, value) = match cursor {
        Some(c) => (kind_str(c.kind).to_string(), Some(c.value)),
        None => ("int64".to_string(), None),
    };
    sqlx::query(
        "INSERT INTO stream_state (pipeline_id, stream_name, cursor_kind, cursor_value, last_run_id, updated_at) \
         VALUES ($1, $2, $3, $4, $5, NOW()) \
         ON CONFLICT (pipeline_id, stream_name) DO UPDATE SET \
           cursor_kind = EXCLUDED.cursor_kind, \
           cursor_value = EXCLUDED.cursor_value, \
           last_run_id = COALESCE(EXCLUDED.last_run_id, stream_state.last_run_id), \
           updated_at = NOW()",
    )
    .bind(pipeline_id.as_uuid())
    .bind(stream_name)
    .bind(kind)
    .bind(value)
    .bind(last_run_id.map(|r| r.as_uuid()))
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get(
    pool: &PgPool,
    pipeline_id: PipelineId,
    stream_name: &str,
) -> sqlx::Result<Option<StreamState>> {
    let row: Option<(
        uuid::Uuid,
        String,
        String,
        Option<String>,
        Option<uuid::Uuid>,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT pipeline_id, stream_name, cursor_kind, cursor_value, last_run_id, updated_at \
         FROM stream_state WHERE pipeline_id = $1 AND stream_name = $2",
    )
    .bind(pipeline_id.as_uuid())
    .bind(stream_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(pid, name, kind, val, lrid, ts)| StreamState {
        pipeline_id: PipelineId::from_uuid_unchecked(pid),
        stream_name: name,
        cursor: val.map(|value| CursorValue {
            kind: parse_kind(&kind),
            value,
        }),
        last_run_id: lrid.map(RunId::from_uuid_unchecked),
        updated_at: ts,
    }))
}
```

- [ ] **Step 4: Expose on `Catalog`**

Edit `crates/catalog/src/lib.rs`. Add module declaration and public re-export:

```rust
pub mod stream_state;
```

Also add the following methods inside `impl Catalog` (append near the other method groups):

```rust
    // Stream state
    pub async fn get_stream_state(
        &self,
        pipeline_id: common_types::ids::PipelineId,
        stream_name: &str,
    ) -> sqlx::Result<Option<stream_state::StreamState>> {
        stream_state::get(&self.pool, pipeline_id, stream_name).await
    }

    pub async fn upsert_stream_state(
        &self,
        pipeline_id: common_types::ids::PipelineId,
        stream_name: &str,
        cursor: Option<common_types::cursor::CursorValue>,
        last_run_id: Option<common_types::ids::RunId>,
    ) -> sqlx::Result<()> {
        stream_state::upsert(&self.pool, pipeline_id, stream_name, cursor, last_run_id).await
    }
```

- [ ] **Step 5: Also update the test-utility truncate to include stream_state**

Edit `crates/catalog/src/lib.rs`. Modify `truncate_all_for_tests`:

```rust
    #[doc(hidden)]
    pub async fn truncate_all_for_tests(&self) -> sqlx::Result<()> {
        sqlx::query("TRUNCATE runs, stream_state, pipelines, connections, tenants CASCADE")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
```

- [ ] **Step 6: Run tests**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p catalog -- --test-threads=1`
Expected: **4 passed** (3 existing + 1 new stream_state_upsert_then_get).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(catalog): stream_state CRUD (get + upsert)

Upsert on (pipeline_id, stream_name) with typed cursor stored as text
+ kind tag. Integration test covers insert then overwrite. truncate
helper extended to include the new table.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Postgres source connector — schema discovery

**Files:**
- Modify: `crates/worker/Cargo.toml`
- Modify: `crates/worker/src/lib.rs`
- Create: `crates/worker/src/connectors/mod.rs`
- Create: `crates/worker/src/connectors/postgres/mod.rs`
- Create: `crates/worker/src/connectors/postgres/discover.rs`

Phase I.2 type coverage: `BIGINT` (`bigint`/`int8`) → Int64, `TEXT` (`text`, `varchar`) → Utf8 (nullable supported), `TIMESTAMPTZ` (`timestamp with time zone`, `timestamptz`) → `Timestamp(Microsecond, Some("+00:00".into()))`. Other Postgres types error out with a clear message — extending coverage is Phase I.4.

- [ ] **Step 1: Add deps to worker**

Edit `crates/worker/Cargo.toml`, add under `[dependencies]`:

```toml
connector-sdk = { workspace = true }
loader-sdk = { workspace = true }
async-trait = { workspace = true }
futures = { workspace = true }
parquet = "53"
base64 = "0.22"
```

Also add `loader-sdk` and `connector-sdk` to the root `Cargo.toml` `[workspace.dependencies]` if not already present:

```toml
connector-sdk = { path = "crates/connector-sdk" }
loader-sdk = { path = "crates/loader-sdk" }
```

- [ ] **Step 2: Wire top-level `connectors` module**

Edit `crates/worker/src/lib.rs`. Append:

```rust
pub mod connectors;
pub mod loaders;
```

Create `crates/worker/src/connectors/mod.rs`:

```rust
//! In-process connector implementations. Phase I.2 scope: postgres only.
//! Phase I.3 moves these behind the WASM Component Model.
pub mod postgres;
```

Create `crates/worker/src/connectors/postgres/mod.rs`:

```rust
//! Rust-native Postgres source connector (Phase I.2).
mod discover;
mod read;

use async_trait::async_trait;
use connector_sdk::{ReadOutcome, SourceConnector};

pub struct PostgresConnector;

#[async_trait]
impl SourceConnector for PostgresConnector {
    async fn discover(
        &self,
        conn: &common_types::connection_config::ConnectionConfig,
        source: &common_types::pipeline_spec::SourceSpec,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        match source {
            common_types::pipeline_spec::SourceSpec::Postgres(pg) => {
                discover::run(conn, pg).await
            }
        }
    }

    async fn read_batch(
        &self,
        conn: &common_types::connection_config::ConnectionConfig,
        source: &common_types::pipeline_spec::SourceSpec,
        cursor: Option<common_types::cursor::CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome> {
        match source {
            common_types::pipeline_spec::SourceSpec::Postgres(pg) => {
                read::run(conn, pg, cursor, batch_size).await
            }
        }
    }
}
```

- [ ] **Step 3: Write discover with a failing test**

Create `crates/worker/src/connectors/postgres/discover.rs`:

```rust
use anyhow::{Context, bail};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::PostgresSourceSpec;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

/// Introspect a Postgres table via `information_schema` and produce an
/// Arrow schema. Phase I.2 supported types: bigint, text, timestamptz.
pub async fn run(
    conn: &ConnectionConfig,
    spec: &PostgresSourceSpec,
) -> anyhow::Result<SchemaRef> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&conn.url)
        .await
        .with_context(|| format!("connecting to source for discover: {}", spec.table))?;

    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT column_name, udt_name, is_nullable = 'YES' \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(&spec.schema)
    .bind(&spec.table)
    .fetch_all(&pool)
    .await
    .with_context(|| format!("introspecting {}.{}", spec.schema, spec.table))?;

    if rows.is_empty() {
        bail!("table {}.{} not found or has no columns", spec.schema, spec.table);
    }

    let mut fields = Vec::with_capacity(rows.len());
    for (col_name, udt, nullable) in rows {
        let dtype = pg_udt_to_arrow(&udt)
            .with_context(|| format!("unsupported column {}: {}", col_name, udt))?;
        fields.push(Field::new(&col_name, dtype, nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn pg_udt_to_arrow(udt: &str) -> anyhow::Result<DataType> {
    Ok(match udt {
        "int8" => DataType::Int64,
        "int4" => DataType::Int32,
        "int2" => DataType::Int16,
        "text" | "varchar" | "bpchar" => DataType::Utf8,
        "bool" => DataType::Boolean,
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "date" => DataType::Date32,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        other => bail!("Phase I.2 does not support Postgres type '{other}'"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::cursor::CursorKind;

    fn test_url() -> String {
        std::env::var("SOURCE_URL")
            .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn discover_customers() {
        let schema = run(
            &ConnectionConfig { url: test_url() },
            &PostgresSourceSpec {
                schema: "public".into(),
                table: "customers".into(),
                cursor_column: "updated_at".into(),
                cursor_kind: CursorKind::TimestampTz,
                pk_columns: vec!["id".into()],
            },
        )
        .await
        .unwrap();

        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name", "email", "created_at", "updated_at"]);

        assert_eq!(schema.field_with_name("id").unwrap().data_type(), &DataType::Int64);
        assert_eq!(schema.field_with_name("name").unwrap().data_type(), &DataType::Utf8);
        assert!(!schema.field_with_name("name").unwrap().is_nullable());
        assert!(schema.field_with_name("email").unwrap().is_nullable());
    }
}
```

- [ ] **Step 4: Run the test**

Ensure source DB is seeded: `bash scripts/seed-source-demo.sh`

Run: `cargo test -p worker discover_customers -- --ignored --nocapture`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker/connectors): Postgres schema discovery

discover() introspects information_schema and maps Phase I.2 types
(bigint, text, timestamptz — nullable-aware) to Arrow. Unsupported
types error with a clear message; extension is Phase I.4.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Postgres source connector — `read_batch`

**Files:**
- Create: `crates/worker/src/connectors/postgres/read.rs`

The cursor is strictly-increasing (Phase I.2 scope). SQL: `SELECT * FROM schema.table WHERE cursor_column > $1 ORDER BY cursor_column ASC LIMIT $2`. First call has `cursor = None` → drop the WHERE. Types covered: Int64, Utf8 (nullable), TimestampTz.

- [ ] **Step 1: Write the file with an ignored test**

Create `crates/worker/src/connectors/postgres/read.rs`:

```rust
use anyhow::{Context, bail};
use arrow::array::{
    ArrayBuilder, ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use common_types::connection_config::ConnectionConfig;
use common_types::cursor::{CursorKind, CursorValue};
use common_types::pipeline_spec::PostgresSourceSpec;
use connector_sdk::ReadOutcome;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, Row, TypeInfo};
use std::sync::Arc;

pub async fn run(
    conn: &ConnectionConfig,
    spec: &PostgresSourceSpec,
    cursor: Option<CursorValue>,
    batch_size: usize,
) -> anyhow::Result<ReadOutcome> {
    let schema = super::discover::run(conn, spec).await?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&conn.url)
        .await
        .context("connecting to source for read_batch")?;

    let column_list = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = match cursor.as_ref() {
        None => format!(
            "SELECT {} FROM \"{}\".\"{}\" ORDER BY \"{}\" ASC LIMIT {}",
            column_list, spec.schema, spec.table, spec.cursor_column, batch_size as i64,
        ),
        Some(_) => format!(
            "SELECT {} FROM \"{}\".\"{}\" WHERE \"{}\" > $1 ORDER BY \"{}\" ASC LIMIT {}",
            column_list,
            spec.schema,
            spec.table,
            spec.cursor_column,
            spec.cursor_column,
            batch_size as i64,
        ),
    };

    let mut query = sqlx::query(&sql);
    if let Some(c) = cursor.as_ref() {
        query = bind_cursor(query, c)?;
    }

    let rows = query.fetch_all(&pool).await.context("executing read_batch")?;
    let row_count = rows.len();

    let batch = rows_to_recordbatch(&schema, &rows)?;

    let new_cursor = if row_count == 0 {
        None
    } else {
        let last = &rows[row_count - 1];
        Some(extract_cursor(last, &spec.cursor_column, spec.cursor_kind)?)
    };

    Ok(ReadOutcome {
        batch,
        new_cursor,
        is_final: row_count < batch_size,
    })
}

fn bind_cursor<'a>(
    q: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    cursor: &CursorValue,
) -> anyhow::Result<sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    Ok(match cursor.kind {
        CursorKind::Int64 => {
            let v: i64 = cursor.value.parse().context("cursor value is not i64")?;
            q.bind(v)
        }
        CursorKind::TimestampTz => {
            let v: DateTime<Utc> = cursor
                .value
                .parse()
                .context("cursor value is not RFC-3339 timestamptz")?;
            q.bind(v)
        }
    })
}

fn extract_cursor(
    row: &sqlx::postgres::PgRow,
    column: &str,
    kind: CursorKind,
) -> anyhow::Result<CursorValue> {
    Ok(match kind {
        CursorKind::Int64 => CursorValue {
            kind,
            value: row.try_get::<i64, _>(column)?.to_string(),
        },
        CursorKind::TimestampTz => {
            let ts: DateTime<Utc> = row.try_get(column)?;
            CursorValue {
                kind,
                value: ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            }
        }
    })
}

fn rows_to_recordbatch(
    schema: &SchemaRef,
    rows: &[sqlx::postgres::PgRow],
) -> anyhow::Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut builders: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type(), rows.len()))
        .collect::<anyhow::Result<Vec<_>>>()?;

    for row in rows {
        for (col_idx, field) in schema.fields().iter().enumerate() {
            append_cell(&mut builders[col_idx], field, row, row.column(col_idx).name())?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

fn make_builder(dtype: &DataType, capacity: usize) -> anyhow::Result<Box<dyn ArrayBuilder>> {
    Ok(match dtype {
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 16)),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => {
            Box::new(
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_timezone("+00:00"),
            )
        }
        other => bail!("no Arrow builder wired for {other:?} in Phase I.2"),
    })
}

fn append_cell(
    builder: &mut Box<dyn ArrayBuilder>,
    field: &arrow::datatypes::Field,
    row: &sqlx::postgres::PgRow,
    col_name: &str,
) -> anyhow::Result<()> {
    match field.data_type() {
        DataType::Int64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("builder type mismatch");
            let v: Option<i64> = row.try_get(col_name)?;
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("builder type mismatch");
            let v: Option<String> = row.try_get(col_name)?;
            match v {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_)) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .expect("builder type mismatch");
            let v: Option<DateTime<Utc>> = row.try_get(col_name)?;
            match v {
                Some(x) => b.append_value(x.timestamp_micros()),
                None => b.append_null(),
            }
        }
        other => bail!("no cell appender wired for {other:?} in Phase I.2"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_url() -> String {
        std::env::var("SOURCE_URL")
            .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
    }

    fn spec() -> PostgresSourceSpec {
        PostgresSourceSpec {
            schema: "public".into(),
            table: "customers".into(),
            cursor_column: "updated_at".into(),
            cursor_kind: CursorKind::TimestampTz,
            pk_columns: vec!["id".into()],
        }
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn fresh_read_returns_first_batch_sorted() {
        let conn = ConnectionConfig { url: test_url() };
        let out = run(&conn, &spec(), None, 3).await.unwrap();
        assert_eq!(out.batch.num_rows(), 3);
        assert!(!out.is_final);
        // Last cursor should match the 3rd-earliest row's updated_at (row id=3).
        assert!(out.new_cursor.unwrap().value.starts_with("2026-04-20T12:00"));
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn cursor_advance_reads_subsequent_rows() {
        let conn = ConnectionConfig { url: test_url() };
        let first = run(&conn, &spec(), None, 3).await.unwrap();
        let second = run(&conn, &spec(), first.new_cursor.clone(), 3).await.unwrap();
        // Rows 4, 5, 6.
        assert_eq!(second.batch.num_rows(), 3);
        let id_col = second
            .batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[4, 5, 6]);
    }

    #[tokio::test]
    #[ignore = "requires dockerized etl_source_demo with seeded customers table"]
    async fn is_final_when_batch_smaller_than_requested() {
        let conn = ConnectionConfig { url: test_url() };
        let out = run(&conn, &spec(), None, 100).await.unwrap();
        assert_eq!(out.batch.num_rows(), 10);
        assert!(out.is_final);
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p worker --lib -- --ignored --nocapture connectors::postgres::read`
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/connectors): Postgres cursor-based read_batch

SELECT * FROM schema.table WHERE cursor > \$1 ORDER BY cursor LIMIT N.
Strictly-increasing semantics (no overlap). Types: Int64, Utf8
nullable, TimestampTz. ReadOutcome returns batch + last-cursor +
is_final (rows<batch_size). Three ignored integration tests against
etl_source_demo: fresh read, cursor advance, is_final detection.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Local Parquet loader

**Files:**
- Create: `crates/worker/src/loaders/mod.rs`
- Create: `crates/worker/src/loaders/parquet_local.rs`

- [ ] **Step 1: Wire module**

Create `crates/worker/src/loaders/mod.rs`:

```rust
//! In-process loader implementations. Phase I.2: local-Parquet only.
pub mod parquet_local;
```

- [ ] **Step 2: Write the loader with unit test**

Create `crates/worker/src/loaders/parquet_local.rs`:

```rust
use anyhow::{Context, bail};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::pipeline_spec::{DestinationSpec, LocalParquetSpec};
use loader_sdk::{DestinationLoader, LoadId, LoadResult};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::PathBuf;

pub struct LocalParquetLoader;

#[async_trait]
impl DestinationLoader for LocalParquetLoader {
    async fn validate(&self, dest: &DestinationSpec) -> anyhow::Result<()> {
        match dest {
            DestinationSpec::LocalParquet(p) => {
                fs::create_dir_all(&p.base_path)
                    .with_context(|| format!("creating {}", p.base_path))?;
                Ok(())
            }
        }
    }

    async fn load(
        &self,
        dest: &DestinationSpec,
        load_id: LoadId,
        batch: RecordBatch,
    ) -> anyhow::Result<LoadResult> {
        let spec = match dest {
            DestinationSpec::LocalParquet(s) => s,
        };
        let path = target_path(spec, &load_id);
        fs::create_dir_all(path.parent().unwrap())
            .with_context(|| format!("creating dir for {}", path.display()))?;

        // Idempotent: overwrite any prior file at this path. Same LoadId => same path
        // => same final bytes after any retry.
        let file = File::create(&path)
            .with_context(|| format!("creating {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .context("constructing ArrowWriter")?;
        if batch.num_rows() > 0 {
            writer.write(&batch).context("writing batch")?;
        }
        writer.close().context("closing ArrowWriter")?;

        let bytes = fs::metadata(&path)?.len();
        Ok(LoadResult {
            rows_loaded: batch.num_rows(),
            bytes_written: bytes,
            path: path.to_string_lossy().into_owned(),
        })
    }
}

fn target_path(spec: &LocalParquetSpec, load_id: &LoadId) -> PathBuf {
    let mut p = PathBuf::from(&spec.base_path);
    p.push(load_id.pipeline_id.as_uuid().to_string());
    p.push(load_id.run_id.as_uuid().to_string());
    p.push(format!("batch-{:05}.parquet", load_id.batch_seq));
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use common_types::ids::{PipelineId, RunId};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn tiny_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn load_writes_parquet_file_readable_back() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = DestinationSpec::LocalParquet(LocalParquetSpec {
            base_path: tmp.path().to_string_lossy().into_owned(),
        });
        let loader = LocalParquetLoader;
        loader.validate(&spec).await.unwrap();

        let load_id = LoadId {
            pipeline_id: PipelineId::new(),
            run_id: RunId::new(),
            batch_seq: 0,
        };
        let res = loader.load(&spec, load_id.clone(), tiny_batch()).await.unwrap();
        assert_eq!(res.rows_loaded, 3);
        assert!(res.bytes_written > 0);

        // Read it back.
        let f = File::open(&res.path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn load_idempotent_for_same_load_id() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = DestinationSpec::LocalParquet(LocalParquetSpec {
            base_path: tmp.path().to_string_lossy().into_owned(),
        });
        let loader = LocalParquetLoader;
        loader.validate(&spec).await.unwrap();
        let load_id = LoadId {
            pipeline_id: PipelineId::new(),
            run_id: RunId::new(),
            batch_seq: 5,
        };
        let r1 = loader.load(&spec, load_id.clone(), tiny_batch()).await.unwrap();
        let r2 = loader.load(&spec, load_id.clone(), tiny_batch()).await.unwrap();
        // Same path, same size (we wrote identical content).
        assert_eq!(r1.path, r2.path);
        assert_eq!(r1.bytes_written, r2.bytes_written);
    }
}
```

- [ ] **Step 3: Add `tempfile` dev-dep to worker**

Edit `crates/worker/Cargo.toml` `[dev-dependencies]`:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p worker --lib loaders::parquet_local`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker/loaders): LocalParquetLoader with idempotent writes

Writes to <base>/<pipeline_id>/<run_id>/batch-<seq>.parquet via the
arrow ArrowWriter. Same LoadId overwrites the same file, so Temporal
retries produce identical final bytes. Unit tests round-trip through
parquet and verify idempotency.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: `SyncActivities` — discover / read / load / commit

**Files:**
- Create: `crates/worker/src/activities/sync/mod.rs`
- Create: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/activities/mod.rs`

The four activities all run on a single `SyncActivities` struct that holds an `Arc<Catalog>` plus the connector and loader instances. Arrow batches move between activities as base64-encoded Arrow IPC bytes inside JSON payloads — serde's default `Vec<u8>`-as-number-array would explode payload size. (Large-batch streaming via staging storage is Phase I.3.)

- [ ] **Step 1: Define activity I/O types**

Create `crates/worker/src/activities/sync/inputs.rs`:

```rust
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::{DestinationSpec, SourceSpec};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverOutput {
    /// Column names from the discovered schema.
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub cursor: Option<CursorValue>,
    pub batch_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchOutput {
    /// Base64-encoded Arrow IPC stream. Empty when rows == 0.
    pub batch_ipc_b64: String,
    pub rows: usize,
    pub new_cursor: Option<CursorValue>,
    pub is_final: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchInput {
    pub destination: DestinationSpec,
    pub batch_ipc_b64: String,
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub batch_seq: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchOutput {
    pub rows_loaded: usize,
    pub bytes_written: u64,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitCursorInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub stream_name: String,
    pub cursor: Option<CursorValue>,
}
```

- [ ] **Step 2: Implement `SyncActivities`**

Create `crates/worker/src/activities/sync/mod.rs`:

```rust
//! Phase I.2 sync activities: discover / read_batch / load_batch / commit_cursor.
//!
//! Large-batch transfer: each read_batch call encodes the Arrow IPC stream as
//! base64 inside the activity output; load_batch decodes. This is pragmatic
//! for Phase I.2's small-batch tests; Phase I.3 migrates to staging storage.

pub mod inputs;

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use catalog::Catalog;
use common_types::connection_config::ConnectionConfig;
use common_types::ids::{PipelineId, RunId};
use connector_sdk::SourceConnector;
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::postgres::PostgresConnector;
use crate::loaders::parquet_local::LocalParquetLoader;
use inputs::*;

pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
}

fn to_retryable(e: anyhow::Error) -> ActivityError {
    ActivityError::Retryable(e.into())
}

fn encode_batch(batch: &RecordBatch) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema())
            .context("StreamWriter::try_new")?;
        if batch.num_rows() > 0 {
            w.write(batch).context("StreamWriter::write")?;
        }
        w.finish().context("StreamWriter::finish")?;
    }
    Ok(BASE64.encode(&buf))
}

fn decode_batch(b64: &str) -> anyhow::Result<RecordBatch> {
    let bytes = BASE64.decode(b64).context("base64 decode")?;
    let mut reader = StreamReader::try_new(&*bytes, None).context("StreamReader::try_new")?;
    let batch = reader
        .next()
        .context("stream had no batches")?
        .context("decoding batch")?;
    Ok(batch)
}

#[activities]
impl SyncActivities {
    /// Introspect the source and return its columns. Used for pre-run validation;
    /// the full Schema is rederived on read_batch.
    #[activity]
    pub async fn discover_stream(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverInput,
    ) -> Result<DiscoverOutput, ActivityError> {
        let schema = PostgresConnector
            .discover(
                &ConnectionConfig { url: input.source_url },
                &input.source,
            )
            .await
            .map_err(to_retryable)?;
        let columns = schema.fields().iter().map(|f| f.name().clone()).collect();
        Ok(DiscoverOutput { columns })
    }

    /// Read up to batch_size rows past `cursor`. Output encodes the Arrow batch
    /// as base64.
    #[activity]
    pub async fn read_batch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadBatchInput,
    ) -> Result<ReadBatchOutput, ActivityError> {
        let outcome = PostgresConnector
            .read_batch(
                &ConnectionConfig { url: input.source_url },
                &input.source,
                input.cursor,
                input.batch_size,
            )
            .await
            .map_err(to_retryable)?;

        let rows = outcome.batch.num_rows();
        let b64 = encode_batch(&outcome.batch).map_err(to_retryable)?;

        Ok(ReadBatchOutput {
            batch_ipc_b64: b64,
            rows,
            new_cursor: outcome.new_cursor,
            is_final: outcome.is_final,
        })
    }

    /// Idempotent load. Retries with same (pipeline_id, run_id, batch_seq)
    /// overwrite the same Parquet file.
    #[activity]
    pub async fn load_batch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: LoadBatchInput,
    ) -> Result<LoadBatchOutput, ActivityError> {
        let batch = decode_batch(&input.batch_ipc_b64).map_err(to_retryable)?;
        let load_id = LoadId {
            pipeline_id: PipelineId::from_uuid_unchecked(input.pipeline_id),
            run_id: RunId::from_uuid_unchecked(input.run_id),
            batch_seq: input.batch_seq,
        };
        let res = LocalParquetLoader
            .load(&input.destination, load_id, batch)
            .await
            .map_err(to_retryable)?;
        Ok(LoadBatchOutput {
            rows_loaded: res.rows_loaded,
            bytes_written: res.bytes_written,
            path: res.path,
        })
    }

    /// Advance the cursor in catalog. Idempotent (UPSERT to same value).
    #[activity]
    pub async fn commit_cursor(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CommitCursorInput,
    ) -> Result<(), ActivityError> {
        let pid = PipelineId::from_uuid_unchecked(input.pipeline_id);
        let rid = Some(RunId::from_uuid_unchecked(input.run_id));
        self.catalog
            .upsert_stream_state(pid, &input.stream_name, input.cursor, rid)
            .await
            .map_err(|e| ActivityError::Retryable(anyhow::anyhow!("upsert cursor: {e}").into()))?;
        Ok(())
    }
}
```

- [ ] **Step 3: Wire into activities/mod.rs**

Replace `crates/worker/src/activities/mod.rs`:

```rust
//! Activities: concrete units of work (RFC-4).
pub mod run_lifecycle;
pub mod sync;
```

- [ ] **Step 4: Build**

Run: `cargo build -p worker`
Expected: compiles clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker/activities): SyncActivities — discover/read/load/commit

Four #[activity] methods on one struct holding Arc<Catalog>. Batches
move between read_batch and load_batch as base64-encoded Arrow IPC
payloads inside JSON (pragmatic for Phase I.2; staging-storage transfer
is Phase I.3). Each activity is idempotent: read is stateless,
load overwrites same path for same LoadId, commit_cursor is upsert.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Rewrite `PipelineRunWorkflow` — real read→load→commit loop

**Files:**
- Modify: `crates/worker/src/workflows/pipeline_run.rs`
- Modify: `crates/worker/src/workflows/mod.rs`

The workflow's state tracks `cursor`. The input carries the spec + source URL + initial cursor so the workflow is self-contained once started.

- [ ] **Step 1: Rewrite the workflow**

Replace `crates/worker/src/workflows/pipeline_run.rs`:

```rust
//! PipelineRunWorkflow: the canonical single-run workflow (RFC-4).
//!
//! Phase I.2 shape:
//!   start_run
//!   → discover_stream
//!   → loop { read_batch → load_batch → commit_cursor }
//!   → complete_run
//!
//! Cursor advances only after load_batch succeeds. Temporal replay +
//! deterministic LoadId + idempotent activities give at-least-once
//! delivery with correct resumption across worker restarts.

use common_types::connection_config::ConnectionConfig;
use common_types::cursor::CursorValue;
use common_types::pipeline_spec::{DestinationSpec, PipelineSpec, SourceSpec};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowContextView, WorkflowResult};
use uuid::Uuid;

use crate::activities::run_lifecycle::RunLifecycleActivities;
use crate::activities::sync::SyncActivities;
use crate::activities::sync::inputs::{
    CommitCursorInput, DiscoverInput, LoadBatchInput, ReadBatchInput,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineRunInput {
    pub run_id: Uuid,
    pub pipeline_id: Uuid,
    pub spec: PipelineSpec,
    pub source_connection: ConnectionConfig,
    pub initial_cursor: Option<CursorValue>,
    /// Name of the stream being synced. Phase I.2 uses the source table name.
    pub stream_name: String,
}

#[workflow]
pub struct PipelineRunWorkflow {
    run_id: Uuid,
    pipeline_id: Uuid,
    spec: PipelineSpec,
    source_connection: ConnectionConfig,
    cursor: Option<CursorValue>,
    stream_name: String,
}

fn opts_short() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

fn opts_long() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(300)),
        ..Default::default()
    }
}

fn source_url(conn: &ConnectionConfig) -> String {
    conn.url.clone()
}

#[workflow_methods]
impl PipelineRunWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, input: PipelineRunInput) -> Self {
        Self {
            run_id: input.run_id,
            pipeline_id: input.pipeline_id,
            spec: input.spec,
            source_connection: input.source_connection,
            cursor: input.initial_cursor,
            stream_name: input.stream_name,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let (run_id, pipeline_id, spec, conn, stream_name) = ctx.state(|s| {
            (
                s.run_id,
                s.pipeline_id,
                s.spec.clone(),
                s.source_connection.clone(),
                s.stream_name.clone(),
            )
        });

        ctx.start_activity(RunLifecycleActivities::start_run, run_id, opts_short())
            .await?;

        ctx.start_activity(
            SyncActivities::discover_stream,
            DiscoverInput {
                source: spec.source.clone(),
                source_url: source_url(&conn),
            },
            opts_short(),
        )
        .await?;

        let mut batch_seq: u32 = 0;
        loop {
            let cursor = ctx.state(|s| s.cursor.clone());

            let read_out = ctx
                .start_activity(
                    SyncActivities::read_batch,
                    ReadBatchInput {
                        source: spec.source.clone(),
                        source_url: source_url(&conn),
                        cursor,
                        batch_size: spec.batch_size,
                    },
                    opts_long(),
                )
                .await?;

            if read_out.rows == 0 {
                break;
            }

            ctx.start_activity(
                SyncActivities::load_batch,
                LoadBatchInput {
                    destination: spec.destination.clone(),
                    batch_ipc_b64: read_out.batch_ipc_b64,
                    pipeline_id,
                    run_id,
                    batch_seq,
                },
                opts_long(),
            )
            .await?;

            ctx.start_activity(
                SyncActivities::commit_cursor,
                CommitCursorInput {
                    pipeline_id,
                    run_id,
                    stream_name: stream_name.clone(),
                    cursor: read_out.new_cursor.clone(),
                },
                opts_short(),
            )
            .await?;

            ctx.state_mut(|s| s.cursor = read_out.new_cursor);

            batch_seq += 1;
            if read_out.is_final {
                break;
            }
        }

        ctx.start_activity(RunLifecycleActivities::complete_run, run_id, opts_short())
            .await?;

        Ok(())
    }
}
```

- [ ] **Step 2: Re-export updated input**

Replace `crates/worker/src/workflows/mod.rs`:

```rust
//! Workflows: orchestration logic (RFC-4).
pub mod pipeline_run;

pub use pipeline_run::{PipelineRunInput, PipelineRunWorkflow};
```

- [ ] **Step 3: Build**

Run: `cargo build -p worker`
Expected: compiles clean. (Some `ActivityOptions` Default::default and `Duration` imports may need tweaks — fix reported errors directly.)

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/workflows): PipelineRunWorkflow with real sync loop

Replaces the 30s timer with start_run → discover → N×(read→load→
commit) → complete_run. Cursor advances only after load_batch. Workflow
state carries spec + connection + cursor + stream_name; input adds
initial_cursor so resumption across pipeline *runs* uses the persisted
value.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Register `SyncActivities` in the worker

**Files:**
- Modify: `crates/worker/src/main.rs`

- [ ] **Step 1: Wire the new activity struct**

Edit `crates/worker/src/main.rs`. Replace the body to include both activity registrations:

```rust
use anyhow::Context;
use catalog::Catalog;
use std::sync::Arc;
use temporalio_common::worker::{
    WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes,
};
use temporalio_sdk::{Worker, WorkerOptions};
use worker::{
    activities::run_lifecycle::RunLifecycleActivities,
    activities::sync::SyncActivities,
    temporal::{make_client, make_runtime, TemporalConfig},
    workflows::PipelineRunWorkflow,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Arc::new(Catalog::connect(&db_url).await?);
    catalog.migrate().await?;

    let cfg = TemporalConfig::from_env()?;
    tracing::info!(
        address = %cfg.address,
        namespace = %cfg.namespace,
        task_queue = %cfg.task_queue,
        "worker booting",
    );

    let runtime = make_runtime()?;
    let client = make_client(&cfg).await?;

    let lifecycle = RunLifecycleActivities { catalog: catalog.clone() };
    let sync = SyncActivities { catalog: catalog.clone() };

    let worker_options = WorkerOptions::new(cfg.task_queue.clone())
        .task_types(WorkerTaskTypes::all())
        .deployment_options(WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "etl".to_owned(),
                build_id: "etl-worker-0.2".to_owned(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        })
        .register_activities(lifecycle)
        .register_activities(sync)
        .register_workflow::<PipelineRunWorkflow>()
        .build();

    let mut w = Worker::new(&runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("Worker::new: {e}"))?;
    tracing::info!("worker polling");
    w.run()
        .await
        .map_err(|e| anyhow::anyhow!("Worker::run: {e}"))?;
    Ok(())
}
```

The `Worker::new(&runtime, client, worker_options)` form matches what Phase I.1's main.rs already uses — the existing `temporal.rs` helpers (`make_runtime`, `make_client`) return what's needed directly.

- [ ] **Step 2: Build and do a smoke-run of the worker**

Run: `cargo build -p worker`

Run (in one terminal): `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo run --bin worker`

Expected logs: `worker booting`, `worker polling`. Ctrl-C to stop.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker): register SyncActivities alongside RunLifecycleActivities

Build-id bumped to etl-worker-0.2. Worker task-types: all (workflow +
activity).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: CLI — build full `PipelineRunInput` from catalog

**Files:**
- Modify: `crates/cli/src/main.rs`

The CLI now reads the pipeline's `spec` (JSONB), the source connection's `config` (JSONB), and the current cursor from `stream_state`. Fails loudly if any is missing or misshaped.

- [ ] **Step 1: Rewrite pipeline_run body**

Edit `crates/cli/src/main.rs`. Replace the entire `pipeline_run` function (and any needed imports at the top):

```rust
use anyhow::{Context, bail};
use catalog::{Catalog, NewRun};
use clap::{Parser, Subcommand};
use common_types::connection_config::ConnectionConfig;
use common_types::ids::{PipelineId, RunId};
use common_types::pipeline_spec::{PipelineSpec, SourceSpec};
use std::time::Duration;
use temporalio_client::WorkflowStartOptions;
use worker::temporal::{make_client, TemporalConfig};
use worker::workflows::{PipelineRunInput, PipelineRunWorkflow};

#[derive(Parser)]
#[command(name = "platform", version, about = "ETL platform CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },
}

#[derive(Subcommand)]
enum PipelineCmd {
    Run {
        id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => pipeline_run(id).await,
    }
}

async fn pipeline_run(id_str: String) -> anyhow::Result<()> {
    let pipeline_id = parse_pipeline_id(&id_str)
        .with_context(|| format!("parsing pipeline id '{}'", id_str))?;

    let db_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let catalog = Catalog::connect(&db_url).await?;
    catalog.migrate().await?;

    let pipeline = catalog
        .get_pipeline(pipeline_id)
        .await?
        .with_context(|| format!("pipeline {} not found", pipeline_id))?;

    // Parse the pipeline spec.
    let spec: PipelineSpec = serde_json::from_value(pipeline.spec.clone())
        .context("pipelines.spec did not deserialize as PipelineSpec")?;

    // Load the source connection.
    let source_conn_row = catalog
        .get_connection(pipeline.source_conn_id)
        .await?
        .with_context(|| format!("source connection {} not found", pipeline.source_conn_id))?;
    let source_connection: ConnectionConfig =
        serde_json::from_value(source_conn_row.config.clone())
            .context("source connections.config did not deserialize as ConnectionConfig")?;

    // Stream name for Phase I.2: the source table name.
    let stream_name = match &spec.source {
        SourceSpec::Postgres(p) => p.table.clone(),
    };

    // Current cursor from stream_state (None on first run).
    let initial_cursor = catalog
        .get_stream_state(pipeline_id, &stream_name)
        .await?
        .and_then(|s| s.cursor);

    // Build run + submit.
    let run_id = RunId::new();
    let workflow_id = format!("run-{}", run_id.as_uuid());

    catalog
        .create_run(NewRun {
            run_id,
            tenant_id: pipeline.tenant_id,
            pipeline_id,
            trigger: "manual".into(),
            temporal_workflow_id: Some(workflow_id.clone()),
        })
        .await?;

    let cfg = TemporalConfig::from_env()?;
    let client = make_client(&cfg).await?;

    let opts = WorkflowStartOptions::new(cfg.task_queue.clone(), workflow_id.clone())
        .execution_timeout(Duration::from_secs(3600))
        .run_timeout(Duration::from_secs(3600))
        .task_timeout(Duration::from_secs(60))
        .build();

    let input = PipelineRunInput {
        run_id: run_id.as_uuid(),
        pipeline_id: pipeline_id.as_uuid(),
        spec,
        source_connection,
        initial_cursor,
        stream_name,
    };

    client
        .start_workflow(PipelineRunWorkflow::run, input, opts)
        .await
        .context("starting PipelineRunWorkflow")?;

    println!("started workflow {}", workflow_id);
    println!("run id: {}", run_id);
    Ok(())
}

fn parse_pipeline_id(s: &str) -> anyhow::Result<PipelineId> {
    if let Ok(p) = s.parse::<PipelineId>() {
        return Ok(p);
    }
    let u = uuid::Uuid::parse_str(s)?;
    Ok(PipelineId::from_uuid_unchecked(u))
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p cli`
Expected: compiles clean.

- [ ] **Step 3: Commit**

```bash
git add crates/cli
git commit -m "feat(cli): construct full PipelineRunInput from catalog

pipeline run <id> now reads spec (JSONB → PipelineSpec), source
connection (JSONB → ConnectionConfig), and current cursor from
stream_state. Fails with a clear error if any is missing or misshaped.
Workflow/run/task timeouts bumped to 1h so long syncs don't time out
mid-loop.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: End-to-end integration test — fresh + incremental sync

**Files:**
- Create: `tests/integration/tests/incremental_sync.rs`

Workflow:
1. Reseed source DB (10 rows, 2026-04-20 → 2026-04-22)
2. Reseed catalog with tenant/connection/pipeline that has a real spec
3. CLI: `platform pipeline run <pipe>` → wait for completion → assert 10 rows in Parquet file(s), cursor = latest updated_at
4. Add 5 more rows dated 2026-04-23
5. CLI: run again → assert only 5 new rows in new Parquet batch, cursor advanced

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/incremental_sync.rs`:

```rust
use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn source_url() -> String {
    std::env::var("SOURCE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
}

async fn reseed_source_10_rows() -> anyhow::Result<()> {
    let status = Command::new("bash")
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());
    Ok(())
}

async fn add_5_more_rows() -> anyhow::Result<()> {
    let sql = r#"
INSERT INTO customers (id, name, email, created_at, updated_at) VALUES
  (11,'Kim',   'kim@example.com',   '2026-04-23 09:00:00+00', '2026-04-23 09:00:00+00'),
  (12,'Leo',   'leo@example.com',   '2026-04-23 10:00:00+00', '2026-04-23 10:00:00+00'),
  (13,'Mia',   NULL,                '2026-04-23 11:00:00+00', '2026-04-23 11:00:00+00'),
  (14,'Ned',   'ned@example.com',   '2026-04-23 12:00:00+00', '2026-04-23 12:00:00+00'),
  (15,'Olga',  'olga@example.com',  '2026-04-23 13:00:00+00', '2026-04-23 13:00:00+00');
"#;
    let status = Command::new("docker")
        .args(["exec", "-i", "etl-postgres", "psql", "-U", "etl", "-d", "etl_source_demo"])
        .stdin(std::process::Stdio::piped())
        .spawn()?
        .wait_with_stdin(sql)
        .await?;
    if !status.success() {
        anyhow::bail!("add_5_more_rows failed");
    }
    Ok(())
}

// The tokio Child API doesn't natively let us pipe a stdin string after .spawn()
// AND wait for exit in one call. Small helper used above.
trait ChildExt {
    async fn wait_with_stdin(self, sql: &str) -> anyhow::Result<std::process::ExitStatus>;
}
impl ChildExt for Child {
    async fn wait_with_stdin(mut self, sql: &str) -> anyhow::Result<std::process::ExitStatus> {
        use tokio::io::AsyncWriteExt;
        if let Some(mut s) = self.stdin.take() {
            s.write_all(sql.as_bytes()).await?;
            s.shutdown().await?;
        }
        Ok(self.wait().await?)
    }
}

async fn spawn_worker(data_path: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .env("ETL_DATA_DIR", data_path) // unused by worker but left for clarity
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

fn workspace_root() -> PathBuf {
    // tests/integration/Cargo.toml → up two
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn seed_catalog(data_base: &Path) -> anyhow::Result<uuid::Uuid> {
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "source-demo".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": source_url() }),
        })
        .await?;

    let spec = json!({
        "source": {
            "type": "postgres",
            "schema": "public",
            "table": "customers",
            "cursor_column": "updated_at",
            "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"]
        },
        "destination": {
            "type": "local_parquet",
            "base_path": data_base.to_string_lossy()
        },
        "batch_size": 4
    });

    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "customers-sync".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;
    Ok(pipe.as_uuid())
}

async fn run_cli(pipe_uuid: uuid::Uuid) -> anyhow::Result<()> {
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &format!("pipe-{}", pipe_uuid)])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn wait_for_last_run_status(
    cat: &Catalog,
    want: RunStatus,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM runs ORDER BY started_at DESC LIMIT 1",
        )
        .fetch_optional(cat.pool())
        .await?;
        if let Some((s,)) = row {
            if RunStatus::parse(&s) == Some(want) {
                return Ok(());
            }
            if RunStatus::parse(&s) == Some(RunStatus::Failed) {
                anyhow::bail!("run failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!("timeout waiting for {:?}", want);
}

fn count_rows_in_dir(dir: &Path) -> usize {
    let mut total = 0usize;
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                total += batch.unwrap().num_rows();
            }
        }
    }
    total
}

#[tokio::test]
#[ignore = "requires docker stack (postgres + temporal) and source-demo seeded"]
async fn incremental_sync_picks_up_only_new_rows() -> anyhow::Result<()> {
    let build = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(build.success());

    reseed_source_10_rows().await?;

    let tmp = tempfile::tempdir()?;
    let data = tmp.path().to_owned();

    let pipe_uuid = seed_catalog(&data).await?;
    let mut w = spawn_worker(&data).await?;

    // --- Run 1: expect 10 rows total ---
    run_cli(pipe_uuid).await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    wait_for_last_run_status(&cat, RunStatus::Completed, Duration::from_secs(60)).await?;
    let run1_total = count_rows_in_dir(&data);
    assert_eq!(run1_total, 10, "run 1 should load 10 rows");

    // Confirm cursor advanced to last row's timestamp.
    let state = cat
        .get_stream_state(
            common_types::ids::PipelineId::from_uuid_unchecked(pipe_uuid),
            "customers",
        )
        .await?
        .unwrap();
    assert!(state.cursor.as_ref().unwrap().value.starts_with("2026-04-22T11:00"));

    // --- Insert 5 more rows ---
    add_5_more_rows().await?;

    // --- Run 2: expect only 5 new rows ---
    run_cli(pipe_uuid).await?;
    wait_for_last_run_status(&cat, RunStatus::Completed, Duration::from_secs(60)).await?;
    let run2_total = count_rows_in_dir(&data);
    assert_eq!(run2_total, 15, "after run 2 total should be 15 (10 + 5 new)");

    let state2 = cat
        .get_stream_state(
            common_types::ids::PipelineId::from_uuid_unchecked(pipe_uuid),
            "customers",
        )
        .await?
        .unwrap();
    assert!(state2.cursor.as_ref().unwrap().value.starts_with("2026-04-23T13:00"));

    w.kill().await?;
    w.wait().await?;
    Ok(())
}
```

- [ ] **Step 2: Add `tempfile` and `walkdir` + required features to integration-tests**

Edit `tests/integration/Cargo.toml`:

```toml
[package]
name = "integration-tests"
version = "0.1.0"
edition.workspace = true
publish = false

[dependencies]
catalog = { workspace = true }
common-types = { workspace = true }
tokio = { workspace = true }
anyhow = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
sqlx = { workspace = true }
tempfile = "3"
walkdir = "2"
parquet = "53"
arrow = { workspace = true }
```

- [ ] **Step 3: Run the test**

Precondition: docker stack up (`docker compose up -d`), source-demo seeded (`bash scripts/seed-source-demo.sh`).

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p integration-tests incremental_sync -- --ignored --nocapture`
Expected: **1 passed** after ~30–60s.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(integration): fresh+incremental sync end-to-end

Seeds source with 10 rows, seeds catalog with a real Postgres→Parquet
pipeline, submits via CLI, asserts 10 rows land in Parquet files and
cursor advances. Adds 5 more rows, runs again, asserts incremental
behavior (total 15, only 5 new). Ignored by default (#[ignore]);
requires docker stack + seed script.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: Durability test — kill worker mid-batch, verify correctness

**Files:**
- Create: `tests/integration/tests/durability_midbatch.rs`

Uses a larger source fixture (100 rows) and small batch size (10) so the run takes many iterations. Kills the worker after a short delay, spawns a fresh worker, verifies eventual completion + correct total row count.

- [ ] **Step 1: Add a helper that seeds 100 rows**

Append to `scripts/seed-source-demo.sh` a new case (or create a new script). For this task, create `scripts/seed-source-demo-big.sh`:

```bash
#!/usr/bin/env bash
# Reseeds etl_source_demo.customers with 100 rows across 100 distinct
# updated_at timestamps (1 per minute starting 2026-04-20T00:00:00Z).
set -euo pipefail

docker exec -i etl-postgres psql -U etl -d postgres <<'SQL'
SELECT 'CREATE DATABASE etl_source_demo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_source_demo')\gexec
SQL

docker exec -i etl-postgres psql -U etl -d etl_source_demo <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id         BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
TRUNCATE customers;
INSERT INTO customers (id, name, email, created_at, updated_at)
SELECT
    g,
    'user-' || g,
    CASE WHEN g % 3 = 0 THEN NULL ELSE 'u' || g || '@example.com' END,
    TIMESTAMPTZ '2026-04-20 00:00:00+00' + (g * interval '1 minute'),
    TIMESTAMPTZ '2026-04-20 00:00:00+00' + (g * interval '1 minute')
FROM generate_series(1, 100) AS g;
SELECT COUNT(*) AS customer_count FROM customers;
SQL
```

Make executable: `chmod +x scripts/seed-source-demo-big.sh`.

- [ ] **Step 2: Write the test**

Create `tests/integration/tests/durability_midbatch.rs`:

```rust
use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn source_url() -> String {
    std::env::var("SOURCE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_source_demo".into())
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawning worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

fn count_parquet_rows(dir: &Path) -> usize {
    let mut total = 0usize;
    for entry in walkdir::WalkDir::new(dir).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                total += batch.unwrap().num_rows();
            }
        }
    }
    total
}

#[tokio::test]
#[ignore = "requires docker stack + big source seed; takes ~2–3 minutes"]
async fn sync_survives_worker_kill_midbatch() -> anyhow::Result<()> {
    // Build binaries.
    let build = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(build.success());

    // Seed 100 rows.
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo-big.sh")
        .status()
        .await?;
    assert!(status.success());

    let tmp = tempfile::tempdir()?;
    let data = tmp.path().to_owned();

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": source_url() }),
        })
        .await?;
    let spec = json!({
        "source": {
            "type": "postgres",
            "schema": "public",
            "table": "customers",
            "cursor_column": "updated_at",
            "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"]
        },
        "destination": {
            "type": "local_parquet",
            "base_path": data.to_string_lossy()
        },
        "batch_size": 10
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "big-sync".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    // Worker #1: run the CLI, let it work for a bit, then kill.
    let mut w1 = spawn_worker().await?;
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &format!("pipe-{}", pipe.as_uuid())])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success());

    // Let the workflow process a handful of batches.
    tokio::time::sleep(Duration::from_secs(5)).await;
    w1.kill().await?;
    w1.wait().await?;

    // Worker #2: resumes.
    let mut w2 = spawn_worker().await?;

    // Wait up to 180s for completion.
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        if std::time::Instant::now() > deadline {
            anyhow::bail!("timed out waiting for completion");
        }
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool())
                .await?;
        if let Some((s,)) = row {
            if s == "completed" {
                break;
            }
            if s == "failed" {
                anyhow::bail!("run failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    w2.kill().await?;
    w2.wait().await?;

    // Verify final state.
    let total = count_parquet_rows(&data);
    assert_eq!(total, 100, "final Parquet row count must equal source rows");

    let state = cat
        .get_stream_state(pipe, "customers")
        .await?
        .unwrap();
    // Cursor of row id=100: 2026-04-20 + 100 minutes.
    assert!(state.cursor.as_ref().unwrap().value.starts_with("2026-04-20T01:40"));

    Ok(())
}
```

- [ ] **Step 3: Run the test**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p integration-tests sync_survives_worker_kill_midbatch -- --ignored --nocapture`
Expected: **1 passed** after ~2–3 min.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(integration): durability — worker kill mid-batch preserves correctness

Seeds 100 rows with batch_size=10 (10 loop iterations). Spawns worker,
submits, sleeps 5s, kills worker, spawns fresh worker. Asserts run
eventually completes, Parquet total = 100 rows (no loss, no duplicates
at the row level — each batch file is idempotent under same LoadId),
cursor at last row's timestamp. Validates the Phase I.2 exit criterion.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 16: README + Phase I.2 completion log

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-22-phase-1-2-first-pipeline.md` (this file)

- [ ] **Step 1: Replace README bootstrap section**

Edit `README.md`. Replace the "Local dev bootstrap" section with the Phase I.2 flow:

````markdown
## Local dev bootstrap

```bash
# 1. Start the full stack: Postgres (catalog + source), Temporal server, Temporal UI
docker compose up -d

# 2. Seed the source-demo database (creates etl_source_demo.customers with 10 rows)
bash scripts/seed-source-demo.sh

# 3. Env
cp .env.example .env
source .env

# 4. Build
cargo build --workspace

# 5. Seed a demo pipeline in the catalog
docker exec -i etl-postgres psql -U etl -d etl_catalog <<'SQL'
INSERT INTO tenants (tenant_id, name)
  VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('22222222-2222-2222-2222-222222222222',
          '11111111-1111-1111-1111-111111111111',
          'source-demo', 'postgres@0.1.0',
          '{"url":"postgres://etl:etl@localhost:5432/etl_source_demo"}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('33333333-3333-3333-3333-333333333333',
          '11111111-1111-1111-1111-111111111111',
          'customers-sync',
          '22222222-2222-2222-2222-222222222222',
          '{"source":{"type":"postgres","schema":"public","table":"customers","cursor_column":"updated_at","cursor_kind":"timestamp_tz","pk_columns":["id"]},"destination":{"type":"local_parquet","base_path":"./data"},"batch_size":4}'::jsonb);
SQL

# 6. Run the worker (separate terminal)
cargo run --bin worker

# 7. Submit the pipeline
cargo run --bin platform -- pipeline run pipe-33333333-3333-3333-3333-333333333333
```

Expected outcome:
- Worker logs show 3 `read_batch` calls (batch_size=4 against 10 rows → 4+4+2) followed by `run completed`
- Parquet files written under `./data/33333333-…/<run_id>/batch-0000{0,1,2}.parquet`
- `docker exec -i etl-postgres psql -U etl -d etl_catalog -c "SELECT stream_name, cursor_value FROM stream_state;"` shows `customers | 2026-04-22T11:00:00.000000Z`

Re-running the command replays zero rows (nothing new). Insert a row into `etl_source_demo.customers` with a later `updated_at`, re-run, and only that row lands in a new Parquet file.
````

- [ ] **Step 2: Append Phase I.2 completion log to this plan**

Append to the bottom of `docs/superpowers/plans/2026-04-22-phase-1-2-first-pipeline.md`:

```markdown

---

## Phase I.2 Completion Log

- [ ] Task 1 — stream_state migration
- [ ] Task 2 — common-types (cursor, connection_config, pipeline_spec)
- [ ] Task 3 — connector-sdk trait
- [ ] Task 4 — loader-sdk trait
- [ ] Task 5 — source demo DB seed
- [ ] Task 6 — catalog stream_state CRUD
- [ ] Task 7 — Postgres discover
- [ ] Task 8 — Postgres read_batch
- [ ] Task 9 — LocalParquetLoader
- [ ] Task 10 — SyncActivities
- [ ] Task 11 — PipelineRunWorkflow rewrite
- [ ] Task 12 — worker main registration
- [ ] Task 13 — CLI loads full spec
- [ ] Task 14 — incremental_sync integration test
- [ ] Task 15 — durability_midbatch integration test
- [ ] Task 16 — README + this log

### Exit criterion — to be marked when Tasks 14 and 15 pass

**At-least-once + PK dedup contract + durability across worker restart**, proven by the two integration tests. Flip to `[x]` when run green.

### Deviations

(Fill in as encountered during execution.)

### Handoff to Phase I.3

Phase I.3 (WASM runtime + SDK) converts the in-process `PostgresConnector` into a WASM Component Model artifact behind the same `SourceConnector` trait. The loader stays Rust-native per RFC-9. Base64-IPC transfer between activities stays until Phase I.3's Tier 3 streaming lands (RFC-5).
```

- [ ] **Step 3: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-22-phase-1-2-first-pipeline.md
git commit -m "docs: Phase I.2 README bootstrap + completion log

README shows the full flow: docker up → seed source → seed catalog →
worker → CLI. Completion log is empty checkboxes ready to be filled as
each task commits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Appendix — Troubleshooting

**`sqlx` fails to connect to `etl_source_demo`.**
If the container was already running before `docker-compose.yml` was updated, the init script never ran. Run `bash scripts/seed-source-demo.sh` manually. To force a re-init: `docker compose down -v` then `docker compose up -d` (destroys ALL volumes including catalog data — only do this in dev).

**Cursor value mismatch after running test suite.**
`truncate_all_for_tests` drops `stream_state` rows. Re-running the CLI after a test treats the pipeline as "never synced" and re-reads everything. This is intentional.

**Activity timeout on `read_batch` with large batches.**
Phase I.2 `opts_long` gives `read_batch` 300s `start_to_close`. If you set `batch_size > ~5000` against a slow Postgres, extend the timeout or reduce batch size.

**Base64 IPC payload exceeds Temporal's 2MB default payload limit.**
Phase I.2 accepts this as a known limitation; keep `batch_size` ≤ 1000 for Phase I.2 demos. Phase I.3 migrates inter-activity transport to staging-storage references.

**Parquet file from a failed run orphaned on disk.**
Expected. Cleanup is Phase I.4 (retention policies). Manually `rm -rf ./data/<pipeline_uuid>` if needed.

## Appendix — What's deferred

Not in Phase I.2:
- Non-decreasing cursor types + overlap-at-boundary + PK dedup at destination (Phase I.5 — transformations can dedupe; Phase II.3 — warehouse MERGE)
- Dead-letter table / rejected-row routing (Phase I.5, RFC-9 subset)
- Schema evolution, schema entities in catalog (Phase I.4)
- WASM sandboxing for connectors (Phase I.3)
- Transformation stage between read and load (Phase I.5)
- Multi-tenancy enforcement (Phase II.1)
- Secrets: source_url currently lives in `connections.config` as plaintext (Phase II.2)
- Retention/compaction of staging and output files (Phase I.4)
- Additional Postgres column types (numeric, jsonb, uuid, arrays) (Phase I.4)
- CDC mode (Phase I.6)

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-22-phase-1-2-first-pipeline.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
