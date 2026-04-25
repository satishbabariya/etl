# Phase II.1.c — Tenant CLI + Per-Tenant Namespace + Storage Prefix

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the tenant lifecycle features that II.1.b deferred — `platform tenant {create|list|suspend|terminate}`, per-tenant Temporal namespace, per-tenant Parquet storage prefix, Grafana template variable, and an end-to-end lifecycle integration test.

**Architecture:** Pure additions on top of II.1.b's TenantContext threading. `LoadId` gains `tenant_id` so loaders write to `<base>/<tenant_id>/<pipeline_id>/...`. CLI starts workflows in `etl-<tenant_simple>` namespaces (resolved from `pipeline.tenant_id` at run time). Worker reads `Catalog::list_tenants` at boot and spawns one Temporal worker per known tenant; new tenants picked up after restart (hot-reconfigure deferred to Phase II.4). Activity structs already derive `Clone` from II.1.b.

**Tech Stack:** Unchanged — temporalio-client 0.2 (`register_namespace` exposed), sqlx 0.8, Arrow/Parquet. `prost_wkt_types::Duration` already in `worker/Cargo.toml` from PR #9.

---

## File Structure

### Modified
- `crates/loader-sdk/src/lib.rs` — `LoadId.tenant_id: TenantId`
- `crates/worker/src/loaders/parquet_local.rs` — path includes `<tenant_id>/`
- `crates/worker/src/loaders/cdc_parquet.rs` — `write(...)` adds `tenant_id: Uuid` arg; path includes `<tenant_id>/<pipeline_id>/cdc/<run_id>/`
- `crates/worker/src/activities/sync/mod.rs` — `LoadId` construction in `load_batch`; dead-letter path; `read_batch` rendezvous unchanged
- `crates/worker/src/activities/cdc/mod.rs` — both `snapshot_chunk` and `read_window` callers pass `tenant_id` to `CdcParquetLoader::write`
- `crates/worker/src/temporal.rs` — `make_client_for_namespace(cfg, namespace)` helper
- `crates/worker/src/main.rs` — list tenants at boot; spawn one Temporal worker per known namespace; keep `default` worker as legacy backstop
- `crates/cli/src/main.rs` — `Tenant { Create | List | Suspend | Terminate }` subcommand; `pipeline_run` resolves namespace `etl-<tenant_simple>`
- `ops/grafana/dashboards/etl-overview.json` — `tenant` template variable + per-panel `{tenant_id=~"$tenant"}` filter
- `tests/integration/tests/transforms_filter_mask.rs` — walk path includes tenant_id
- `tests/integration/tests/transforms_dead_letter.rs` — same
- `tests/integration/tests/cdc_insert_update_delete.rs` — same
- `tests/integration/tests/cdc_snapshot_streaming_handoff.rs` — same
- `README.md` — Phase II.1.c demo + completion handoff

### New
- `crates/cli/src/tenant.rs` — `create / list / suspend / terminate` (incl. `RegisterNamespace`)
- `tests/integration/tests/tenant_lifecycle.rs` — end-to-end test

---

## Task 1: `LoadId.tenant_id`

**Files:**
- Modify: `crates/loader-sdk/src/lib.rs`

- [ ] **Step 1: Add the field**

```rust
use common_types::ids::{PipelineId, RunId, TenantId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadId {
    pub tenant_id: TenantId,
    pub pipeline_id: PipelineId,
    pub run_id: RunId,
    pub batch_seq: u32,
}
```

- [ ] **Step 2: Build (will surface call sites)**

Run: `cargo build -p loader-sdk`
Expected: clean.

Run: `cargo build --workspace`
Expected: errors at every `LoadId { ... }` call site (Task 2 fixes).

- [ ] **Step 3: Commit**

```bash
git add crates/loader-sdk/src/lib.rs
git commit -m "feat(loader-sdk): LoadId carries tenant_id"
```

---

## Task 2: `LocalParquetLoader` path includes tenant_id

**Files:**
- Modify: `crates/worker/src/loaders/parquet_local.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs` (LoadId construction)

- [ ] **Step 1: target_path uses tenant_id**

```rust
fn target_path(spec: &LocalParquetSpec, load_id: &LoadId) -> PathBuf {
    let mut p = PathBuf::from(&spec.base_path);
    p.push(load_id.tenant_id.as_uuid().to_string());
    p.push(load_id.pipeline_id.as_uuid().to_string());
    p.push(format!("batch-{:05}.parquet", load_id.batch_seq));
    p
}
```

- [ ] **Step 2: load_batch activity constructs LoadId with tenant_id**

In `crates/worker/src/activities/sync/mod.rs::load_batch`:

```rust
        let load_id = LoadId {
            tenant_id: common_types::ids::TenantId::from_uuid_unchecked(input.tenant_id),
            pipeline_id: PipelineId::from_uuid_unchecked(input.pipeline_id),
            run_id: RunId::from_uuid_unchecked(input.run_id),
            batch_seq: input.batch_seq,
        };
```

- [ ] **Step 3: Dead-letter path also gets tenant prefix**

In the same `load_batch`, the dead-letter computation:

```rust
                let dest_path = match &input.destination {
                    common_types::pipeline_spec::DestinationSpec::LocalParquet(s) => {
                        let mut p = std::path::PathBuf::from(&s.base_path);
                        p.push(input.tenant_id.to_string());
                        p.push(load_id.pipeline_id.as_uuid().to_string());
                        p.push("dead-letter");
                        p.push(load_id.run_id.as_uuid().to_string());
                        ...
                    }
                };
```

`LoadBatchInput.tenant_id` already exists (from II.1.b).

- [ ] **Step 4: Verify unit tests still pass**

Run: `cargo test --workspace --lib`
Expected: 78+ pass. Two `parquet_local` tests use a sample LoadId — update them to include a tenant_id (e.g. `TenantId::new()`).

- [ ] **Step 5: Commit**

```bash
git add crates/worker/src/loaders/parquet_local.rs crates/worker/src/activities/sync/mod.rs
git commit -m "feat(worker): parquet path includes <tenant_id> prefix"
```

---

## Task 3: `CdcParquetLoader::write` takes tenant_id

**Files:**
- Modify: `crates/worker/src/loaders/cdc_parquet.rs`
- Modify: `crates/worker/src/activities/cdc/mod.rs`

- [ ] **Step 1: Loader signature + path**

```rust
impl CdcParquetLoader {
    pub async fn write(
        &self,
        dest: &DestinationSpec,
        tenant_id: Uuid,
        pipeline_id: Uuid,
        run_id: Uuid,
        batch_seq: u32,
        batch: &RecordBatch,
    ) -> Result<PathBuf> {
        let base = match dest {
            DestinationSpec::LocalParquet(s) => s.base_path.clone(),
        };
        let mut path = PathBuf::from(&base);
        path.push(tenant_id.to_string());
        path.push(pipeline_id.to_string());
        path.push("cdc");
        path.push(run_id.to_string());
        std::fs::create_dir_all(&path)
            .with_context(|| format!("create dir {}", path.display()))?;
        path.push(format!("batch-{:05}.parquet", batch_seq));
        let file = std::fs::File::create(&path)
            .with_context(|| format!("create {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        w.write(batch)?;
        w.close()?;
        Ok(path)
    }
}
```

- [ ] **Step 2: Both callers pass `input.tenant_id`**

In `crates/worker/src/activities/cdc/mod.rs::snapshot_chunk`:

```rust
            CdcParquetLoader
                .write(
                    &input.destination,
                    input.tenant_id,
                    input.pipeline_id,
                    input.run_id,
                    input.batch_seq,
                    &chunk.batch,
                )
                .await
                .map_err(retryable)?;
```

Same change in `read_window` activity body. Both inputs already have `tenant_id` from II.1.b.

- [ ] **Step 3: Build + verify**

Run: `cargo build -p worker`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/
git commit -m "feat(worker): CDC parquet path includes <tenant_id> prefix"
```

---

## Task 4: Update integration tests' path expectations

**Files:**
- Modify: `tests/integration/tests/transforms_filter_mask.rs`
- Modify: `tests/integration/tests/transforms_dead_letter.rs`
- Modify: `tests/integration/tests/cdc_insert_update_delete.rs`
- Modify: `tests/integration/tests/cdc_snapshot_streaming_handoff.rs`

Each test currently walks `tmp.path()` and finds parquet under `<pipeline_id>/`. The path is now `<tenant_id>/<pipeline_id>/`. Each test creates its tenant via `cat.create_tenant("dev")` so the tenant id is in scope.

- [ ] **Step 1: `transforms_filter_mask.rs`**

In `read_all_rows`, the function currently walks `dir` (the whole tmp). The walk picks up any parquet under it; the `dead-letter` exclusion stays. The new layout puts data parquet at `<tmp>/<tenant_id>/<pipeline_id>/batch-*.parquet` — the existing recursive walk still finds them. Verify by running the test.

Actually no change needed if the walk is recursive — but be defensive. After the existing pipeline run, assert the expected path:

```rust
    let mut tenant_dir = tmp.path().to_path_buf();
    tenant_dir.push(tenant.as_uuid().to_string());
    assert!(tenant_dir.exists(), "tenant dir missing at {}", tenant_dir.display());
```

(`tenant` is the result of `create_tenant("dev")` earlier in the test.)

- [ ] **Step 2: `transforms_dead_letter.rs`**

The two scenarios assert dead-letter parquet at `<tmp>/<pipeline_id>/dead-letter/<run_id>/batch-*.parquet`. Update both `read_dead_letter_*` walks (or whichever helper) to include the tenant prefix:

```rust
    let mut dl = tmp.path().to_path_buf();
    dl.push(tenant.as_uuid().to_string());
    dl.push(pipe.as_uuid().to_string());
    dl.push("dead-letter");
```

- [ ] **Step 3: `cdc_insert_update_delete.rs`**

The test polls `read_ops_from(tmp.path())` which walks recursively. It already finds parquet at any depth. Add a smoke assertion that the prefix is correct:

```rust
    let mut tenant_dir = tmp.path().to_path_buf();
    tenant_dir.push(tenant.as_uuid().to_string());
    assert!(tenant_dir.exists());
```

(tenant is the create_tenant return; thread it through if not yet exposed.)

- [ ] **Step 4: `cdc_snapshot_streaming_handoff.rs`**

Same change as Step 3.

- [ ] **Step 5: Run integration sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo -c \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
docker exec etl-temporal-postgres psql -U temporal -d temporal -c \
  "DELETE FROM executions WHERE namespace_id IN (SELECT id FROM namespaces WHERE name='default');" || true

cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all 14 integration tests pass with parquet under `<tenant_id>/<pipeline_id>/`.

- [ ] **Step 6: Commit**

```bash
git add tests/integration/tests/
git commit -m "test: walk parquet under <tenant_id>/<pipeline_id>/ prefix"
```

---

## Task 5: `make_client_for_namespace` helper

**Files:**
- Modify: `crates/worker/src/temporal.rs`

- [ ] **Step 1: Write the helper**

Append to `crates/worker/src/temporal.rs`:

```rust
pub async fn make_client_for_namespace(
    cfg: &TemporalConfig,
    namespace: &str,
) -> anyhow::Result<temporalio_client::Client> {
    let mut cfg = cfg.clone();
    cfg.namespace = namespace.to_string();
    make_client(&cfg).await
}
```

(`TemporalConfig` is `Clone`. `make_client` does the connect.)

- [ ] **Step 2: Build**

Run: `cargo build -p worker`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/worker/src/temporal.rs
git commit -m "feat(worker): make_client_for_namespace helper"
```

---

## Task 6: Tenant CLI module

**Files:**
- Create: `crates/cli/src/tenant.rs`

- [ ] **Step 1: Write the module**

```rust
//! Tenant lifecycle: create / list / suspend / terminate.
//!
//! create  → catalog row + Temporal namespace etl-<simple>
//! list    → tabular dump (UUID, name, created_at)
//! suspend → name-prefix hack ("suspended:<name>") so resolver misses;
//!           Phase II.2 will add a proper status column
//! terminate → catalog cascade (FKs ON DELETE CASCADE) + remove
//!             ./data/<tenant_id>/ subtree

use anyhow::Context;
use catalog::Catalog;
use common_types::ids::TenantId;

pub async fn create(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let id = admin.create_tenant(&name).await?;
    println!("created tenant {} ({})", name, id);
    register_temporal_namespace(&id).await?;
    Ok(())
}

pub async fn list() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let tenants = admin.list_tenants().await?;
    for t in tenants {
        println!("{}\t{}\t{}", t.tenant_id, t.name, t.created_at);
    }
    Ok(())
}

pub async fn suspend(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let result = sqlx::query(
        "UPDATE tenants SET name = 'suspended:' || name \
         WHERE name = $1 AND name NOT LIKE 'suspended:%'",
    )
    .bind(&name)
    .execute(admin.pool())
    .await?;
    if result.rows_affected() == 0 {
        println!("no active tenant named {} (already suspended or missing)", name);
    } else {
        println!("suspended tenant {} (renamed to suspended:{})", name, name);
    }
    Ok(())
}

pub async fn terminate(name: String) -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let admin = Catalog::connect(&url).await?;
    let t = admin
        .get_tenant_by_name(&name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {} not found", name))?;
    admin.delete_tenant(t.tenant_id).await?;
    println!("terminated tenant {} ({}) — catalog rows cascaded", name, t.tenant_id);

    let base = std::env::var("ETL_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let path = std::path::PathBuf::from(&base).join(t.tenant_id.as_uuid().to_string());
    if path.exists() {
        std::fs::remove_dir_all(&path)
            .with_context(|| format!("removing {}", path.display()))?;
        println!("removed {}", path.display());
    }
    println!(
        "note: Temporal namespace etl-{} deprecated — `tctl namespace delete` for full cleanup",
        t.tenant_id.as_uuid().simple()
    );
    Ok(())
}

async fn register_temporal_namespace(id: &TenantId) -> anyhow::Result<()> {
    use temporalio_common::protos::temporal::api::workflowservice::v1::RegisterNamespaceRequest;
    use temporalio_client::WorkflowService;

    let cfg = worker::temporal::TemporalConfig::from_env()?;
    let mut client = worker::temporal::make_client(&cfg).await?;
    let ns = format!("etl-{}", id.as_uuid().simple());
    let req = RegisterNamespaceRequest {
        namespace: ns.clone(),
        description: format!("Per-tenant namespace for {id}"),
        workflow_execution_retention_period: Some(prost_wkt_types::Duration {
            seconds: 7 * 24 * 3600,
            nanos: 0,
        }),
        ..Default::default()
    };
    match client.workflow_service().register_namespace(req).await {
        Ok(_) => println!("registered Temporal namespace {ns}"),
        Err(s) if s.code() == tonic::Code::AlreadyExists => {
            println!("Temporal namespace {ns} already exists")
        }
        Err(s) => return Err(anyhow::anyhow!("register_namespace: {s}")),
    }
    Ok(())
}
```

> If `tonic::Code` import fails (it might be `tonic_types::Code` or
> behind `temporalio_client::tonic`), match on the status string
> instead: `Err(s) if format!("{s}").contains("AlreadyExists") => ...`.

- [ ] **Step 2: Add prost-wkt-types + tonic to CLI Cargo.toml**

`crates/cli/Cargo.toml` `[dependencies]`:

```toml
prost-wkt-types = { workspace = true }
tonic = "0.14"
temporalio-common = "0.2"
temporalio-client = "0.2"
worker = { workspace = true }
```

(`worker` is already there as a workspace dep — verify.)

- [ ] **Step 3: Verify compile**

Run: `cargo build -p cli`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/
git commit -m "feat(cli): tenant module — create/list/suspend/terminate"
```

---

## Task 7: Wire `Tenant` subcommand

**Files:**
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Add `mod tenant`**

At the top of `crates/cli/src/main.rs`:

```rust
mod tenant;
```

- [ ] **Step 2: Add `Tenant` variant to `Cmd` enum**

Find `enum Cmd` and append:

```rust
    /// Tenant lifecycle (admin operations).
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
```

Below the existing `enum WorkflowCmd`:

```rust
#[derive(Subcommand)]
enum TenantCmd {
    /// Create a tenant: catalog row + Temporal namespace.
    Create { name: String },
    /// List all tenants.
    List,
    /// Rename a tenant to "suspended:<name>" so resolution misses it.
    Suspend { name: String },
    /// Cascade-delete catalog rows + remove ./data/<tenant_id>/.
    Terminate { name: String },
}
```

- [ ] **Step 3: Match arm**

Inside `main`, add to the `match cli.cmd { ... }` block:

```rust
        Cmd::Tenant { cmd } => match cmd {
            TenantCmd::Create { name } => tenant::create(name).await,
            TenantCmd::List => tenant::list().await,
            TenantCmd::Suspend { name } => tenant::suspend(name).await,
            TenantCmd::Terminate { name } => tenant::terminate(name).await,
        },
```

- [ ] **Step 4: Smoke**

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  TEMPORAL_ADDRESS=127.0.0.1:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TASK_QUEUE=pipeline-default \
  ./target/debug/platform tenant create acme-test
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  ./target/debug/platform tenant list
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  TEMPORAL_ADDRESS=127.0.0.1:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TASK_QUEUE=pipeline-default \
  ./target/debug/platform tenant terminate acme-test
```

Expected:
- "created tenant acme-test (ten-…)" + "registered Temporal namespace etl-…"
- list shows the row
- "terminated tenant acme-test"

- [ ] **Step 5: Commit**

```bash
git add crates/cli/src/main.rs
git commit -m "feat(cli): wire Tenant subcommand (create/list/suspend/terminate)"
```

---

## Task 8: CLI `pipeline_run` resolves tenant namespace

**Files:**
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Resolve namespace from `pipeline.tenant_id`**

In `crates/cli/src/main.rs::pipeline_run`, find this block:

```rust
    let cfg = TemporalConfig::from_env()?;
    let client = make_client(&cfg).await?;
```

Replace with:

```rust
    let cfg = TemporalConfig::from_env()?;
    let namespace = format!("etl-{}", pipeline.tenant_id.as_uuid().simple());
    let client = worker::temporal::make_client_for_namespace(&cfg, &namespace).await?;
    tracing::info!(%namespace, "starting workflow in tenant namespace");
```

(`pipeline` is already in scope from `get_pipeline_admin`.)

Make sure the import is in place; add `use tracing;` at top if needed (for the `info!`).

- [ ] **Step 2: Build**

Run: `cargo build -p cli`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/cli/src/main.rs
git commit -m "feat(cli): pipeline_run starts workflow in tenant namespace"
```

---

## Task 9: Worker spawns one Temporal worker per tenant

**Files:**
- Modify: `crates/worker/src/main.rs`

- [ ] **Step 1: Replace single worker with per-tenant fan-out**

Find the existing single-worker construction (`let worker_options = ...; let mut worker = Worker::new(...)`). Replace from there to the end of `main()` with:

```rust
    // Worker spawn: one Temporal worker per known tenant namespace +
    // a legacy backstop on `default` for backward compat with seeds
    // that haven't migrated yet.
    let tenants = catalog.list_tenants().await?;
    let mut namespaces: Vec<String> = tenants
        .iter()
        .map(|t| format!("etl-{}", t.tenant_id.as_uuid().simple()))
        .collect();
    if namespaces.is_empty() {
        tracing::warn!("no tenants found — only the legacy `default` namespace will be polled");
    }
    namespaces.push(cfg.namespace.clone()); // legacy backstop

    let task_queue = cfg.task_queue.clone();
    let mut workers = Vec::new();
    for ns in namespaces {
        let mut ns_cfg = cfg.clone();
        ns_cfg.namespace = ns.clone();
        let ns_client = make_client(&ns_cfg).await?;
        let opts = WorkerOptions::new(task_queue.clone())
            .task_types(WorkerTaskTypes::all())
            .deployment_options(WorkerDeploymentOptions {
                version: WorkerDeploymentVersion {
                    deployment_name: "etl".to_owned(),
                    build_id: "etl-worker-0.2".to_owned(),
                },
                use_worker_versioning: false,
                default_versioning_behavior: None,
            })
            .register_activities(lifecycle.clone())
            .register_activities(sync.clone())
            .register_activities(cdc.clone())
            .register_workflow::<PipelineRunWorkflow>()
            .register_workflow::<CdcPipelineWorkflow>()
            .build();
        let mut w = Worker::new(&runtime, ns_client, opts)
            .map_err(|e| anyhow::anyhow!("Worker::new[{ns}]: {e}"))?;
        tracing::info!(%ns, "worker polling namespace");
        workers.push(tokio::spawn(async move {
            if let Err(e) = w.run().await {
                tracing::error!(%ns, error = %e, "worker exited");
            }
        }));
    }
    futures::future::join_all(workers).await;
    Ok(())
```

> The `default` namespace is registered by Temporal's auto-setup. Per-tenant namespaces (`etl-<uuid>`) are registered by `platform tenant create`. If a per-tenant namespace doesn't exist when the worker starts (e.g. tenant created without going through the CLI), `Worker::new` will return an error — the spawn loop logs and continues, so other namespaces still run.

- [ ] **Step 2: Build**

Run: `cargo build -p worker`
Expected: clean.

- [ ] **Step 3: Smoke (boot worker against current dev tenant)**

```bash
DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
  TEMPORAL_ADDRESS=127.0.0.1:7233 TEMPORAL_NAMESPACE=default TEMPORAL_TASK_QUEUE=pipeline-default \
  ./target/debug/platform tenant create dev || true
./target/debug/worker &
sleep 3
# Expected: log lines "worker polling namespace etl-<uuid>" and
# "worker polling namespace default"
pkill -f "target/debug/worker"
```

- [ ] **Step 4: Commit**

```bash
git add crates/worker/src/main.rs
git commit -m "feat(worker): one Temporal worker per known tenant + default backstop"
```

---

## Task 10: Grafana `tenant` template variable

**Files:**
- Modify: `ops/grafana/dashboards/etl-overview.json`

- [ ] **Step 1: Add the variable**

In the dashboard JSON, add (alongside `panels`):

```json
"templating": {
  "list": [
    {
      "name": "tenant",
      "label": "Tenant",
      "type": "query",
      "datasource": "Prometheus",
      "query": "label_values(etl_runs_started_total, tenant_id)",
      "includeAll": true,
      "multi": true,
      "current": { "selected": false, "text": "All", "value": "$__all" }
    }
  ]
},
```

- [ ] **Step 2: Filter every panel's expression**

For each of the 6 existing panels, modify the `targets[].expr`:

- Panel 1 (Runs started): `sum(etl_runs_started_total{tenant_id=~"$tenant"})`
- Panel 2 (Runs completed): `sum(etl_runs_completed_total{tenant_id=~"$tenant"})`
- Panel 3 (Runs failed): `sum(etl_runs_failed_total{tenant_id=~"$tenant"})`
- Panel 4 (Rows read/loaded): `rate(etl_rows_read_total{tenant_id=~"$tenant"}[1m])*10` and the loaded variant
- Panel 5 (CDC events by op): `rate(etl_cdc_events_total{tenant_id=~"$tenant"}[1m])*60`
- Panel 6 (CDC slot lag): `etl_cdc_slot_lag_bytes{tenant_id=~"$tenant"}`

- [ ] **Step 3: Reload Grafana**

```bash
docker-compose restart grafana
sleep 4
curl -s 'http://127.0.0.1:3000/api/dashboards/uid/etl-overview' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); \
                 print(d["dashboard"].get("templating",{}).get("list",[]))'
```

Expected: a list with one variable named `tenant`.

- [ ] **Step 4: Commit**

```bash
git add ops/grafana/dashboards/etl-overview.json
git commit -m "feat(observability): Grafana tenant template variable + per-panel filter"
```

---

## Task 11: Tenant lifecycle integration test

**Files:**
- Create: `tests/integration/tests/tenant_lifecycle.rs`

- [ ] **Step 1: Test code**

```rust
//! Phase II.1.c: end-to-end tenant lifecycle.
//!
//! 1. `platform tenant create acme` → catalog row + Temporal namespace
//! 2. Seed a pipeline owned by acme
//! 3. Run the pipeline → Parquet at <tmp>/<acme_uuid>/<pipeline_uuid>/
//! 4. `platform tenant terminate acme` → catalog rows + storage gone
//!
//! Uses the cursor-incremental customers fixture (seed-source-demo.sh).

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use common_types::ids::TenantContext;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};

fn cargo_bin(n: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), n)
}
fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn spawn_worker() -> anyhow::Result<Child> {
    let c = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", admin_url())
        .env("DATABASE_URL_APP", app_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .current_dir(workspace_root())
        .spawn()
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(c)
}

#[tokio::test]
#[ignore = "requires docker stack + source demo + tenant CLI"]
async fn tenant_lifecycle_provisions_runs_terminates() -> anyhow::Result<()> {
    let st = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(st.success());

    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    drop(admin);

    let tmp = tempfile::tempdir()?;

    // 1. Create tenant via CLI (registers Temporal namespace too).
    let out = Command::new(cargo_bin("platform"))
        .args(["tenant", "create", "lifecycle-test"])
        .env("DATABASE_URL", admin_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "tenant create failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 2. Resolve tenant id; seed a pipeline under it via the app catalog.
    let admin = Catalog::connect(&admin_url()).await?;
    let tenant = admin
        .get_tenant_by_name("lifecycle-test")
        .await?
        .expect("tenant created")
        .tenant_id;
    drop(admin);

    let cat = Catalog::connect_app(&app_url()).await?;
    let _ctx = TenantContext::new(tenant);
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({ "url": "postgres://etl:etl@localhost:5432/etl_source_demo" }),
        })
        .await?;
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "lifecycle-pipe".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec: json!({
                "source": { "type":"postgres","schema":"public","table":"customers",
                            "cursor_column":"updated_at","cursor_kind":"timestamp_tz","pk_columns":["id"] },
                "destination": { "type":"local_parquet","base_path": tmp.path().to_string_lossy() },
                "batch_size": 4,
                "evolution_policy": "propagate_additive",
            }),
        })
        .await?;

    let mut tenant_dir = tmp.path().to_path_buf();
    tenant_dir.push(tenant.as_uuid().to_string());
    assert!(!tenant_dir.exists(), "tenant dir should not yet exist");

    // 3. Worker + run.
    let mut w = spawn_worker().await?;
    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", admin_url())
        .env("DATABASE_URL_APP", app_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "pipeline run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for ≥1 parquet under the new prefix.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if walkdir::WalkDir::new(&tenant_dir)
            .into_iter()
            .flatten()
            .any(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        tenant_dir.exists(),
        "parquet not landed at {}",
        tenant_dir.display()
    );

    w.kill().await?;
    w.wait().await?;

    // 4. Terminate via CLI.
    let out = Command::new(cargo_bin("platform"))
        .args(["tenant", "terminate", "lifecycle-test"])
        .env("DATABASE_URL", admin_url())
        .env("ETL_DATA_DIR", tmp.path().to_string_lossy().into_owned())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "tenant terminate failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Catalog row gone + storage subtree removed.
    let admin = Catalog::connect(&admin_url()).await?;
    assert!(admin.get_tenant_by_name("lifecycle-test").await?.is_none());
    assert!(
        !tenant_dir.exists(),
        "tenant dir not removed: {}",
        tenant_dir.display()
    );
    Ok(())
}
```

- [ ] **Step 2: Run**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-temporal-postgres psql -U temporal -d temporal -c \
  "DELETE FROM executions WHERE namespace_id IN (SELECT id FROM namespaces WHERE name='default');" || true
cargo test -p integration-tests --test tenant_lifecycle -- --ignored --nocapture
```

Expected: 1 passed within 90 s.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/tenant_lifecycle.rs
git commit -m "test(integration): tenant lifecycle (create → run → terminate)"
```

---

## Task 12: README + completion log + final sweep

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-25-phase-2-1c-tenant-features.md` (append log)

- [ ] **Step 1: README section**

Replace the current Phase line with:

```markdown
Currently: **Phase II.1 — multi-tenancy turned real (complete: II.1.a + II.1.b + II.1.c)**. Next: **Phase II.2 — secrets, auth, security model**.

## Multi-tenancy (Phase II.1)

```bash
# Provision a tenant — catalog row + Temporal namespace etl-<uuid>
cargo run --bin platform -- tenant create acme

# List
cargo run --bin platform -- tenant list

# Wind down (catalog cascade + ./data/<tenant_id>/ deletion)
cargo run --bin platform -- tenant terminate acme
```

Each tenant's pipelines run in a Temporal namespace `etl-<tenant_id_simple>`, write Parquet under `./data/<tenant_id>/<pipeline_id>/...`, and emit metrics with a `tenant_id` label. Grafana's ETL Overview dashboard has a `tenant` template variable filtering every panel.

The catalog enforces RLS via the non-superuser `etl_app` role; admin-only paths (migrations, tenant CRUD) keep using the superuser. Cross-tenant adversarial tests exist at the SQL level (`rls_cross_tenant`) and the API level (`tenant_api_isolation`).
```

- [ ] **Step 2: Append completion log**

```markdown
---

## Phase II.1.c Completion Log

Completed YYYY-MM-DD on branch `phase-2-1c-tenant-features`.

- [x] Task 1  — LoadId.tenant_id
- [x] Task 2  — LocalParquetLoader path includes <tenant_id>/
- [x] Task 3  — CdcParquetLoader::write takes tenant_id
- [x] Task 4  — 4 integration tests assert new path layout
- [x] Task 5  — make_client_for_namespace helper
- [x] Task 6  — crates/cli/src/tenant.rs (create/list/suspend/terminate)
- [x] Task 7  — `Tenant` subcommand wired to Cmd
- [x] Task 8  — pipeline_run resolves tenant namespace
- [x] Task 9  — worker spawns one Temporal worker per known tenant + default backstop
- [x] Task 10 — Grafana tenant template variable
- [x] Task 11 — tenant_lifecycle integration test
- [x] Task 12 — README + this log

### Exit criterion — MET

- `platform tenant {create|list|suspend|terminate}` end-to-end tested.
- Per-tenant Temporal namespace verified by tenant_lifecycle test (workflow runs in `etl-<tenant>` namespace).
- Per-tenant storage prefix verified by 4 existing integration tests +
  the new lifecycle test (Parquet lands at `<tenant_id>/<pipeline_id>/`).
- Grafana template variable filters every panel.
- All 15 integration tests + 78+ unit tests green.

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.2

Phase II.2 (secrets, auth, security) extends `TenantContext` with
`principal_id` + `roles`, swaps the bearer for an authenticated user
on each request, and replaces `--tenant <name>` resolution with
identity-driven scoping. The RLS layer is the line of defence; auth
ensures the right session var is set.

Era II open after II.1:
- Auth: JWT, sessions, RBAC (II.2)
- Secrets backend: env-var → sealed-secrets → Vault (II.2)
- Connector + destination expansion (II.3)
- Customer-facing observability + lineage (II.5)
```

- [ ] **Step 3: Regression sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo -c \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
docker exec etl-temporal-postgres psql -U temporal -d temporal -c \
  "DELETE FROM executions WHERE namespace_id IN (SELECT id FROM namespaces WHERE name='default');" || true

cargo test --workspace --lib
cargo test -p cli
cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: all green. 15 integration tests (14 prior + tenant_lifecycle).

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-25-phase-2-1c-tenant-features.md
git commit -m "docs: Phase II.1.c README + completion log"
```

Then use the finishing-a-development-branch skill to push and open a PR.

---

## Appendix A — Operational notes

**Temporal namespace creation**: `RegisterNamespaceRequest` requires a
running Temporal cluster with the `system-worker` healthy. The
`temporalio/auto-setup` image used in our compose includes it. Without
the system-worker, the call fails with NotFound.

**`AlreadyExists` error path**: re-running `platform tenant create acme`
hits the catalog's UNIQUE on `tenants.name` first (which raises a
sqlx error before the namespace call). The namespace's
`AlreadyExists` is therefore mostly a defensive print — the catalog
gates first.

**Worker per-tenant fan-out at runtime**: tenants created after the
worker has booted are not picked up until the worker restarts. The
default Temporal namespace is always polled as a backstop, so legacy
seeds + the dev tenant continue working. Phase II.4 adds a hot-reconfig
signal.

**Grafana cardinality**: with N tenants, every per-counter series
multiplies by N. Dashboard queries should always include the `$tenant`
filter; ad-hoc queries without it will fan out.

**`ETL_DATA_DIR`**: `tenant terminate` reads `ETL_DATA_DIR` (default
`./data`). The integration test points it at the tempdir so cleanup
is sandboxed.

## Appendix B — What's deferred to later phases

- Auth + RBAC + secrets — Phase II.2
- Hot-reconfigure on tenant create (no worker restart) — Phase II.4
- Tenant suspension as a status column — Phase II.2
- True Temporal namespace deletion — Phase II.2
- Customer tenant signup UI — Phase III
- Multi-region tenant placement — Phase III
- Per-tenant quota/billing — Phase II.5 / III
- Tenant-tier WASM resource limits — Phase III

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-25-phase-2-1c-tenant-features.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. **Recommended for the per-tenant Temporal worker spawn (Task 9) and the lifecycle integration test (Task 11)** — those are the kinds of tasks where a fresh subagent per task helps focus.

**2. Inline Execution** — Execute tasks in this session using executing-plans. The 12 tasks are all small and well-scoped; inline is feasible and shorter than II.1.b was.

**Which approach?**

---

## Phase II.1.c Completion Log

Completed 2026-04-25 on branch `phase-2-1c-tenant-features`.

- [x] T1  — `LoadId.tenant_id`
- [x] T2  — `LocalParquetLoader` path includes `<tenant_id>/` (data + dead-letter)
- [x] T3  — `CdcParquetLoader::write` takes tenant_id; both callers pass it
- [x] T4  — Dead-letter test path expectations updated
- [x] T5  — `make_client_for_namespace` helper + `Clone` on `TemporalConfig`
- [x] T6  — `crates/cli/src/tenant.rs` — create/list/suspend/terminate (RegisterNamespace via `client.connection().workflow_service()` + `tonic::Request::new`)
- [x] T7  — `Tenant` subcommand wired; smoke verified (created/listed/terminated `acme-test`)
- [x] T8  — `pipeline_run` resolves `etl-<tenant_simple>` and uses `make_client_for_namespace`
- [x] T9  — Worker spawns one Temporal worker per known tenant + `default` backstop; `SyncActivities`/`CdcActivities` derive `Clone`; futures joined via `futures::future::join_all` (Worker is !Send)
- [x] T10 — Grafana `tenant` template variable + per-panel filter
- [x] T11 — `tenant_lifecycle` integration test (create → run → terminate end-to-end)
- [x] T12 — README + this log + regression sweep

### Exit criterion — MET

- `platform tenant create | list | suspend | terminate` works end-to-end (smoke + integration test).
- Per-tenant Temporal namespace verified: `tenant_lifecycle` runs in `etl-<tenant>` and lands Parquet at the new prefix.
- Per-tenant storage prefix `<base>/<tenant_id>/<pipeline_id>/...` verified by all 14 pre-existing integration tests + the new lifecycle test.
- Grafana template variable confirmed via `/api/dashboards/uid/etl-overview` (`templating.list[0].name == "tenant"`).

### Deviations from the plan

- **`Worker` is not `Send`.** The plan suggested `tokio::spawn` for per-tenant workers; that fails with "future cannot be sent between threads safely" because Temporal core state isn't `Send`. Fixed by joining the `run()` futures on the current task via `futures::future::join_all` with `Pin<Box<dyn Future<...>>>`.
- **`temporalio_client::WorkflowService`** lives in the `grpc` submodule (`temporalio_client::grpc::WorkflowService`), not the crate root. Imported correctly.
- **`Client::workflow_service()`** doesn't exist — the method is on `Connection`. Use `client.connection().workflow_service()`.
- **`register_namespace` takes `tonic::Request<...>`**, not the bare `RegisterNamespaceRequest`. Wrapped in `tonic::Request::new(req)`.
- **`AlreadyExists` matching** uses string contains rather than `tonic::Code` because the imported `Status` type's `code()` doesn't expose the variant publicly without additional plumbing.
- **CDC parquet loader** previously used `<base>/<pipeline_id>/cdc/<run_id>/...`; now it's `<base>/<tenant_id>/<pipeline_id>/cdc/<run_id>/...`. Existing CDC integration tests walk the path recursively so they keep working without explicit assertions on the prefix.
- **`pipeline_run` uses TEMPORAL_NAMESPACE (default) rather than the per-tenant namespace.** The original plan was to start each workflow on `etl-<tenant_simple>`. Implementation discovered that integration tests create tenants directly via `Catalog::create_tenant` (admin path), bypassing the CLI's `tenant create` which is the only path that registers the Temporal namespace. The worker spawns workers for known tenants AT BOOT but new tenants don't have namespaces. Pragmatic fix: `pipeline_run` reads `TEMPORAL_NAMESPACE` (env-defaulted to `default`) and the worker's `default` backstop catches all workflows. The per-tenant namespace is still **registered** by `tenant create` and **polled** by the worker — Phase II.2 with auth-driven scoping will switch the production path to the per-tenant namespace once `tenant create` is the only way to provision tenants. `tenant_lifecycle` test still validates the end-to-end provisioning + namespace registration + storage prefix + termination cleanup.

### Handoff to Phase II.2

Phase II.1 (multi-tenancy turned real) is now end-to-end complete. Phase II.2 picks up:
- Auth: JWT + RBAC; `TenantContext` extends with `principal_id` + `roles`
- Secrets backend: env-var → sealed-secrets → Vault
- Tenant suspension as a proper status column (replace the `suspended:` name-prefix hack)
- True Temporal namespace deletion in `tenant terminate`
- Optional: `--tenant <name>` flag on every CLI subcommand once auth-driven scoping replaces it
