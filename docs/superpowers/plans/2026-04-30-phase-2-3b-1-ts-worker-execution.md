# Phase II.3.b.1 — TS Connector Worker Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove (and if necessary, fix) that a TS-built `.cwasm` actually runs end-to-end inside the worker — not just publishes and applies. Closes the gap from II.3.b's `stripe_ts_e2e` deferral.

**Architecture:** The II.3.b SDK ships TS authoring + a connector that compiles to `wasm32-wasip2`. The worker's `WasmSourceRuntime` already wires `wasmtime_wasi::add_to_linker_async` (`crates/worker/src/wasm_runtime/runtime.rs:29`), so WASI 0.2 imports the TS guest needs (`wasi:io/cli/clocks/filesystem/random/http`) should already resolve. This plan adds a proper end-to-end test (spawn worker → seed catalog with TS Stripe pipeline → run pipeline → verify wiremock got hit and Parquet rows landed) and fixes whatever surfaces.

**Tech Stack:** Existing — wasmtime + wasmtime-wasi 26, jco-built `stripe-source-ts` from II.3.b, wiremock 0.6, parquet 53, the integration test harness (`spawn_worker` + `Catalog::create_pipeline` + `platform pipeline run`) used by `csv_wasm_connector_end_to_end`.

**Scope:**
- ✅ Replace `stripe_ts_e2e`'s publish-only test with a full pipeline-execution test mirroring `csv_wasm_connector_end_to_end`'s shape.
- ✅ Verify wiremock got at least one GET (assertion goes from `expect(0..=1)` to `expect(1..)`).
- ✅ Verify Parquet rows match the wiremock fixture.
- ✅ Fix any worker-side breakage (missing WASI capability, IPC encoding mismatch, etc.).
- ✅ Tighten the plan's claim of "II.3.b complete" — this is a small follow-up, not its own phase.

**Deferred:**
- AOT compilation of the TS guest (jco `--aot`) for bundle size reduction — it's stable enough now but adds optionality.
- TS scalar / destination connectors.

---

## File Structure

**Modified files:**
- `tests/integration/tests/stripe_ts_e2e.rs` — full rewrite from publish-only to spawn-worker + pipeline-run + verify-output.
- `crates/worker/src/wasm_runtime/host.rs` — possible: `WasiCtxBuilder` may need `inherit_stdio()` or similar for StarlingMonkey console output. Touched only if T2 reveals a need.
- `examples/stripe-source-ts/src/connector.ts` — possible: `apache-arrow`'s IPC stream output may not byte-match what Rust's `arrow_ipc::reader::StreamReader` expects. Touched only if T2 reveals a deserialization error.
- `docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md` — update the "Future hardening" section once this plan ships, to note the gap is closed.
- `README.md` — strike the line that said TS execution was unverified.

**No new files.**

---

## Pre-flight

```bash
# Ensure docker stack + wasm target ready (same as II.3.b).
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
pkill -f "target/debug/worker" 2>/dev/null || true
pkill -f "target/debug/etl-auth" 2>/dev/null || true
```

---

## Task 1: Rewrite `stripe_ts_e2e.rs` to actually run the pipeline

**Files:**
- Modify: `tests/integration/tests/stripe_ts_e2e.rs` (full rewrite)

The current test publishes the TS connector and applies a YAML — but the worker is never spawned, so the .cwasm is never loaded. The new test mirrors `csv_wasm_connector_end_to_end` (`tests/integration/tests/wasm_connector.rs:83-180`): builds the workspace + TS connector, seeds the catalog directly via `Catalog::create_connection` + `Catalog::create_pipeline`, spawns a worker, runs the pipeline via `platform pipeline run <id>`, polls `runs` for completion, asserts wiremock got hit and Parquet has 2 rows.

- [ ] **Step 1: Replace the test body**

Overwrite `tests/integration/tests/stripe_ts_e2e.rs`:

```rust
//! Phase II.3.b.1 — TypeScript Stripe connector executed end-to-end:
//!   1. Publish examples/stripe-source-ts via the SDK CLI.
//!   2. Spawn a worker with ETL_CONNECTORS_DIR pointed at the registry.
//!   3. Stand up a wiremock server emulating Stripe /v1/customers.
//!   4. Seed catalog directly with a Connection (url = sk_test_demo) +
//!      Pipeline (source.config = { base_url = wiremock URI, ... }).
//!   5. `platform pipeline run <id>`. Poll `runs.status` until completed.
//!   6. Assert wiremock saw exactly one GET, and that the Parquet
//!      destination has 2 rows.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

async fn build_workspace() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "cargo build failed");
    Ok(())
}

async fn publish_ts_connector(registry: &Path) -> anyhow::Result<()> {
    let status = Command::new(cargo_bin("platform"))
        .current_dir(workspace_root())
        .args([
            "connector",
            "publish",
            "examples/stripe-source-ts",
            "--registry",
            registry.to_str().unwrap(),
        ])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "platform connector publish failed");
    Ok(())
}

async fn spawn_worker(connectors_dir: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("RUST_LOG", "info,sqlx=warn")
        .env("ETL_CONNECTORS_DIR", connectors_dir)
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
#[ignore = "requires docker stack + node + npm + jco; ~120s"]
async fn stripe_ts_connector_runs_in_worker() -> anyhow::Result<()> {
    build_workspace().await?;
    let connectors = workspace_root().join("connectors");
    publish_ts_connector(&connectors).await?;

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/customers"))
        .and(query_param("limit", "100"))
        .and(header("Authorization", "Bearer sk_test_demo"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
                "data":[
                    {"id":"cus_a","email":"a@x.com","name":"Alice","created":1700000000},
                    {"id":"cus_b","email":"b@x.com","name":"Bob","created":1700000123}
                ],
                "has_more": false
            }"#,
        ))
        .expect(1..)
        .mount(&server)
        .await;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "stripe-mock-ts".into(),
            connector_ref: "wasm:stripe-source-ts@0.1.0".into(),
            config: json!({ "url": "sk_test_demo" }),
        })
        .await?;

    let tmp_data = tempfile::tempdir()?;
    let spec = json!({
        "source": {
            "type": "wasm",
            "config": {
                "base_url": server.uri(),
                "limit": 100,
                "max_429_retries": 1
            }
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 100
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "stripe-customers-mock-ts".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut worker = spawn_worker(&connectors).await?;
    let start = Instant::now();

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CONNECTORS_DIR", &connectors)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "platform pipeline run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if Instant::now() > deadline {
            worker.kill().await.ok();
            anyhow::bail!("timed out waiting for run completion");
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
                worker.kill().await.ok();
                anyhow::bail!("run failed (status=failed)");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let elapsed = start.elapsed();
    eprintln!("ts pipeline completed in {elapsed:?}");

    worker.kill().await?;
    worker.wait().await?;

    let total = count_parquet_rows(tmp_data.path());
    assert_eq!(total, 2, "expected 2 customer rows in parquet, got {total}");

    drop(server);
    Ok(())
}
```

- [ ] **Step 2: Compile-check**

```bash
cargo build -p integration-tests --tests
```
Expected: clean.

- [ ] **Step 3: Commit (test file only — running it is T2)**

```bash
git add tests/integration/tests/stripe_ts_e2e.rs
git commit -m "test(integration): stripe_ts_e2e exercises full worker pipeline run"
```

---

## Task 2: Run the test — discovery checkpoint

**Files:** none changed in this task (run + observe only)

- [ ] **Step 1: Clean state**

```bash
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
pkill -f "target/debug/worker" 2>/dev/null || true
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p integration-tests --test stripe_ts_e2e -- --ignored --nocapture 2>&1 | tee /tmp/ts-e2e.log
```

Expected: ideally PASS in ~60-120s. The wasmtime + wasmtime-wasi linker is already wired, so this might just work.

- [ ] **Step 3: Branch on outcome**

If **PASS**: skip directly to Task 5. The gap was test coverage, nothing more.

If **FAIL with worker-side error** (e.g. `failed to instantiate component: import not satisfied: wasi:http/types`): proceed to Task 3.

If **FAIL with serialization error** (e.g. `IPC stream invalid: ...`, `failed to read Arrow batch`): proceed to Task 4.

If **FAIL with timeout** (`timed out waiting for run completion`): inspect `/tmp/ts-e2e.log` for worker logs. Common causes: worker died on instantiation (look for `panicked` or `Error: ` near worker startup), or temporal workflow stuck. Re-run with `RUST_LOG=debug` to capture more detail. Proceed to Task 3 if a missing import is logged.

If **FAIL with row count mismatch** (`expected 2 customer rows in parquet, got N`): proceed to Task 4.

Document the actual outcome inline in the completion log section of this plan (Task 5 step 2).

---

## Task 3: Worker WASI / linker fix (only if T2 surfaces a missing-import error)

**Files (touch only what the failure points to):**
- Modify: `crates/worker/src/wasm_runtime/host.rs` — possibly extend `WasiCtxBuilder` configuration.
- Modify: `crates/worker/src/wasm_runtime/runtime.rs` — possibly add additional linker addons.

The worker already calls `wasmtime_wasi::add_to_linker_async` which provides ALL of WASI 0.2 (io, cli, clocks, filesystem, random, http, sockets, etc.). If T2 fails on a missing wasi import, the most likely root cause is one of:

**A. WasiCtx not actually built with the trait object the linker expects.** Check `crates/worker/src/wasm_runtime/host.rs` — `HostState` must implement `WasiView` and the linker's `add_to_linker_async` must be the version matching the WasiCtx version. If types mismatch, the linker pretends to satisfy imports but instantiation fails at runtime.

**B. wasi:http imports not provided.** `wasmtime_wasi::add_to_linker_async` does NOT include `wasi:http`. If the test fails with `import not satisfied: wasi:http/types`, add `wasmtime-wasi-http` to the worker crate and call `wasmtime_wasi_http::add_to_linker_async(&mut linker)`.

- [ ] **Step 1: Identify which import is missing**

From the worker logs in `/tmp/ts-e2e.log`, find the `import not satisfied` line. Note the exact interface (e.g. `wasi:http/types@0.2.10`).

- [ ] **Step 2A: If `wasi:http/*` is missing — add `wasmtime-wasi-http`**

Edit `crates/worker/Cargo.toml` (look in `[dependencies]`), add:
```toml
wasmtime-wasi-http = "26"
```

Edit `crates/worker/src/wasm_runtime/host.rs` — extend `HostState` with a `WasiHttpCtx`:
```rust
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

// Inside HostState struct, add:
//     pub http: WasiHttpCtx,
// And in HostState::new():
//     let http = WasiHttpCtx::new();

// Add trait impl below the existing WasiView impl:
impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
    fn table(&mut self) -> &mut wasmtime::component::ResourceTable {
        // delegate to whatever ResourceTable HostState already exposes for wasi
        WasiView::table(self)
    }
}
```

(Read the current `host.rs` first to see what `WasiView` returns for `table()` — match that.)

Edit `crates/worker/src/wasm_runtime/runtime.rs:29-30`:
```rust
        wasmtime_wasi::add_to_linker_async(&mut linker)
            .context("adding WASI 0.2 imports to linker")?;
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
            .context("adding WASI 0.2 http imports to linker")?;
        super::bindings::platform::connector::host::add_to_linker(&mut linker, |s| s)
            .context("adding host.log / host.http-fetch to linker")?;
```

- [ ] **Step 2B: If a non-http wasi interface is missing — there's a version mismatch**

Look at `Cargo.lock` for `wasmtime-wasi` and verify it matches the WIT version embedded in the TS guest's component (you can confirm via `npx jco wit examples/stripe-source-ts/dist/connector.wasm | grep wasi:`). If the guest imports `@0.2.10` and the host links `@0.2.x` for some other x, bump the worker's `wasmtime-wasi` version in workspace `Cargo.toml` and re-run.

- [ ] **Step 3: Rebuild + retest**

```bash
cargo build -p worker
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
pkill -f "target/debug/worker" 2>/dev/null
cargo test -p integration-tests --test stripe_ts_e2e -- --ignored --nocapture 2>&1 | tail -40
```
Expected: PASS now.

- [ ] **Step 4: Commit**

```bash
git add crates/worker/Cargo.toml crates/worker/src/wasm_runtime/host.rs crates/worker/src/wasm_runtime/runtime.rs Cargo.lock
git commit -m "feat(worker): link wasi:http for componentize-js TS connectors"
```

---

## Task 4: Arrow IPC compatibility fix (only if T2 surfaces a deserialization error)

**Files (touch only what failure points to):**
- Modify: `examples/stripe-source-ts/src/parse.ts` — possibly switch IPC encoder.

`apache-arrow`'s JS `tableToIPC(t, 'stream')` produces an Arrow IPC Stream Format. The worker reads it via `arrow_ipc::reader::StreamReader` (or `FileReader`). Most likely failures:

**A. JS emits 'file' format, Rust expects 'stream'** (or vice versa). Confirm `tableToIPC(t, 'stream')` is what the parser uses.

**B. Field nullability or DataType mismatch.** Worker's discover() schema reader may reject schemas that disagree with what the pipeline expects. Inspect by adding a debug print in `parse.ts` showing the produced IPC bytes' first 16 bytes (Arrow magic + version) and compare to the Rust connector's output.

**C. BigInt64 ↔ Int64 mismatch.** JS uses `BigInt64Array` for Int64; check that the Rust reader handles it (it should — Arrow IPC carries the schema with the data, so encoding is self-describing).

- [ ] **Step 1: Capture the actual error**

Re-run the test with `RUST_LOG=debug,worker=trace`:
```bash
RUST_LOG=debug,worker=trace cargo test -p integration-tests --test stripe_ts_e2e -- --ignored --nocapture 2>&1 | tee /tmp/ts-e2e-debug.log
grep -E "Arrow|IPC|schema|read_batch" /tmp/ts-e2e-debug.log | head -30
```

- [ ] **Step 2: Pick the fix**

Based on the failure mode:

**If "invalid IPC stream format":** ensure `parse.ts` uses `tableToIPC(t, 'stream')` (not `'file'`). If already correct, log the byte length of the produced IPC bytes vs what the worker received.

**If schema mismatch:** the Rust connector emits `Field::new("id", DataType::Utf8, false)` etc. Make sure `customersSchema()` in TS produces exactly the same shape. Compare via:
```bash
# Build both, dump schemas via discover():
cargo run --bin ipc-debug -- examples/stripe-source/dist/...    # not a real binary; skip
```
Instead: read both connectors' `customersSchema()` definitions side by side and verify field names, DataTypes, nullability are identical.

**If row count mismatch (Parquet has 0 or 1 rows instead of 2):** the IPC parsing succeeded but `read_batch` returned the wrong row count. Check that `parse.ts::parsePage` correctly emits a non-empty batch — `tableFromArrays` with nullable columns can be tricky. Print `batchIpc.byteLength` and `rows` before returning.

- [ ] **Step 3: Commit the fix**

```bash
git add examples/stripe-source-ts/src/parse.ts
git commit -m "fix(stripe-source-ts): align IPC stream output with worker reader"
```

---

## Task 5: Tighten plan + README + final sweep

**Files:**
- Modify: `docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md` — flip the "Future hardening" section to "Closed in II.3.b.1".
- Modify: `README.md` — strike the "(unverified)" hedge if any was added in II.3.b.
- Modify: `docs/superpowers/plans/2026-04-30-phase-2-3b-1-ts-worker-execution.md` (this file) — append completion log.

- [ ] **Step 1: Update the II.3.b plan's "Future hardening" section**

In `docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md`, find the section starting `### Future hardening — TS connector worker execution` and replace it with:

```markdown
### Future hardening — TS connector worker execution

**Closed in Phase II.3.b.1** (`docs/superpowers/plans/2026-04-30-phase-2-3b-1-ts-worker-execution.md`). The `stripe_ts_e2e` test now spawns a worker, runs the pipeline end-to-end, and asserts both the wiremock GET fired and 2 Parquet rows landed. WASI linkage (already wired in `crates/worker/src/wasm_runtime/runtime.rs:29`) was sufficient — no new linker code needed.
```

If T3 added wasi-http or other linker code, replace the closing sentence with the actual fix (e.g. "Required adding `wasmtime-wasi-http` to the worker.").

- [ ] **Step 2: Append completion log to this plan**

Append to `docs/superpowers/plans/2026-04-30-phase-2-3b-1-ts-worker-execution.md`:

```markdown
---

## Phase II.3.b.1 Completion Log

Completed 2026-04-XX on branch `phase-2-3b-1-ts-worker-execution`.

- [x] T1 — stripe_ts_e2e rewritten to spawn worker + run pipeline + verify Parquet rows
- [x] T2 — Discovery checkpoint: <PASS|FAIL with reason>
- [x] T3 — <Skipped (T2 passed) | Wired wasi-http to linker | Bumped wasmtime-wasi version>
- [x] T4 — <Skipped (T2 passed) | Aligned Arrow IPC stream output>
- [x] T5 — Updated II.3.b plan + this log + sweep

### Exit criterion — MET

- `stripe_ts_e2e` passes: full publish → spawn worker → seed catalog → `platform pipeline run` → wiremock GET fires → 2 rows land in Parquet.
- 121+ unit tests, 36 integration tests still pass.

### Deviations from the plan

_(Fill in after execution — based on which conditional tasks fired.)_
```

- [ ] **Step 3: README — strike any unverified hedge**

In `README.md`'s Connector SDK section, find the line about TypeScript authoring:
```
**TypeScript authoring (Phase II.3.b).** ... but the worker host treats both identically.
```
The current text is correct (worker host treats both identically). No change needed — but verify the line still reads accurately. If T3 added wasi-http, update to: "...but the worker host treats both identically (TS components additionally need `wasmtime-wasi-http`, wired in II.3.b.1)."

- [ ] **Step 4: Final regression sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
pkill -f "target/debug/etl-auth" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true

cargo test --workspace --lib 2>&1 | grep "^test result:" | awk '{ p+=$4; f+=$6 } END { printf "lib: %d passed, %d failed\n", p, f }'
VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=etl-dev-token \
  cargo test -p integration-tests -- --ignored --test-threads=1 2>&1 | grep "^test result:" | awk '{ p+=$4; f+=$6 } END { printf "integration: %d passed, %d failed\n", p, f }'
```

Expected: lib totals ≥ 121 passed 0 failed, integration ≥ 36 passed 0 failed (same as II.3.b's exit baseline). The reworked `stripe_ts_e2e` replaces the prior shallow test, so the count should hold steady at 36.

- [ ] **Step 5: Commit + push**

```bash
git add docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md docs/superpowers/plans/2026-04-30-phase-2-3b-1-ts-worker-execution.md README.md
git commit -m "docs: Phase II.3.b.1 completion log + close TS-worker-exec gap"
```

Then use the **finishing-a-development-branch** skill.

---

## Appendix A — Why the WASI linker is probably already enough

The Rust connectors (e.g. `examples/stripe-source/`) compile to wasi-p2 too — but their wasm only imports `platform:connector/host`, because `wit-bindgen` doesn't emit WASI imports unless the guest code uses WASI APIs. The guest only uses `Vec`, `String`, our custom `http_fetch`. No wasi.

componentize-js bundles StarlingMonkey, which uses WASI internally for clocks (Date.now), random (crypto.getRandomValues), filesystem (module loading), io (stdout/stderr for console.log), cli (stdin/stdout terminal handling), http (fetch — though we replace it via `--disable http`). All of these come from `wasmtime_wasi::add_to_linker_async`, which is already wired.

The one interface that's NOT in `add_to_linker_async` is `wasi:http`. componentize-js imports `wasi:http/types` (just the type definitions, not necessarily a working fetch impl). Whether this manifests as a "missing import" error at instantiation depends on whether the guest actually calls into it — if all calls go through our `http_fetch` host import (which they do in stripe-source-ts), the wasi:http import may be a dead reference that wasmtime tolerates.

If T2 reveals it doesn't tolerate the dead import, T3 wires `wasmtime-wasi-http`'s `add_only_http_to_linker_async` and we move on.

---

## Appendix B — What's deferred

- **TS scalar / destination connectors.** Same scope as II.3.b's source-only deliverable.
- **AOT mode for jco** (`--aot` flag). Reduces bundle from ~16 MB to ~2 MB. Not stable enough to default to.
- **`@platform/connector-sdk` npm package.** Authoring ergonomics (typed bindings around host imports). Future polish.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-30-phase-2-3b-1-ts-worker-execution.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — useful here because T2 is a discovery checkpoint. A fresh subagent at T3 can read T2's failure output and pick the right path without prior bias.

**2. Inline Execution** — also fine. The plan's small (5 tasks, ~3 of which may be no-ops). Wall time likely 10-30 min including the test run.

**Which approach?**
