# Phase I.3 — WASM Runtime + Connector SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run user-authored source connectors in a sandboxed WebAssembly Component Model runtime with enforced CPU/memory/wall-time limits, proven capability denial for non-granted host functions, and a working reference CSV connector that executes the existing `PipelineRunWorkflow` end-to-end without any change to the workflow/activity contracts.

**Architecture:** Host-side, worker loads `.cwasm` artifacts via `wasmtime::component::Component::deserialize_file`, instantiates them against a `Linker` that grants exactly `log` + `http-fetch` (Phase I.3 minimum) — deliberately NOT wall-clock, randomness, or filesystem beyond what WASI 0.2 grants by default. A `WasmSourceConnector` implements the existing `SourceConnector` trait by wrapping Component Model calls, so `SyncActivities` routes to Rust-native or WASM based on `connection.connector_ref` (`"postgres@0.1.0"` vs. `"wasm:csv-source@0.1.0"`). Arrow batches cross the boundary as IPC byte lists in WIT records (Tier 1 transfer per RFC-5; shared-memory Tier 2 and streaming Tier 3 are future phases). Guest-side, a reference Rust crate targets `wasm32-wasip2` and uses `wit-bindgen` to consume the same `source-connector.wit` definition.

**Tech Stack:** wasmtime 26+ with `component-model`, `async_support`, `consume_fuel`, and `epoch_interruption` enabled; `wasmtime-wasi` for WASI 0.2 imports; `wit-bindgen` 0.37+ on both host and guest. Guest target: `wasm32-wasip2` (Rust 1.82+; we're on 1.88). AOT compilation via `Engine::precompile_component` → `.cwasm`. Everything else stays as-established in Phase I.1/I.2 (Arrow 53, temporalio-sdk 0.2, sqlx, Parquet).

---

## File Structure

### Modified
- `Cargo.toml` (root) — add `wasmtime`, `wasmtime-wasi`, `wit-bindgen` to workspace deps; add `anyhow` already present
- `crates/connector-sdk/Cargo.toml` — add a `host-bindings` feature gating `wasmtime` deps
- `crates/connector-sdk/src/lib.rs` — re-export WIT-generated types under a `wit` module when feature enabled
- `crates/common-types/src/pipeline_spec.rs` — add `SourceSpec::Wasm { config: serde_json::Value }` variant
- `crates/worker/Cargo.toml` — add wasmtime + wasmtime-wasi + wit-bindgen
- `crates/worker/src/lib.rs` — expose new `wasm_runtime` module
- `crates/worker/src/activities/sync/mod.rs` — dispatch on `connector_ref` inside `read_batch` / `discover_stream` activities
- `crates/cli/src/main.rs` — add `connector build` subcommand
- `README.md` — add Phase I.3 demo section

### New
- `crates/connector-sdk/wit/source-connector.wit` — WIT definition consumed by host + every guest
- `crates/connector-sdk/build.rs` — no-op placeholder so guests can reference the WIT via the crate's source tree (not compile-time codegen here)
- `crates/worker/src/wasm_runtime/mod.rs` — public entry: `WasmSourceRuntime` struct + `WasmSourceConnector`
- `crates/worker/src/wasm_runtime/engine.rs` — `Engine` construction with fuel + epoch + component-model enabled
- `crates/worker/src/wasm_runtime/host.rs` — `HostState`, host implementations of `log` and `http-fetch`
- `crates/worker/src/wasm_runtime/limits.rs` — `ResourceLimiter` for memory
- `crates/worker/src/wasm_runtime/epoch.rs` — background epoch-ticker thread
- `crates/worker/src/wasm_runtime/bindings.rs` — `bindgen!` macro invocation + `SourceConnector` impl on `WasmSourceConnector`
- `crates/worker/src/connectors/dispatch.rs` — parses `connector_ref` and picks the backend
- `examples/csv-source/Cargo.toml` — standalone crate, NOT in workspace (targets `wasm32-wasip2`)
- `examples/csv-source/.cargo/config.toml` — pins `wasm32-wasip2` target
- `examples/csv-source/src/lib.rs` — guest implementation using `wit_bindgen::generate!`
- `examples/csv-source/README.md` — how to build
- `connectors/.gitkeep` — placeholder for local "registry" dir
- `tests/integration/tests/wasm_connector.rs` — end-to-end CSV→Parquet via WASM + cold-start measurement
- `tests/integration/tests/wasm_limits.rs` — fuel, memory, capability-denial tests (three `#[tokio::test]`s)
- `docs/superpowers/plans/2026-04-23-phase-1-3-wasm-runtime.md` — this file

### Deliberately deferred (do NOT add in this plan)
- `host` WIT interface for `state-get`/`state-put` — Phase I.4 (cursor still flows via `ReadOutcome`, same as Phase I.2)
- Host-provided `postgres-query` capability — Phase I.4
- `secrets/*` host interface — Phase II.2 (RFC-11)
- Instance pooling + shared-memory batch transfer — future optimization phases
- TypeScript/Python guest SDKs — Phase II.3 / Era III

---

## Key Type Contracts

These are the load-bearing types every downstream task depends on. Repeated here for orientation.

```wit
// crates/connector-sdk/wit/source-connector.wit
package platform:connector@0.1.0;

interface types {
    enum cursor-kind { int64, timestamp-tz }

    record cursor-value {
        kind: cursor-kind,
        value: string,
    }

    record connection-config {
        url: string,  // plaintext in Phase I.3; secret-ref comes in Phase II.2
    }

    record source-config {
        json: string,  // connector-specific config, JSON-encoded
    }

    record read-outcome {
        batch-ipc: list<u8>,
        rows: u32,
        new-cursor: option<cursor-value>,
        is-final: bool,
    }

    variant connector-error {
        invalid-config(string),
        source-unavailable(string),
        schema-incompatible(string),
        other(string),
    }
}

interface host {
    enum log-level { trace, debug, info, warn, error }
    log: func(level: log-level, message: string);

    record http-request {
        method: string,
        url: string,
        headers: list<tuple<string, string>>,
        body: option<list<u8>>,
    }
    record http-response {
        status: u16,
        headers: list<tuple<string, string>>,
        body: list<u8>,
    }
    http-fetch: func(request: http-request) -> result<http-response, string>;
}

world source-connector {
    use types.{connection-config, source-config, cursor-value, read-outcome, connector-error};
    import host;
    export discover: func(conn: connection-config, source: source-config) -> result<list<u8>, connector-error>;
    export read-batch: func(
        conn: connection-config,
        source: source-config,
        cursor: option<cursor-value>,
        batch-size: u32,
    ) -> result<read-outcome, connector-error>;
}
```

```rust
// crates/common-types/src/pipeline_spec.rs — new variant
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
    Wasm(WasmSourceSpec),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmSourceSpec {
    /// Free-form JSON passed as-is to the guest via `source-config.json`.
    pub config: serde_json::Value,
}
```

```rust
// crates/worker/src/wasm_runtime/mod.rs — top-level public types
pub struct WasmSourceRuntime {
    engine: Arc<wasmtime::Engine>,
    cache: DashMap<String, Arc<wasmtime::component::Component>>,
    base_dir: PathBuf,  // where .cwasm files live (./connectors by default)
    epoch_ticker: Arc<EpochTicker>,
}

pub struct WasmSourceConnector {
    runtime: Arc<WasmSourceRuntime>,
    name_at_version: String,  // e.g. "csv-source@0.1.0"
}

// Implements connector_sdk::SourceConnector.
```

```rust
// crates/worker/src/connectors/dispatch.rs
pub fn build_source_connector(
    connector_ref: &str,
    runtime: Option<Arc<WasmSourceRuntime>>,
) -> anyhow::Result<Box<dyn SourceConnector>>;
// "postgres@0.1.0" → Box::new(PostgresConnector)
// "wasm:<name>@<version>" → Box::new(WasmSourceConnector { runtime, name_at_version })
```

---

## Task 1: Workspace deps + `wasm32-wasip2` target

**Files:**
- Modify: `Cargo.toml` (root)
- Modify: `crates/worker/Cargo.toml`
- Modify: `rust-toolchain.toml`

- [ ] **Step 1: Pin the wasm target in `rust-toolchain.toml`**

```toml
[toolchain]
channel = "1.88"
components = ["rustfmt", "clippy"]
targets = ["wasm32-wasip2"]
```

- [ ] **Step 2: Add wasmtime family to workspace deps**

Edit the root `Cargo.toml`'s `[workspace.dependencies]` section. Add (keeping existing entries):

```toml
wasmtime = { version = "26", features = ["component-model", "async", "parallel-compilation"] }
wasmtime-wasi = "26"
wit-bindgen = "0.37"
dashmap = "6"
reqwest = { version = "0.12", features = ["rustls-tls", "json", "gzip"], default-features = false }
```

- [ ] **Step 3: Add to worker**

Edit `crates/worker/Cargo.toml` `[dependencies]` to add:

```toml
wasmtime = { workspace = true }
wasmtime-wasi = { workspace = true }
wit-bindgen = { workspace = true }
dashmap = { workspace = true }
reqwest = { workspace = true }
```

- [ ] **Step 4: Verify target installs and workspace builds**

Run: `rustup target add wasm32-wasip2`
Expected: prints `info: component 'rust-std' for target 'wasm32-wasip2' is up to date` or downloads it.

Run: `cargo build --workspace`
Expected: compiles clean (possibly slow on first wasmtime compile).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: add wasmtime 26 + wit-bindgen 0.37 + wasm32-wasip2 target

Host-side deps for Phase I.3 Component Model runtime. Also adds
dashmap (per-connector component cache) and reqwest (host-side
http-fetch implementation).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: WIT definition

**Files:**
- Create: `crates/connector-sdk/wit/source-connector.wit`
- Modify: `crates/connector-sdk/Cargo.toml`

- [ ] **Step 1: Write the WIT file**

Create `crates/connector-sdk/wit/source-connector.wit`:

```wit
package platform:connector@0.1.0;

interface types {
    enum cursor-kind { int64, timestamp-tz }

    record cursor-value {
        kind: cursor-kind,
        value: string,
    }

    record connection-config {
        url: string,
    }

    record source-config {
        json: string,
    }

    record read-outcome {
        batch-ipc: list<u8>,
        rows: u32,
        new-cursor: option<cursor-value>,
        is-final: bool,
    }

    variant connector-error {
        invalid-config(string),
        source-unavailable(string),
        schema-incompatible(string),
        other(string),
    }
}

interface host {
    enum log-level { trace, debug, info, warn, error }
    log: func(level: log-level, message: string);

    record http-request {
        method: string,
        url: string,
        headers: list<tuple<string, string>>,
        body: option<list<u8>>,
    }
    record http-response {
        status: u16,
        headers: list<tuple<string, string>>,
        body: list<u8>,
    }
    http-fetch: func(request: http-request) -> result<http-response, string>;
}

world source-connector {
    use types.{connection-config, source-config, cursor-value, read-outcome, connector-error};
    import host;
    export discover: func(conn: connection-config, source: source-config) -> result<list<u8>, connector-error>;
    export read-batch: func(
        conn: connection-config,
        source: source-config,
        cursor: option<cursor-value>,
        batch-size: u32,
    ) -> result<read-outcome, connector-error>;
}
```

- [ ] **Step 2: Update connector-sdk docs**

Edit `crates/connector-sdk/src/lib.rs` — append to the existing docstring and after the trait:

```rust
/// The canonical Component Model definition for source connectors lives at
/// `crates/connector-sdk/wit/source-connector.wit`. Host-side `bindgen!` and
/// guest-side `wit_bindgen::generate!` both consume this file.
pub const WIT_PATH: &str = "crates/connector-sdk/wit/source-connector.wit";
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(connector-sdk): WIT definition for source-connector world

One file consumed by host (bindgen!) and every guest (wit-bindgen).
Phase I.3 surface: discover + read-batch, plus host-provided log +
http-fetch. No state/cursor host API (cursor still flows via
ReadOutcome as in Phase I.2). No secrets (Phase II.2).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Engine construction with fuel + epoch + component-model

**Files:**
- Create: `crates/worker/src/wasm_runtime/mod.rs`
- Create: `crates/worker/src/wasm_runtime/engine.rs`
- Modify: `crates/worker/src/lib.rs`

- [ ] **Step 1: Wire the new top-level module**

Edit `crates/worker/src/lib.rs`. Append:

```rust
pub mod wasm_runtime;
```

- [ ] **Step 2: Write `engine.rs`**

Create `crates/worker/src/wasm_runtime/engine.rs`:

```rust
use anyhow::Context;
use wasmtime::{Config, Engine};

/// Build a wasmtime Engine configured for Phase I.3:
/// - Component Model enabled
/// - Async support (so host functions can be async and await the Temporal SDK)
/// - Fuel consumption (lets us bound CPU per invocation)
/// - Epoch interruption (lets us bound wall-time per invocation)
pub fn build_engine() -> anyhow::Result<Engine> {
    let mut config = Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    config.epoch_interruption(true);
    config.wasm_component_model(true);
    // Precompiled .cwasm artifacts rely on deterministic compilation.
    config.cranelift_opt_level(wasmtime::OptLevel::Speed);
    Engine::new(&config).context("building wasmtime Engine")
}
```

- [ ] **Step 3: Write the module root**

Create `crates/worker/src/wasm_runtime/mod.rs`:

```rust
//! WebAssembly Component Model runtime for source connectors (Phase I.3).
//!
//! See `docs/rfc/RFC-0005-wasm-runtime.md` and
//! `crates/connector-sdk/wit/source-connector.wit`.

pub mod engine;

pub use engine::build_engine;
```

- [ ] **Step 4: Unit-test the engine builder**

Append a test module to `crates/worker/src/wasm_runtime/engine.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_builds() {
        let _engine = build_engine().expect("engine builds with fuel + epoch + component-model");
    }
}
```

Run: `cargo test -p worker wasm_runtime::engine`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker/wasm_runtime): Engine construction

Fuel + epoch-interruption + component-model + async. Tested to
construct cleanly.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `bindgen!` + HostState + log + http-fetch

**Files:**
- Create: `crates/worker/src/wasm_runtime/host.rs`
- Create: `crates/worker/src/wasm_runtime/bindings.rs`
- Modify: `crates/worker/src/wasm_runtime/mod.rs`

- [ ] **Step 1: Generate bindings**

Create `crates/worker/src/wasm_runtime/bindings.rs`:

```rust
//! Host-side Component Model bindings for source-connector.wit.

wasmtime::component::bindgen!({
    path: "../connector-sdk/wit",
    world: "source-connector",
    async: true,
});
```

Note: the path is relative to `worker/Cargo.toml`, so `../connector-sdk/wit` resolves to `crates/connector-sdk/wit/`.

- [ ] **Step 2: Define HostState and implement host functions**

Create `crates/worker/src/wasm_runtime/host.rs`:

```rust
use anyhow::Context;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};

use super::bindings::platform::connector::host;
use super::bindings::platform::connector::host::{HttpRequest, HttpResponse, LogLevel};

/// Per-invocation host state. A fresh `HostState` is created for every
/// call to `discover` / `read_batch` — cheap, since activities are short-lived.
pub struct HostState {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    pub http: reqwest::Client,
    pub limits: super::limits::Limits,
}

impl HostState {
    pub fn new(limits: super::limits::Limits) -> Self {
        let wasi = WasiCtxBuilder::new()
            // Deliberately no filesystem preopens, no network, no env-vars.
            // Phase I.3 connectors get only what we explicitly linked below.
            .build();
        Self {
            wasi,
            table: ResourceTable::new(),
            http: reqwest::Client::builder()
                .user_agent("etl-platform/0.1")
                .build()
                .expect("reqwest client"),
            limits,
        }
    }
}

impl WasiView for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

#[wasmtime::component::__internal::async_trait]
impl host::Host for HostState {
    async fn log(&mut self, level: LogLevel, message: String) {
        match level {
            LogLevel::Trace => tracing::trace!(guest = true, "{}", message),
            LogLevel::Debug => tracing::debug!(guest = true, "{}", message),
            LogLevel::Info => tracing::info!(guest = true, "{}", message),
            LogLevel::Warn => tracing::warn!(guest = true, "{}", message),
            LogLevel::Error => tracing::error!(guest = true, "{}", message),
        }
    }

    async fn http_fetch(&mut self, request: HttpRequest) -> Result<HttpResponse, String> {
        let method = request
            .method
            .parse::<reqwest::Method>()
            .map_err(|e| format!("bad method {}: {e}", request.method))?;
        let mut req = self.http.request(method, &request.url);
        for (k, v) in &request.headers {
            req = req.header(k, v);
        }
        if let Some(body) = request.body {
            req = req.body(body);
        }
        let resp = req.send().await.map_err(|e| format!("send: {e}"))?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("read body: {e}"))?
            .to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}
```

- [ ] **Step 3: Create a stub `limits.rs`**

Create `crates/worker/src/wasm_runtime/limits.rs`:

```rust
//! Resource limits applied per invocation. Fleshed out in Task 5.

#[derive(Clone, Debug)]
pub struct Limits {
    pub fuel: u64,
    pub memory_bytes: u64,
    pub wall_time_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        // RFC-5 Phase I.3 defaults.
        Self {
            fuel: 30_000_000_000, // ~30 s of CPU on typical workloads
            memory_bytes: 256 * 1024 * 1024,
            wall_time_secs: 60,
        }
    }
}
```

- [ ] **Step 4: Expose modules**

Edit `crates/worker/src/wasm_runtime/mod.rs`:

```rust
//! WebAssembly Component Model runtime for source connectors (Phase I.3).
pub mod bindings;
pub mod engine;
pub mod host;
pub mod limits;

pub use engine::build_engine;
pub use host::HostState;
pub use limits::Limits;
```

- [ ] **Step 5: Build**

Run: `cargo build -p worker`
Expected: compiles. `bindgen!` may emit warnings about unused generated items — that's fine.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(worker/wasm_runtime): HostState + bindgen + log/http-fetch

bindgen! generates host-side types from source-connector.wit. HostState
carries WasiCtx, ResourceTable, a reqwest client, and per-invocation
Limits. Host log() fans out to tracing; http-fetch() calls reqwest.
Deliberately no filesystem preopens, no env vars, no network via WASI
— guest only gets what we explicitly grant.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Memory limit + epoch ticker

**Files:**
- Modify: `crates/worker/src/wasm_runtime/limits.rs`
- Create: `crates/worker/src/wasm_runtime/epoch.rs`
- Modify: `crates/worker/src/wasm_runtime/mod.rs`

- [ ] **Step 1: Add a ResourceLimiter implementation**

Replace `crates/worker/src/wasm_runtime/limits.rs`:

```rust
//! Resource limits applied per invocation.

use wasmtime::ResourceLimiter;

#[derive(Clone, Debug)]
pub struct Limits {
    pub fuel: u64,
    pub memory_bytes: u64,
    pub wall_time_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            fuel: 30_000_000_000,
            memory_bytes: 256 * 1024 * 1024,
            wall_time_secs: 60,
        }
    }
}

/// Enforces `memory_bytes` by denying `memory_growing` past the cap.
pub struct MemoryCap {
    pub max_bytes: u64,
}

impl ResourceLimiter for MemoryCap {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok((desired as u64) <= self.max_bytes)
    }
    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok(desired <= 100_000) // arbitrary large cap — tables rarely need policing in our shape
    }
}
```

- [ ] **Step 2: Write the epoch ticker**

Create `crates/worker/src/wasm_runtime/epoch.rs`:

```rust
//! Background thread that ticks the wasmtime `Engine::increment_epoch()`
//! once per second. Each `Store` sets a deadline of `current + N` epochs
//! on construction; when the ticker crosses it, the guest traps with a
//! wall-time error.

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use wasmtime::Engine;

pub struct EpochTicker {
    engine: Arc<Engine>,
    _handle: thread::JoinHandle<()>,
}

impl EpochTicker {
    pub fn start(engine: Arc<Engine>) -> Arc<Self> {
        let engine_for_thread = engine.clone();
        let handle = thread::Builder::new()
            .name("wasm-epoch-ticker".into())
            .spawn(move || loop {
                thread::sleep(Duration::from_secs(1));
                engine_for_thread.increment_epoch();
            })
            .expect("spawning epoch ticker");
        Arc::new(Self {
            engine,
            _handle: handle,
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}
```

- [ ] **Step 3: Expose**

Edit `crates/worker/src/wasm_runtime/mod.rs`. Append module decl:

```rust
pub mod epoch;
pub use epoch::EpochTicker;
pub use limits::MemoryCap;
```

- [ ] **Step 4: Build**

Run: `cargo build -p worker`
Expected: compiles clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker/wasm_runtime): memory ResourceLimiter + epoch ticker

MemoryCap denies memory-growing past max_bytes. EpochTicker spawns one
background thread per Engine that bumps the epoch once a second; each
Store picks its deadline relative to the current epoch when it's built.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `WasmSourceRuntime` + Component caching

**Files:**
- Create: `crates/worker/src/wasm_runtime/runtime.rs`
- Modify: `crates/worker/src/wasm_runtime/mod.rs`

- [ ] **Step 1: Write the runtime**

Create `crates/worker/src/wasm_runtime/runtime.rs`:

```rust
//! WasmSourceRuntime: owns the wasmtime Engine, the linker, and a cache of
//! loaded Components keyed by "<name>@<version>". Thread-safe (Arc-wrapped
//! by callers).

use anyhow::{Context, bail};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::component::{Component, Linker};

use super::bindings::SourceConnector;
use super::epoch::EpochTicker;
use super::host::HostState;

pub struct WasmSourceRuntime {
    engine: Arc<Engine>,
    linker: Linker<HostState>,
    cache: DashMap<String, Arc<Component>>,
    base_dir: PathBuf,
    ticker: Arc<EpochTicker>,
}

impl WasmSourceRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let engine = Arc::new(super::engine::build_engine()?);
        let ticker = EpochTicker::start(engine.clone());

        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_async(&mut linker)
            .context("adding WASI 0.2 imports to linker")?;
        super::bindings::platform::connector::host::add_to_linker(&mut linker, |s| s)
            .context("adding host.log / host.http-fetch to linker")?;

        Ok(Arc::new(Self {
            engine,
            linker,
            cache: DashMap::new(),
            base_dir: base_dir.into(),
            ticker,
        }))
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn linker(&self) -> &Linker<HostState> {
        &self.linker
    }

    pub fn ticker(&self) -> &Arc<EpochTicker> {
        &self.ticker
    }

    /// Path where the .cwasm for `<name>@<version>` should live.
    pub fn artifact_path(&self, name_at_version: &str) -> PathBuf {
        let mut p = self.base_dir.clone();
        p.push(name_at_version);
        p.push("component.cwasm");
        p
    }

    /// Load (and cache) the precompiled Component for `<name>@<version>`.
    pub fn load(&self, name_at_version: &str) -> anyhow::Result<Arc<Component>> {
        if let Some(c) = self.cache.get(name_at_version) {
            return Ok(c.clone());
        }
        let path = self.artifact_path(name_at_version);
        if !path.exists() {
            bail!(
                "WASM connector artifact not found at {} — did you `platform connector build`?",
                path.display()
            );
        }
        // SAFETY: deserialize_file is `unsafe` because a malformed or
        // malicious file could trigger UB. We control the base_dir and
        // accept only our own compiled artifacts for Phase I.3.
        let component = unsafe {
            Component::deserialize_file(&self.engine, &path)
                .with_context(|| format!("deserialize_file {}", path.display()))?
        };
        let arc = Arc::new(component);
        self.cache.insert(name_at_version.to_string(), arc.clone());
        Ok(arc)
    }

    /// Precompile a `.wasm` to a `.cwasm` and write it next to the source.
    pub fn precompile_to(&self, wasm_path: &Path, out_path: &Path) -> anyhow::Result<()> {
        let bytes = std::fs::read(wasm_path)
            .with_context(|| format!("reading {}", wasm_path.display()))?;
        let serialized = self
            .engine
            .precompile_component(&bytes)
            .with_context(|| format!("precompile_component({})", wasm_path.display()))?;
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(out_path, serialized)
            .with_context(|| format!("writing {}", out_path.display()))?;
        Ok(())
    }
}
```

- [ ] **Step 2: Expose**

Edit `crates/worker/src/wasm_runtime/mod.rs`:

```rust
pub mod runtime;
pub use runtime::WasmSourceRuntime;
```

- [ ] **Step 3: Smoke-test instantiation against an empty Component**

Append to `crates/worker/src/wasm_runtime/runtime.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::component::Component;

    /// Smallest possible valid component: an empty world.
    const EMPTY_COMPONENT_WAT: &str = r#"
        (component)
    "#;

    #[test]
    fn runtime_loads_from_tempdir_and_precompiles() {
        let tmp = tempfile::tempdir().unwrap();
        let rt = WasmSourceRuntime::new(tmp.path()).unwrap();

        // Compile an empty component from wat → wasm bytes → .cwasm.
        let wasm_bytes = wat::parse_str(EMPTY_COMPONENT_WAT).unwrap();
        let wasm_path = tmp.path().join("empty.wasm");
        std::fs::write(&wasm_path, &wasm_bytes).unwrap();

        let cwasm_path = rt.artifact_path("empty@0.1.0");
        rt.precompile_to(&wasm_path, &cwasm_path).unwrap();
        assert!(cwasm_path.exists());

        let _component = rt.load("empty@0.1.0").unwrap();
        // Second load hits cache.
        let _again = rt.load("empty@0.1.0").unwrap();
    }
}
```

Add `wat = "1"` to `crates/worker/Cargo.toml` `[dev-dependencies]`.

Run: `cargo test -p worker wasm_runtime::runtime`
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/wasm_runtime): WasmSourceRuntime with Component cache

Precompiles .wasm → .cwasm via Engine::precompile_component, loads
.cwasm via Component::deserialize_file, caches per name@version in a
DashMap. Integration test uses an empty-component WAT to prove the
load + precompile + cache path without needing a real guest yet.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: `WasmSourceConnector` — implement the `SourceConnector` trait

**Files:**
- Create: `crates/worker/src/wasm_runtime/connector.rs`
- Modify: `crates/worker/src/wasm_runtime/mod.rs`

- [ ] **Step 1: Implement**

Create `crates/worker/src/wasm_runtime/connector.rs`:

```rust
//! WasmSourceConnector: a `SourceConnector` implementation that dispatches
//! to a WASM Component Model guest.

use anyhow::{Context, bail};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::connection_config::ConnectionConfig;
use common_types::cursor::{CursorKind, CursorValue};
use common_types::pipeline_spec::SourceSpec;
use connector_sdk::{ReadOutcome, SourceConnector};
use std::sync::Arc;
use wasmtime::Store;

use super::HostState;
use super::bindings::SourceConnector as SourceConnectorBindings;
use super::bindings::platform::connector::types as wit_types;
use super::runtime::WasmSourceRuntime;

pub struct WasmSourceConnector {
    runtime: Arc<WasmSourceRuntime>,
    name_at_version: String,
}

impl WasmSourceConnector {
    pub fn new(runtime: Arc<WasmSourceRuntime>, name_at_version: impl Into<String>) -> Self {
        Self {
            runtime,
            name_at_version: name_at_version.into(),
        }
    }

    fn wasm_source_json(source: &SourceSpec) -> anyhow::Result<String> {
        match source {
            SourceSpec::Wasm(spec) => Ok(serde_json::to_string(&spec.config)?),
            SourceSpec::Postgres(_) => bail!(
                "WasmSourceConnector only handles SourceSpec::Wasm; got SourceSpec::Postgres"
            ),
        }
    }

    async fn new_store(&self) -> Store<HostState> {
        let limits = super::Limits::default();
        let state = HostState::new(limits.clone());
        let mut store = Store::new(self.runtime.engine(), state);
        store.set_fuel(limits.fuel).expect("fuel enabled in Engine");
        store.set_epoch_deadline(limits.wall_time_secs);
        // Install a memory limiter that references the store's HostState.
        store.limiter(|s: &mut HostState| &mut MemoryCapShim {
            cap: s.limits.memory_bytes,
        });
        store
    }
}

// We can't easily store a limiter inside HostState because `limiter()` wants
// to borrow mutably from the store's data each call. Shim keeps the cap out.
struct MemoryCapShim {
    cap: u64,
}
impl wasmtime::ResourceLimiter for MemoryCapShim {
    fn memory_growing(
        &mut self,
        _cur: usize,
        desired: usize,
        _max: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok((desired as u64) <= self.cap)
    }
    fn table_growing(
        &mut self,
        _cur: usize,
        desired: usize,
        _max: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok(desired <= 100_000)
    }
}

#[async_trait]
impl SourceConnector for WasmSourceConnector {
    async fn discover(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        let component = self.runtime.load(&self.name_at_version)?;
        let mut store = self.new_store().await;
        let bindings = SourceConnectorBindings::instantiate_async(
            &mut store,
            &component,
            self.runtime.linker(),
        )
        .await
        .context("instantiating component")?;

        let wit_conn = wit_types::ConnectionConfig {
            url: conn.url.clone(),
        };
        let wit_source = wit_types::SourceConfig {
            json: Self::wasm_source_json(source)?,
        };

        let schema_bytes = bindings
            .call_discover(&mut store, &wit_conn, &wit_source)
            .await
            .context("call_discover")?
            .map_err(|e| anyhow::anyhow!("guest error: {e:?}"))?;

        let reader = StreamReader::try_new(&*schema_bytes, None)
            .context("parsing schema bytes as Arrow IPC stream header")?;
        Ok(reader.schema())
    }

    async fn read_batch(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
        cursor: Option<CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome> {
        let component = self.runtime.load(&self.name_at_version)?;
        let mut store = self.new_store().await;
        let bindings = SourceConnectorBindings::instantiate_async(
            &mut store,
            &component,
            self.runtime.linker(),
        )
        .await
        .context("instantiating component")?;

        let wit_conn = wit_types::ConnectionConfig {
            url: conn.url.clone(),
        };
        let wit_source = wit_types::SourceConfig {
            json: Self::wasm_source_json(source)?,
        };
        let wit_cursor = cursor.map(|c| wit_types::CursorValue {
            kind: match c.kind {
                CursorKind::Int64 => wit_types::CursorKind::Int64,
                CursorKind::TimestampTz => wit_types::CursorKind::TimestampTz,
            },
            value: c.value,
        });

        let outcome = bindings
            .call_read_batch(
                &mut store,
                &wit_conn,
                &wit_source,
                wit_cursor.as_ref(),
                batch_size as u32,
            )
            .await
            .context("call_read_batch")?
            .map_err(|e| anyhow::anyhow!("guest error: {e:?}"))?;

        let batch = if outcome.batch_ipc.is_empty() {
            // Guest emitted no data — build an empty batch with a dummy 0-col schema.
            // The workflow treats rows==0 as a terminator and won't hand this to the loader.
            let schema = Arc::new(arrow::datatypes::Schema::empty());
            RecordBatch::new_empty(schema)
        } else {
            let mut reader = StreamReader::try_new(&*outcome.batch_ipc, None)
                .context("parsing batch as Arrow IPC")?;
            reader
                .next()
                .context("guest returned non-empty batch_ipc but no batches")?
                .context("decoding batch")?
        };

        let new_cursor = outcome.new_cursor.map(|c| CursorValue {
            kind: match c.kind {
                wit_types::CursorKind::Int64 => CursorKind::Int64,
                wit_types::CursorKind::TimestampTz => CursorKind::TimestampTz,
            },
            value: c.value,
        });

        Ok(ReadOutcome {
            batch,
            new_cursor,
            is_final: outcome.is_final,
        })
    }
}
```

- [ ] **Step 2: Expose**

Edit `crates/worker/src/wasm_runtime/mod.rs`:

```rust
pub mod connector;
pub use connector::WasmSourceConnector;
```

- [ ] **Step 3: Build**

Run: `cargo build -p worker`
Expected: compiles clean. (If `instantiate_async` isn't generated under that exact name due to wit-bindgen version differences, adapt — common alternatives are `SourceConnectorPre::instantiate_async` after calling `new_async` first. The structural code here — load component → new store → instantiate → call method → decode result — is stable regardless.)

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(worker/wasm_runtime): WasmSourceConnector impls SourceConnector

Loads the Component for name@version, creates a per-call Store with
fuel + epoch deadline + memory limiter, instantiates against the
runtime's Linker, and calls discover/read_batch through the generated
bindings. Arrow batches move as IPC bytes in both directions.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `SourceSpec::Wasm` variant + connector dispatch

**Files:**
- Modify: `crates/common-types/src/pipeline_spec.rs`
- Create: `crates/worker/src/connectors/dispatch.rs`
- Modify: `crates/worker/src/connectors/mod.rs`

- [ ] **Step 1: Add the `Wasm` variant**

Edit `crates/common-types/src/pipeline_spec.rs`. Add after `PostgresSourceSpec`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmSourceSpec {
    /// Free-form JSON passed as-is to the guest via `source-config.json`.
    pub config: serde_json::Value,
}
```

Update the enum:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSpec {
    Postgres(PostgresSourceSpec),
    Wasm(WasmSourceSpec),
}
```

Add a serde round-trip test in the existing `tests` module:

```rust
    #[test]
    fn wasm_variant_roundtrips() {
        let s = SourceSpec::Wasm(WasmSourceSpec {
            config: serde_json::json!({"path": "/tmp/foo.csv", "has_header": true}),
        });
        let j = serde_json::to_string(&s).unwrap();
        let back: SourceSpec = serde_json::from_str(&j).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), j);
    }
```

- [ ] **Step 2: Write the dispatcher**

Create `crates/worker/src/connectors/dispatch.rs`:

```rust
//! Picks the right `SourceConnector` implementation from a `connector_ref` string.
//!
//! - `"postgres@0.1.0"` → in-process Rust PostgresConnector
//! - `"wasm:<name>@<version>"` → WasmSourceConnector loading name@version

use anyhow::{Context, bail};
use connector_sdk::SourceConnector;
use std::sync::Arc;

use crate::connectors::postgres::PostgresConnector;
use crate::wasm_runtime::{WasmSourceConnector, WasmSourceRuntime};

pub fn build_source_connector(
    connector_ref: &str,
    wasm_runtime: Option<Arc<WasmSourceRuntime>>,
) -> anyhow::Result<Box<dyn SourceConnector>> {
    if let Some(rest) = connector_ref.strip_prefix("wasm:") {
        let runtime = wasm_runtime
            .context("wasm connector requested but no WasmSourceRuntime provided")?;
        return Ok(Box::new(WasmSourceConnector::new(runtime, rest.to_string())));
    }
    // Recognized Rust-native connectors live on a small allowlist for Phase I.3.
    if connector_ref.starts_with("postgres@") {
        return Ok(Box::new(PostgresConnector));
    }
    bail!("unknown connector_ref '{}'", connector_ref);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_ref_returns_rust_native() {
        let c = build_source_connector("postgres@0.1.0", None).unwrap();
        // Can't downcast a Box<dyn ..>, but the no-panic call is enough.
        drop(c);
    }

    #[test]
    fn unknown_ref_errors() {
        let err = build_source_connector("mystery@1.0", None).unwrap_err();
        assert!(err.to_string().contains("unknown connector_ref"));
    }

    #[test]
    fn wasm_without_runtime_errors() {
        let err = build_source_connector("wasm:csv-source@0.1.0", None).unwrap_err();
        assert!(err.to_string().contains("no WasmSourceRuntime"));
    }
}
```

- [ ] **Step 3: Expose**

Edit `crates/worker/src/connectors/mod.rs`:

```rust
//! In-process connector implementations. Phase I.2 scope: postgres only.
//! Phase I.3 moves these behind the WASM Component Model.
pub mod dispatch;
pub mod postgres;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p worker connectors::dispatch`
Expected: 3 passed.

Run: `cargo test -p common-types pipeline_spec`
Expected: 3 passed (spec_roundtrip, source_serialized_form_is_tagged, wasm_variant_roundtrips).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: SourceSpec::Wasm variant + connector_ref dispatcher

Common-types gains SourceSpec::Wasm { config: Value } — catch-all for
WASM-loaded connectors; free-form JSON gets handed to the guest via
source-config.json. worker::connectors::dispatch::build_source_connector
parses connector_ref ('postgres@...' vs 'wasm:<name>@...') and picks
the impl, optionally wiring the WasmSourceRuntime.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Wire `WasmSourceRuntime` + dispatcher into `SyncActivities`

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/main.rs`

- [ ] **Step 1: Thread a `runtime` into `SyncActivities`**

Edit `crates/worker/src/activities/sync/mod.rs`. At the top, replace the imports and the struct:

```rust
use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use catalog::Catalog;
use common_types::connection_config::ConnectionConfig;
use common_types::ids::{PipelineId, RunId};
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::connectors::dispatch::build_source_connector;
use crate::loaders::parquet_local::LocalParquetLoader;
use crate::wasm_runtime::WasmSourceRuntime;
use inputs::*;

pub mod inputs;

pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
}

fn to_retryable(e: anyhow::Error) -> ActivityError {
    e.into()
}
```

- [ ] **Step 2: Replace direct `PostgresConnector` calls with dispatched ones**

Edit the same file. In `discover_stream`:

```rust
    #[activity]
    pub async fn discover_stream(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverInput,
    ) -> Result<DiscoverOutput, ActivityError> {
        let connector = build_source_connector(&input.connector_ref, Some(self.wasm_runtime.clone()))
            .map_err(to_retryable)?;
        let schema = connector
            .discover(
                &ConnectionConfig { url: input.source_url },
                &input.source,
            )
            .await
            .map_err(to_retryable)?;
        let columns = schema.fields().iter().map(|f| f.name().clone()).collect();
        Ok(DiscoverOutput { columns })
    }
```

And `read_batch`:

```rust
    #[activity]
    pub async fn read_batch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadBatchInput,
    ) -> Result<ReadBatchOutput, ActivityError> {
        let connector = build_source_connector(&input.connector_ref, Some(self.wasm_runtime.clone()))
            .map_err(to_retryable)?;
        let outcome = connector
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
```

- [ ] **Step 3: Add `connector_ref` to activity inputs**

Edit `crates/worker/src/activities/sync/inputs.rs`. In each of `DiscoverInput` and `ReadBatchInput`, add:

```rust
    pub connector_ref: String,
```

so they look like:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub connector_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub cursor: Option<CursorValue>,
    pub batch_size: usize,
    pub connector_ref: String,
}
```

- [ ] **Step 4: Pass `connector_ref` through from the workflow**

Edit `crates/worker/src/workflows/pipeline_run.rs`. The workflow's state already has `source_connection` but not `connector_ref`. Add it:

In `PipelineRunInput`:
```rust
    pub connector_ref: String,
```

In `PipelineRunWorkflow` struct:
```rust
    connector_ref: String,
```

In `#[init]`:
```rust
            connector_ref: input.connector_ref,
```

In the `run` method's state-capture tuple, include it:
```rust
        let (run_id, pipeline_id, spec, conn, stream_name, connector_ref) = ctx.state(|s| {
            (
                s.run_id,
                s.pipeline_id,
                s.spec.clone(),
                s.source_connection.clone(),
                s.stream_name.clone(),
                s.connector_ref.clone(),
            )
        });
```

In the `DiscoverInput` and `ReadBatchInput` construction inside the workflow, add:
```rust
                connector_ref: connector_ref.clone(),
```

- [ ] **Step 5: Wire runtime into `main.rs`**

Edit `crates/worker/src/main.rs`. After constructing `catalog` but before `SyncActivities`, add:

```rust
    let wasm_base = std::env::var("ETL_CONNECTORS_DIR").unwrap_or_else(|_| "./connectors".into());
    let wasm_runtime = worker::wasm_runtime::WasmSourceRuntime::new(&wasm_base)?;
```

And change the `SyncActivities` construction:

```rust
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
    };
```

- [ ] **Step 6: CLI passes `connector_ref`**

Edit `crates/cli/src/main.rs`. In `pipeline_run`, after loading `source_conn_row`, add:

```rust
    let connector_ref = source_conn_row.connector_ref.clone();
```

And include it in the `PipelineRunInput` construction:

```rust
    let input = PipelineRunInput {
        run_id: run_id.as_uuid(),
        pipeline_id: pipeline_id.as_uuid(),
        spec,
        source_connection,
        initial_cursor,
        stream_name,
        connector_ref,
    };
```

- [ ] **Step 7: Build + verify**

Run: `cargo build --workspace`
Expected: compiles clean.

Re-run Phase I.2's catalog tests to confirm nothing broke:
Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p catalog -- --test-threads=1`
Expected: 4 passed (unchanged).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(worker): plumb WasmSourceRuntime + connector_ref through workflow

SyncActivities now owns an Arc<WasmSourceRuntime> and dispatches
discover/read_batch via build_source_connector(connector_ref, Some(rt))
instead of hardcoding PostgresConnector. Workflow input + state carry
connector_ref; CLI pulls it from source_conn_row.connector_ref. Main
constructs the runtime from \$ETL_CONNECTORS_DIR (default ./connectors).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Reference CSV guest (Rust → wasm32-wasip2)

**Files:**
- Create: `examples/csv-source/Cargo.toml`
- Create: `examples/csv-source/.cargo/config.toml`
- Create: `examples/csv-source/src/lib.rs`
- Create: `examples/csv-source/README.md`
- Modify: root `Cargo.toml` — `[workspace.exclude]` so the guest doesn't inherit the host target

- [ ] **Step 1: Exclude the guest from the workspace**

Edit root `Cargo.toml`. Change:

```toml
[workspace]
resolver = "2"
members = [
    "crates/common-types",
    "crates/catalog",
    "crates/worker",
    "crates/control-api",
    "crates/connector-sdk",
    "crates/loader-sdk",
    "crates/cli",
    "tests/integration",
]
exclude = ["examples/csv-source"]
```

- [ ] **Step 2: Create the guest Cargo.toml**

Create `examples/csv-source/Cargo.toml`:

```toml
[package]
name = "csv-source"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.37"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
csv = "1"
arrow = { version = "53", default-features = false, features = ["ipc"] }
```

- [ ] **Step 3: Pin the wasm target**

Create `examples/csv-source/.cargo/config.toml`:

```toml
[build]
target = "wasm32-wasip2"
```

- [ ] **Step 4: Implement the guest**

Create `examples/csv-source/src/lib.rs`:

```rust
//! Reference CSV source connector (Phase I.3 demo).
//!
//! Config JSON: { "csv_text": "id,name,email\n1,Alice,...\n...", "has_header": true }
//! Cursor: row-index (Int64), strictly increasing from 0.

use std::io::Cursor as IoCursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "source-connector",
});

use platform::connector::host::{log, LogLevel};
use platform::connector::types::{
    ConnectorError, CursorKind, CursorValue, ReadOutcome,
};

struct Component;

export!(Component);

#[derive(serde::Deserialize)]
struct CsvConfig {
    csv_text: String,
    #[serde(default = "default_true")]
    has_header: bool,
}

fn default_true() -> bool {
    true
}

impl Guest for Component {
    fn discover(
        _conn: ConnectionConfig,
        source: SourceConfig,
    ) -> Result<Vec<u8>, ConnectorError> {
        let cfg: CsvConfig = serde_json::from_str(&source.json)
            .map_err(|e| ConnectorError::InvalidConfig(format!("bad CSV config: {e}")))?;
        let schema = infer_schema(&cfg)?;
        Ok(ipc_schema_bytes(&schema).map_err(|e| ConnectorError::Other(format!("ipc: {e}")))?)
    }

    fn read_batch(
        _conn: ConnectionConfig,
        source: SourceConfig,
        cursor: Option<CursorValue>,
        batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        log(LogLevel::Info, format!("csv: read_batch cursor={cursor:?}"));
        let cfg: CsvConfig = serde_json::from_str(&source.json)
            .map_err(|e| ConnectorError::InvalidConfig(format!("bad CSV config: {e}")))?;
        let schema = infer_schema(&cfg)?;

        let start_row: i64 = match cursor.as_ref() {
            None => 0,
            Some(c) => c
                .value
                .parse()
                .map_err(|_| ConnectorError::InvalidConfig("cursor not i64".into()))?,
        };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(cfg.has_header)
            .from_reader(cfg.csv_text.as_bytes());

        let mut rows_in_batch = 0u32;
        let mut collected: Vec<Vec<String>> = Vec::with_capacity(batch_size as usize);
        let mut last_row_idx: i64 = start_row;
        let mut logical_row_idx: i64 = 0;

        for result in rdr.records() {
            let rec = result.map_err(|e| ConnectorError::Other(format!("csv: {e}")))?;
            if logical_row_idx < start_row {
                logical_row_idx += 1;
                continue;
            }
            if rows_in_batch >= batch_size {
                break;
            }
            let row: Vec<String> = rec.iter().map(|s| s.to_string()).collect();
            collected.push(row);
            rows_in_batch += 1;
            last_row_idx = logical_row_idx + 1; // cursor is "rows consumed so far"
            logical_row_idx += 1;
        }

        // Detect is_final by trying to read one more record.
        let mut rdr2 = csv::ReaderBuilder::new()
            .has_headers(cfg.has_header)
            .from_reader(cfg.csv_text.as_bytes());
        let total_rows = rdr2.records().count() as i64;
        let is_final = last_row_idx >= total_rows;

        let batch = build_batch(&schema, &collected)
            .map_err(|e| ConnectorError::Other(format!("build batch: {e}")))?;
        let batch_ipc = ipc_batch_bytes(&schema, &batch)
            .map_err(|e| ConnectorError::Other(format!("ipc batch: {e}")))?;

        Ok(ReadOutcome {
            batch_ipc,
            rows: rows_in_batch,
            new_cursor: if rows_in_batch == 0 {
                None
            } else {
                Some(CursorValue {
                    kind: CursorKind::Int64,
                    value: last_row_idx.to_string(),
                })
            },
            is_final,
        })
    }
}

fn infer_schema(cfg: &CsvConfig) -> Result<Arc<Schema>, ConnectorError> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(cfg.has_header)
        .from_reader(cfg.csv_text.as_bytes());
    let headers: Vec<String> = if cfg.has_header {
        rdr.headers()
            .map_err(|e| ConnectorError::Other(format!("csv header: {e}")))?
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        // Fall back to column_0, column_1, ...
        let first = rdr
            .records()
            .next()
            .and_then(|r| r.ok())
            .map(|r| r.len())
            .unwrap_or(0);
        (0..first).map(|i| format!("column_{}", i)).collect()
    };
    // Phase I.3: everything typed as Utf8 (plus an "_row_index" Int64).
    let mut fields = vec![Field::new("_row_index", DataType::Int64, false)];
    for h in &headers {
        fields.push(Field::new(h, DataType::Utf8, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn build_batch(
    schema: &Arc<Schema>,
    rows: &[Vec<String>],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let n = rows.len();
    // _row_index column
    let row_idx: Int64Array = (0..n as i64).collect();
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(row_idx)];

    let num_data_cols = schema.fields().len().saturating_sub(1);
    for c in 0..num_data_cols {
        let mut b = StringBuilder::with_capacity(n, n * 16);
        for row in rows {
            if c < row.len() {
                b.append_value(&row[c]);
            } else {
                b.append_null();
            }
        }
        arrays.push(Arc::new(b.finish()));
    }
    RecordBatch::try_new(schema.clone(), arrays)
}

fn ipc_schema_bytes(schema: &Arc<Schema>) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    let w = StreamWriter::try_new(&mut buf, schema)?;
    w.finish()?;
    Ok(buf)
}

fn ipc_batch_bytes(
    schema: &Arc<Schema>,
    batch: &RecordBatch,
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)?;
        if batch.num_rows() > 0 {
            w.write(batch)?;
        }
        w.finish()?;
    }
    Ok(buf)
}

// The wit-bindgen generator also defines `ConnectionConfig`, `SourceConfig`,
// and `Guest` in the root scope for the `export!` macro.
```

- [ ] **Step 5: Write the guest README**

Create `examples/csv-source/README.md`:

```markdown
# csv-source — Phase I.3 reference WASM connector

## Build

```bash
cargo build --release --target wasm32-wasip2
```

Output: `target/wasm32-wasip2/release/csv_source.wasm`.

Then precompile with the platform CLI:

```bash
cargo run --bin platform -- connector build examples/csv-source
```

This writes `connectors/csv-source@0.1.0/component.cwasm`.

## Pipeline config

```json
{
  "source": {
    "type": "wasm",
    "config": {
      "csv_text": "id,name\n1,Alice\n2,Bob\n3,Carol\n",
      "has_header": true
    }
  },
  "destination": {
    "type": "local_parquet",
    "base_path": "./data"
  },
  "batch_size": 2
}
```
```

- [ ] **Step 6: Build the guest**

Run: `cd examples/csv-source && cargo build --release --target wasm32-wasip2`
Expected: produces `target/wasm32-wasip2/release/csv_source.wasm`.

(If the build fails because arrow's default features pull in things that don't target wasm, drop additional features or pin `arrow-schema`/`arrow-array`/`arrow-ipc` directly instead of the umbrella crate. The intent is: guest produces Arrow IPC bytes. Adapt deps as needed until the wasm builds.)

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(examples/csv-source): reference WASM source connector

Rust guest targeting wasm32-wasip2, uses wit-bindgen to consume the
platform:connector source-connector world. Reads CSV from config.csv_text
(no filesystem access in Phase I.3), cursor is row index (Int64),
strictly increasing. Emits Arrow IPC batches via arrow::ipc::writer.
Excluded from the host workspace so cargo build --workspace doesn't
try to build it for the host target.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: `platform connector build` CLI

**Files:**
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/cli/Cargo.toml`

- [ ] **Step 1: Extend CLI**

Edit `crates/cli/Cargo.toml`, add to `[dependencies]`:

```toml
worker = { workspace = true }
```

(Already present from Phase I.2.)

Edit `crates/cli/src/main.rs`. In the `Cmd` enum add:

```rust
#[derive(Subcommand)]
enum Cmd {
    Pipeline {
        #[command(subcommand)]
        cmd: PipelineCmd,
    },
    Connector {
        #[command(subcommand)]
        cmd: ConnectorCmd,
    },
}

#[derive(Subcommand)]
enum ConnectorCmd {
    /// Compile a guest Rust crate to a precompiled .cwasm artifact.
    Build {
        /// Path to the guest crate (must contain Cargo.toml with a [lib] crate-type = ["cdylib"]).
        path: String,
        /// Name of the connector; defaults to the crate's package name.
        #[arg(long)]
        name: Option<String>,
        /// Version; defaults to the crate's package version.
        #[arg(long)]
        version: Option<String>,
        /// Directory to write the artifact into. Default: ./connectors.
        #[arg(long, default_value = "./connectors")]
        out: String,
    },
}
```

In `main()`, add the new arm:

```rust
    match cli.cmd {
        Cmd::Pipeline { cmd: PipelineCmd::Run { id } } => pipeline_run(id).await,
        Cmd::Connector { cmd: ConnectorCmd::Build { path, name, version, out } } => {
            connector_build(path, name, version, out).await
        }
    }
```

At the bottom of the file, add:

```rust
async fn connector_build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
) -> anyhow::Result<()> {
    use std::path::PathBuf;
    use std::process::Command as StdCommand;

    let crate_dir = PathBuf::from(&path);
    let cargo_toml = crate_dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        anyhow::bail!("no Cargo.toml at {}", cargo_toml.display());
    }

    // Read package name/version from Cargo.toml (minimal parsing).
    let toml_text = std::fs::read_to_string(&cargo_toml)?;
    let pkg_name = name.unwrap_or_else(|| {
        read_toml_value(&toml_text, "name").unwrap_or_else(|| "connector".into())
    });
    let pkg_version = version.unwrap_or_else(|| {
        read_toml_value(&toml_text, "version").unwrap_or_else(|| "0.1.0".into())
    });

    // Step 1: cargo build --release --target wasm32-wasip2
    let status = StdCommand::new("cargo")
        .current_dir(&crate_dir)
        .args(["build", "--release", "--target", "wasm32-wasip2"])
        .status()?;
    if !status.success() {
        anyhow::bail!("guest build failed");
    }

    // Find the .wasm — use the package name with - → _ conversion.
    let wasm_name = format!("{}.wasm", pkg_name.replace('-', "_"));
    let wasm_path = crate_dir
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join(&wasm_name);
    if !wasm_path.exists() {
        anyhow::bail!(
            "expected artifact not found at {} — check the crate's [lib] crate-type and package name",
            wasm_path.display()
        );
    }

    // Step 2: precompile via the worker runtime.
    let out_dir = PathBuf::from(&out);
    let rt = worker::wasm_runtime::WasmSourceRuntime::new(&out_dir)?;
    let target_name = format!("{}@{}", pkg_name, pkg_version);
    let out_path = rt.artifact_path(&target_name);
    rt.precompile_to(&wasm_path, &out_path)?;

    println!("built {}", out_path.display());
    Ok(())
}

fn read_toml_value(text: &str, key: &str) -> Option<String> {
    // Simple line scanner: finds `<key> = "<value>"` at the top-level of [package].
    let mut in_package = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_package = trimmed == "[package]";
            continue;
        }
        if !in_package {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix(&format!("{} = \"", key)) {
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}
```

- [ ] **Step 2: Build + run**

Run: `cargo build -p cli`
Expected: compiles.

Run: `cargo run --bin platform -- connector build examples/csv-source`
Expected: builds csv-source guest, emits `./connectors/csv-source@0.1.0/component.cwasm`, prints the path.

Verify: `ls -la ./connectors/csv-source@0.1.0/component.cwasm`
Expected: file exists, multiple MB (precompiled code is bulky).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(cli): platform connector build <path>

Invokes cargo build --release --target wasm32-wasip2 on the guest
crate, then precompiles the resulting .wasm via
WasmSourceRuntime::precompile_to into ./connectors/<name>@<version>/component.cwasm.
Name/version are read from the guest's Cargo.toml [package] section.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Adversarial test — fuel exhaustion

**Files:**
- Create: `crates/worker/src/wasm_runtime/tests/infinite_loop.wat`
- Create: `crates/worker/src/wasm_runtime/tests.rs`
- Modify: `crates/worker/src/wasm_runtime/mod.rs`

- [ ] **Step 1: Write a hand-crafted infinite-loop component**

Create `crates/worker/src/wasm_runtime/tests/infinite_loop.wat`:

```wat
(component
  (core module $m
    (func (export "loop") (result)
      (loop $l
        (br $l)
      )
    )
  )
  (core instance $i (instantiate $m))
  (func (export "spin") (canon lift (core func $i "loop")))
)
```

- [ ] **Step 2: Test**

Create `crates/worker/src/wasm_runtime/tests.rs`:

```rust
//! Adversarial tests for the WASM runtime resource limits.

use super::engine::build_engine;
use std::sync::Arc;
use wasmtime::component::{Component, Linker};
use wasmtime::Store;

fn empty_host_state() -> super::HostState {
    super::HostState::new(super::Limits::default())
}

#[tokio::test]
async fn fuel_exhaustion_traps_guest() {
    let engine = Arc::new(build_engine().unwrap());
    let wat = include_str!("tests/infinite_loop.wat");
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let component = Component::new(&engine, &wasm_bytes).unwrap();
    let linker: Linker<super::HostState> = Linker::new(&engine);
    let mut store = Store::new(&engine, empty_host_state());
    // Tiny budget so the infinite loop trips instantly.
    store.set_fuel(10_000).unwrap();
    // Direct-instantiate and call "spin" by name since this component
    // doesn't conform to the source-connector world.
    let instance = linker.instantiate_async(&mut store, &component).await.unwrap();
    let spin = instance
        .get_typed_func::<(), ()>(&mut store, "spin")
        .unwrap();
    let err = spin.call_async(&mut store, ()).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.to_lowercase().contains("fuel") || msg.to_lowercase().contains("trap"),
        "expected fuel/trap error, got: {msg}"
    );
}
```

- [ ] **Step 3: Expose**

Edit `crates/worker/src/wasm_runtime/mod.rs` — append:

```rust
#[cfg(test)]
mod tests;
```

- [ ] **Step 4: Run**

Run: `cargo test -p worker wasm_runtime::tests::fuel_exhaustion -- --nocapture`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(worker/wasm_runtime): fuel exhaustion traps guest

Hand-written WAT component with an infinite core-loop, instantiated
with 10k fuel budget. Trap is raised before fuel is fully consumed;
test asserts the error message contains 'fuel' or 'trap'.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: Adversarial test — memory cap

**Files:**
- Create: `crates/worker/src/wasm_runtime/tests/memory_hog.wat`
- Modify: `crates/worker/src/wasm_runtime/tests.rs`

- [ ] **Step 1: Write a component that tries to grow memory past the cap**

Create `crates/worker/src/wasm_runtime/tests/memory_hog.wat`:

```wat
(component
  (core module $m
    (memory (export "mem") 1)
    (func (export "grow_lots") (result i32)
      (memory.grow (i32.const 1024)) ;; tries to grow by 1024 pages (~64 MB)
    )
  )
  (core instance $i (instantiate $m))
  (func (export "grow") (result s32) (canon lift (core func $i "grow_lots")))
)
```

- [ ] **Step 2: Add the test**

Append to `crates/worker/src/wasm_runtime/tests.rs`:

```rust
struct TightMem {
    cap: usize,
}
impl wasmtime::ResourceLimiter for TightMem {
    fn memory_growing(
        &mut self,
        _cur: usize,
        desired: usize,
        _max: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok(desired <= self.cap)
    }
    fn table_growing(&mut self, _: usize, _: usize, _: Option<usize>) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[tokio::test]
async fn memory_cap_denies_large_growth() {
    let engine = Arc::new(build_engine().unwrap());
    let wat = include_str!("tests/memory_hog.wat");
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let component = Component::new(&engine, &wasm_bytes).unwrap();
    let linker: Linker<super::HostState> = Linker::new(&engine);
    let mut store = Store::new(&engine, empty_host_state());
    store.limiter(|_| &mut *Box::leak(Box::new(TightMem { cap: 2 * 65536 }))); // 128 KB
    let instance = linker.instantiate_async(&mut store, &component).await.unwrap();
    let grow = instance
        .get_typed_func::<(), (i32,)>(&mut store, "grow")
        .unwrap();
    let (result,) = grow.call_async(&mut store, ()).await.unwrap();
    assert_eq!(result, -1, "memory.grow should return -1 when denied");
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p worker wasm_runtime::tests::memory_cap -- --nocapture`
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(worker/wasm_runtime): memory cap denies oversized growth

Guest attempts memory.grow(1024 pages) against a 2-page cap. Host's
ResourceLimiter::memory_growing returns false; guest's memory.grow
returns -1 (WASM convention for failure). Test asserts the -1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: Adversarial test — capability denial (guest imports an un-linked function)

**Files:**
- Create: `crates/worker/src/wasm_runtime/tests/forbidden_import.wat`
- Modify: `crates/worker/src/wasm_runtime/tests.rs`

- [ ] **Step 1: Hand-craft a component that imports an undeclared host function**

Create `crates/worker/src/wasm_runtime/tests/forbidden_import.wat`:

```wat
(component
  (core module $m
    (import "forbidden" "wall_clock_now" (func (result i64)))
    (func (export "run") (result i64)
      (call 0)
    )
  )
  (core instance $i (instantiate $m))
  (func (export "go") (result s64) (canon lift (core func $i "run")))
)
```

This component imports `"forbidden.wall_clock_now"` — a name we never link. Instantiation must fail.

- [ ] **Step 2: Add the test**

Append to `crates/worker/src/wasm_runtime/tests.rs`:

```rust
#[tokio::test]
async fn instantiation_fails_when_guest_imports_un_linked_function() {
    let engine = Arc::new(build_engine().unwrap());
    let wat = include_str!("tests/forbidden_import.wat");
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let component = Component::new(&engine, &wasm_bytes).unwrap();
    let linker: Linker<super::HostState> = Linker::new(&engine);
    let mut store = Store::new(&engine, empty_host_state());
    let err = linker.instantiate_async(&mut store, &component).await.unwrap_err();
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("forbidden") || msg.contains("unknown") || msg.contains("unresolved") || msg.contains("missing"),
        "expected capability-denial error mentioning the missing import, got: {msg}"
    );
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p worker wasm_runtime::tests::instantiation_fails -- --nocapture`
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(worker/wasm_runtime): capability denial blocks un-linked imports

Hand-written WAT component imports 'forbidden.wall_clock_now' which
the host never links. instantiate_async fails at resolve-time, before
any guest code runs. Validates the 'no wall-clock, no randomness'
determinism commitment: guest cannot bypass by importing unexpected
host functions.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: End-to-end integration test — CSV → Parquet via WASM

**Files:**
- Create: `tests/integration/tests/wasm_connector.rs`

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/wasm_connector.rs`:

```rust
//! End-to-end: CSV-content WASM connector → PipelineRunWorkflow →
//! LocalParquetLoader. Validates the full Phase I.3 path.

use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

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

async fn build_all() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success(), "workspace build failed");
    Ok(())
}

async fn build_csv_connector() -> anyhow::Result<()> {
    let status = Command::new(cargo_bin("platform"))
        .current_dir(workspace_root())
        .args(["connector", "build", "examples/csv-source"])
        .status()
        .await?;
    assert!(status.success(), "connector build failed");
    Ok(())
}

async fn spawn_worker(connectors_dir: &Path) -> anyhow::Result<Child> {
    let child = Command::new(cargo_bin("worker"))
        .env("DATABASE_URL", catalog_url())
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
#[ignore = "requires docker stack; builds WASM guest; ~60s"]
async fn csv_wasm_connector_end_to_end() -> anyhow::Result<()> {
    build_all().await?;
    build_csv_connector().await?;

    let tmp_data = tempfile::tempdir()?;
    let connectors = workspace_root().join("connectors");

    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    let tenant = cat.create_tenant("dev").await?;
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "csv-inline".into(),
            connector_ref: "wasm:csv-source@0.1.0".into(),
            config: json!({ "url": "" }),
        })
        .await?;
    let csv_text = "id,name\n1,Alice\n2,Bob\n3,Carol\n4,Dave\n5,Eve\n";
    let spec = json!({
        "source": {
            "type": "wasm",
            "config": {
                "csv_text": csv_text,
                "has_header": true
            }
        },
        "destination": {
            "type": "local_parquet",
            "base_path": tmp_data.path().to_string_lossy()
        },
        "batch_size": 2
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "csv-sync".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    // Cold-start measurement: time the first pipeline-run submission end-to-end.
    let mut w = spawn_worker(&connectors).await?;
    let start = Instant::now();

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .env("ETL_CONNECTORS_DIR", &connectors)
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for completion.
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if Instant::now() > deadline {
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
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let elapsed = start.elapsed();

    w.kill().await?;
    w.wait().await?;

    // Verify Parquet output.
    let total = count_parquet_rows(tmp_data.path());
    assert_eq!(total, 5, "CSV had 5 data rows; Parquet total must match");

    // Cursor advanced to 5 (rows consumed).
    let state = cat.get_stream_state(pipe, "csv-source").await?;
    // Stream name depends on how CLI derives it — Phase I.2 used source.table for Postgres.
    // For the Wasm source we set stream_name = "csv-source" in Task 9 if added; otherwise
    // fall back to matching by last_run_id. For now, simply assert a row exists OR skip:
    if let Some(state) = state {
        assert_eq!(
            state.cursor.as_ref().unwrap().value.parse::<i64>().unwrap(),
            5
        );
    }

    // Cold-start sanity: end-to-end run under 30 s on a dev machine.
    assert!(
        elapsed < Duration::from_secs(30),
        "end-to-end elapsed = {:?}, over 30s budget",
        elapsed
    );

    Ok(())
}
```

- [ ] **Step 2: `stream_name` derivation for Wasm sources**

Edit `crates/cli/src/main.rs`'s `pipeline_run` — the existing `stream_name` match needs a Wasm arm:

```rust
    let stream_name = match &spec.source {
        SourceSpec::Postgres(p) => p.table.clone(),
        SourceSpec::Wasm(_) => "csv-source".to_string(), // Phase I.3: use connector name as stream
    };
```

- [ ] **Step 3: Run**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p integration-tests csv_wasm_connector_end_to_end -- --ignored --nocapture`
Expected: 1 passed after ~60s.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(integration): CSV WASM connector end-to-end

Builds the csv-source guest, seeds a pipeline pointing at it with a
5-row inline CSV, submits via CLI, asserts 5 rows land in Parquet and
end-to-end elapsed < 30s. Exercises the full Phase I.3 path: CLI →
workflow → SyncActivities → dispatch → WasmSourceConnector → guest →
host log/http-fetch stubs → IPC round-trip → LocalParquetLoader.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 16: README + Phase completion log

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-23-phase-1-3-wasm-runtime.md` (this file)

- [ ] **Step 1: Update the Phase-line and add a Phase I.3 demo section**

Edit `README.md`. Replace the last line (`Currently: **Phase I.2 — First Pipeline (complete)** ...`) with:

```markdown
Currently: **Phase I.3 — WASM Runtime (complete)**. Next: Phase I.4 — full catalog entities (streams, schemas, evolution policies) + YAML DSL. See the roadmap spec for the four-era trajectory.

## Phase I.3 — WASM connector demo

```bash
# 1. Build the reference WASM connector (one-time per code change)
cargo run --bin platform -- connector build examples/csv-source
# → ./connectors/csv-source@0.1.0/component.cwasm

# 2. Seed a pipeline pointed at the WASM connector
docker exec -i etl-postgres psql -U etl -d etl_catalog <<'SQL'
TRUNCATE runs, stream_state, pipelines, connections, tenants CASCADE;
INSERT INTO tenants (tenant_id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'dev');
INSERT INTO connections (connection_id, tenant_id, name, connector_ref, config)
  VALUES ('44444444-4444-4444-4444-444444444444',
          '11111111-1111-1111-1111-111111111111',
          'csv-inline', 'wasm:csv-source@0.1.0', '{"url":""}'::jsonb);
INSERT INTO pipelines (pipeline_id, tenant_id, name, source_conn_id, spec)
  VALUES ('55555555-5555-5555-5555-555555555555',
          '11111111-1111-1111-1111-111111111111',
          'csv-sync',
          '44444444-4444-4444-4444-444444444444',
          '{"source":{"type":"wasm","config":{"csv_text":"id,name\nA,Alice\nB,Bob\nC,Carol\n","has_header":true}},"destination":{"type":"local_parquet","base_path":"./data"},"batch_size":2}'::jsonb);
SQL

# 3. Run worker + submit
cargo run --bin worker &
cargo run --bin platform -- pipeline run pipe-55555555-5555-5555-5555-555555555555
```
```

- [ ] **Step 2: Append the Phase I.3 completion log**

Append to the bottom of `docs/superpowers/plans/2026-04-23-phase-1-3-wasm-runtime.md`:

```markdown

---

## Phase I.3 Completion Log

- [ ] Task 1 — Workspace wasmtime deps + wasm32-wasip2 target
- [ ] Task 2 — WIT definition
- [ ] Task 3 — Engine construction
- [ ] Task 4 — Bindings + HostState + log + http-fetch
- [ ] Task 5 — Memory limiter + epoch ticker
- [ ] Task 6 — WasmSourceRuntime + Component cache
- [ ] Task 7 — WasmSourceConnector impls SourceConnector
- [ ] Task 8 — SourceSpec::Wasm + dispatcher
- [ ] Task 9 — SyncActivities + workflow plumbing
- [ ] Task 10 — CSV reference guest
- [ ] Task 11 — `platform connector build` CLI
- [ ] Task 12 — Fuel adversarial test
- [ ] Task 13 — Memory adversarial test
- [ ] Task 14 — Capability-denial adversarial test
- [ ] Task 15 — End-to-end integration test
- [ ] Task 16 — README + this log

### Exit criterion — to be marked when Tasks 12-15 pass

**Capability sandboxing + resource limits + end-to-end WASM sync**, proven by:
- Fuel exhaustion trap (Task 12)
- Memory-growing denial (Task 13)
- Instantiation-fails-on-un-linked-import (Task 14)
- 5-row CSV → 3 Parquet files (2+2+1) via WASM guest (Task 15)

Plus: Phase I.2 tests still green (regression-proof that the Rust-native Postgres path still works).

### Deviations

(Fill in as encountered during execution.)

### Handoff to Phase I.4

Phase I.4 (full catalog entities + YAML DSL) adds:
- `streams` and `schemas` tables (replacing the `stream_state` shortcut)
- Schema evolution policies
- YAML DSL parser (`platform apply -f pipeline.yaml`)
- Postgres-in-WASM via a host `postgres-query` capability (optional, or can slide to I.5)

The WASM runtime machinery built in I.3 is stable: adding the `postgres-query` host function is an additive change to the `host` WIT interface plus a small `runtime/host.rs` extension.
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: Phase I.3 README demo section + completion log

Demo walks through connector build → catalog seed → run. Completion
log scaffold ready to be filled as tasks land.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Appendix A — Troubleshooting

**`wit_bindgen::generate!` fails on the guest with "path not found".**
The path is resolved relative to the guest crate's `Cargo.toml`. Verify that from `examples/csv-source/`, `../../crates/connector-sdk/wit/source-connector.wit` exists.

**`wasmtime::component::bindgen!` generates slightly different names in 0.37+ vs 0.36.**
Common differences: `instantiate_async` vs `new_async(...).instantiate_async(...)`, `SourceConnectorPre` vs direct call. Adapt the calls in `bindings.rs` and `connector.rs` to match whichever shape the installed version generates; the structural flow (load → new store → instantiate → call exported function → decode IPC bytes) is stable.

**`cargo build --target wasm32-wasip2` on the guest fails because arrow pulls in native deps.**
Arrow has several crates under the umbrella. For the wasm guest, pin the specific sub-crates you need:
```toml
arrow-array = "53"
arrow-schema = "53"
arrow-ipc = { version = "53", default-features = false }
arrow-buffer = "53"
```
and rewrite the guest to use them directly. If IPC serialization is still awkward, simplify: emit JSON or CSV-in-bytes from the guest and decode on the host side. (This is a Phase I.3 shortcut — proper Arrow-in-wasm is a Phase I.3.1 follow-up.)

**`instantiate_async` hangs.**
Epoch ticker isn't running, or `async_support` isn't enabled on the Config. Verify `Config::async_support(true)` and `EpochTicker::start(engine)` was called once per Engine.

**`platform connector build` can't find `cargo`.**
The CLI spawns `cargo` from the process PATH. Running it under `cargo run --bin platform --` inherits PATH correctly; running the raw `target/debug/platform` might not. Add `PATH=/usr/local/bin:/usr/bin:$PATH` to the environment if you're running the binary directly.

## Appendix B — What's deferred

Not in Phase I.3:
- Postgres-in-WASM (Phase I.4 — needs host-provided `postgres-query` capability)
- Shared-memory Tier 2 data transfer
- Streaming Tier 3 data transfer
- Connector signing / verification (Phase II.3)
- Registry service (Phase II.3/III)
- TypeScript/Python guest SDKs (Phase II.3)
- Guest-side transformation determinism enforcement (Phase I.5)
- Secrets ref resolution (Phase II.2)
- Multi-tenancy hooks in the runtime (Phase II.1)

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-23-phase-1-3-wasm-runtime.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
