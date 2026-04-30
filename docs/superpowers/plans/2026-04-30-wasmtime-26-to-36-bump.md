# wasmtime 26 → 36.0.7 Bump Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock-step bump `wasmtime`, `wasmtime-wasi`, and `wasmtime-wasi-http` from 26.0.1 to 36.0.7 (matching dependabot PR #4), adapt the host runtime to API breakage in the 27.x–36.x range, and verify both lib tests and the Stripe TS connector e2e still execute end-to-end.

**Architecture:** Pure dependency upgrade. The wasmtime 27.x release introduced the `IoView` trait split (table accessor moved out of `WasiView`/`WasiHttpView`), and 28+ refined linker registration signatures. Our touch points are `crates/worker/src/wasm_runtime/{host.rs,runtime.rs,bindings.rs,scalar_runtime.rs,scalar_bindings.rs}` plus the `wasmtime-wasi-http` crate dep in `crates/worker/Cargo.toml`. Plan is discovery-shaped: bump first, observe `cargo build` failures, fix per known patterns.

**Tech Stack:** wasmtime 36.0.7 + wasmtime-wasi 36 + wasmtime-wasi-http 36; existing wit-bindgen 0.37 (guest-side only, independent of host wasmtime version).

**Predecessor PR:** #4 (dependabot, currently open). Spec lives in this plan; release-note context is in the dependabot PR body.

---

## Task 1: Workspace dep bumps + first build observation

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/worker/Cargo.toml` (literal `"26"` for `wasmtime-wasi-http`)

- [ ] **Step 1: Bump the four wasmtime crates in workspace deps**

In `Cargo.toml`, replace the existing block:

```toml
# WASM
wasmtime = { version = "26", features = ["component-model", "async", "parallel-compilation"] }
wasmtime-wasi = "26"
wit-bindgen = "0.37"
```

with:

```toml
# WASM
wasmtime = { version = "36", features = ["component-model", "async", "parallel-compilation"] }
wasmtime-wasi = "36"
wasmtime-wasi-http = "36"
wit-bindgen = "0.37"
```

(`wit-bindgen` 0.37 is for *guest-side* binding generation in the example connectors and is independent of the host wasmtime version. It stays put.)

- [ ] **Step 2: Promote `wasmtime-wasi-http` to a workspace dep in worker/Cargo.toml**

In `crates/worker/Cargo.toml` replace the literal `"26"`:

```toml
wasmtime-wasi-http = "26"
```

with the workspace reference:

```toml
wasmtime-wasi-http = { workspace = true }
```

- [ ] **Step 3: First build — observe categories of breakage**

Run: `cargo build -p worker 2>&1 | tee /tmp/wasmtime-bump-errors.txt | tail -80`
Expected: errors. Capture the full stderr to `/tmp/wasmtime-bump-errors.txt` for triage.

The likely categories (from upstream changelogs in the 27.x–36.x window):
- `WasiView` / `WasiHttpView` trait split — `table()` moved to a new `IoView` trait.
- `add_to_linker_async` may have a different generic signature or import path.
- `wasmtime::component::bindgen!` may emit different code (e.g. `with` option semantics changed).
- `wasmtime_wasi::WasiCtxBuilder` chained methods may have moved.

If errors appear in unrelated crates first, run `cargo build --workspace 2>&1 | tee /tmp/wasmtime-bump-errors.txt`. Use the categorization to drive Tasks 2-5.

- [ ] **Step 4: No commit yet**

The build is broken — we'll commit at the end of each fix task once that category compiles. Do not commit Task 1 alone (Cargo.lock will be inconsistent until builds succeed).

---

## Task 2: Adapt WasiView / WasiHttpView impls to the IoView split

**Files:**
- Modify: `crates/worker/src/wasm_runtime/host.rs`

Wasmtime 27 split the `table()` accessor out of `WasiView`/`WasiHttpView` into a new `IoView` trait. After 27, `WasiView: IoView` and `WasiHttpView: IoView` — but the impls have to be split correspondingly.

- [ ] **Step 1: Add `IoView` impl, slim `WasiView` impl**

Replace the block at `crates/worker/src/wasm_runtime/host.rs:1-54`:

```rust
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};
```

with the imports including `IoView`:

```rust
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{IoView, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};
```

Then replace the two trait impls:

```rust
impl WasiView for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}
```

with:

```rust
impl IoView for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http
    }
}
```

(Note: if `IoView` lives at a different path in 36 — e.g. `wasmtime_wasi::p2::IoView` or in a re-export module — the `cargo build` error will name the correct path; adjust the `use` accordingly. The `table` method body is unchanged.)

- [ ] **Step 2: Build host.rs surface**

Run: `cargo build -p worker 2>&1 | grep -E "host.rs|IoView|WasiView|WasiHttpView" | head -20`
Expected: no errors mentioning `IoView`, `WasiView`, `WasiHttpView`. If new errors appear elsewhere, they're for Tasks 3-5.

- [ ] **Step 3: Hold commit until full build succeeds**

Don't commit yet — Cargo.lock has the new wasmtime version and we want one atomic "wasmtime 36 bump" commit per task category. Commit comes after each task category compiles cleanly.

---

## Task 3: Adapt linker registration calls

**Files:**
- Modify: `crates/worker/src/wasm_runtime/runtime.rs`
- Modify: `crates/worker/src/wasm_runtime/scalar_runtime.rs`

The `wasmtime_wasi::add_to_linker_async` and `wasmtime_wasi_http::add_only_http_to_linker_async` signatures may have changed in the 27.x–36.x range. Common changes: explicit type bound on the `T` of `Linker<T>`, a new `LinkOptions` parameter, or a renamed function (e.g. `wasmtime_wasi::p2::add_to_linker_async`).

- [ ] **Step 1: Identify the actual API shape in 36**

Run: `cargo build -p worker 2>&1 | grep -A 4 "add_to_linker_async\|add_only_http_to_linker_async" | head -40`
Expected: errors will name the new signature or path.

- [ ] **Step 2: Apply the most likely fix pattern**

The most common 36.x shape: `wasmtime_wasi::p2::add_to_linker_async` (the `p2` namespace was introduced to disambiguate from p1/preview1). If `cargo build` confirms this:

In `crates/worker/src/wasm_runtime/runtime.rs:29-32`, replace:

```rust
        wasmtime_wasi::add_to_linker_async(&mut linker)
            .context("adding WASI 0.2 imports to linker")?;
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
            .context("adding WASI 0.2 http imports to linker")?;
```

with:

```rust
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .context("adding WASI 0.2 imports to linker")?;
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
            .context("adding WASI 0.2 http imports to linker")?;
```

If `wasmtime-wasi-http` also moved to `p2`, apply the same `::p2::` pattern there. If the error message mentions a new generic parameter (e.g. `add_to_linker_async::<T, _>`), match the form printed by the compiler.

- [ ] **Step 3: Apply parallel fix in scalar_runtime.rs if needed**

`crates/worker/src/wasm_runtime/scalar_runtime.rs` doesn't call `wasmtime_wasi::add_to_linker_async` (scalar UDFs don't need WASI), so it likely only needs path adjustments if `Linker::new` or component bindings changed. Run:

`cargo build -p worker 2>&1 | grep -A 3 "scalar_runtime\|scalar_bindings" | head -20`

If errors exist, apply the analogous fix.

- [ ] **Step 4: Hold commit**

Same as Task 2 — wait for full clean build before committing.

---

## Task 4: Adapt `wasmtime::component::bindgen!` macro options

**Files:**
- Modify: `crates/worker/src/wasm_runtime/bindings.rs`
- Modify: `crates/worker/src/wasm_runtime/scalar_bindings.rs`

The `bindgen!` macro options surface (`with`, `trappable_imports`, `path`, etc.) sometimes changes between major versions. Most likely the existing call still works; if not, the cargo error will name the new option name.

- [ ] **Step 1: Identify any bindgen errors**

Run: `cargo build -p worker 2>&1 | grep -A 4 "bindgen!\|bindings.rs:\|scalar_bindings.rs:" | head -30`

If output is empty: skip to Task 5; bindgen is already compatible.
If output shows errors: continue.

- [ ] **Step 2: Common pattern — `trappable_imports: true` syntax change**

In wasmtime 36, the macro often expects the literal struct form. If the current `bindings.rs` uses:

```rust
wasmtime::component::bindgen!({
    path: "wit/source-connector.wit",
    world: "source-connector",
    async: true,
});
```

…and the error mentions an unknown option, check the wasmtime 36 docs for the canonical form. The `path`, `world`, and `async` options are stable; if errors are about something else, list it here and apply the fix.

- [ ] **Step 3: Hold commit**

---

## Task 5: Full workspace build green

**Files:** any remaining sites the previous tasks didn't catch.

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace 2>&1 | grep -E "^error" | head -20`
Expected: empty (no errors).

If errors remain, fix per the same diagnostic pattern: read the error, identify the API change, apply the local fix. Common stragglers:
- `wasmtime::Engine::new` may take a `Config` differently — unlikely but possible.
- `Component::deserialize_file` is `unsafe` and stable.
- `Store::set_fuel` is stable.

Loop until `cargo build --workspace` is clean.

- [ ] **Step 2: First commit — workspace deps + host adaptations**

```bash
git add Cargo.toml Cargo.lock crates/worker/Cargo.toml crates/worker/src/wasm_runtime
git commit -m "$(cat <<'EOF'
wasmtime 26 → 36.0.7: workspace deps + host runtime adaptation

- Bump wasmtime/wasmtime-wasi/wasmtime-wasi-http 26 → 36 in workspace
  deps; promote wasmtime-wasi-http to a workspace dep in worker.
- Split WasiView/WasiHttpView impls per the IoView refactor
  introduced in wasmtime-wasi 27.x.
- Update linker registration call sites to the new API path
  (likely wasmtime_wasi::p2::add_to_linker_async).

Addresses dependabot PR #4 and the security advisories in the 27.x–36.x
range.
EOF
)"
```

---

## Task 6: Library tests still pass

**Files:** none modified — verification only.

- [ ] **Step 1: Run the lib test suite**

Run: `cargo test --workspace --lib 2>&1 | tail -10`
Expected: `test result: ok.` for each crate, with the same ~147 passing tests as before the bump.

If new test failures appear, they likely stem from:
- A behavior change in `wasmtime_wasi::WasiCtxBuilder` defaults (e.g. inheriting more or fewer host capabilities).
- Different Component instantiation error messages (test asserts on substrings).

Read each failure and apply the minimal local fix. Don't change test intent.

- [ ] **Step 2: No commit unless tests changed**

If any test code changed, commit:

```bash
git add crates/worker/src/wasm_runtime/tests.rs <other-test-files>
git commit -m "wasmtime 36: adapt test assertions to new error messages"
```

---

## Task 7: Stripe TS connector e2e — verify guest WASM still executes

**Files:** none modified — runtime verification.

This is the load-bearing check: the wasmtime 36 runtime must still load and execute our existing `.cwasm` artifacts, including the precompiled Stripe TS connector that Phase II.3.b.1 shipped. **`.cwasm` files precompiled by wasmtime 26 are NOT compatible with wasmtime 36** — they need to be re-precompiled. The integration test does this via `platform connector publish`.

- [ ] **Step 1: Stack up**

Run:
```bash
docker compose up -d postgres temporal-postgres temporal
until nc -z 127.0.0.1 5432 && nc -z 127.0.0.1 7233; do sleep 2; done
```
Expected: both ports accepting connections.

- [ ] **Step 2: Run the Stripe TS e2e**

Run:
```bash
DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock \
  cargo test -p integration-tests --test stripe_ts_e2e -- --ignored --nocapture 2>&1 | tail -40
```
Expected: PASS. The test publishes the TS connector (re-precompiles the `.cwasm` under wasmtime 36), spawns the worker, and verifies 2 Stripe customer rows land in Parquet.

If the test fails with an instantiation error mentioning a WIT type or import that didn't change, the bindgen/linker work in Tasks 3-4 likely missed something. Re-read the error and fix.

- [ ] **Step 3: Run the MySQL CDC e2e for completeness**

Run:
```bash
DOCKER_HOST=unix:///Users/$USER/.docker/run/docker.sock \
  cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture 2>&1 | tail -30
```
Expected: PASS. (This connector is native, not WASM, so it's unaffected by the wasmtime bump — but running it is cheap and confirms nothing else broke from the dep churn.)

- [ ] **Step 4: No commit — verification only**

---

## Task 8: README note + close dependabot PR

**Files:**
- Modify: `README.md` (one-line currency note)

- [ ] **Step 1: Update README's "Currently:" line**

In `README.md`, find the line that lists current platform state (set during Phase II.3.d ship) and append a reference to the wasmtime version. The change should be a single line, not a section. E.g. if the current line ends with "MySQL CDC streaming-only.", append " On wasmtime 36."

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README note for wasmtime 36 bump"
```

- [ ] **Step 3: Close dependabot PR #4 in favor of this branch**

After this branch is merged, dependabot PR #4 will rebase to a no-op or auto-close. If it doesn't, comment on it: "Superseded by [this PR's URL]." Don't close it manually before merge — dependabot will react cleanly to a merged equivalent.
