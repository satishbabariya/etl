# Phase II.3.i — CI Harness with E2E Enablement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up GitHub Actions CI from zero. Run lib tests on every push/PR; run a curated 4-test e2e subset on pushes to `main`. Fix the `auth::jwt::rs256_round_trip_via_inline_jwks` flake that breaks parallel `cargo test --workspace`.

**Architecture:** Two workflows (`lib-tests.yml` and `e2e-tests.yml`) under `.github/workflows/`. The e2e workflow uses the existing `docker-compose.yml` for Postgres + Temporal + Vault, plus a new `ci/wait-for-stack.sh` that polls until both services are ready. A new `Makefile` mirrors the CI flow locally. Auth flake fixed via `serial_test` crate marking all `ETL_MASTER_KEY`-touching tests with `#[serial(env_master_key)]`.

**Tech Stack:** GitHub Actions (ubuntu-latest), `Swatinem/rust-cache@v2`, docker-compose v2, `serial_test = "3"`.

---

## File structure

| Path | Action |
|---|---|
| `.github/workflows/lib-tests.yml` | **New** |
| `.github/workflows/e2e-tests.yml` | **New** |
| `ci/wait-for-stack.sh` | **New** |
| `Makefile` | **New** |
| `crates/auth/Cargo.toml` | Modify (add serial_test dev-dep) |
| `crates/auth/src/keystore.rs` | Modify (replace ENV_LOCK with #[serial]) |
| `crates/auth/src/jwt.rs` | Modify (mark master-key-reading tests with #[serial]) |
| `README.md` | Modify (CI badges, Currently: line) |

---

## Task 1: lib-tests CI workflow

**Files:**
- Create: `.github/workflows/lib-tests.yml`

- [ ] **Step 1: Create the workflow file**

`.github/workflows/lib-tests.yml`:

```yaml
name: lib-tests

on:
  push:
    branches: ["**"]
  pull_request:

permissions:
  contents: read

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2

      - uses: Swatinem/rust-cache@v2

      - name: cargo build --workspace
        run: cargo build --workspace --locked

      - name: cargo test --workspace --lib
        run: cargo test --workspace --lib --locked
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/lib-tests.yml && \
git commit -m "phase-2-3i-1: lib-tests GitHub Actions workflow

Runs cargo build --workspace + cargo test --workspace --lib on every
push (any branch) and pull_request. ubuntu-latest, Rust stable,
wasm32-wasip2 target installed for the example connectors. Cargo
cache via Swatinem/rust-cache@v2.

First-run will surface the auth keystore flake — Task 2 fixes that."
```

---

## Task 2: Fix auth keystore parallel-test flake

**Files:**
- Modify: `crates/auth/Cargo.toml` (add serial_test dev-dep)
- Modify: `crates/auth/src/keystore.rs` (drop ENV_LOCK, add #[serial])
- Modify: `crates/auth/src/jwt.rs` (mark master-key-reading tests with #[serial])

- [ ] **Step 1: Add serial_test dev-dependency**

In `crates/auth/Cargo.toml`, find the `[dev-dependencies]` section and add:

```toml
serial_test = "3"
```

If there's no `[dev-dependencies]` section, create one. Run:

```bash
grep -n "dev-dependencies" crates/auth/Cargo.toml
```

If the grep returns no lines, append `[dev-dependencies]\nserial_test = "3"\n` to the file. Otherwise add `serial_test = "3"` under the existing section.

- [ ] **Step 2: Update keystore.rs to use #[serial]**

In `crates/auth/src/keystore.rs`, find the `mod tests` block. Replace the existing test setup:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn run_with_env<F: FnOnce()>(key: &str, val: Option<&str>, f: F) {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var(key).ok();
        match val {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        f();
        match prev {
            Some(p) => std::env::set_var(key, p),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn init_writes_keypair_and_marks_active() {
        let dir = tempfile::tempdir().unwrap();
        run_with_env("ETL_MASTER_KEY", None, || {
            let ks = Keystore::open(dir.path().to_path_buf());
            let kid = ks.init().unwrap();
            assert!(!kid.is_empty());
            assert_eq!(ks.active_kid().unwrap(), kid);
        });
    }
```

with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn with_env<F: FnOnce()>(key: &str, val: Option<&str>, f: F) {
        let prev = std::env::var(key).ok();
        match val {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        f();
        match prev {
            Some(p) => std::env::set_var(key, p),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    #[serial(env_master_key)]
    fn init_writes_keypair_and_marks_active() {
        let dir = tempfile::tempdir().unwrap();
        with_env("ETL_MASTER_KEY", None, || {
            let ks = Keystore::open(dir.path().to_path_buf());
            let kid = ks.init().unwrap();
            assert!(!kid.is_empty());
            assert_eq!(ks.active_kid().unwrap(), kid);
        });
    }
```

Then update `init_writes_enc_when_master_key_set` and `seal_in_place_upgrades_pem_to_enc` similarly: add `#[serial(env_master_key)]` and replace `run_with_env(...)` with `with_env(...)`.

- [ ] **Step 3: Mark jwt.rs master-key-reading tests with #[serial]**

In `crates/auth/src/jwt.rs`, find `mod tests`. Add at the top:

```rust
    use serial_test::serial;
```

Then mark each test that calls `Keystore::open(...).init()` with `#[serial(env_master_key)]`. Per the existing test list, that's at minimum:

- `rs256_round_trip_via_inline_jwks` (the known-flaky one)
- `unknown_kid_rejects` (calls `Keystore::open`)
- `wrong_audience_rejects` (calls `Keystore::open`)

Look for any other test in `jwt.rs` that opens a Keystore — mark all of them.

- [ ] **Step 4: Run cargo test for auth crate to verify**

```bash
cargo test -p auth --lib
```

Expected: all auth tests pass.

- [ ] **Step 5: Run cargo test --workspace --lib 5 times to confirm no flake**

```bash
for i in 1 2 3 4 5; do
    cargo test --workspace --lib 2>&1 | tail -3 | grep -E "test result|^error" || true
    echo "---"
done
```

Expected: every iteration shows `test result: ok` for every crate, no errors.

- [ ] **Step 6: Commit**

```bash
git add crates/auth/Cargo.toml crates/auth/src/keystore.rs crates/auth/src/jwt.rs && \
git commit -m "phase-2-3i-2: fix auth keystore env-var test flake via serial_test

Root cause: keystore tests use ETL_MASTER_KEY env-var manipulation
under a private ENV_LOCK Mutex, but jwt tests that call Keystore::init()
don't acquire that lock — under cargo's parallel test runner the jwt
test could read a master key set by a sibling test, then fail to
decrypt later when that key got reset.

Fix: drop the bespoke ENV_LOCK in favor of serial_test crate. Mark
every test that touches ETL_MASTER_KEY (3 in keystore.rs, 3+ in
jwt.rs) with #[serial(env_master_key)] so they run one at a time
across the whole crate's test binary.

cargo test --workspace --lib passes 5/5 consecutive runs."
```

---

## Task 3: wait-for-stack.sh + Makefile

**Files:**
- Create: `ci/wait-for-stack.sh`
- Create: `Makefile`

- [ ] **Step 1: Create wait-for-stack.sh**

`ci/wait-for-stack.sh`:

```bash
#!/usr/bin/env bash
# Polls the docker-compose stack until Postgres + Temporal are ready.
# Exits 0 when both are responsive; exits 1 on timeout.

set -e

echo "waiting for postgres on 127.0.0.1:5432..."
for i in $(seq 1 60); do
    if pg_isready -h 127.0.0.1 -p 5432 -U etl 2>/dev/null; then
        echo "postgres ready ($i s)"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "postgres failed to come up in 60s"
        docker compose ps
        exit 1
    fi
    sleep 1
done

echo "waiting for temporal on 127.0.0.1:7233..."
for i in $(seq 1 120); do
    # gRPC port open is necessary but not sufficient — namespace registration
    # takes another ~10s after the port opens. Probe via tctl namespace list.
    if nc -z 127.0.0.1 7233 2>/dev/null; then
        if docker exec etl-temporal tctl --address temporal:7233 namespace list \
            > /dev/null 2>&1; then
            echo "temporal ready ($i s)"
            exit 0
        fi
    fi
    if [ "$i" -eq 120 ]; then
        echo "temporal failed to come up in 120s"
        docker compose ps
        docker compose logs temporal | tail -50
        exit 1
    fi
    sleep 1
done

exit 1
```

Make it executable:

```bash
chmod +x ci/wait-for-stack.sh
```

- [ ] **Step 2: Create Makefile**

`Makefile`:

```makefile
.PHONY: stack stack-down stack-logs lib-tests build e2e

stack:
	docker compose up -d postgres temporal-postgres temporal vault
	./ci/wait-for-stack.sh

stack-down:
	docker compose down

stack-logs:
	docker compose logs -f --tail=100

lib-tests:
	cargo test --workspace --lib

build:
	cargo build --workspace

e2e: stack build
	cargo test -p integration-tests --test mysql_cdc_wasm_e2e -- --ignored --nocapture
	cargo test -p integration-tests --test postgres_cdc_wasm_e2e -- --ignored --nocapture
	cargo test -p integration-tests --test wasm_connector -- --ignored --nocapture
	cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture
```

- [ ] **Step 3: Smoke-test the Makefile locally**

Skip if Docker isn't running. If Docker is running:

```bash
make stack
make lib-tests
make stack-down
```

Expected: stack comes up, lib-tests pass, stack-down cleans up. If your local machine doesn't have Docker, skip this step and rely on CI to validate.

- [ ] **Step 4: Commit**

```bash
git add ci/wait-for-stack.sh Makefile && \
git commit -m "phase-2-3i-3: ci/wait-for-stack.sh + Makefile

ci/wait-for-stack.sh polls Postgres (60s budget) then Temporal
(120s budget — auto-setup namespace registration takes ~30-60s
after the gRPC port opens). Probes Temporal readiness via tctl
namespace list rather than just port-open, since Temporal accepts
connections before the namespace DB is provisioned.

Makefile: stack / stack-down / stack-logs / lib-tests / build /
e2e targets so contributors can mirror CI locally. e2e depends
on stack + build, runs the curated 4-test set."
```

---

## Task 4: e2e-tests CI workflow

**Files:**
- Create: `.github/workflows/e2e-tests.yml`

- [ ] **Step 1: Create the workflow**

`.github/workflows/e2e-tests.yml`:

```yaml
name: e2e-tests

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2

      - uses: Swatinem/rust-cache@v2

      - name: Install Postgres client (pg_isready)
        run: sudo apt-get update && sudo apt-get install -y postgresql-client netcat-openbsd

      - name: Bring up docker-compose stack
        run: docker compose up -d postgres temporal-postgres temporal vault

      - name: Wait for stack readiness
        run: ./ci/wait-for-stack.sh

      - name: Build workspace
        run: cargo build --workspace --locked

      - name: e2e — mysql_cdc_wasm
        run: cargo test -p integration-tests --test mysql_cdc_wasm_e2e -- --ignored --nocapture

      - name: e2e — postgres_cdc_wasm
        run: cargo test -p integration-tests --test postgres_cdc_wasm_e2e -- --ignored --nocapture

      - name: e2e — wasm_connector
        run: cargo test -p integration-tests --test wasm_connector -- --ignored --nocapture

      - name: e2e — mysql_cdc (native streaming-only)
        run: cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture

      - name: docker compose logs (on failure)
        if: failure()
        run: |
          echo "=== docker compose ps ==="
          docker compose ps
          echo "=== temporal logs ==="
          docker compose logs temporal | tail -200
          echo "=== postgres logs ==="
          docker compose logs postgres | tail -100

      - name: Tear down
        if: always()
        run: docker compose down -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/e2e-tests.yml && \
git commit -m "phase-2-3i-4: e2e-tests GitHub Actions workflow

Triggers on push to main + manual workflow_dispatch. Brings up the
docker-compose stack (postgres, temporal-postgres, temporal, vault),
waits via ci/wait-for-stack.sh, builds the workspace, then runs the
curated 4-test e2e subset:

- mysql_cdc_wasm_e2e (II.3.e/h validation)
- postgres_cdc_wasm_e2e (II.3.f/h validation)
- wasm_connector (Phase I.3 anchor)
- mysql_cdc_e2e (II.3.d native CDC)

Captures docker compose logs on failure and tears down with -v so
volumes don't leak between runs. timeout-minutes=45 so a hung test
doesn't sit forever."
```

---

## Task 5: First green CI run

**Files:** none — this task verifies the workflows land green.

- [ ] **Step 1: Push the branch**

```bash
git push -u origin phase-2-3i-ci-harness
```

The `lib-tests` workflow fires on push (every branch). Wait for it.

- [ ] **Step 2: Monitor lib-tests**

```bash
gh run list --workflow=lib-tests.yml --limit 3
```

Expected: a run associated with the latest commit on `phase-2-3i-ci-harness`. Wait for it to finish:

```bash
gh run watch --exit-status
```

If it fails:
- Check `cargo test --workspace --locked` reproduces the failure locally with `--locked`.
- Check `cargo build --workspace --locked` for missing target features (e.g. wasm32-wasip2 needs to be installed in the action).
- Iterate fixes; commit and push; re-watch.

If it passes: done with the lib-tests verification.

- [ ] **Step 3: Verify e2e-tests does NOT trigger on this branch**

```bash
gh run list --workflow=e2e-tests.yml --limit 5
```

Expected: no run for the current commit. e2e only triggers on push to main. We won't validate e2e until the PR merges.

- [ ] **Step 4: No commit (this task is verification only)**

The workflow files committed in Tasks 1 and 4 are sufficient. This task ends without a commit — it's a checkpoint.

---

## Task 6: README + final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add CI badges + update Currently: line**

Open `README.md` and add immediately after the project title (top of file), insert:

```markdown
[![lib-tests](https://github.com/satishbabariya/etl/actions/workflows/lib-tests.yml/badge.svg?branch=main)](https://github.com/satishbabariya/etl/actions/workflows/lib-tests.yml)
[![e2e-tests](https://github.com/satishbabariya/etl/actions/workflows/e2e-tests.yml/badge.svg?branch=main)](https://github.com/satishbabariya/etl/actions/workflows/e2e-tests.yml)
```

Then find the "Currently:" line and replace it with:

```markdown
Currently: **Phase II.3.i — CI harness with e2e enablement (complete)** on top of II.3.h. GitHub Actions runs `cargo test --workspace --lib` on every push and PR via `lib-tests.yml`. On every push to `main`, `e2e-tests.yml` brings up the docker-compose stack (Postgres + Temporal + Vault) and runs a curated 4-test e2e subset: `mysql_cdc_wasm_e2e`, `postgres_cdc_wasm_e2e`, `wasm_connector`, `mysql_cdc_e2e`. The auth keystore parallel-test flake (`ETL_MASTER_KEY` env-var contention between keystore.rs and jwt.rs tests) is fixed via `serial_test`. `make stack` / `make lib-tests` / `make e2e` mirror the CI flow locally. Multi-table CDC (II.3.g) is the next big phase — wants its own brainstorm decomposition. Then real **Phase II.4** (Helm + Terraform + `platform install`) and **II.5** (customer dashboards + lineage + read-only UI).
```

- [ ] **Step 2: Final lib-tests sweep locally**

```bash
cargo test --workspace --lib 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 3: Commit + push**

```bash
git add README.md && \
git commit -m "phase-2-3i-5: README — Phase II.3.i CI harness complete

CI badges + Currently: line bump. Workflows merged in Tasks 1 + 4.
Auth keystore env-var flake fix (Task 2) restored cargo test
--workspace --lib green. Local make targets (Task 3) mirror the
CI flow.

Untested before merge: e2e-tests.yml only triggers on push to main,
so the curated 4-test e2e subset proves out only after this PR
merges. Manual workflow_dispatch on the e2e workflow can be invoked
to validate from the branch."
```

---

## Self-review

### Spec coverage

| Spec section | Plan task |
|---|---|
| lib-tests workflow | Task 1 |
| Auth keystore flake fix | Task 2 |
| wait-for-stack.sh + Makefile | Task 3 |
| e2e-tests workflow | Task 4 |
| First green CI verification | Task 5 |
| README + final verification | Task 6 |
| Curated 4-test e2e list | Task 3 (Makefile) + Task 4 (workflow) |
| Acceptance: 5/5 lib-tests passes | Task 2 step 5 |
| CI badge | Task 6 step 1 |

All spec sections covered.

### Placeholder scan

The "If your local machine doesn't have Docker, skip this step" hedge in Task 3 is a real conditional (Docker presence is an environmental fact, not a placeholder for unwritten work). Acceptable.

The Task 5 "iterate fixes" guidance is intentional — we cannot predict which CI environment differences will surface on first run. Acceptable for a CI-validation task.

No "TBD"/"TODO"/"implement later".

### Type consistency

- Workflow names (`lib-tests`, `e2e-tests`) match between Makefile, workflow files, README badges.
- Service names (`postgres`, `temporal-postgres`, `temporal`, `vault`) match docker-compose.yml entries (verified during spec).
- `ETL_MASTER_KEY` env var name consistent across keystore.rs / jwt.rs / serial_test marker.
- `serial_test = "3"` version consistent.
- The four curated e2e test paths are spelled the same in Makefile and e2e-tests.yml.

All checked.
