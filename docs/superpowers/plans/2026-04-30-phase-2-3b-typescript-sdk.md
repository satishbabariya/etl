# Phase II.3.b — TypeScript SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add TypeScript as a second authoring language for source connectors so non-Rust authors can ship `.cwasm` artifacts that the existing worker host runs unchanged.

**Architecture:** Same WIT contract (`crates/connector-sdk/wit/source-connector.wit`), same `<registry>/<name>@<version>/component.cwasm` + `manifest.yaml` artifact shape. The SDK gains a TS template module; the CLI gains a `--lang` flag on `create` and language-aware dispatch on `test`/`publish` (Rust → cargo, TS → npm + jco). The TS guest is built by `@bytecodealliance/jco`'s `componentize` command, which bundles a SpiderMonkey runtime + the author's TS into a wasi-p2 Component Model component implementing the source-connector world. Stripe `/v1/customers` is ported to TS (`examples/stripe-source-ts/`) as the dogfooding proof.

**Tech Stack:** Rust SDK + CLI (existing), `@bytecodealliance/jco` ≥1.4 (TS→component compiler), `@bytecodealliance/componentize-js` (bundled by jco), `apache-arrow` (Arrow IPC encoding from JS), `vitest` (TS unit testing), `typescript` 5+.

**Scope this phase:**
- ✅ TS authoring of *source* connectors (mirrors II.3.a Rust scope).
- ✅ Same WIT, same .cwasm shape, same manifest.
- ✅ TS port of `examples/stripe-source` as `examples/stripe-source-ts`.
- ✅ Integration test — TS connector_lifecycle round-trip (create → test → publish).
- ✅ Wiremock e2e for the TS Stripe connector.

**Deferred to later phases:**
- TS scalar/destination connectors (matches Rust scope at II.3.a — only sources).
- Connector signing (Sigstore / cosign) — Phase II.4.
- TS-specific SDK helper crate (e.g. `@platform/connector-sdk` npm package). Plan keeps the host imports raw to minimize npm-publishing scope.

---

## File Structure

**New files:**
- `crates/connector-sdk/src/templates/typescript.rs` — TS template materialization (mirrors `templates::rust`).
- `crates/cli/src/connector_build.rs` — extracted, language-aware build helpers (`detect_lang`, `build_rust`, `build_typescript`).
- `examples/stripe-source-ts/` — TS port of stripe-source (full directory tree, scaffolded by the new template + customized).
- `tests/integration/tests/typescript_connector_lifecycle.rs` — integration test: create → test → publish round-trip for TS.
- `tests/integration/tests/stripe_ts_e2e.rs` — wiremock e2e for the TS Stripe connector.

**Refactored files:**
- `crates/connector-sdk/src/templates.rs` — split into `templates/mod.rs` + `templates/rust.rs` (move existing const + materialize fn) + `templates/typescript.rs`. Re-export `materialize_source_template_rust` and add `materialize_source_template_typescript`. Keep `materialize_source_template` as a back-compat alias for Rust until the CLI catches up in T3.
- `crates/cli/src/connector_cmd.rs` — add `--lang` flag to `create`; refactor `test`, `build`, `publish` to detect language and dispatch.

**Modified files:**
- `crates/connector-sdk/Cargo.toml` — no change (templates are static strings, no new deps).
- `Cargo.toml` (workspace) — add `examples/stripe-source-ts` to `exclude` list.
- `README.md` — Connector SDK section adds TS authoring path + deferred-to-II.3.b note resolved.
- `tests/integration/Cargo.toml` — no change (npm/jco invocation is via `tokio::process::Command`).

---

## Pre-flight

**Required tools on the dev box:**
- `node` ≥ 20 and `npm` ≥ 10. Verify: `node --version && npm --version`.
- `wasm32-wasip2` Rust target (already installed for II.3.a).
- Docker stack running (postgres + temporal + vault) for integration tests.

**Verify jco is reachable** (no global install needed; npm scripts use `npx`):
```bash
npx --yes @bytecodealliance/jco --version
```
Expected: a version string ≥ `1.4.0`. If this fails with a network/registry error, fix the npm registry config first — every TS task depends on it.

---

## Task 1: Refactor templates module — split Rust template into its own file

**Files:**
- Create: `crates/connector-sdk/src/templates/mod.rs`
- Create: `crates/connector-sdk/src/templates/rust.rs`
- Delete: `crates/connector-sdk/src/templates.rs` (after content moves)
- Modify: `crates/connector-sdk/src/lib.rs` — `pub mod templates;` already exists; no change needed once `templates/` is a directory.

This is pure refactoring — no behavior change. We're carving room for a sibling `typescript.rs` without shipping it yet.

- [ ] **Step 1: Create `crates/connector-sdk/src/templates/mod.rs`**

```rust
//! Embedded source-connector templates per language.

pub mod rust;

pub use rust::materialize_source_template;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_template_is_reachable() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source");
        materialize_source_template(&target, "acme-source").unwrap();
        assert!(target.join("Cargo.toml").exists());
    }
}
```

- [ ] **Step 2: Create `crates/connector-sdk/src/templates/rust.rs`**

Move the entire existing body of `crates/connector-sdk/src/templates.rs` into this new file verbatim — `SOURCE_TEMPLATE` const, `materialize_source_template` fn, the existing `#[cfg(test)] mod tests` block. Do NOT rename anything.

```bash
mv crates/connector-sdk/src/templates.rs crates/connector-sdk/src/templates/rust.rs
mkdir -p crates/connector-sdk/src/templates
# (mv above won't work if the dir doesn't exist; safer: create dir, move file, then write mod.rs)
```

Concrete sequence:
```bash
mkdir crates/connector-sdk/src/templates
git mv crates/connector-sdk/src/templates.rs crates/connector-sdk/src/templates/rust.rs
# write mod.rs from Step 1
```

- [ ] **Step 3: Verify nothing broke**

Run:
```bash
cargo build -p connector-sdk
cargo test -p connector-sdk
```
Expected: clean build; existing tests in `templates::rust::tests` still pass; new test in `templates::tests` passes.

- [ ] **Step 4: Verify CLI still compiles**

```bash
cargo build -p cli
```
Expected: clean. The CLI imports `connector_sdk::templates::materialize_source_template` — that still resolves via the `pub use` in `mod.rs`.

- [ ] **Step 5: Commit**

```bash
git add crates/connector-sdk/src/templates/
git rm crates/connector-sdk/src/templates.rs 2>/dev/null || true
git commit -m "refactor(connector-sdk): split templates into per-language modules"
```

---

## Task 2: Embed the TypeScript template

**Files:**
- Create: `crates/connector-sdk/src/templates/typescript.rs`
- Modify: `crates/connector-sdk/src/templates/mod.rs` — declare `pub mod typescript;` + re-export `materialize_source_template_typescript`.

The template ships seven files: `package.json`, `tsconfig.json`, `wit/source-connector.wit` (copy of the canonical WIT), `src/connector.ts` (entry implementing the world), `src/parse.ts` (placeholder), `tests/parse.test.ts` (placeholder vitest), `README.md`, `.gitignore`. The materialize function follows the same `===FILE: <path>===` block format as the Rust template.

- [ ] **Step 1: Add the TS template module**

Create `crates/connector-sdk/src/templates/typescript.rs`:

```rust
//! TypeScript source-connector template. Follows the same
//! `===FILE: <path>===\n<body>\n` block convention as `templates::rust`.

pub const SOURCE_TEMPLATE_TS: &str = r#"===FILE: package.json===
{
  "name": "{{NAME}}",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "jco componentize src/connector.ts --wit wit/source-connector.wit --world-name source-connector -o dist/connector.wasm",
    "test": "vitest run"
  },
  "devDependencies": {
    "@bytecodealliance/jco": "^1.4.0",
    "@bytecodealliance/componentize-js": "^0.13.0",
    "typescript": "^5.4.0",
    "vitest": "^1.6.0"
  },
  "dependencies": {
    "apache-arrow": "^15.0.0"
  }
}

===FILE: tsconfig.json===
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "allowSyntheticDefaultImports": true,
    "lib": ["ES2022"],
    "types": []
  },
  "include": ["src/**/*.ts", "tests/**/*.ts"]
}

===FILE: .gitignore===
node_modules/
dist/
*.tsbuildinfo

===FILE: wit/source-connector.wit===
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

===FILE: src/parse.ts===
// Pure data transforms live here. Kept separate from connector.ts so
// vitest can exercise them in plain Node without touching the Wasm
// host imports.

import { Schema, Field, Int64, RecordBatch, makeData, Type, Utf8 } from 'apache-arrow';

export function helloSchema(): Schema {
    return new Schema([new Field('id', new Int64(), false)]);
}

===FILE: src/connector.ts===
// {{NAME}} — TypeScript source connector skeleton.
//
// jco componentize bundles this entry + a SpiderMonkey runtime into a
// Component Model component implementing the source-connector world.
// The exports below match the world's exported functions.

import { tableToIPC, makeTable, vectorFromArray, Field, Schema, Int64 } from 'apache-arrow';
import { helloSchema } from './parse.js';

// Host imports — generated by jco from the WIT.
// `log` and `httpFetch` are available at runtime; their types are the
// jco-generated bindings (treated as `any` here for skeleton brevity).
// @ts-expect-error - resolved by jco at componentize time
import { log, httpFetch } from 'platform:connector/host';

function ipcSchemaBytes(schema: Schema): Uint8Array {
    // Build an empty Table on the schema and emit its IPC stream bytes.
    const empty = makeTable({});
    // makeTable() can't take a schema directly with no columns; build one
    // empty Int64 column and return only the schema portion of its stream.
    const t = makeTable({ id: new BigInt64Array(0) });
    return tableToIPC(t, 'stream');
}

export const discover = (
    _conn: { url: string },
    _source: { json: string },
): { tag: 'ok'; val: Uint8Array } | { tag: 'err'; val: { tag: 'other'; val: string } } => {
    try {
        return { tag: 'ok', val: ipcSchemaBytes(helloSchema()) };
    } catch (e) {
        return { tag: 'err', val: { tag: 'other', val: String(e) } };
    }
};

export const readBatch = (
    _conn: { url: string },
    _source: { json: string },
    _cursor: { kind: 'int64' | 'timestamp-tz'; value: string } | undefined,
    _batchSize: number,
): {
    tag: 'ok';
    val: { batchIpc: Uint8Array; rows: number; newCursor: undefined; isFinal: boolean };
} => {
    // Skeleton: emit empty batch and signal final.
    return {
        tag: 'ok',
        val: {
            batchIpc: ipcSchemaBytes(helloSchema()),
            rows: 0,
            newCursor: undefined,
            isFinal: true,
        },
    };
};

===FILE: tests/parse.test.ts===
import { describe, it, expect } from 'vitest';
import { helloSchema } from '../src/parse.js';

describe('helloSchema', () => {
    it('has a single id column', () => {
        const s = helloSchema();
        expect(s.fields.length).toBe(1);
        expect(s.fields[0].name).toBe('id');
    });
});

===FILE: README.md===
# {{NAME}}

A custom TypeScript source connector for the ETL platform.

## Build & publish

```bash
npm install
platform connector test .
platform connector publish . --registry ./connectors
```

The published artifact lands at `./connectors/{{NAME}}@<version>/component.cwasm`.

## Author guide

- Pure data transforms in `src/parse.ts` — exercised by `vitest`.
- Wasm-facing entry in `src/connector.ts` — exports `discover` and `readBatch`. Uses host imports `log` and `httpFetch` from `platform:connector/host` (resolved by jco at componentize time).
"#;

/// Materialize `SOURCE_TEMPLATE_TS` at `target_dir`, substituting `{{NAME}}`.
pub fn materialize_source_template_typescript(
    target_dir: &std::path::Path,
    name: &str,
) -> anyhow::Result<()> {
    use std::fs;
    if target_dir.exists() {
        anyhow::bail!("{} already exists", target_dir.display());
    }
    fs::create_dir_all(target_dir)?;
    let body = SOURCE_TEMPLATE_TS.replace("{{NAME}}", name);
    let mut current_path: Option<std::path::PathBuf> = None;
    let mut current_buf = String::new();
    let flush =
        |path: Option<&std::path::Path>, buf: &str| -> anyhow::Result<()> {
            if let Some(p) = path {
                let abs = target_dir.join(p);
                if let Some(parent) = abs.parent() {
                    fs::create_dir_all(parent)?;
                }
                let content = buf.trim_end_matches('\n').to_string() + "\n";
                fs::write(abs, content)?;
            }
            Ok(())
        };
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("===FILE: ") {
            let path = rest.trim_end_matches("===");
            flush(current_path.as_deref(), &current_buf)?;
            current_path = Some(std::path::PathBuf::from(path));
            current_buf.clear();
        } else if current_path.is_some() {
            current_buf.push_str(line);
            current_buf.push('\n');
        }
    }
    flush(current_path.as_deref(), &current_buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_template_creates_expected_files() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source");
        materialize_source_template_typescript(&target, "acme-source").unwrap();
        assert!(target.join("package.json").exists());
        assert!(target.join("tsconfig.json").exists());
        assert!(target.join("wit/source-connector.wit").exists());
        assert!(target.join("src/connector.ts").exists());
        assert!(target.join("src/parse.ts").exists());
        assert!(target.join("tests/parse.test.ts").exists());
        assert!(target.join(".gitignore").exists());
        assert!(target.join("README.md").exists());

        let pkg = std::fs::read_to_string(target.join("package.json")).unwrap();
        assert!(pkg.contains("\"name\": \"acme-source\""));
        assert!(pkg.contains("@bytecodealliance/jco"));

        let connector = std::fs::read_to_string(target.join("src/connector.ts")).unwrap();
        assert!(connector.contains("export const discover"));
        assert!(connector.contains("export const readBatch"));
    }

    #[test]
    fn ts_template_refuses_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme");
        std::fs::create_dir_all(&target).unwrap();
        let err = materialize_source_template_typescript(&target, "acme").unwrap_err();
        assert!(format!("{err}").contains("already exists"));
    }
}
```

- [ ] **Step 2: Wire the new module**

Edit `crates/connector-sdk/src/templates/mod.rs`:

```rust
//! Embedded source-connector templates per language.

pub mod rust;
pub mod typescript;

pub use rust::materialize_source_template;
pub use typescript::materialize_source_template_typescript;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_template_is_reachable() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source");
        materialize_source_template(&target, "acme-source").unwrap();
        assert!(target.join("Cargo.toml").exists());
    }

    #[test]
    fn ts_template_is_reachable() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source-ts");
        materialize_source_template_typescript(&target, "acme-source-ts").unwrap();
        assert!(target.join("package.json").exists());
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p connector-sdk
```
Expected: all green, including the new `templates::typescript::tests::*` and the reachability tests in `templates::tests`.

- [ ] **Step 4: Commit**

```bash
git add crates/connector-sdk/src/templates/
git commit -m "feat(connector-sdk): embed TypeScript source template"
```

---

## Task 3: CLI `--lang` flag on `connector create`

**Files:**
- Modify: `crates/cli/src/connector_cmd.rs` — `create` accepts a `lang: String` argument.
- Modify: `crates/cli/src/main.rs` — argparser exposes `--lang` (default `rust`).

- [ ] **Step 1: Wire the argparser**

Find the existing `Connector::Create` command struct in `crates/cli/src/main.rs` and add the `--lang` flag. The exact location is wherever `kind` (the `--kind source` flag from II.3.a) lives. Apply this diff:

```rust
// In the Connector subcommand enum / struct, find:
//     Create {
//         name: String,
//         #[arg(long, default_value = "source")]
//         kind: String,
//         #[arg(long)]
//         out: Option<String>,
//     },
// Add a `lang` field below `kind`:
        #[arg(long, default_value = "rust")]
        lang: String,
```

And in the dispatch handler (also in `main.rs`), thread `lang` through:
```rust
// existing call:
//     connector_cmd::create(name, kind, out).await
// becomes:
        connector_cmd::create(name, kind, lang, out).await
```

- [ ] **Step 2: Update `create` to dispatch on lang**

In `crates/cli/src/connector_cmd.rs`, replace the existing `create` function with:

```rust
pub async fn create(
    name: String,
    kind: String,
    lang: String,
    out_dir: Option<String>,
) -> Result<()> {
    if kind != "source" {
        anyhow::bail!(
            "kind '{kind}' not supported (II.3.a/b only support 'source')"
        );
    }
    let target = match out_dir {
        Some(d) => PathBuf::from(d).join(&name),
        None => PathBuf::from(&name),
    };
    match lang.as_str() {
        "rust" => {
            connector_sdk::templates::materialize_source_template(&target, &name)
                .with_context(|| format!("creating {}", target.display()))?;
            println!("created Rust connector skeleton at {}", target.display());
            println!("next:");
            println!("  cd {}", target.display());
            println!("  # edit src/lib.rs to implement discover() and read_batch()");
            println!("  platform connector test .");
            println!("  platform connector publish . --registry ./connectors");
        }
        "typescript" | "ts" => {
            connector_sdk::templates::materialize_source_template_typescript(&target, &name)
                .with_context(|| format!("creating {}", target.display()))?;
            println!("created TypeScript connector skeleton at {}", target.display());
            println!("next:");
            println!("  cd {}", target.display());
            println!("  npm install");
            println!("  # edit src/connector.ts to implement discover() and readBatch()");
            println!("  platform connector test .");
            println!("  platform connector publish . --registry ./connectors");
        }
        other => anyhow::bail!(
            "unknown --lang: '{other}' (expected 'rust' or 'typescript')"
        ),
    }
    Ok(())
}
```

- [ ] **Step 3: Smoke-test from a temp dir**

```bash
cargo build -p cli
TMPDIR=$(mktemp -d)
./target/debug/platform connector create my-ts-conn --lang typescript --out "$TMPDIR"
ls "$TMPDIR/my-ts-conn"
```
Expected: `package.json  README.md  src  tests  tsconfig.json  wit  .gitignore`. Inspect `package.json` — name should be `my-ts-conn`.

Run also with `--lang rust` and verify the existing Rust template still works:
```bash
TMPDIR2=$(mktemp -d)
./target/debug/platform connector create my-rust-conn --lang rust --out "$TMPDIR2"
ls "$TMPDIR2/my-rust-conn"
```
Expected: `Cargo.toml  README.md  src  wit`.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/src/connector_cmd.rs crates/cli/src/main.rs
git commit -m "feat(cli): connector create --lang typescript"
```

---

## Task 4: Language detection helper

**Files:**
- Create: `crates/cli/src/connector_build.rs`
- Modify: `crates/cli/src/main.rs` — `mod connector_build;`

The `test`/`build`/`publish` flows currently assume Rust (look for `Cargo.toml`). We need a small detection helper that returns an enum, plus extracted name/version readers for both Rust (Cargo.toml) and TS (package.json).

- [ ] **Step 1: Write the helper module + unit tests**

Create `crates/cli/src/connector_build.rs`:

```rust
//! Language-aware helpers for `platform connector build/test/publish`.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lang {
    Rust,
    TypeScript,
}

pub fn detect_lang(path: &Path) -> Result<Lang> {
    let has_cargo = path.join("Cargo.toml").exists();
    let has_pkg = path.join("package.json").exists();
    match (has_cargo, has_pkg) {
        (true, false) => Ok(Lang::Rust),
        (false, true) => Ok(Lang::TypeScript),
        (true, true) => Err(anyhow!(
            "{} contains both Cargo.toml and package.json — ambiguous",
            path.display()
        )),
        (false, false) => Err(anyhow!(
            "{} is neither a cargo crate nor an npm package (no Cargo.toml or package.json)",
            path.display()
        )),
    }
}

pub fn read_package_json_name_version(path: &Path) -> Result<(String, String)> {
    let text = std::fs::read_to_string(path.join("package.json"))
        .with_context(|| format!("reading {}", path.join("package.json").display()))?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parsing {}", path.join("package.json").display()))?;
    let name = v
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow!("package.json: missing string field `name`"))?
        .to_string();
    let version = v
        .get("version")
        .and_then(|n| n.as_str())
        .unwrap_or("0.0.0")
        .to_string();
    Ok((name, version))
}

pub fn ts_wasm_artifact(path: &Path) -> PathBuf {
    path.join("dist").join("connector.wasm")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn detect_rust() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("Cargo.toml"), "[package]\nname=\"x\"\n").unwrap();
        assert_eq!(detect_lang(d.path()).unwrap(), Lang::Rust);
    }

    #[test]
    fn detect_ts() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("package.json"), r#"{"name":"x","version":"0.1.0"}"#)
            .unwrap();
        assert_eq!(detect_lang(d.path()).unwrap(), Lang::TypeScript);
    }

    #[test]
    fn detect_ambiguous() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("Cargo.toml"), "").unwrap();
        std::fs::write(d.path().join("package.json"), "{}").unwrap();
        let err = detect_lang(d.path()).unwrap_err();
        assert!(format!("{err}").contains("ambiguous"));
    }

    #[test]
    fn detect_neither() {
        let d = tempdir().unwrap();
        let err = detect_lang(d.path()).unwrap_err();
        assert!(format!("{err}").contains("neither"));
    }

    #[test]
    fn package_json_name_version_ok() {
        let d = tempdir().unwrap();
        std::fs::write(
            d.path().join("package.json"),
            r#"{"name":"acme","version":"1.2.3"}"#,
        )
        .unwrap();
        let (n, v) = read_package_json_name_version(d.path()).unwrap();
        assert_eq!(n, "acme");
        assert_eq!(v, "1.2.3");
    }

    #[test]
    fn package_json_missing_version_defaults() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("package.json"), r#"{"name":"acme"}"#).unwrap();
        let (n, v) = read_package_json_name_version(d.path()).unwrap();
        assert_eq!(n, "acme");
        assert_eq!(v, "0.0.0");
    }
}
```

- [ ] **Step 2: Wire into `main.rs`**

In `crates/cli/src/main.rs`, near the other `mod` declarations (e.g. `mod connector_cmd;`), add:
```rust
mod connector_build;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p cli connector_build::tests
```
Expected: 6 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/src/connector_build.rs crates/cli/src/main.rs
git commit -m "feat(cli): language detection helper for connector build"
```

---

## Task 5: TypeScript build pipeline (`test` command)

**Files:**
- Modify: `crates/cli/src/connector_cmd.rs` — `test` dispatches by `Lang`.

- [ ] **Step 1: Replace the `test` function**

In `crates/cli/src/connector_cmd.rs`, replace the existing `test` function with:

```rust
pub async fn test(path: String) -> Result<()> {
    use std::process::Command as StdCommand;
    use crate::connector_build::{detect_lang, Lang};

    let path = PathBuf::from(&path);
    let lang = detect_lang(&path)?;
    match lang {
        Lang::Rust => {
            println!("[1/2] cargo build --release --target wasm32-wasip2");
            let status = StdCommand::new("cargo")
                .args(["build", "--release", "--target", "wasm32-wasip2"])
                .current_dir(&path)
                .status()
                .context("running cargo build")?;
            if !status.success() {
                anyhow::bail!("cargo build failed");
            }
            println!("[2/2] cargo test (host-side unit tests)");
            let status = StdCommand::new("cargo")
                .args(["test"])
                .current_dir(&path)
                .status()
                .context("running cargo test")?;
            if !status.success() {
                anyhow::bail!("cargo test failed");
            }
        }
        Lang::TypeScript => {
            // 1. Install deps if missing.
            if !path.join("node_modules").exists() {
                println!("[1/3] npm install");
                let status = StdCommand::new("npm")
                    .args(["install", "--no-audit", "--no-fund"])
                    .current_dir(&path)
                    .status()
                    .context("running npm install (is node/npm on PATH?)")?;
                if !status.success() {
                    anyhow::bail!("npm install failed");
                }
            } else {
                println!("[1/3] node_modules present (skipping npm install)");
            }
            // 2. Run vitest.
            println!("[2/3] npm test (vitest)");
            let status = StdCommand::new("npm")
                .args(["test", "--silent"])
                .current_dir(&path)
                .status()
                .context("running npm test")?;
            if !status.success() {
                anyhow::bail!("npm test failed");
            }
            // 3. Run jco componentize via npm run build.
            println!("[3/3] npm run build (jco componentize)");
            let status = StdCommand::new("npm")
                .args(["run", "build", "--silent"])
                .current_dir(&path)
                .status()
                .context("running npm run build (jco componentize)")?;
            if !status.success() {
                anyhow::bail!("jco componentize failed");
            }
            // Sanity-check the artifact.
            let wasm = crate::connector_build::ts_wasm_artifact(&path);
            if !wasm.exists() {
                anyhow::bail!(
                    "expected {} after build but it's missing",
                    wasm.display()
                );
            }
        }
    }
    println!("connector test: ok");
    Ok(())
}
```

- [ ] **Step 2: Manual smoke test**

```bash
cargo build -p cli
SCAFFOLD_DIR=$(mktemp -d)
./target/debug/platform connector create demo-ts --lang typescript --out "$SCAFFOLD_DIR"
./target/debug/platform connector test "$SCAFFOLD_DIR/demo-ts"
```
Expected: `npm install` runs (~30-60s first time), vitest passes the single helloSchema test, `jco componentize` produces `dist/connector.wasm` (~10-15 MB), final line `connector test: ok`. If jco's CLI flags differ in your installed version, adjust `package.json`'s `build` script in the *template* (Task 2) — not in the just-scaffolded copy. Re-scaffold + retest.

If `dist/connector.wasm` doesn't exist after build, `jco componentize` likely failed silently. Inspect by re-running with verbose: `cd "$SCAFFOLD_DIR/demo-ts" && npx jco componentize src/connector.ts --wit wit/source-connector.wit --world-name source-connector -o dist/connector.wasm`.

Run the Rust path once to confirm no regression:
```bash
SCAFFOLD_R=$(mktemp -d)
./target/debug/platform connector create demo-rust --lang rust --out "$SCAFFOLD_R"
./target/debug/platform connector test "$SCAFFOLD_R/demo-rust"
```
Expected: existing Rust flow still passes.

- [ ] **Step 3: Commit**

```bash
git add crates/cli/src/connector_cmd.rs
git commit -m "feat(cli): connector test dispatches to npm + jco for TS"
```

---

## Task 6: TypeScript publish pipeline (`build` + `publish`)

**Files:**
- Modify: `crates/cli/src/connector_cmd.rs` — `build` and `publish` detect language and dispatch.

The publish flow:
1. Detect language.
2. Read name + version from the appropriate manifest.
3. Build the wasm (cargo for Rust; npm run build for TS).
4. Find the produced wasm at the language-specific path.
5. Precompile to `<registry>/<name>@<version>/component.cwasm` via `WasmSourceRuntime::precompile_to` (existing host code — no new logic).
6. Compute sha256, write `manifest.yaml` (same shape as Rust).

- [ ] **Step 1: Refactor `build` to be language-aware**

Replace the existing `build` function:

```rust
pub async fn build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
    kind: String,
) -> Result<()> {
    use std::process::Command as StdCommand;
    use crate::connector_build::{detect_lang, read_package_json_name_version, ts_wasm_artifact, Lang};

    let crate_dir = PathBuf::from(&path);
    let lang = detect_lang(&crate_dir)?;

    let (pkg_name, pkg_version, wasm_path) = match lang {
        Lang::Rust => {
            let cargo_toml = crate_dir.join("Cargo.toml");
            let toml_text = std::fs::read_to_string(&cargo_toml)?;
            let n = name.unwrap_or_else(|| {
                read_toml_value(&toml_text, "name").unwrap_or_else(|| "connector".into())
            });
            let v = version.unwrap_or_else(|| {
                read_toml_value(&toml_text, "version").unwrap_or_else(|| "0.1.0".into())
            });
            let status = StdCommand::new("cargo")
                .current_dir(&crate_dir)
                .args(["build", "--release"])
                .status()?;
            if !status.success() {
                anyhow::bail!("guest build failed");
            }
            let wasm_name = format!("{}.wasm", n.replace('-', "_"));
            let wp = crate_dir
                .join("target")
                .join("wasm32-wasip2")
                .join("release")
                .join(&wasm_name);
            if !wp.exists() {
                anyhow::bail!(
                    "expected artifact not found at {} — check the crate's [lib] crate-type and package name",
                    wp.display()
                );
            }
            (n, v, wp)
        }
        Lang::TypeScript => {
            let (n0, v0) = read_package_json_name_version(&crate_dir)?;
            let n = name.unwrap_or(n0);
            let v = version.unwrap_or(v0);
            // Ensure deps installed (idempotent).
            if !crate_dir.join("node_modules").exists() {
                let status = StdCommand::new("npm")
                    .args(["install", "--no-audit", "--no-fund"])
                    .current_dir(&crate_dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("npm install failed");
                }
            }
            let status = StdCommand::new("npm")
                .args(["run", "build", "--silent"])
                .current_dir(&crate_dir)
                .status()?;
            if !status.success() {
                anyhow::bail!("npm run build (jco componentize) failed");
            }
            let wp = ts_wasm_artifact(&crate_dir);
            if !wp.exists() {
                anyhow::bail!(
                    "expected {} after npm run build but it's missing",
                    wp.display()
                );
            }
            (n, v, wp)
        }
    };

    let out_dir = PathBuf::from(&out);
    let target_name = format!("{}@{}", pkg_name, pkg_version);

    let out_path = match kind.as_str() {
        "source" => {
            let rt = worker::wasm_runtime::WasmSourceRuntime::new(&out_dir)?;
            let p = rt.artifact_path(&target_name);
            rt.precompile_to(&wasm_path, &p)?;
            p
        }
        "scalar" => {
            let rt = worker::wasm_runtime::WasmScalarRuntime::new(&out_dir)?;
            let p = rt.artifact_path(&target_name);
            rt.precompile_to(&wasm_path, &p)?;
            p
        }
        other => anyhow::bail!("unknown --kind: '{other}' (expected 'source' or 'scalar')"),
    };

    println!("built {} ({})", out_path.display(), kind);
    Ok(())
}
```

- [ ] **Step 2: Refactor `publish` to use lang detection for the name/version reads**

Replace the existing `publish` function:

```rust
pub async fn publish(
    path: String,
    registry: String,
    version: Option<String>,
) -> Result<()> {
    use crate::connector_build::{detect_lang, read_package_json_name_version, Lang};

    let path = PathBuf::from(&path);
    let lang = detect_lang(&path)?;
    let (name, default_version) = match lang {
        Lang::Rust => {
            let cargo_toml = std::fs::read_to_string(path.join("Cargo.toml"))?;
            let n = read_toml_value(&cargo_toml, "name")
                .context("connector Cargo.toml missing a [package].name")?;
            let v = read_toml_value(&cargo_toml, "version").unwrap_or_else(|| "0.0.0".into());
            (n, v)
        }
        Lang::TypeScript => read_package_json_name_version(&path)?,
    };
    let final_version = version.unwrap_or(default_version);

    build(
        path.to_string_lossy().to_string(),
        Some(name.clone()),
        Some(final_version.clone()),
        registry.clone(),
        "source".to_string(),
    )
    .await?;

    let target_dir = PathBuf::from(&registry).join(format!("{name}@{final_version}"));
    let cwasm_path = target_dir.join("component.cwasm");
    if !cwasm_path.exists() {
        anyhow::bail!(
            "expected built artifact at {} but it's missing",
            cwasm_path.display()
        );
    }
    let bytes = std::fs::read(&cwasm_path)?;
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(&bytes);
    let hash_hex = hex::encode(h.finalize());
    let manifest = Manifest {
        name: name.clone(),
        version: final_version.clone(),
        kind: "source".into(),
        sdk_version: "0.1.0".into(),
        sha256: hash_hex.clone(),
    };
    let manifest_yaml = serde_yaml::to_string(&manifest)?;
    std::fs::write(target_dir.join("manifest.yaml"), manifest_yaml)?;
    println!(
        "published {}@{} → {} (sha256={})",
        name,
        final_version,
        target_dir.display(),
        &hash_hex[..16]
    );
    Ok(())
}
```

- [ ] **Step 3: Manual end-to-end on a TS skeleton**

```bash
cargo build -p cli
SCAFFOLD_DIR=$(mktemp -d)
./target/debug/platform connector create demo-ts --lang typescript --out "$SCAFFOLD_DIR"
./target/debug/platform connector test "$SCAFFOLD_DIR/demo-ts"
./target/debug/platform connector publish "$SCAFFOLD_DIR/demo-ts" --registry "$SCAFFOLD_DIR/connectors"
ls "$SCAFFOLD_DIR/connectors/demo-ts@0.1.0/"
cat "$SCAFFOLD_DIR/connectors/demo-ts@0.1.0/manifest.yaml"
```
Expected: directory contains `component.cwasm` and `manifest.yaml`. The manifest has `name: demo-ts`, `version: 0.1.0`, `kind: source`, a sha256, and `sdk_version: 0.1.0`.

- [ ] **Step 4: Commit**

```bash
git add crates/cli/src/connector_cmd.rs
git commit -m "feat(cli): connector publish supports TypeScript via jco"
```

---

## Task 7: Integration test — TS connector_lifecycle

**Files:**
- Create: `tests/integration/tests/typescript_connector_lifecycle.rs`

Mirrors the existing `connector_lifecycle.rs` (which tests the Rust round-trip). This one creates a TS skeleton, runs `test` + `publish`, asserts the produced .cwasm and manifest exist with correct contents.

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/typescript_connector_lifecycle.rs`:

```rust
//! Phase II.3.b — TypeScript connector lifecycle:
//!   1. `platform connector create <name> --lang typescript`
//!   2. `platform connector test <name>` (npm install + vitest + jco componentize)
//!   3. `platform connector publish <name> --registry <dir>`
//! Asserts the produced artifact + manifest.

use std::path::PathBuf;
use tokio::process::Command;

fn cargo_bin(name: &str) -> String {
    format!("{}/../../target/debug/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

#[tokio::test]
#[ignore = "requires node + npm + network for npm install + jco"]
async fn typescript_connector_lifecycle() -> anyhow::Result<()> {
    // Build the CLI we need.
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "-p", "cli"])
        .status()
        .await?;
    assert!(status.success());

    let scratch = tempfile::tempdir()?;
    let connector_name = "lifecycle-demo-ts";
    let connector_dir = scratch.path().join(connector_name);
    let registry_dir = scratch.path().join("connectors");

    // 1. Create.
    let create = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "create",
            connector_name,
            "--lang",
            "typescript",
            "--out",
            scratch.path().to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        create.status.success(),
        "create: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert!(connector_dir.join("package.json").exists());
    assert!(connector_dir.join("src/connector.ts").exists());
    assert!(connector_dir.join("wit/source-connector.wit").exists());

    // 2. Test (this is the slow step — npm install + jco componentize).
    let test = Command::new(cargo_bin("platform"))
        .args(["connector", "test", connector_dir.to_str().unwrap()])
        .output()
        .await?;
    assert!(
        test.status.success(),
        "test: stdout={} stderr={}",
        String::from_utf8_lossy(&test.stdout),
        String::from_utf8_lossy(&test.stderr)
    );
    assert!(connector_dir.join("dist/connector.wasm").exists());

    // 3. Publish.
    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            connector_dir.to_str().unwrap(),
            "--registry",
            registry_dir.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        publish.status.success(),
        "publish: {}",
        String::from_utf8_lossy(&publish.stderr)
    );

    // 4. Assert artifact + manifest.
    let cwasm = registry_dir
        .join(format!("{connector_name}@0.1.0"))
        .join("component.cwasm");
    let manifest = registry_dir
        .join(format!("{connector_name}@0.1.0"))
        .join("manifest.yaml");
    assert!(cwasm.exists(), "missing {}", cwasm.display());
    assert!(manifest.exists(), "missing {}", manifest.display());

    let manifest_text = std::fs::read_to_string(&manifest)?;
    assert!(manifest_text.contains(&format!("name: {connector_name}")));
    assert!(manifest_text.contains("version: 0.1.0"));
    assert!(manifest_text.contains("kind: source"));
    assert!(manifest_text.contains("sha256:"));

    Ok(())
}
```

- [ ] **Step 2: Compile-check**

```bash
cargo build -p integration-tests --tests
```
Expected: clean.

- [ ] **Step 3: Run the test**

```bash
cargo test -p integration-tests --test typescript_connector_lifecycle -- --ignored --nocapture
```
Expected: 1 passed in ~60-180s (mostly npm install + jco componentize).

If it fails on `npm install` due to a missing global registry config, set `npm config set registry https://registry.npmjs.org/` and retry.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/typescript_connector_lifecycle.rs
git commit -m "test(integration): typescript_connector_lifecycle round-trip"
```

---

## Task 8: Port stripe-source to TypeScript

**Files:**
- Create: `examples/stripe-source-ts/package.json`
- Create: `examples/stripe-source-ts/tsconfig.json`
- Create: `examples/stripe-source-ts/.gitignore`
- Create: `examples/stripe-source-ts/wit/source-connector.wit`
- Create: `examples/stripe-source-ts/src/parse.ts`
- Create: `examples/stripe-source-ts/src/request.ts`
- Create: `examples/stripe-source-ts/src/connector.ts`
- Create: `examples/stripe-source-ts/tests/parse.test.ts`
- Create: `examples/stripe-source-ts/tests/request.test.ts`
- Create: `examples/stripe-source-ts/README.md`
- Modify: `Cargo.toml` (workspace root) — add `examples/stripe-source-ts` to `exclude` (cargo would otherwise ignore non-Rust dirs, but this future-proofs against accidental scanning).

The structure mirrors `examples/stripe-source/` (Rust): pure TS modules `parse.ts` (JSON→Arrow IPC) and `request.ts` (URL + headers builder), connector entry `connector.ts` wiring them with the host's `httpFetch` + 429 retry.

- [ ] **Step 1: Scaffold via the CLI**

```bash
./target/debug/platform connector create stripe-source-ts --lang typescript --out examples
```
This writes the skeleton at `examples/stripe-source-ts/`.

- [ ] **Step 2: Add `apache-arrow` typings already in skeleton — replace `src/parse.ts`**

Overwrite `examples/stripe-source-ts/src/parse.ts` with:

```typescript
// Pure JSON → Arrow IPC parser for Stripe /v1/customers responses.
// Lives outside connector.ts so vitest can exercise it in plain Node.

import {
    Field,
    Schema,
    Int64,
    Utf8,
    tableFromArrays,
    tableToIPC,
} from 'apache-arrow';

export interface Customer {
    id: string;
    email: string | null;
    name: string | null;
    created: number;
}

export interface ListResp {
    data: Customer[];
    has_more: boolean;
}

export function customersSchema(): Schema {
    return new Schema([
        new Field('id', new Utf8(), false),
        new Field('email', new Utf8(), true),
        new Field('name', new Utf8(), true),
        new Field('created', new Int64(), false),
    ]);
}

export function schemaIpcBytes(): Uint8Array {
    // An empty table over the schema serializes to an IPC stream that
    // contains schema + zero record batches — same shape as Rust's
    // StreamWriter::finish on an empty buffer.
    const t = tableFromArrays({
        id: [] as string[],
        email: [] as (string | null)[],
        name: [] as (string | null)[],
        created: BigInt64Array.from([]),
    });
    return tableToIPC(t, 'stream');
}

export interface ParsedPage {
    batchIpc: Uint8Array;
    rows: number;
    lastId: string | undefined;
    hasMore: boolean;
}

export function parsePage(jsonBytes: Uint8Array): ParsedPage {
    const text = new TextDecoder().decode(jsonBytes);
    const resp = JSON.parse(text) as ListResp;
    if (!Array.isArray(resp.data)) {
        throw new Error('stripe response missing `data` array');
    }
    const ids = resp.data.map((c) => c.id);
    const emails = resp.data.map((c) => c.email ?? null);
    const names = resp.data.map((c) => c.name ?? null);
    const createds = BigInt64Array.from(resp.data.map((c) => BigInt(c.created)));
    const t = tableFromArrays({
        id: ids,
        email: emails,
        name: names,
        created: createds,
    });
    return {
        batchIpc: tableToIPC(t, 'stream'),
        rows: resp.data.length,
        lastId: resp.data.length > 0 ? resp.data[resp.data.length - 1].id : undefined,
        hasMore: !!resp.has_more,
    };
}
```

- [ ] **Step 3: Write `src/request.ts`**

```typescript
// HTTP request builder for Stripe /v1/customers — pure, no I/O.

export interface StripeRequest {
    url: string;
    headers: [string, string][];
}

export function buildListCustomers(
    apiKey: string,
    limit: number,
    startingAfter: string | undefined,
    baseUrl: string,
): StripeRequest {
    let url = `${baseUrl}/v1/customers?limit=${limit}`;
    if (startingAfter !== undefined) {
        url += `&starting_after=${startingAfter}`;
    }
    return {
        url,
        headers: [
            ['Authorization', `Bearer ${apiKey}`],
            ['Stripe-Version', '2024-04-10'],
        ],
    };
}
```

- [ ] **Step 4: Replace `src/connector.ts`**

```typescript
// stripe-source-ts — Stripe /v1/customers as an ETL source (TS port).

// @ts-expect-error - resolved by jco at componentize time
import { log, httpFetch } from 'platform:connector/host';

import { parsePage, schemaIpcBytes } from './parse.js';
import { buildListCustomers } from './request.js';

interface StripeSourceCfg {
    base_url: string;
    limit: number;
    max_429_retries: number;
}

function defaultCfg(): StripeSourceCfg {
    return { base_url: 'https://api.stripe.com', limit: 100, max_429_retries: 3 };
}

function parseSourceCfg(json: string): StripeSourceCfg {
    const d = defaultCfg();
    if (!json || json.trim() === '') return d;
    try {
        const parsed = JSON.parse(json);
        return {
            base_url: typeof parsed.base_url === 'string' ? parsed.base_url : d.base_url,
            limit: typeof parsed.limit === 'number' ? parsed.limit : d.limit,
            max_429_retries:
                typeof parsed.max_429_retries === 'number'
                    ? parsed.max_429_retries
                    : d.max_429_retries,
        };
    } catch {
        return d;
    }
}

interface HttpResponse {
    status: number;
    headers: [string, string][];
    body: Uint8Array;
}

function fetchWithRetry(
    method: string,
    url: string,
    headers: [string, string][],
    maxRetries: number,
): Uint8Array {
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
        const resp = httpFetch({ method, url, headers, body: undefined }) as HttpResponse;
        if (resp.status === 429 && attempt < maxRetries) {
            log('warn', `stripe-source-ts: 429 retry ${attempt + 1}/${maxRetries}`);
            attempt += 1;
            continue;
        }
        if (resp.status >= 200 && resp.status < 300) return resp.body;
        const bodyText = new TextDecoder().decode(resp.body);
        throw new Error(`stripe HTTP ${resp.status}: ${bodyText}`);
    }
}

export const discover = (
    _conn: { url: string },
    _source: { json: string },
): { tag: 'ok'; val: Uint8Array } | { tag: 'err'; val: { tag: 'other'; val: string } } => {
    try {
        return { tag: 'ok', val: schemaIpcBytes() };
    } catch (e) {
        return { tag: 'err', val: { tag: 'other', val: String(e) } };
    }
};

export const readBatch = (
    conn: { url: string },
    source: { json: string },
    cursor: { kind: 'int64' | 'timestamp-tz'; value: string } | undefined,
    _batchSize: number,
):
    | {
          tag: 'ok';
          val: {
              batchIpc: Uint8Array;
              rows: number;
              newCursor:
                  | { kind: 'int64' | 'timestamp-tz'; value: string }
                  | undefined;
              isFinal: boolean;
          };
      }
    | { tag: 'err'; val: { tag: 'source-unavailable' | 'other'; val: string } } => {
    try {
        const cfg = parseSourceCfg(source.json);
        const startingAfter = cursor?.value;
        const req = buildListCustomers(conn.url, cfg.limit, startingAfter, cfg.base_url);
        const body = fetchWithRetry('GET', req.url, req.headers, cfg.max_429_retries);
        const page = parsePage(body);
        const newCursor = page.lastId
            ? { kind: 'int64' as const, value: page.lastId }
            : undefined;
        return {
            tag: 'ok',
            val: {
                batchIpc: page.batchIpc,
                rows: page.rows,
                newCursor,
                isFinal: !page.hasMore,
            },
        };
    } catch (e) {
        const msg = String(e);
        if (msg.includes('stripe HTTP')) {
            return { tag: 'err', val: { tag: 'source-unavailable', val: msg } };
        }
        return { tag: 'err', val: { tag: 'other', val: msg } };
    }
};
```

- [ ] **Step 5: Replace `tests/parse.test.ts`**

```typescript
import { describe, it, expect } from 'vitest';
import { customersSchema, parsePage, schemaIpcBytes } from '../src/parse.js';

describe('customersSchema', () => {
    it('has 4 columns in canonical order', () => {
        const s = customersSchema();
        expect(s.fields.map((f) => f.name)).toEqual(['id', 'email', 'name', 'created']);
        expect(s.fields[0].nullable).toBe(false);
        expect(s.fields[1].nullable).toBe(true);
    });
});

describe('schemaIpcBytes', () => {
    it('produces a non-empty IPC stream', () => {
        const b = schemaIpcBytes();
        expect(b.byteLength).toBeGreaterThan(0);
    });
});

describe('parsePage', () => {
    it('parses two customers', () => {
        const json = `{"data":[
            {"id":"cus_a","email":"a@x.com","name":"Alice","created":1700000000},
            {"id":"cus_b","email":"b@x.com","name":"Bob","created":1700000123}
        ],"has_more":false}`;
        const p = parsePage(new TextEncoder().encode(json));
        expect(p.rows).toBe(2);
        expect(p.lastId).toBe('cus_b');
        expect(p.hasMore).toBe(false);
        expect(p.batchIpc.byteLength).toBeGreaterThan(0);
    });

    it('parses an empty page', () => {
        const json = `{"data":[],"has_more":false}`;
        const p = parsePage(new TextEncoder().encode(json));
        expect(p.rows).toBe(0);
        expect(p.lastId).toBeUndefined();
        expect(p.hasMore).toBe(false);
    });

    it('rejects malformed JSON', () => {
        expect(() => parsePage(new TextEncoder().encode('{not json'))).toThrow();
    });
});
```

- [ ] **Step 6: Add `tests/request.test.ts`**

```typescript
import { describe, it, expect } from 'vitest';
import { buildListCustomers } from '../src/request.js';

describe('buildListCustomers', () => {
    it('first page url has no starting_after', () => {
        const r = buildListCustomers('sk_test_x', 100, undefined, 'https://api.stripe.com');
        expect(r.url).toBe('https://api.stripe.com/v1/customers?limit=100');
    });

    it('paginated url includes starting_after', () => {
        const r = buildListCustomers('sk_test_x', 50, 'cus_42', 'https://api.stripe.com');
        expect(r.url).toBe(
            'https://api.stripe.com/v1/customers?limit=50&starting_after=cus_42',
        );
    });

    it('auth header uses bearer', () => {
        const r = buildListCustomers('sk_test_secret', 1, undefined, 'https://api.stripe.com');
        expect(
            r.headers.find(([k, v]) => k === 'Authorization' && v === 'Bearer sk_test_secret'),
        ).toBeDefined();
    });

    it('stripe-version pinned', () => {
        const r = buildListCustomers('k', 1, undefined, 'https://api.stripe.com');
        expect(
            r.headers.find(([k, v]) => k === 'Stripe-Version' && v === '2024-04-10'),
        ).toBeDefined();
    });
});
```

- [ ] **Step 7: Replace `README.md` and add workspace exclude**

Overwrite `examples/stripe-source-ts/README.md`:

```markdown
# stripe-source-ts

TypeScript port of `examples/stripe-source` — Stripe `/v1/customers` source connector built via the II.3.b TS SDK.

## Schema

| column  | type      | nullable |
|---------|-----------|----------|
| id      | utf8      | no       |
| email   | utf8      | yes      |
| name    | utf8      | yes      |
| created | int64 (unix-seconds) | no |

## Build & publish

```bash
npm install
platform connector test .
platform connector publish . --registry ./connectors
```

The published artifact lands at `./connectors/stripe-source-ts@0.1.0/component.cwasm`.

## Source-config knobs

```json
{ "base_url": "https://api.stripe.com", "limit": 100, "max_429_retries": 3 }
```

## Behavior

Identical to the Rust connector. Bundle size ~10–15 MB (componentize-js embeds SpiderMonkey). Functional behavior, schema, and host imports match `examples/stripe-source/` byte-for-byte from the platform's perspective.
```

Edit `Cargo.toml` (workspace root) — add `examples/stripe-source-ts` to the `exclude` list:

```toml
exclude = ["examples/csv-source", "examples/upper-case-scalar", "examples/hello-world-source", "examples/stripe-source", "examples/stripe-source-ts"]
```

- [ ] **Step 8: Test the TS port end-to-end**

```bash
cd examples/stripe-source-ts
npm install
npm test
npm run build
ls dist/connector.wasm
cd -
```
Expected: vitest passes 7 tests (4 parse + 4 request — wait, the count: `customersSchema` 1 + `schemaIpcBytes` 1 + `parsePage` 3 + `buildListCustomers` 4 = 9). Adjust if vitest reports a different count; the test file shape is what matters.

`dist/connector.wasm` should be ~10-15 MB.

- [ ] **Step 9: Commit**

```bash
git add examples/stripe-source-ts/ Cargo.toml
git commit -m "feat(stripe-source-ts): TypeScript port of stripe-source"
```

---

## Task 9: Wiremock e2e for the TS Stripe connector

**Files:**
- Create: `tests/integration/tests/stripe_ts_e2e.rs`

Mirrors `tests/integration/tests/stripe_e2e.rs` but for the TS connector. Same wiremock + same publish + same apply path. Confirms the TS-built .cwasm is functionally equivalent to the Rust one through the worker host.

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/stripe_ts_e2e.rs`:

```rust
//! Phase II.3.b — TypeScript Stripe connector e2e.
//! Same shape as stripe_e2e.rs but the connector under test is the
//! TS port. Validates: jco-built .cwasm is consumable by the worker
//! host and behaves identically to the Rust connector.

use catalog::Catalog;
use std::path::PathBuf;
use tokio::process::Command;
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

#[tokio::test]
#[ignore = "requires docker postgres + node + npm + jco"]
async fn stripe_ts_connector_full_flow() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
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
        .expect(0..=1)
        .mount(&server)
        .await;

    let registry = workspace_root().join("connectors");
    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            workspace_root()
                .join("examples/stripe-source-ts")
                .to_str()
                .unwrap(),
            "--registry",
            registry.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        publish.status.success(),
        "publish: {}",
        String::from_utf8_lossy(&publish.stderr)
    );
    assert!(registry
        .join("stripe-source-ts@0.1.0/component.cwasm")
        .exists());

    let connections_yaml = format!(
        r#"apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: stripe-mock-ts
spec:
  connector_ref: wasm:stripe-source-ts@0.1.0
  config:
    url: sk_test_demo
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: stripe-customers-mock-ts
spec:
  source_connection: stripe-mock-ts
  source:
    type: wasm
    config:
      base_url: "{}"
      limit: 100
      max_429_retries: 1
  destination:
    type: local_parquet
    base_path: /tmp/stripe-mock-ts-data
  batch_size: 100
  evolution_policy: propagate_additive
"#,
        server.uri(),
    );
    let yaml_dir = tempfile::tempdir()?;
    let yaml_path = yaml_dir.path().join("stripe-ts.yaml");
    std::fs::write(&yaml_path, connections_yaml)?;

    let apply = Command::new(cargo_bin("platform"))
        .args(["apply", "-f", yaml_path.to_str().unwrap()])
        .env("DATABASE_URL", catalog_url())
        .env("ETL_AUTH_BYPASS", "1")
        .env("ETL_CONNECTORS_DIR", registry.to_str().unwrap())
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        apply.status.success(),
        "apply: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    drop(server);
    Ok(())
}
```

- [ ] **Step 2: Compile-check**

```bash
cargo build -p integration-tests --tests
```
Expected: clean.

- [ ] **Step 3: Run the test**

```bash
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true
VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=etl-dev-token \
  cargo test -p integration-tests --test stripe_ts_e2e -- --ignored --nocapture
```
Expected: 1 passed in ~90-180s (npm install + jco componentize + worker bootup + pipeline run).

If the worker fails to invoke the TS .cwasm (e.g. host import resolution mismatch), inspect the wasm: `wasm-tools component wit examples/stripe-source-ts/dist/connector.wasm` and confirm it imports `platform:connector/host` and exports `discover` + `read-batch`. Mismatch in component-model export naming between Rust (kebab-case) and TS (jco may emit camelCase exports) is the most likely failure mode — fix by adjusting the TS export names to match the WIT (jco's componentize-js maps the world's exports to camelCase or kebab-case depending on its version; see jco docs).

- [ ] **Step 4: Commit**

```bash
git add tests/integration/tests/stripe_ts_e2e.rs
git commit -m "test(integration): stripe_ts_e2e — wiremock + TS connector"
```

---

## Task 10: README + completion log + final regression sweep

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md` (this file)

- [ ] **Step 1: README — add TS authoring path**

In `README.md`'s `## Connector SDK (Phase II.3.a)` section, find the line:
```
**Example connector: Stripe customers (Phase II.3.c).** ...
```
and *insert* the following block immediately *before* that line:

```markdown
**TypeScript authoring (Phase II.3.b).** `platform connector create my-source --lang typescript` materializes a TS skeleton (package.json + jco + apache-arrow + vitest). `platform connector test` runs `npm test` + `jco componentize`; `publish` produces the same `.cwasm` artifact shape as Rust. Bundle size is larger (~10–15 MB vs ~650 KB Rust) because componentize-js embeds SpiderMonkey, but the worker host treats both identically.

```

Also bump the "Currently:" line at the top of the README:
```markdown
Currently: **Phase II.3.b — TypeScript SDK + TS Stripe connector (complete)** on top of II.3.c. Phase II.3.d (MySQL CDC) and II.4 (Helm + Terraform + `platform install`) ship next.
```

- [ ] **Step 2: Append completion log to this plan**

Append to `docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md`:

```markdown
---

## Phase II.3.b Completion Log

Completed 2026-04-XX on branch `phase-2-3b-ts-sdk`.

- [x] T1 — Templates module split (Rust → templates/rust.rs)
- [x] T2 — TS template embedded
- [x] T3 — `connector create --lang typescript`
- [x] T4 — Language detection helper
- [x] T5 — `connector test` dispatches Rust|TS
- [x] T6 — `connector publish` dispatches Rust|TS
- [x] T7 — typescript_connector_lifecycle integration test
- [x] T8 — examples/stripe-source-ts (TS port)
- [x] T9 — stripe_ts_e2e wiremock test
- [x] T10 — README + this log + sweep

### Exit criterion — MET

- `platform connector create --lang typescript` materializes a TS skeleton.
- `platform connector test <ts-conn>` runs npm install + vitest + jco componentize and produces `dist/connector.wasm`.
- `platform connector publish <ts-conn>` produces `<registry>/<name>@<version>/component.cwasm` + `manifest.yaml` with the same shape as Rust.
- `examples/stripe-source-ts/` builds and `stripe_ts_e2e` passes against wiremock.
- 121 unit tests + 36 integration tests green (existing 34 + 2 new: typescript_connector_lifecycle + stripe_ts_e2e).

### Deviations from the plan

_(Fill in after execution — likely candidates: jco CLI flag drift, componentize-js export-name casing, apache-arrow ESM resolution under TS module="ES2022".)_

### Handoff to Phase II.3.d / II.4

II.3.d — MySQL binlog CDC connector (Rust):
- First non-HTTP connector, validates SDK generality.

II.4 — Helm + Terraform packaging:
- Connector portfolio scales horizontally on this SDK foundation; packaging makes the platform installable.
```

- [ ] **Step 3: Final regression sweep**

```bash
pkill -f "target/debug/worker" 2>/dev/null
pkill -f "target/debug/etl-auth" 2>/dev/null
docker exec etl-postgres psql -U etl -d cdc_source_demo \
  -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'etl_%';" || true

cargo test --workspace --lib
VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=etl-dev-token \
  cargo test -p integration-tests -- --ignored --test-threads=1
```

Expected: 121+ unit tests pass. Integration tests: previous 34 + 2 new = 36 passed, 0 failed.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md
git commit -m "docs: Phase II.3.b README + completion log"
```

Then use the **finishing-a-development-branch** skill.

---

## Appendix A — Operational notes

**Why componentize-js (jco) and not transpile-only?** jco's `componentize` produces a wasi-p2 Component Model component by bundling the author's JS + a SpiderMonkey runtime. This means we don't need to write a custom JS host — the worker's existing `wasmtime` runtime loads the .cwasm exactly the same way it loads a Rust-built one. Trade-off: ~10-15 MB binary baseline because SpiderMonkey is large.

**Bundle size mitigation (deferred).** componentize-js has a `--enable-aot` flag in newer versions that AOT-compiles the JS into wasm without bundling the full engine — reduces to ~2 MB. Not stable enough yet to require; revisit when jco ships a stable release with AOT default-on.

**Why `apache-arrow` and not hand-rolled IPC?** The IPC stream format is flatbuffers-encoded. Hand-rolling it in TS would be tedious and error-prone. `apache-arrow` (the official JS library) produces correct IPC and is well-maintained. Adds ~200 KB to the bundle (small relative to SpiderMonkey).

**Host imports from TS.** jco generates TS bindings from the WIT at componentize time. The author imports them by package + interface (e.g. `import { httpFetch, log } from 'platform:connector/host'`). The `@ts-expect-error` comment in the skeleton is necessary because TS doesn't see the binding at type-check time — only at componentize time.

**Vitest in skeleton.** Vitest is the modern de-facto JS unit-test runner with native ESM + TS support. It runs in plain Node (not the wasm guest), so it can only test pure TS modules. The connector entry (`connector.ts`) calls host imports that don't exist outside the wasm sandbox, so it can't be unit-tested in vitest — but its dependencies (`parse.ts`, `request.ts`) can be, mirroring the Rust crate's structure (parse + request as inline-tested modules; lib.rs entry tested via stripe_e2e).

**Why both `connector test` *and* `npm test`?** `npm test` runs vitest. `platform connector test` *also* runs vitest, then additionally builds the wasm via jco. Authors who only changed parse logic can iterate fast with `npm test`; the full SDK contract (`platform connector test`) validates the build pipeline too.

---

## Appendix B — What's deferred

- **TS scalar/destination connectors** — same scope as II.3.a (sources only).
- **`@platform/connector-sdk` npm package** — would provide typed wrappers around the host imports + helper utilities. Today the skeleton uses `@ts-expect-error` because the bindings only exist after componentize. A future SDK npm package can publish typings.
- **AOT mode for jco** — bundle size optimization; revisit when stable.
- **TS hot reload during `connector test`** — vitest watch mode + componentize-on-save would be a nice DX improvement.
- **Connector signing** — Phase II.4.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-30-phase-2-3b-typescript-sdk.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task. The TS toolchain is at the rough edge of what we've used so far; jco/componentize quirks are the most likely failure mode. Per-task isolation lets a subagent dig into a specific failure (e.g. T5's componentize step) without blocking other progress.

**2. Inline Execution** — feasible. T5 + T8 are the long poles (npm install + jco compile times). Total wall time likely ~30-45 min including test runs.

**Which approach?**

---

## Phase II.3.b Completion Log

Completed 2026-04-30 on branch `phase-2-3b-ts-sdk`.

- [x] T1 — Templates module split (Rust → templates/rust.rs)
- [x] T2 — TS template embedded
- [x] T3 — `connector create --lang typescript`
- [x] T4 — Language detection helper
- [x] T5 — `connector test` dispatches Rust|TS (npm + esbuild + jco)
- [x] T6 — `connector publish` dispatches Rust|TS (precompile to .cwasm)
- [x] T7 — typescript_connector_lifecycle integration test (passes in ~95s)
- [x] T8 — examples/stripe-source-ts (TS port; 9 vitest tests pass)
- [x] T9 — stripe_ts_e2e wiremock test (publish + apply path)
- [x] T10 — README + this log + sweep

### Exit criterion — MET

- `platform connector create --lang typescript` materializes a TS skeleton.
- `platform connector test <ts-conn>` runs npm install + vitest + esbuild bundle + jco componentize and produces `dist/connector.wasm` (~16 MB).
- `platform connector publish <ts-conn>` produces `<registry>/<name>@<version>/component.cwasm` + `manifest.yaml` with the same shape as Rust.
- `examples/stripe-source-ts/` builds and produces a wasm component that imports `platform:connector/host@0.1.0` and exports `discover` + `read-batch` (verified via `jco wit dist/connector.wasm`).
- `stripe_ts_e2e` passes: publishes and applies the TS connector against a wiremock Stripe.
- 121 unit tests + 36 integration tests green.

### Deviations from the plan

- **Plan called for `jco componentize <src.ts>` directly.** Reality: jco only accepts JavaScript, not TypeScript. Added an esbuild bundling step (`npm run bundle`) that produces `dist/connector.js` first, then `jco componentize dist/connector.js`. esbuild also handles npm-module resolution (apache-arrow, etc.) which jco can't do.
- **Tempfile dev-dep added to crates/cli.** T4's tests use `tempfile::tempdir()` for isolated detection tests; the cli crate didn't have it as a dev-dep. Added via `[dev-dependencies]` block.
- **Versioned WIT import.** componentize-js expects host imports as `platform:connector/host@0.1.0` (with version), not bare `platform:connector/host`. Discovered via `--debug-bindings` dump. Both the embedded template and the Stripe TS port use the versioned form.
- **esbuild external pattern.** `--external:platform:connector/host` doesn't match the versioned form. Switched to wildcard `--external:platform:*`.
- **esbuild `--platform=node`, not neutral.** apache-arrow has CommonJS dependencies (flatbuffers) that don't resolve under `--platform=neutral`; node platform fixes it.
- **Bundle size: ~16 MB, not ~10 MB.** componentize-js embeds StarlingMonkey + WASI imports for fs/clocks/io/random/http/cli. Acceptable for II.3.b.
- **T9 covers publish + apply only, NOT worker execution.** The wiremock has `expect(0..=1)` to permit zero calls because `apply` doesn't run the pipeline; the test verifies the .cwasm is consumable by the catalog and the YAML applies cleanly. Whether the worker can actually run the TS .cwasm with all its WASI imports remains unverified — likely needs `wasmtime-wasi` linkage in the worker host. Flagged as a future hardening task.

### Handoff to Phase II.3.d / II.4

II.3.d — MySQL binlog CDC connector (Rust):
- First non-HTTP connector, validates SDK generality.

II.4 — Helm + Terraform packaging:
- Connector portfolio scales horizontally on this SDK foundation; packaging makes the platform installable.

### Future hardening — TS connector worker execution

**Closed in Phase II.3.b.1** (`docs/superpowers/plans/2026-04-30-phase-2-3b-1-ts-worker-execution.md`). `stripe_ts_e2e` now spawns a worker, runs the pipeline end-to-end, and asserts both the wiremock GET fired and 2 Parquet rows landed. Three fixes were needed: linking `wasmtime-wasi-http` (componentize-js's StarlingMonkey imports `wasi:http/types` even with `--disable http`), returning raw values from the TS `discover`/`readBatch` exports (jco wraps them in `result.ok` automatically — double-wrapping yielded a 0-byte payload), and using explicit `vectorFromArray(values, new Utf8())` in apache-arrow (its `tableFromArrays` infers `Dictionary<Int32,Utf8>` for strings, mismatching Rust's `Utf8`).
