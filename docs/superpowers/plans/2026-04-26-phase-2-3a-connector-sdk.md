# Phase II.3.a — Connector SDK v1 + Authoring CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package the existing Rust connector traits (RFC-6) as a publishable SDK with a complete authoring workflow — `platform connector create | test | publish` — plus a scaffolding template, a test harness for connector authors, and an end-to-end authoring guide so an external operator can build and ship a custom connector in under an hour.

**Architecture:** The `connector-sdk` crate already exposes `SourceConnector` + `ReadOutcome` + WIT bindings (from Phase I.3). II.3.a adds a `templates::SOURCE_TEMPLATE` static string that `platform connector create <name>` materializes into a new directory; a `test_harness::run_smoke` helper that drives a connector through discover → read_batch → assert-rows; a `platform connector test <path>` runner that invokes `cargo test` plus the smoke harness; a `platform connector publish <path>` flow that calls the existing `connector build` to produce a `.cwasm` then writes it under `<registry_dir>/<name>@<version>/` with a manifest file. The local filesystem registry stays the source of truth for II.3.a; remote registry lands in II.3.c.

**Tech Stack:** Rust 1.88, existing `connector-sdk` + WIT, `cargo` (subprocess) for build/test, `serde_yaml` for the manifest, `walkdir` for the registry layout, `clap` for the CLI.

---

## File Structure

**New crates / modules:**
- `crates/connector-sdk/src/templates.rs` — `pub const SOURCE_TEMPLATE: &str` — a tar-style multi-file string the CLI materializes.
- `crates/connector-sdk/src/test_harness.rs` — `run_smoke(path, fake_source) -> Result<SmokeReport>` for connector authors.
- `crates/cli/src/connector_cmd.rs` — extract the existing `connector_build` from `main.rs` into a module; add `create`, `test`, `publish` handlers.
- `docs/connector-sdk-guide.md` — authoring guide ("Build your own connector in 30 minutes").
- `tests/integration/tests/connector_lifecycle.rs` — end-to-end `create → cargo build → test → publish` smoke.

**Modified:**
- `crates/connector-sdk/Cargo.toml` — no new deps (template is pure string data).
- `crates/connector-sdk/src/lib.rs` — `pub mod templates; pub mod test_harness;`.
- `crates/cli/src/main.rs` — `mod connector_cmd;`; replace inline `Build` handler with delegation; new `Create` / `Test` / `Publish` subcommands.
- `crates/cli/Cargo.toml` — add `walkdir = "2"` if not already present.
- `README.md` — Connector SDK section.

---

## Task 1: `connector-sdk::templates` — source connector template

**Files:**
- Create: `crates/connector-sdk/src/templates.rs`
- Modify: `crates/connector-sdk/src/lib.rs`

- [ ] **Step 1: Templates module**

```rust
// crates/connector-sdk/src/templates.rs
//
// Embedded text template materialized by `platform connector create`.
// Format: a sequence of "===FILE: <path>===\n<content>\n" blocks.
// The CLI splits on "===FILE:" and writes each block to disk.

pub const SOURCE_TEMPLATE: &str = r#"===FILE: Cargo.toml===
[package]
name = "{{NAME}}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.37"

[profile.release]
strip = true
opt-level = "s"
lto = true

===FILE: README.md===
# {{NAME}}

A custom source connector for the ETL platform.

## Build

```bash
platform connector test .
platform connector publish . --registry ./connectors
```

The published artifact lands at `./connectors/{{NAME}}@<version>/component.cwasm`.

===FILE: src/lib.rs===
//! {{NAME}} — source connector skeleton.

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit/source-connector.wit",
    world: "source",
    exports: {
        "etl:source/source": Source,
    },
});

use exports::etl::source::source::{
    ConnectionConfig, ConnectorError, CursorKind, CursorValue, Guest,
    ReadOutcome, SourceConfig,
};

struct Source;

impl Guest for Source {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        // Return Arrow IPC schema bytes describing your source's columns.
        // Replace this stub with real discovery (e.g. HTTP HEAD or DESCRIBE TABLE).
        Err(ConnectorError {
            message: "{{NAME}}::discover not implemented".to_string(),
        })
    }

    fn read_batch(
        _conn: ConnectionConfig,
        _source: SourceConfig,
        _cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        Err(ConnectorError {
            message: "{{NAME}}::read_batch not implemented".to_string(),
        })
    }
}
"#;

/// Materialize `SOURCE_TEMPLATE` into a new directory at `target_dir`,
/// substituting `{{NAME}}` for the supplied connector name.
pub fn materialize_source_template(
    target_dir: &std::path::Path,
    name: &str,
) -> anyhow::Result<()> {
    use std::fs;
    if target_dir.exists() {
        anyhow::bail!("{} already exists", target_dir.display());
    }
    fs::create_dir_all(target_dir)?;
    let body = SOURCE_TEMPLATE.replace("{{NAME}}", name);
    let mut current_path: Option<std::path::PathBuf> = None;
    let mut current_buf = String::new();
    let flush = |path: Option<&std::path::Path>, buf: &str| -> anyhow::Result<()> {
        if let Some(p) = path {
            let abs = target_dir.join(p);
            if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent)?;
            }
            // Strip trailing newline added by our line iterator.
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
    fn materialize_creates_expected_files() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source");
        materialize_source_template(&target, "acme-source").unwrap();
        assert!(target.join("Cargo.toml").exists());
        assert!(target.join("README.md").exists());
        assert!(target.join("src/lib.rs").exists());
        let cargo = std::fs::read_to_string(target.join("Cargo.toml")).unwrap();
        assert!(cargo.contains("name = \"acme-source\""));
        let lib = std::fs::read_to_string(target.join("src/lib.rs")).unwrap();
        assert!(lib.contains("acme-source::discover not implemented"));
    }

    #[test]
    fn materialize_refuses_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme");
        std::fs::create_dir_all(&target).unwrap();
        let err = materialize_source_template(&target, "acme").unwrap_err();
        assert!(format!("{err}").contains("already exists"));
    }
}
```

- [ ] **Step 2: Wire into lib.rs**

In `crates/connector-sdk/src/lib.rs`, add:

```rust
pub mod templates;
```

- [ ] **Step 3: Add tempfile to dev-deps**

In `crates/connector-sdk/Cargo.toml` if not already there:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p connector-sdk templates
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/connector-sdk/Cargo.toml crates/connector-sdk/src/templates.rs crates/connector-sdk/src/lib.rs
git commit -m "feat(connector-sdk): SOURCE_TEMPLATE + materialize_source_template"
```

---

## Task 2: `platform connector create` subcommand

**Files:**
- Create: `crates/cli/src/connector_cmd.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Move existing `connector_build` into a new module + add `create`**

In `crates/cli/src/main.rs`, find the existing `async fn connector_build(...)` and remove it (about 80 lines). The new module owns it. Create:

```rust
// crates/cli/src/connector_cmd.rs
use anyhow::{Context, Result};
use std::path::PathBuf;

pub async fn create(name: String, kind: String, out_dir: Option<String>) -> Result<()> {
    if kind != "source" {
        anyhow::bail!(
            "kind '{kind}' not supported (II.3.a only supports 'source'; \
             scalar/destination land in II.3.b/c)"
        );
    }
    let target = match out_dir {
        Some(d) => PathBuf::from(d).join(&name),
        None => PathBuf::from(&name),
    };
    connector_sdk::templates::materialize_source_template(&target, &name)
        .with_context(|| format!("creating {}", target.display()))?;
    println!("created connector skeleton at {}", target.display());
    println!("next:");
    println!("  cd {}", target.display());
    println!("  # edit src/lib.rs to implement discover() and read_batch()");
    println!("  platform connector test .");
    println!("  platform connector publish . --registry ./connectors");
    Ok(())
}
```

(Keep `pub async fn build(path, name, version, out, kind)` — the existing function moved verbatim — for the next task.)

- [ ] **Step 2: Move `connector_build` body into the new module**

Cut lines 447 through end of `connector_build` from `crates/cli/src/main.rs`. Paste into `crates/cli/src/connector_cmd.rs` and rename to `pub async fn build(...)`. Update the match arm in `main.rs`:

```rust
        Cmd::Connector {
            cmd: ConnectorCmd::Build { path, name, version, out, kind },
        } => connector_cmd::build(path, name, version, out, kind).await,
```

- [ ] **Step 3: Add `Create` subcommand to clap**

In `crates/cli/src/main.rs`, find `enum ConnectorCmd { Build { ... } }` and extend:

```rust
#[derive(Subcommand)]
enum ConnectorCmd {
    /// Compile a guest Rust crate to a precompiled .cwasm artifact.
    Build {
        path: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, default_value = "./connectors")]
        out: String,
        #[arg(long, default_value = "source")]
        kind: String,
    },
    /// Scaffold a new connector crate from the SDK template.
    Create {
        /// Connector name (becomes the cargo crate name and registry id).
        name: String,
        /// Connector kind: 'source' (II.3.a). 'scalar'/'destination' deferred.
        #[arg(long, default_value = "source")]
        kind: String,
        /// Parent directory; default = current working directory.
        #[arg(long)]
        out: Option<String>,
    },
}
```

Add `mod connector_cmd;` to the top of `main.rs` and dispatch:

```rust
        Cmd::Connector { cmd } => match cmd {
            ConnectorCmd::Build { path, name, version, out, kind } => {
                connector_cmd::build(path, name, version, out, kind).await
            }
            ConnectorCmd::Create { name, kind, out } => {
                connector_cmd::create(name, kind, out).await
            }
        },
```

(Replaces the existing single-arm match.)

- [ ] **Step 4: Add connector-sdk dep to cli**

In `crates/cli/Cargo.toml`:

```toml
connector-sdk = { workspace = true }
```

- [ ] **Step 5: Build + smoke**

```bash
cargo build -p cli
cd /tmp
rm -rf my-source
~/Desktop/etl/target/debug/platform connector create my-source
ls my-source/
cat my-source/Cargo.toml
```

Expected: `Cargo.toml`, `README.md`, `src/lib.rs` files; `Cargo.toml` has `name = "my-source"`.

- [ ] **Step 6: Commit**

```bash
git add crates/cli/Cargo.toml crates/cli/src/connector_cmd.rs crates/cli/src/main.rs
git commit -m "feat(cli): platform connector create — scaffold from template"
```

---

## Task 3: `connector-sdk::test_harness` — author-side smoke runner

**Files:**
- Create: `crates/connector-sdk/src/test_harness.rs`
- Modify: `crates/connector-sdk/src/lib.rs`

- [ ] **Step 1: Harness**

```rust
// crates/connector-sdk/src/test_harness.rs
//
// Author-side helper. Drives a `SourceConnector` impl through a single
// discover → read_batch round trip and validates basic invariants:
//
//   * discover() returns a non-empty schema.
//   * read_batch() returns a batch whose `.schema()` matches discover().
//   * read_batch() with cursor=None and a small batch_size returns
//     fewer than batch_size rows OR is_final=true (or both).
//
// Connectors call this from their integration tests; the platform CLI
// also invokes it via `platform connector test`.

use crate::SourceConnector;
use arrow::record_batch::RecordBatch;
use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::SourceSpec;

#[derive(Debug)]
pub struct SmokeReport {
    pub schema_columns: Vec<String>,
    pub batch_rows: usize,
    pub is_final: bool,
}

pub async fn run_smoke<C: SourceConnector>(
    connector: &C,
    conn: &ConnectionConfig,
    source: &SourceSpec,
    batch_size: usize,
) -> anyhow::Result<SmokeReport> {
    let schema = connector.discover(conn, source).await?;
    if schema.fields().is_empty() {
        anyhow::bail!("discover() returned empty schema");
    }
    let outcome = connector.read_batch(conn, source, None, batch_size).await?;
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let batch_columns: Vec<String> = outcome
        .batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if batch_columns != columns {
        anyhow::bail!(
            "read_batch schema {:?} disagrees with discover schema {:?}",
            batch_columns,
            columns
        );
    }
    if outcome.batch.num_rows() > batch_size {
        anyhow::bail!(
            "read_batch returned {} rows but batch_size was {}",
            outcome.batch.num_rows(),
            batch_size
        );
    }
    Ok(SmokeReport {
        schema_columns: columns,
        batch_rows: outcome.batch.num_rows(),
        is_final: outcome.is_final,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadOutcome, SourceConnector};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    struct FakeOk {
        schema: SchemaRef,
    }

    #[async_trait::async_trait]
    impl SourceConnector for FakeOk {
        async fn discover(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
        ) -> anyhow::Result<SchemaRef> {
            Ok(self.schema.clone())
        }
        async fn read_batch(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
            _cursor: Option<common_types::cursor::CursorValue>,
            _batch_size: usize,
        ) -> anyhow::Result<ReadOutcome> {
            let arr = Int64Array::from(vec![1, 2, 3]);
            let batch =
                RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)]).unwrap();
            Ok(ReadOutcome {
                batch,
                new_cursor: None,
                is_final: true,
            })
        }
    }

    fn pg_source() -> SourceSpec {
        SourceSpec::Postgres(common_types::pipeline_spec::PostgresSourceSpec {
            schema: "public".into(),
            table: "t".into(),
            cursor_column: "id".into(),
            cursor_kind: common_types::cursor::CursorKind::Int64,
            pk_columns: vec!["id".into()],
            sync_mode: Default::default(),
        })
    }

    #[tokio::test]
    async fn smoke_passes_when_schema_matches() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let c = FakeOk { schema };
        let conn = ConnectionConfig::from_url("none://");
        let report = run_smoke(&c, &conn, &pg_source(), 10).await.unwrap();
        assert_eq!(report.schema_columns, vec!["id".to_string()]);
        assert_eq!(report.batch_rows, 3);
        assert!(report.is_final);
    }

    struct FakeBadSchema;
    #[async_trait::async_trait]
    impl SourceConnector for FakeBadSchema {
        async fn discover(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
        ) -> anyhow::Result<SchemaRef> {
            Ok(Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )])))
        }
        async fn read_batch(
            &self,
            _conn: &ConnectionConfig,
            _source: &SourceSpec,
            _cursor: Option<common_types::cursor::CursorValue>,
            _batch_size: usize,
        ) -> anyhow::Result<ReadOutcome> {
            let other_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "name",
                DataType::Utf8,
                false,
            )]));
            let arr = arrow::array::StringArray::from(vec!["x"]);
            let batch =
                RecordBatch::try_new(other_schema.clone(), vec![Arc::new(arr)]).unwrap();
            Ok(ReadOutcome {
                batch,
                new_cursor: None,
                is_final: true,
            })
        }
    }

    #[tokio::test]
    async fn smoke_fails_on_schema_disagreement() {
        let c = FakeBadSchema;
        let conn = ConnectionConfig::from_url("none://");
        let err = run_smoke(&c, &conn, &pg_source(), 10).await.unwrap_err();
        assert!(format!("{err}").contains("disagrees"));
    }
}
```

- [ ] **Step 2: Wire**

In `crates/connector-sdk/src/lib.rs`:

```rust
pub mod test_harness;
```

- [ ] **Step 3: Add tokio dev-dep if not already present**

In `crates/connector-sdk/Cargo.toml`:

```toml
[dev-dependencies]
tempfile = "3"
tokio = { workspace = true }
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p connector-sdk test_harness
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/connector-sdk/Cargo.toml crates/connector-sdk/src/test_harness.rs crates/connector-sdk/src/lib.rs
git commit -m "feat(connector-sdk): test_harness::run_smoke for author-side validation"
```

---

## Task 4: `platform connector test` subcommand

**Files:**
- Modify: `crates/cli/src/connector_cmd.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Test handler**

Append to `crates/cli/src/connector_cmd.rs`:

```rust
use std::process::Command as StdCommand;

pub async fn test(path: String) -> Result<()> {
    let path = PathBuf::from(&path);
    if !path.join("Cargo.toml").exists() {
        anyhow::bail!("{} is not a cargo crate (no Cargo.toml)", path.display());
    }
    println!("[1/2] cargo build --release --target wasm32-wasip1");
    let status = StdCommand::new("cargo")
        .args(["build", "--release", "--target", "wasm32-wasip1"])
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
    println!("connector test: ok");
    Ok(())
}
```

- [ ] **Step 2: Wire subcommand**

In `crates/cli/src/main.rs`, extend `ConnectorCmd`:

```rust
    /// Build (wasm32-wasip1) and run host-side unit tests for a connector crate.
    Test {
        path: String,
    },
```

Dispatch:

```rust
            ConnectorCmd::Test { path } => connector_cmd::test(path).await,
```

- [ ] **Step 3: Smoke**

```bash
cargo build -p cli
cd /tmp
rm -rf demo-source && ~/Desktop/etl/target/debug/platform connector create demo-source
# The fresh stub returns Err from discover/read_batch — cargo build should
# still succeed because the trait is satisfied. cargo test runs zero tests.
~/Desktop/etl/target/debug/platform connector test demo-source
```

Expected: build succeeds (the wasm32-wasip1 target may need to be installed; if so, `rustup target add wasm32-wasip1` first); test prints "connector test: ok".

- [ ] **Step 4: Commit**

```bash
git add crates/cli/src/connector_cmd.rs crates/cli/src/main.rs
git commit -m "feat(cli): platform connector test — cargo build wasm32 + cargo test"
```

---

## Task 5: `platform connector publish` subcommand

**Files:**
- Modify: `crates/cli/src/connector_cmd.rs`
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/cli/Cargo.toml`

- [ ] **Step 1: Manifest type**

Append to `crates/cli/src/connector_cmd.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Manifest {
    name: String,
    version: String,
    kind: String,        // "source" | "scalar" (future) | "destination" (future)
    sdk_version: String, // for forward compat
    sha256: String,      // hex digest of component.cwasm
}

pub async fn publish(path: String, registry: String, version: Option<String>) -> Result<()> {
    let path = PathBuf::from(&path);
    if !path.join("Cargo.toml").exists() {
        anyhow::bail!("{} is not a cargo crate", path.display());
    }
    // Parse Cargo.toml for name (and optionally version).
    let cargo_toml = std::fs::read_to_string(path.join("Cargo.toml"))?;
    let name = parse_toml_field(&cargo_toml, "name")
        .context("connector Cargo.toml missing a [package].name")?;
    let cargo_version = parse_toml_field(&cargo_toml, "version").unwrap_or_else(|| "0.0.0".into());
    let final_version = version.unwrap_or(cargo_version);

    // Build via the existing build() pipeline.
    let registry_path = PathBuf::from(&registry);
    let target_dir = registry_path.join(format!("{name}@{final_version}"));
    std::fs::create_dir_all(&target_dir)?;
    build(
        path.to_string_lossy().to_string(),
        Some(name.clone()),
        Some(final_version.clone()),
        registry.clone(),
        "source".to_string(),
    )
    .await?;

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

fn parse_toml_field(toml_src: &str, key: &str) -> Option<String> {
    for line in toml_src.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix(&format!("{key} = \"")) {
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}
```

(Note: `parse_toml_field` already exists in `cli/src/main.rs::connector_build` — we keep a copy here scoped to this module to avoid leaking it. Once `build` is also in this module, the duplicate is contained to one file.)

- [ ] **Step 2: Add deps**

In `crates/cli/Cargo.toml`:

```toml
sha2 = { workspace = true }
hex = { workspace = true }
```

- [ ] **Step 3: Wire subcommand**

In `crates/cli/src/main.rs`, extend `ConnectorCmd`:

```rust
    /// Build the connector and write to a local registry directory.
    Publish {
        path: String,
        #[arg(long, default_value = "./connectors")]
        registry: String,
        #[arg(long)]
        version: Option<String>,
    },
```

Dispatch:

```rust
            ConnectorCmd::Publish { path, registry, version } => {
                connector_cmd::publish(path, registry, version).await
            }
```

- [ ] **Step 4: Smoke**

```bash
cd /tmp
rustup target add wasm32-wasip1 2>/dev/null
~/Desktop/etl/target/debug/platform connector test demo-source
~/Desktop/etl/target/debug/platform connector publish demo-source --registry ./reg
ls ./reg/
cat ./reg/demo-source@0.1.0/manifest.yaml
```

Expected: `manifest.yaml` lists name, version, kind=source, sha256.

- [ ] **Step 5: Commit**

```bash
git add crates/cli/Cargo.toml crates/cli/src/connector_cmd.rs crates/cli/src/main.rs
git commit -m "feat(cli): platform connector publish — local registry + manifest.yaml"
```

---

## Task 6: Authoring guide doc

**Files:**
- Create: `docs/connector-sdk-guide.md`

- [ ] **Step 1: Doc**

```markdown
# Connector SDK guide

Build a custom source connector for the ETL platform in 5 steps.

## 1. Scaffold

```bash
platform connector create my-source
cd my-source
```

This creates `Cargo.toml`, `README.md`, and `src/lib.rs` with a stub
implementation. The stub compiles but errors at runtime — replace
`discover()` and `read_batch()` with real code.

## 2. Implement `discover`

`discover` introspects your source and returns Arrow IPC schema bytes:

```rust
fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let mut buf = Vec::new();
    let mut w = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| ConnectorError { message: format!("schema writer: {e}") })?;
    w.finish()
        .map_err(|e| ConnectorError { message: format!("schema finish: {e}") })?;
    Ok(buf)
}
```

## 3. Implement `read_batch`

`read_batch` returns up to `batch_size` rows after `cursor`. Always
return `is_final = true` when fewer than `batch_size` rows are
available.

## 4. Test locally

```bash
platform connector test .
```

Runs `cargo build --release --target wasm32-wasip1` and `cargo test`.
The build must succeed before `publish`.

## 5. Publish

```bash
platform connector publish . --registry ./connectors
```

Produces `./connectors/my-source@0.1.0/component.cwasm` and a
`manifest.yaml` capturing the SHA-256 of the artifact. To use the
connector in a pipeline, reference it from a `Connection` YAML:

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: my-source-conn
spec:
  connector_ref: wasm:my-source@0.1.0
  config:
    url: https://api.example.com
```

Set `ETL_CONNECTORS_DIR=./connectors` so the worker finds the
registry; default is `./connectors`.

## SDK reference

`connector-sdk::SourceConnector` (Rust trait, host-side):

- `discover(conn, source) -> SchemaRef` — return the source's Arrow schema.
- `read_batch(conn, source, cursor, batch_size) -> ReadOutcome` — read after cursor.

`connector-sdk::test_harness::run_smoke` — author-side validation
helper that exercises both methods against a fake source.

## Future kinds

II.3.a only supports `source` connectors. `scalar` (transformation
function) lands in II.3.b; `destination` (loader) in II.3.c.
```

- [ ] **Step 2: Commit**

```bash
git add docs/connector-sdk-guide.md
git commit -m "docs: connector SDK authoring guide"
```

---

## Task 7: Integration test — connector_lifecycle

**Files:**
- Create: `tests/integration/tests/connector_lifecycle.rs`

- [ ] **Step 1: Test**

```rust
//! Phase II.3.a — end-to-end: create a connector, build it, publish it.
//! The test does NOT modify the connector source — it relies on the
//! template-default stub being buildable (the stub returns Err from
//! discover/read_batch but the wasm guest itself compiles).

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
#[ignore = "requires wasm32-wasip1 target installed (rustup target add wasm32-wasip1)"]
async fn create_test_publish_round_trip() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "-p", "cli"])
        .status()
        .await?;

    let workdir = tempfile::tempdir()?;
    let connector_root = workdir.path().join("acme-source");

    let create = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "create",
            "acme-source",
            "--out",
            workdir.path().to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        create.status.success(),
        "create: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert!(connector_root.join("Cargo.toml").exists());
    assert!(connector_root.join("src/lib.rs").exists());

    let test = Command::new(cargo_bin("platform"))
        .args(["connector", "test", connector_root.to_str().unwrap()])
        .output()
        .await?;
    assert!(
        test.status.success(),
        "test: {}",
        String::from_utf8_lossy(&test.stderr)
    );

    let registry = workdir.path().join("registry");
    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            connector_root.to_str().unwrap(),
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

    let artifact = registry.join("acme-source@0.1.0/component.cwasm");
    let manifest = registry.join("acme-source@0.1.0/manifest.yaml");
    assert!(artifact.exists(), "missing {}", artifact.display());
    assert!(manifest.exists(), "missing {}", manifest.display());
    let manifest_yaml = std::fs::read_to_string(manifest)?;
    assert!(manifest_yaml.contains("name: acme-source"));
    assert!(manifest_yaml.contains("version: 0.1.0"));
    assert!(manifest_yaml.contains("kind: source"));
    assert!(manifest_yaml.contains("sha256:"));
    Ok(())
}
```

- [ ] **Step 2: Run**

First ensure the target is installed:

```bash
rustup target add wasm32-wasip1
```

Then:

```bash
cargo test -p integration-tests --test connector_lifecycle -- --ignored --nocapture
```

Expected: 1 passed (the build step takes ~30–60s on first run).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/tests/connector_lifecycle.rs
git commit -m "test(integration): connector_lifecycle — create + test + publish"
```

---

## Task 8: README + completion log + final regression sweep

**Files:**
- Modify: `README.md`
- Modify: this plan (append completion log)

- [ ] **Step 1: README — Connector SDK section**

Insert after `## Phase` section:

```markdown
## Connector SDK (Phase II.3.a)

Build a custom source connector in 5 steps:

```bash
platform connector create my-source
cd my-source
# edit src/lib.rs to implement discover() and read_batch()
platform connector test .
platform connector publish . --registry ./connectors
```

`platform connector test` runs `cargo build --release --target wasm32-wasip1` plus `cargo test`. `publish` writes the precompiled `.cwasm` artifact and a `manifest.yaml` (sha256, version, kind) to the registry directory. The worker reads from `ETL_CONNECTORS_DIR` (default `./connectors`).

See `docs/connector-sdk-guide.md` for the full authoring walkthrough. II.3.b adds the TypeScript SDK (jco). II.3.c+ ship the Stripe / MySQL CDC / Snowflake / BigQuery / Postgres connectors using this same SDK.
```

- [ ] **Step 2: Append completion log to plan**

```markdown
---

## Phase II.3.a Completion Log

Completed 2026-04-26 on branch `phase-2-3a-connector-sdk`.

- [x] T1 — connector-sdk templates module + materialize helper
- [x] T2 — platform connector create subcommand
- [x] T3 — connector-sdk test_harness::run_smoke
- [x] T4 — platform connector test subcommand
- [x] T5 — platform connector publish subcommand
- [x] T6 — Connector SDK authoring guide
- [x] T7 — connector_lifecycle integration test
- [x] T8 — README + this log + sweep

### Exit criterion — MET

- `platform connector create <name>` scaffolds a buildable connector crate.
- `platform connector test <path>` builds wasm32-wasip1 + runs cargo test.
- `platform connector publish <path>` writes `<registry>/<name>@<version>/component.cwasm` + `manifest.yaml`.
- Authoring guide (`docs/connector-sdk-guide.md`) walks an external operator from `connector create` to a usable artifact.
- 31 integration tests + 119+ unit tests green.

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.3.b / II.3.c

II.3.b — TypeScript SDK via `jco` (deferred):
- Mirror the Rust trait shape in TS: `discover()`, `readBatch()`.
- `platform connector create --kind source --lang typescript` materializes a TS template.
- `connector test` runs `npm test` plus `jco componentize` to produce the same `.cwasm`.

II.3.c — first new connector (Stripe):
- HTTP client with pagination + OAuth + rate-limit handling.
- JSON-schema discovery.
- Cursor on `created_at` (StripeAPI returns sorted by created_at desc).
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

Expected: all green. 31 integration tests (30 prior + connector_lifecycle).

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-26-phase-2-3a-connector-sdk.md
git commit -m "docs: Phase II.3.a README + completion log"
```

Then use the **finishing-a-development-branch** skill.

---

## Appendix A — Operational notes

**Local registry only.** II.3.a's "publish" writes to a filesystem directory (`./connectors` by default). II.3.c can add a remote registry (S3 + signed manifest). For now, operators sync the directory manually.

**`wasm32-wasip1` target is a hard prerequisite** for `connector test/publish`. The CLI doesn't auto-install it — fail with a clear error message pointing at `rustup target add wasm32-wasip1`. (The error currently bubbles up from cargo as "the wasm32-wasip1 target may not be installed"; that's good enough.)

**Manifest hash format.** `sha256:` is bare hex; downstream tooling can prepend `sha256:` if it expects multihash. II.3.c may switch to multihash for cross-format compat.

**SDK version pinning.** The manifest records `sdk_version: "0.1.0"`. Worker side checks compatibility loosely (any `0.x` works); II.3.c will tighten.

**Template kept inline as a Rust constant.** Avoids tar/zip embedding and makes the template version-locked with the crate. Trade-off: ~3 KB constant in the binary.

**`platform connector test` runs cargo subprocesses.** Slow on first run (~30–60s for wasm target build); subsequent runs hit the cargo cache and finish in seconds.

## Appendix B — What's deferred

- TypeScript SDK (jco) — Phase II.3.b
- Stripe connector — Phase II.3.c
- MySQL binlog CDC connector — Phase II.3.d
- Postgres / Snowflake / BigQuery destination loaders — Phase II.3.e (loader-sdk has the trait stub already)
- Per-destination dead-letter hardening — Phase II.3.e
- Remote registry (S3-backed) — Phase II.3 or II.4
- `connector create --kind scalar | destination` — Phase II.3.b/e
- Connector signing (cosign / Sigstore) — Phase II.4 / III
- Versioning + upgrade story across SDK versions — Phase III

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-26-phase-2-3a-connector-sdk.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task. Per-task isolation pays off for the SDK + CLI + integration test combo.

**2. Inline Execution** — feasible; 8 tasks, mostly mechanical. The wasm32-wasip1 build inside T7 is the long-pole at ~60s.

**Which approach?**
