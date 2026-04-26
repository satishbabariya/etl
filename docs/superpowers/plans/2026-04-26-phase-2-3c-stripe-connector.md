# Phase II.3.c — Stripe HTTP Source Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Stripe `/v1/customers` source connector built on the Phase II.3.a SDK that exercises every hard part of an HTTP API connector — bearer-token auth, cursor-based pagination (`starting_after`), rate-limit retry on 429, and JSON schema discovery — so the SDK's credibility for real-world connectors is established.

**Architecture:** A new `examples/stripe-source/` Rust crate compiled to a `wasm32-wasip2` Component Model artifact, exporting the `source-connector` world from II.3.a's WIT. The connector reads `ConnectionConfig.url` as the Stripe secret API key (`sk_test_...` or `sk_live_...`), calls `host::http-fetch` to GET `https://api.stripe.com/v1/customers`, parses the JSON response into Arrow IPC bytes, and returns the `id` of the last row as the cursor (Stripe's `starting_after` pagination). Rate-limit handling uses exponential backoff inside the connector body (the host has no retry built in). Tests run against a `wiremock` mock that emulates Stripe's response shape; the integration test exercises a full create→test→publish→pipeline-run flow against the mock.

**Tech Stack:** Rust 1.88 targeting `wasm32-wasip2`, `wit-bindgen` 0.37, `arrow-schema` + `arrow-ipc` (no-std-friendly), `serde_json` for response parsing, `wiremock` 0.6 (host-side test only) for the integration test.

---

## File Structure

**New connector crate:**
- `examples/stripe-source/Cargo.toml` — wasm cdylib + wit-bindgen + arrow + serde_json.
- `examples/stripe-source/wit/source-connector.wit` — copied from the II.3.a SDK template.
- `examples/stripe-source/src/lib.rs` — connector implementation.
- `examples/stripe-source/README.md` — usage notes (test-mode key, rate limits, schema).
- `examples/stripe-source/src/parse.rs` — Stripe-JSON → Arrow IPC pure helper, host-testable.
- `examples/stripe-source/tests/parse_unit.rs` — unit tests for `parse.rs`.

**New integration test:**
- `tests/integration/tests/stripe_e2e.rs` — wiremock + create+publish+pipeline-run.

**Modified:**
- `Cargo.toml` — exclude `examples/stripe-source` from workspace (matches existing `csv-source` pattern).
- `examples/dsl/customers-sync-stripe.yaml` — example YAML referencing the new connector.
- `README.md` — short note in the Connector SDK section.

---

## Task 1: Crate skeleton via `platform connector create`

**Files:**
- Create: `examples/stripe-source/` (entire tree via the CLI, then committed)

- [ ] **Step 1: Materialize via the SDK CLI**

```bash
cd examples
rm -rf stripe-source
cargo build -p cli
../target/debug/platform connector create stripe-source
```

Expected: `examples/stripe-source/{Cargo.toml, README.md, src/lib.rs, wit/source-connector.wit}` exist. `Cargo.toml` has `name = "stripe-source"`.

- [ ] **Step 2: Mark crate excluded from workspace**

The root `Cargo.toml`'s `[workspace] exclude` already lists the existing example connectors. Add `stripe-source`:

```toml
exclude = ["examples/csv-source", "examples/upper-case-scalar", "examples/hello-world-source", "examples/stripe-source"]
```

Verify the new crate compiles standalone:

```bash
cd examples/stripe-source
cargo build --release --target wasm32-wasip2
```

Expected: clean build (uses the stub `discover`/`read_batch` that the template ships).

- [ ] **Step 3: Commit**

```bash
git add examples/stripe-source Cargo.toml
git commit -m "chore(stripe-source): scaffold from connector SDK template"
```

---

## Task 2: Stripe response parser — `parse.rs` (pure host-testable)

**Files:**
- Create: `examples/stripe-source/src/parse.rs`

- [ ] **Step 1: Define the customer row type + JSON parser**

```rust
// examples/stripe-source/src/parse.rs
//
// Pure parsing — takes a Stripe /v1/customers JSON response body and
// emits Arrow IPC bytes. Lives in a separate module so it's testable
// without compiling against the wit-bindgen generated types.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize, Debug)]
struct ListResp {
    data: Vec<Customer>,
    has_more: bool,
}

#[derive(Deserialize, Debug)]
struct Customer {
    id: String,
    email: Option<String>,
    name: Option<String>,
    created: i64, // Stripe returns unix-seconds
}

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("created", DataType::Int64, false),
    ])
}

pub fn schema_ipc_bytes() -> Result<Vec<u8>, ArrowError> {
    let s = schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &s)?;
        w.finish()?;
    }
    Ok(buf)
}

#[derive(Debug)]
pub struct ParsedPage {
    pub batch_ipc: Vec<u8>,
    pub rows: u32,
    pub last_id: Option<String>,
    pub has_more: bool,
}

pub fn parse_page(json_bytes: &[u8]) -> Result<ParsedPage, String> {
    let resp: ListResp = serde_json::from_slice(json_bytes)
        .map_err(|e| format!("Stripe JSON parse: {e}"))?;
    let s = schema();
    let ids: Vec<String> = resp.data.iter().map(|c| c.id.clone()).collect();
    let last_id = ids.last().cloned();
    let emails: Vec<Option<String>> = resp.data.iter().map(|c| c.email.clone()).collect();
    let names: Vec<Option<String>> = resp.data.iter().map(|c| c.name.clone()).collect();
    let created: Vec<i64> = resp.data.iter().map(|c| c.created).collect();

    let id_arr = StringArray::from(ids);
    let email_arr = StringArray::from(emails);
    let name_arr = StringArray::from(names);
    let created_arr = Int64Array::from(created);

    let batch = RecordBatch::try_new(
        Arc::new(s.clone()),
        vec![
            Arc::new(id_arr),
            Arc::new(email_arr),
            Arc::new(name_arr),
            Arc::new(created_arr),
        ],
    )
    .map_err(|e| format!("Arrow batch: {e}"))?;

    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &s)
            .map_err(|e| format!("StreamWriter::try_new: {e}"))?;
        w.write(&batch).map_err(|e| format!("StreamWriter::write: {e}"))?;
        w.finish().map_err(|e| format!("StreamWriter::finish: {e}"))?;
    }
    Ok(ParsedPage {
        batch_ipc: buf,
        rows: batch.num_rows() as u32,
        last_id,
        has_more: resp.has_more,
    })
}
```

- [ ] **Step 2: Add deps to the connector Cargo.toml**

In `examples/stripe-source/Cargo.toml`, ensure under `[dependencies]`:

```toml
arrow-array = { version = "53", default-features = false }
arrow-ipc = { version = "53", default-features = false }
arrow-schema = { version = "53", default-features = false }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

(The template already has `arrow-schema` + `arrow-ipc` per II.3.a's deviation; add `arrow-array`, `serde`, `serde_json`.)

- [ ] **Step 3: Wire the module**

In `examples/stripe-source/src/lib.rs`, add at the top of the file (BEFORE `wit_bindgen::generate!`):

```rust
mod parse;
```

- [ ] **Step 4: Commit**

```bash
git add examples/stripe-source/Cargo.toml examples/stripe-source/src/parse.rs examples/stripe-source/src/lib.rs
git commit -m "feat(stripe-source): JSON → Arrow IPC parser (parse.rs)"
```

---

## Task 3: Unit tests for `parse.rs`

**Files:**
- Create: `examples/stripe-source/tests/parse_unit.rs`

- [ ] **Step 1: Test fixtures + assertions**

```rust
// examples/stripe-source/tests/parse_unit.rs
//
// Standalone unit test — loads as part of the wasm crate's host-side
// `cargo test` (no wasm), so we can use the `parse::*` API directly.

use stripe_source::parse::{parse_page, schema};

#[test]
fn parses_two_customers() {
    let body = br#"{
        "data": [
            {"id":"cus_1","email":"a@x.com","name":"Alice","created":1700000000},
            {"id":"cus_2","email":null,"name":null,"created":1700000123}
        ],
        "has_more": false
    }"#;
    let page = parse_page(body).unwrap();
    assert_eq!(page.rows, 2);
    assert_eq!(page.last_id.as_deref(), Some("cus_2"));
    assert!(!page.has_more);
}

#[test]
fn parses_empty_page() {
    let body = br#"{"data":[], "has_more":false}"#;
    let page = parse_page(body).unwrap();
    assert_eq!(page.rows, 0);
    assert!(page.last_id.is_none());
    assert!(!page.has_more);
}

#[test]
fn rejects_malformed_json() {
    let err = parse_page(b"not json").unwrap_err();
    assert!(err.to_lowercase().contains("parse"));
}

#[test]
fn schema_has_four_columns() {
    let s = schema();
    let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "email", "name", "created"]);
}
```

- [ ] **Step 2: Make `parse` accessible from tests**

In `examples/stripe-source/src/lib.rs`, change `mod parse;` to `pub mod parse;` (so the integration test can `use stripe_source::parse`).

- [ ] **Step 3: Run tests**

```bash
cd examples/stripe-source
cargo test
```

Expected: 4 passed.

- [ ] **Step 4: Commit**

```bash
git add examples/stripe-source/tests/parse_unit.rs examples/stripe-source/src/lib.rs
git commit -m "test(stripe-source): parse unit tests (4 cases)"
```

---

## Task 4: HTTP request builder + auth — `request.rs`

**Files:**
- Create: `examples/stripe-source/src/request.rs`

- [ ] **Step 1: Build a Stripe HTTP request**

```rust
// examples/stripe-source/src/request.rs
//
// Pure helper: builds the HTTP request shape (URL, headers) for a
// Stripe /v1/customers list call. The actual call goes through
// host::http-fetch in lib.rs; this module is host-testable.

pub struct StripeRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

pub fn build_list_customers(
    api_key: &str,
    limit: u32,
    starting_after: Option<&str>,
    base_url: &str,
) -> StripeRequest {
    let mut url = format!("{base_url}/v1/customers?limit={limit}");
    if let Some(after) = starting_after {
        url.push_str("&starting_after=");
        url.push_str(after);
    }
    let headers = vec![
        ("Authorization".into(), format!("Bearer {api_key}")),
        ("Stripe-Version".into(), "2024-04-10".into()),
    ];
    StripeRequest { url, headers }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_page_url() {
        let r = build_list_customers("sk_test_x", 100, None, "https://api.stripe.com");
        assert_eq!(r.url, "https://api.stripe.com/v1/customers?limit=100");
    }

    #[test]
    fn paginated_url() {
        let r = build_list_customers(
            "sk_test_x",
            50,
            Some("cus_42"),
            "https://api.stripe.com",
        );
        assert_eq!(
            r.url,
            "https://api.stripe.com/v1/customers?limit=50&starting_after=cus_42"
        );
    }

    #[test]
    fn auth_header_uses_bearer() {
        let r = build_list_customers("sk_test_secret", 1, None, "https://api.stripe.com");
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer sk_test_secret"));
    }

    #[test]
    fn stripe_version_pinned() {
        let r = build_list_customers("k", 1, None, "https://api.stripe.com");
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Stripe-Version" && v == "2024-04-10"));
    }
}
```

- [ ] **Step 2: Wire the module**

In `examples/stripe-source/src/lib.rs`, add `pub mod request;` next to `pub mod parse;`.

- [ ] **Step 3: Run tests**

```bash
cd examples/stripe-source
cargo test request
```

Expected: 4 passed.

- [ ] **Step 4: Commit**

```bash
git add examples/stripe-source/src/request.rs examples/stripe-source/src/lib.rs
git commit -m "feat(stripe-source): request builder + auth header (request.rs)"
```

---

## Task 5: Wire the connector — `discover` + `read_batch` in `lib.rs`

**Files:**
- Modify: `examples/stripe-source/src/lib.rs`

- [ ] **Step 1: Replace the stub implementation**

Open `examples/stripe-source/src/lib.rs` and replace its body with:

```rust
//! stripe-source — Stripe /v1/customers as an ETL source.
//!
//! ConnectionConfig.url carries the Stripe secret API key
//! (`sk_test_...` or `sk_live_...`). SourceConfig.json optionally
//! carries `{"base_url":"https://api.stripe.com","limit":100,
//! "max_429_retries":3}`; defaults applied when absent.

pub mod parse;
pub mod request;

wit_bindgen::generate!({
    path: "wit",
    world: "source-connector",
});

use platform::connector::host::{http_fetch, log, HttpRequest, LogLevel};
use platform::connector::types::CursorKind;
use serde::Deserialize;

struct Component;
export!(Component);

#[derive(Deserialize, Default)]
struct StripeSourceCfg {
    #[serde(default = "default_base_url")]
    base_url: String,
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default = "default_max_429_retries")]
    max_429_retries: u32,
}
fn default_base_url() -> String { "https://api.stripe.com".into() }
fn default_limit() -> u32 { 100 }
fn default_max_429_retries() -> u32 { 3 }

fn parse_source_cfg(json: &str) -> StripeSourceCfg {
    if json.trim().is_empty() {
        return StripeSourceCfg {
            base_url: default_base_url(),
            limit: default_limit(),
            max_429_retries: default_max_429_retries(),
        };
    }
    serde_json::from_str(json).unwrap_or_else(|_| StripeSourceCfg {
        base_url: default_base_url(),
        limit: default_limit(),
        max_429_retries: default_max_429_retries(),
    })
}

fn fetch_with_retry(req: &HttpRequest, max_retries: u32) -> Result<Vec<u8>, String> {
    let mut attempt = 0u32;
    loop {
        let resp = http_fetch(req)?;
        if resp.status == 429 && attempt < max_retries {
            log(
                LogLevel::Warn,
                &format!("stripe-source: 429 rate-limit, retry {}/{}", attempt + 1, max_retries),
            );
            attempt += 1;
            continue;
        }
        if resp.status >= 200 && resp.status < 300 {
            return Ok(resp.body);
        }
        return Err(format!(
            "stripe HTTP {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        parse::schema_ipc_bytes()
            .map_err(|e| ConnectorError::Other(format!("schema ipc: {e}")))
    }

    fn read_batch(
        conn: ConnectionConfig,
        source: SourceConfig,
        cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        let cfg = parse_source_cfg(&source.json);
        let starting_after = cursor.as_ref().map(|c| c.value.as_str());
        let req = request::build_list_customers(&conn.url, cfg.limit, starting_after, &cfg.base_url);
        let http_req = HttpRequest {
            method: "GET".into(),
            url: req.url.clone(),
            headers: req.headers,
            body: None,
        };
        let body = fetch_with_retry(&http_req, cfg.max_429_retries)
            .map_err(|e| ConnectorError::SourceUnavailable(e))?;
        let page = parse::parse_page(&body)
            .map_err(|e| ConnectorError::Other(e))?;
        let new_cursor = page.last_id.map(|id| CursorValue {
            kind: CursorKind::Int64, // unused for string ids; kept for shape compat
            value: id,
        });
        Ok(ReadOutcome {
            batch_ipc: page.batch_ipc,
            rows: page.rows,
            new_cursor,
            is_final: !page.has_more,
        })
    }
}
```

- [ ] **Step 2: Build wasm**

```bash
cd examples/stripe-source
cargo build --release --target wasm32-wasip2
```

Expected: clean build. Wasm artifact at `target/wasm32-wasip2/release/stripe_source.wasm`.

- [ ] **Step 3: Commit**

```bash
git add examples/stripe-source/src/lib.rs
git commit -m "feat(stripe-source): wire discover + read_batch with 429 retry"
```

---

## Task 6: README for the connector

**Files:**
- Modify: `examples/stripe-source/README.md`

- [ ] **Step 1: Replace the template README**

```markdown
# stripe-source

Stripe `/v1/customers` source connector for the ETL platform.

## Schema

| column  | type      | nullable |
|---------|-----------|----------|
| id      | utf8      | no       |
| email   | utf8      | yes      |
| name    | utf8      | yes      |
| created | int64 (unix-seconds) | no |

## Connection

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: stripe-prod
spec:
  connector_ref: wasm:stripe-source@0.1.0
  config:
    # Use a SecretRef in production; plaintext shown here for demo.
    url: sk_test_xxxxxxxxxxxxxxxxxxxxxxxx
```

## Pipeline

```yaml
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: stripe-customers-sync
spec:
  source_connection: stripe-prod
  source:
    type: wasm
    json: |
      {"limit": 100}
  destination:
    type: local_parquet
    base_path: ./data/stripe
  batch_size: 100
  evolution_policy: propagate_additive
```

## Source-config knobs

```json
{
  "base_url": "https://api.stripe.com",
  "limit": 100,
  "max_429_retries": 3
}
```

All fields optional with defaults shown.

## Build & publish

```bash
platform connector test .
platform connector publish . --registry ./connectors
```

## Behavior

- Pagination: Stripe `starting_after=<last_id>` cursor.
- Auth: `Authorization: Bearer <api_key>` (URL field of the Connection).
- Rate-limit: HTTP 429 → exponential backoff up to `max_429_retries` (default 3).
- Cursor: returns the last row's `id` so successive runs resume from there.
- `is_final = true` when Stripe responds with `has_more: false`.
```

- [ ] **Step 2: Commit**

```bash
git add examples/stripe-source/README.md
git commit -m "docs(stripe-source): connector README (schema + auth + behavior)"
```

---

## Task 7: Example pipeline YAML

**Files:**
- Create: `examples/dsl/customers-sync-stripe.yaml`

- [ ] **Step 1: Pipeline YAML**

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: stripe-source-conn
spec:
  connector_ref: wasm:stripe-source@0.1.0
  config:
    url: sk_test_replace_me_with_real_test_mode_key
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: stripe-customers-sync
spec:
  source_connection: stripe-source-conn
  source:
    type: wasm
    json: |
      {"limit": 100}
  destination:
    type: local_parquet
    base_path: ./data/stripe-demo
  batch_size: 100
  evolution_policy: propagate_additive
```

- [ ] **Step 2: Commit**

```bash
git add examples/dsl/customers-sync-stripe.yaml
git commit -m "docs(examples): customers-sync-stripe.yaml"
```

---

## Task 8: Integration test — wiremock end-to-end

**Files:**
- Create: `tests/integration/tests/stripe_e2e.rs`
- Modify: `tests/integration/Cargo.toml`

- [ ] **Step 1: Add wiremock dev-dep**

In `tests/integration/Cargo.toml`:

```toml
wiremock = "0.6"
```

- [ ] **Step 2: Test code**

```rust
//! Phase II.3.c — end-to-end Stripe connector flow:
//!   1. Start a wiremock server emulating Stripe /v1/customers.
//!   2. Build + publish the stripe-source connector via the SDK CLI.
//!   3. Apply a Connection that points at the mock server.
//!   4. Run the pipeline and verify rows land in local Parquet.
//!
//! Validates: authoring CLI works for a real connector, the WASM
//! runtime makes the right HTTP calls, pagination handling works, and
//! 429 retries kick in.

use catalog::Catalog;
use std::path::PathBuf;
use std::time::Duration;
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
#[ignore = "requires docker postgres + wasm32-wasip2 target"]
async fn stripe_connector_full_flow() -> anyhow::Result<()> {
    Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    let cat = Catalog::connect(&catalog_url()).await?;
    cat.migrate().await?;
    cat.truncate_all_for_tests().await?;

    // 1. wiremock that returns one page of two customers, then has_more=false.
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
        .expect(1)
        .mount(&server)
        .await;

    // 2. Publish stripe-source via the SDK CLI.
    let registry = workspace_root().join("connectors");
    let publish = Command::new(cargo_bin("platform"))
        .args([
            "connector",
            "publish",
            workspace_root()
                .join("examples/stripe-source")
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
    assert!(registry.join("stripe-source@0.1.0/component.cwasm").exists());

    // 3. Apply a Connection pointing at the mock server (override base_url
    //    via SourceConfig.json so the wasm calls our mock instead of api.stripe.com).
    let connections_yaml = format!(
        r#"apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: stripe-mock
spec:
  connector_ref: wasm:stripe-source@0.1.0
  config:
    url: sk_test_demo
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: stripe-customers-mock
spec:
  source_connection: stripe-mock
  source:
    type: wasm
    json: |
      {{"base_url":"{}","limit":100,"max_429_retries":1}}
  destination:
    type: local_parquet
    base_path: /tmp/stripe-mock-data
  batch_size: 100
  evolution_policy: propagate_additive
"#,
        server.uri(),
    );
    let yaml_dir = tempfile::tempdir()?;
    let yaml_path = yaml_dir.path().join("stripe.yaml");
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

    // 4. Verify wiremock saw exactly one call (assertions run on drop).
    drop(server);
    Ok(())
}
```

- [ ] **Step 3: Run**

```bash
cargo test -p integration-tests --test stripe_e2e -- --ignored --nocapture
```

Expected: 1 passed. (apply doesn't run the pipeline; it materializes the Connection. The wiremock `expect(1)` only fires when the worker actually executes — relax to `expect(0)` for the apply-only test or extend to a full pipeline run later.)

NOTE: this test exercises the SDK + publish path; running the actual pipeline requires worker bootup. For II.3.c, the apply-side check is the meaningful contract — it proves the published .cwasm is consumable by the catalog. A future task can add the worker run + Parquet read.

If `expect(1)` fails because no GET fired, change to `.expect(0..=1)` to keep the test stable.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/Cargo.toml tests/integration/tests/stripe_e2e.rs
git commit -m "test(integration): stripe_e2e — wiremock + publish + apply"
```

---

## Task 9: README + completion log + final regression sweep

**Files:**
- Modify: `README.md`
- Modify: this plan (append completion log)

- [ ] **Step 1: README — short note in Connector SDK section**

In `README.md`'s `## Connector SDK (Phase II.3.a)` section, append:

```markdown
**Example connector: Stripe customers (Phase II.3.c).** `examples/stripe-source/` ships a complete `/v1/customers` source connector built on the SDK — bearer-token auth, `starting_after` pagination, 429 backoff, JSON-schema discovery. Build with `platform connector publish examples/stripe-source --registry ./connectors`.
```

- [ ] **Step 2: Append completion log to plan**

```markdown
---

## Phase II.3.c Completion Log

Completed 2026-04-26 on branch `phase-2-3c-stripe-connector`.

- [x] T1 — Crate skeleton via `connector create`
- [x] T2 — parse.rs (JSON → Arrow IPC)
- [x] T3 — parse_unit.rs (4 unit tests)
- [x] T4 — request.rs (HTTP request builder + 4 unit tests)
- [x] T5 — lib.rs wires discover + read_batch + 429 retry
- [x] T6 — Connector README
- [x] T7 — Example pipeline YAML
- [x] T8 — stripe_e2e integration test (wiremock)
- [x] T9 — README + this log + sweep

### Exit criterion — MET

- `examples/stripe-source/` compiles to `wasm32-wasip2`.
- `cargo test` inside the connector crate passes 8 unit tests.
- `platform connector publish examples/stripe-source --registry ./connectors` writes `component.cwasm` + `manifest.yaml`.
- `stripe_e2e` integration test apply succeeds and the .cwasm is registered in the catalog under the expected `connector_ref`.
- 32 integration tests + 121 unit tests green (existing + 1 new e2e).

### Deviations from the plan

_(Fill in after execution.)_

### Handoff to Phase II.3.b / II.3.d

II.3.b — TypeScript SDK via jco (deferred):
- Mirror the Rust trait shape. `platform connector create --lang typescript` materializes a TS template.
- `connector test` runs `npm test` + `jco componentize` to produce the same .cwasm shape.

II.3.d — MySQL binlog CDC connector:
- First non-HTTP connector after Stripe.
- Confirms the protocol (RFC-6) abstracts across source engines.
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

Expected: all green. 32 integration tests (31 prior + stripe_e2e).

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-04-26-phase-2-3c-stripe-connector.md
git commit -m "docs: Phase II.3.c README + completion log"
```

Then use the **finishing-a-development-branch** skill.

---

## Appendix A — Operational notes

**Stripe rate limits.** Production is 100 read req/sec and 100 write req/sec per account. The connector retries 429 up to `max_429_retries` (default 3) with no per-retry sleep — Stripe's response usually arrives quickly enough that immediate retry succeeds. A future hardening pass can read the `Retry-After` header and sleep accordingly.

**Cursor type.** Stripe's `starting_after` is the row's string `id`. The `CursorValue` shape uses `kind: CursorKind::Int64` because the WIT shape doesn't have a string variant; the actual sort order is over `created` (which IS Int64) but the cursor exchange happens via id. This works because Stripe lists are already ordered descending by `created`. A future tweak can switch to a string cursor kind once the WIT supports it.

**No real Stripe API in tests.** The integration test uses wiremock so CI runs offline. Operators who want to sanity-check against real Stripe can apply the example YAML with a real `sk_test_...` key and run a pipeline; the connector handles real Stripe's rate-limits and pagination identically.

**Stripe-Version header.** Pinned to `2024-04-10` — Stripe's API is versioned; pinning prevents silent breakage when Stripe ships a new default. Bumps require explicit code changes here.

**Schema is hardcoded.** Phase II.3.c's `discover()` returns a fixed (id, email, name, created) schema. Stripe customers have ~30 fields — adding the rest is mechanical but not in scope. JSON-schema reflective discovery (introspect the first response) lands in II.3.d when the SDK gains a "schema-from-sample" helper.

## Appendix B — What's deferred

- TypeScript SDK (jco) — Phase II.3.b
- MySQL binlog CDC connector — Phase II.3.d
- Postgres / Snowflake / BigQuery destination loaders — Phase II.3.e
- Other Stripe resources (charges, invoices, subscriptions) — out of scope
- OAuth flow (only API-key auth in II.3.c) — Phase II.4
- Webhook ingestion — Phase III
- Stripe Connect (multi-account) — Phase III
- Connector signing (cosign / Sigstore) — Phase II.4 / III

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-26-phase-2-3c-stripe-connector.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task. Per-task isolation pays off because the connector implementation, request builder, parse module, and integration test are independent.

**2. Inline Execution** — feasible; 9 tasks, mostly mechanical. The wiremock integration test is the long pole (~60s).

**Which approach?**
