# Phase I.5 — Transformation Layer + Dead-Letter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Insert a declarative, statically-schema-derivable transformation DAG between `read_batch` and `load_batch` — six MVP operators (select, filter, mask, add_column, validate, wasm_scalar) — with dead-letter routing for validation failures and a new scalar-UDF WASM surface that reuses the Phase I.3 runtime under a tighter capability set.

**Architecture:** Transforms are stateless, in-memory, Arrow-native. `SyncActivities::read_batch` now calls `transform::apply(batch, &operators)` which chains operators linearly, returning a `TransformOutcome { kept: RecordBatch, rejected: Option<RecordBatch> }`. The kept batch proceeds to `load_batch`; rejected rows (from `validate`) are written to `<base_path>/<pipeline_id>/dead-letter/<run_id>/batch-<seq>.parquet`. Each operator has a pure `derive_schema(input_schema, config) -> output_schema` method, composed by `transform::derive_schema` — this schema is what `schema_evolution::record_and_resolve` stores in the `schemas` table (tracks what lands at destination, not what the source emitted). WASM scalar UDFs use a new WIT world (`platform:udf/scalar-udf`) with only `log` imported — no http-fetch, no wall-clock, no randomness — running inside the same wasmtime Engine/ticker set up in Phase I.3. Pipelines without a `transform` field behave identically to Phase I.4 (backward-compat via `#[serde(default)] transform: Option<TransformSpec>`).

**Tech Stack:** Unchanged — Rust 1.88, arrow 53 (adds `arrow::compute::filter`, `cast`), wasmtime 26, temporalio-sdk 0.2, blake3 (already present for schema fingerprinting, reused for `mask` hash strategy), serde/sqlx/tracing.

---

## File Structure

### Modified
- `crates/common-types/src/pipeline_spec.rs` — add `PipelineSpec.transform: Option<TransformSpec>`
- `crates/common-types/src/lib.rs` — expose `transform` module
- `crates/worker/src/lib.rs` — expose `transform` module
- `crates/worker/src/activities/sync/mod.rs` — `read_batch` applies transform before encoding; dead-letter written inline
- `crates/worker/src/activities/sync/inputs.rs` — `ReadBatchOutput` gains `rejected_b64: Option<String>` + `rows_rejected: usize`
- `crates/worker/src/workflows/pipeline_run.rs` — dead-letter path passed through so `load_batch` can route rejected rows
- `crates/worker/src/loaders/parquet_local.rs` — no behavioral change, but used by a new "dead-letter" destination variant constructed inside `load_batch`
- `crates/worker/src/wasm_runtime/mod.rs` — expose `scalar_bindings` + `WasmScalarRuntime`
- `crates/cli/src/main.rs` — `connector build` gains `--kind source|scalar` (default: `source`)
- `README.md` — Phase I.5 section

### New
- `crates/common-types/src/transform.rs` — `TransformSpec`, `Operator`, `MaskStrategy`, `ValidationRule`, `LiteralValue`
- `crates/connector-sdk/wit/scalar-udf.wit` — separate world from `source-connector`, tighter imports
- `crates/worker/src/transform/mod.rs` — entry point: `apply(batch, &operators)` + `derive_schema(input_schema, &operators)`
- `crates/worker/src/transform/predicate.rs` — hand-written subset-SQL parser + evaluator
- `crates/worker/src/transform/operators/mod.rs` — trait/dispatch
- `crates/worker/src/transform/operators/select.rs`
- `crates/worker/src/transform/operators/filter.rs`
- `crates/worker/src/transform/operators/mask.rs`
- `crates/worker/src/transform/operators/add_column.rs`
- `crates/worker/src/transform/operators/validate.rs`
- `crates/worker/src/transform/operators/wasm_scalar.rs`
- `crates/worker/src/wasm_runtime/scalar_bindings.rs` — `bindgen!` for the scalar-udf world
- `crates/worker/src/wasm_runtime/scalar_runtime.rs` — `WasmScalarRuntime`
- `examples/upper-case-scalar/Cargo.toml` + `src/lib.rs` + `.cargo/config.toml` + `README.md` — reference scalar UDF
- `examples/dsl/customers-with-transform.yaml` — demo YAML
- `tests/integration/tests/transforms_filter_mask.rs`
- `tests/integration/tests/transforms_dead_letter.rs`
- `docs/superpowers/plans/2026-04-23-phase-1-5-transformations.md` (this file)

### Deliberately deferred (do NOT add in this plan)
- Operators: `project`, `cast`, `rename`, full-feature `validate` (with rules beyond `not_null`), `dedupe`, `flatten`, `enrich_with_reference_data` — Phase I.5+
- Batch-crossing state (dedupe across batches, continuous aggregations) — Phase I.6+
- Full-SQL predicate language for `filter` — Phase IV (query engine)
- Aggregate operators — Phase IV
- Multi-input / fan-in transforms — post-launch
- Column-level evolution overrides (per-field ignore/freeze) — Phase I.6+
- Batch-typed WASM UDFs (columns beyond Utf8) — Phase I.5+
- Postgres-in-WASM via host postgres-query — still deferred

---

## Key Type Contracts

All types derive `Clone, Debug, Serialize, Deserialize` unless noted.

```rust
// common-types/src/transform.rs
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TransformSpec {
    pub operators: Vec<Operator>,
    #[serde(default = "default_dead_letter_threshold")]
    pub dead_letter_threshold: f64, // fraction rejected / total; run fails above this
}
fn default_dead_letter_threshold() -> f64 { 0.01 }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operator {
    /// Subset columns: output keeps only `columns`, in the listed order.
    Select { columns: Vec<String> },
    /// Row filter: keep rows matching `predicate`.
    Filter { predicate: String },
    /// Replace a column's values using `strategy`.
    Mask { column: String, strategy: MaskStrategy },
    /// Append a new column with constant `value`.
    AddColumn { name: String, value: LiteralValue },
    /// Row-level validation. Rows failing any rule are routed to dead-letter.
    Validate { rules: Vec<ValidationRule> },
    /// Invoke a WASM scalar UDF on a single string column; result stored in output_column.
    WasmScalar {
        udf: String,             // "<name>@<version>"
        input_column: String,
        output_column: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaskStrategy {
    /// Replace with BLAKE3-hex (or configured length prefix).
    Hash,
    /// Replace with NULL (column must be nullable).
    Null,
    /// Replace with a fixed redaction string, default "[REDACTED]".
    Redact { replacement: Option<String> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum ValidationRule {
    NotNull { column: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}
```

```rust
// worker/src/transform/mod.rs
pub struct TransformOutcome {
    pub kept: arrow::record_batch::RecordBatch,
    pub rejected: Option<arrow::record_batch::RecordBatch>, // original-schema batch of failed rows
    pub per_operator: Vec<OperatorMetrics>,
}

pub struct OperatorMetrics {
    pub op_index: usize,
    pub op_kind: &'static str,   // "select", "filter", ...
    pub rows_in: usize,
    pub rows_out: usize,
    pub rows_rejected: usize,
}

pub async fn apply(
    batch: RecordBatch,
    operators: &[Operator],
    scalar_runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<TransformOutcome>;

pub fn derive_schema(
    input_schema: &Schema,
    operators: &[Operator],
) -> anyhow::Result<Schema>;
```

```rust
// worker/src/transform/predicate.rs
pub enum Predicate {
    IsNull(String),
    IsNotNull(String),
    Eq(String, Literal),
    In(String, Vec<Literal>),
}
pub enum Literal { Int(i64), Float(f64), String(String), Bool(bool), Null }

pub fn parse(s: &str) -> anyhow::Result<Predicate>;
pub fn evaluate(predicate: &Predicate, batch: &RecordBatch) -> anyhow::Result<BooleanArray>;
```

```wit
// crates/connector-sdk/wit/scalar-udf.wit
package platform:udf@0.1.0;

interface host {
    enum log-level { trace, debug, info, warn, error }
    log: func(level: log-level, message: string);
}

world scalar-udf {
    import host;
    /// Apply the UDF to each string in `input`, returning the same-length
    /// list of transformed strings. Implementations MUST be deterministic.
    export apply-scalar: func(input: list<string>) -> result<list<string>, string>;
}
```

```rust
// worker/src/wasm_runtime/scalar_runtime.rs
pub struct WasmScalarRuntime {
    engine: Arc<wasmtime::Engine>,
    linker: Linker<HostState>,
    cache: DashMap<String, Arc<Component>>,
    base_dir: PathBuf,
    ticker: Arc<EpochTicker>,
}

impl WasmScalarRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>>;
    pub async fn apply(&self, name_at_version: &str, input: Vec<String>) -> anyhow::Result<Vec<String>>;
}
```

---

## Task 1: `common-types` — `TransformSpec`, `Operator`, `PipelineSpec.transform`

**Files:**
- Create: `crates/common-types/src/transform.rs`
- Modify: `crates/common-types/src/lib.rs`
- Modify: `crates/common-types/src/pipeline_spec.rs`

- [ ] **Step 1: Write transform.rs**

Create `crates/common-types/src/transform.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransformSpec {
    #[serde(default)]
    pub operators: Vec<Operator>,
    #[serde(default = "default_dead_letter_threshold")]
    pub dead_letter_threshold: f64,
}

impl Default for TransformSpec {
    fn default() -> Self {
        Self {
            operators: Vec::new(),
            dead_letter_threshold: default_dead_letter_threshold(),
        }
    }
}

fn default_dead_letter_threshold() -> f64 {
    0.01
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operator {
    Select { columns: Vec<String> },
    Filter { predicate: String },
    Mask { column: String, strategy: MaskStrategy },
    AddColumn { name: String, value: LiteralValue },
    Validate { rules: Vec<ValidationRule> },
    WasmScalar {
        udf: String,
        input_column: String,
        output_column: String,
    },
}

impl Operator {
    pub fn kind(&self) -> &'static str {
        match self {
            Operator::Select { .. } => "select",
            Operator::Filter { .. } => "filter",
            Operator::Mask { .. } => "mask",
            Operator::AddColumn { .. } => "add_column",
            Operator::Validate { .. } => "validate",
            Operator::WasmScalar { .. } => "wasm_scalar",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaskStrategy {
    Hash,
    Null,
    Redact {
        #[serde(default)]
        replacement: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum ValidationRule {
    NotNull { column: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum LiteralValue {
    // Order matters for #[serde(untagged)]: bool first so "true"/"false" don't hit Int.
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_roundtrips_filter() {
        let op = Operator::Filter { predicate: "email IS NOT NULL".into() };
        let j = serde_json::to_string(&op).unwrap();
        assert!(j.contains("\"type\":\"filter\""));
        let back: Operator = serde_json::from_str(&j).unwrap();
        assert!(matches!(back, Operator::Filter { .. }));
    }

    #[test]
    fn mask_strategy_tagged() {
        let m = MaskStrategy::Hash;
        let j = serde_json::to_string(&m).unwrap();
        assert_eq!(j, r#"{"kind":"hash"}"#);
    }

    #[test]
    fn transform_spec_default_threshold() {
        let j = r#"{"operators":[]}"#;
        let t: TransformSpec = serde_json::from_str(j).unwrap();
        assert!((t.dead_letter_threshold - 0.01).abs() < 1e-9);
    }
}
```

- [ ] **Step 2: Wire into `lib.rs`**

Edit `crates/common-types/src/lib.rs`:

```rust
//! Shared newtype identifiers and primitive types for the platform.
pub mod connection_config;
pub mod cursor;
pub mod dsl;
pub mod evolution;
pub mod ids;
pub mod pipeline_spec;
pub mod schema_fingerprint;
pub mod transform;
```

- [ ] **Step 3: Add `transform` to PipelineSpec**

Edit `crates/common-types/src/pipeline_spec.rs`. Update `PipelineSpec`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    pub batch_size: usize,
    #[serde(default)]
    pub transform: Option<crate::transform::TransformSpec>,
}
```

Note: `evolution_policy` doesn't currently live on `PipelineSpec` (it's read from the `pipelines.spec` JSONB directly in the CLI). Keep that pattern for `transform` too — add it as a field here only if current PipelineSpec struct contains evolution_policy; otherwise, it stays as a JSONB-level field only. Check the Phase I.4 PipelineDslSpec (DSL has evolution_policy, catalog PipelineSpec may not).

- [ ] **Step 4: Add `transform` to `PipelineDslSpec`**

Edit `crates/common-types/src/dsl.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineDslSpec {
    pub source_connection: String,
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    pub batch_size: usize,
    #[serde(default)]
    pub evolution_policy: EvolutionPolicy,
    #[serde(default)]
    pub transform: Option<crate::transform::TransformSpec>,
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p common-types`
Expected: all previous tests plus 3 new (operator_roundtrips_filter, mask_strategy_tagged, transform_spec_default_threshold) → **18 passed**.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(common-types): TransformSpec + Operator enum + PipelineSpec.transform

Six MVP operators (select, filter, mask, add_column, validate,
wasm_scalar) as tagged enum variants. MaskStrategy (hash/null/redact)
and ValidationRule (not_null) also tagged. LiteralValue is untagged
(Bool first to avoid Int ambiguity). Default dead_letter_threshold 0.01.
transform field on PipelineSpec + PipelineDslSpec is Option to stay
backward-compat with Phase I.4 pipelines.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `worker::transform` scaffolding + operator trait

**Files:**
- Create: `crates/worker/src/transform/mod.rs`
- Create: `crates/worker/src/transform/operators/mod.rs`
- Modify: `crates/worker/src/lib.rs`

- [ ] **Step 1: Wire module declaration**

Edit `crates/worker/src/lib.rs`. Append:

```rust
pub mod transform;
```

- [ ] **Step 2: Transform entry point**

Create `crates/worker/src/transform/mod.rs`:

```rust
//! Transformation DAG (RFC-12). Operators run in-memory between read_batch
//! and load_batch. Transforms are stateless and batch-local — no cross-batch
//! state lives here (that's Phase I.6 CDC + Phase IV aggregates).

pub mod operators;
pub mod predicate;

use anyhow::Context;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common_types::transform::Operator;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

#[derive(Debug)]
pub struct TransformOutcome {
    pub kept: RecordBatch,
    pub rejected: Option<RecordBatch>,
    pub per_operator: Vec<OperatorMetrics>,
}

#[derive(Debug, Clone)]
pub struct OperatorMetrics {
    pub op_index: usize,
    pub op_kind: &'static str,
    pub rows_in: usize,
    pub rows_out: usize,
    pub rows_rejected: usize,
}

/// Apply operators in order. Rejected rows (only produced by `validate`) are
/// collected under the ORIGINAL (pre-transform) schema — which is the schema
/// at the point just before the validate that rejected them. For Phase I.5,
/// validate is required to be the LAST operator if present (enforced at
/// apply time); this keeps the dead-letter batch's schema well-defined.
pub async fn apply(
    batch: RecordBatch,
    operators: &[Operator],
    scalar_runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<TransformOutcome> {
    let mut current = batch;
    let mut rejected: Option<RecordBatch> = None;
    let mut per_operator = Vec::with_capacity(operators.len());

    for (idx, op) in operators.iter().enumerate() {
        let rows_in = current.num_rows();
        let (next, rej) = operators::apply_one(op, current, scalar_runtime).await
            .with_context(|| format!("operator {idx} ({})", op.kind()))?;
        let rows_rejected = rej.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        per_operator.push(OperatorMetrics {
            op_index: idx,
            op_kind: op.kind(),
            rows_in,
            rows_out: next.num_rows(),
            rows_rejected,
        });
        if let Some(r) = rej {
            if rejected.is_some() {
                anyhow::bail!(
                    "Phase I.5: only one operator may emit rejected rows (validate), and it must be the last — got rejections from op {idx}"
                );
            }
            rejected = Some(r);
        }
        current = next;
    }

    Ok(TransformOutcome {
        kept: current,
        rejected,
        per_operator,
    })
}

/// Pure-function schema derivation. Each operator's `derive_schema` is
/// testable in isolation; this composer just chains them.
pub fn derive_schema(input_schema: &Schema, operators: &[Operator]) -> anyhow::Result<Schema> {
    let mut current = input_schema.clone();
    for (idx, op) in operators.iter().enumerate() {
        current = operators::derive_one(op, &current)
            .with_context(|| format!("derive_schema: operator {idx} ({})", op.kind()))?;
    }
    Ok(current)
}
```

- [ ] **Step 3: Operator dispatch module**

Create `crates/worker/src/transform/operators/mod.rs`:

```rust
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common_types::transform::Operator;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

pub mod add_column;
pub mod filter;
pub mod mask;
pub mod select;
pub mod validate;
pub mod wasm_scalar;

/// Apply one operator. Returns (kept, rejected-or-none).
pub async fn apply_one(
    op: &Operator,
    batch: RecordBatch,
    scalar_runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<(RecordBatch, Option<RecordBatch>)> {
    match op {
        Operator::Select { columns } => Ok((select::apply(batch, columns)?, None)),
        Operator::Filter { predicate } => Ok((filter::apply(batch, predicate)?, None)),
        Operator::Mask { column, strategy } => Ok((mask::apply(batch, column, strategy)?, None)),
        Operator::AddColumn { name, value } => {
            Ok((add_column::apply(batch, name, value)?, None))
        }
        Operator::Validate { rules } => {
            let (kept, rejected) = validate::apply(batch, rules)?;
            Ok((kept, rejected))
        }
        Operator::WasmScalar { udf, input_column, output_column } => {
            Ok((wasm_scalar::apply(batch, udf, input_column, output_column, scalar_runtime).await?, None))
        }
    }
}

pub fn derive_one(op: &Operator, input: &Schema) -> anyhow::Result<Schema> {
    match op {
        Operator::Select { columns } => select::derive_schema(input, columns),
        Operator::Filter { .. } => Ok(input.clone()),
        Operator::Mask { column, strategy } => mask::derive_schema(input, column, strategy),
        Operator::AddColumn { name, value } => add_column::derive_schema(input, name, value),
        Operator::Validate { .. } => Ok(input.clone()),
        Operator::WasmScalar { input_column, output_column, .. } => {
            wasm_scalar::derive_schema(input, input_column, output_column)
        }
    }
}
```

- [ ] **Step 4: Stub operators so the module compiles**

Create each of the six operator files with stub `apply`/`derive_schema` that `unimplemented!()`. Subsequent tasks fill them in.

`crates/worker/src/transform/operators/select.rs`:
```rust
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

pub fn apply(_batch: RecordBatch, _columns: &[String]) -> anyhow::Result<RecordBatch> {
    anyhow::bail!("select::apply not implemented yet (Task 4)")
}
pub fn derive_schema(_input: &Schema, _columns: &[String]) -> anyhow::Result<Schema> {
    anyhow::bail!("select::derive_schema not implemented yet (Task 4)")
}
```

`crates/worker/src/transform/operators/filter.rs`:
```rust
use arrow::record_batch::RecordBatch;

pub fn apply(_batch: RecordBatch, _predicate: &str) -> anyhow::Result<RecordBatch> {
    anyhow::bail!("filter::apply not implemented yet (Task 5)")
}
```

`crates/worker/src/transform/operators/mask.rs`:
```rust
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common_types::transform::MaskStrategy;

pub fn apply(
    _batch: RecordBatch,
    _column: &str,
    _strategy: &MaskStrategy,
) -> anyhow::Result<RecordBatch> {
    anyhow::bail!("mask::apply not implemented yet (Task 6)")
}
pub fn derive_schema(
    input: &Schema,
    _column: &str,
    _strategy: &MaskStrategy,
) -> anyhow::Result<Schema> {
    Ok(input.clone())
}
```

`crates/worker/src/transform/operators/add_column.rs`:
```rust
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common_types::transform::LiteralValue;

pub fn apply(
    _batch: RecordBatch,
    _name: &str,
    _value: &LiteralValue,
) -> anyhow::Result<RecordBatch> {
    anyhow::bail!("add_column::apply not implemented yet (Task 7)")
}
pub fn derive_schema(
    _input: &Schema,
    _name: &str,
    _value: &LiteralValue,
) -> anyhow::Result<Schema> {
    anyhow::bail!("add_column::derive_schema not implemented yet (Task 7)")
}
```

`crates/worker/src/transform/operators/validate.rs`:
```rust
use arrow::record_batch::RecordBatch;
use common_types::transform::ValidationRule;

pub fn apply(
    _batch: RecordBatch,
    _rules: &[ValidationRule],
) -> anyhow::Result<(RecordBatch, Option<RecordBatch>)> {
    anyhow::bail!("validate::apply not implemented yet (Task 8)")
}
```

`crates/worker/src/transform/operators/wasm_scalar.rs`:
```rust
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

pub async fn apply(
    _batch: RecordBatch,
    _udf: &str,
    _input_column: &str,
    _output_column: &str,
    _runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<RecordBatch> {
    anyhow::bail!("wasm_scalar::apply not implemented yet (Task 12)")
}
pub fn derive_schema(
    _input: &Schema,
    _input_column: &str,
    _output_column: &str,
) -> anyhow::Result<Schema> {
    anyhow::bail!("wasm_scalar::derive_schema not implemented yet (Task 12)")
}
```

- [ ] **Step 5: Write predicate.rs with stub parse/evaluate**

Create `crates/worker/src/transform/predicate.rs`:

```rust
//! Subset-SQL predicate parser + evaluator. Phase I.5 grammar:
//!   <predicate> ::= <col> IS [NOT] NULL
//!                 | <col> = <literal>
//!                 | <col> IN ( <literal-list> )
//!   <literal>   ::= NULL | TRUE | FALSE | <int> | <string>
//! Fleshed out in Task 3.

use anyhow::Result;
use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    IsNull(String),
    IsNotNull(String),
    Eq(String, Literal),
    In(String, Vec<Literal>),
}

pub fn parse(_s: &str) -> Result<Predicate> {
    anyhow::bail!("predicate::parse not implemented yet (Task 3)")
}

pub fn evaluate(_p: &Predicate, _batch: &RecordBatch) -> Result<BooleanArray> {
    anyhow::bail!("predicate::evaluate not implemented yet (Task 3)")
}
```

- [ ] **Step 6: Note — `WasmScalarRuntime` is defined in Task 10**

The scaffolding above references `crate::wasm_runtime::WasmScalarRuntime`. Task 10 adds that type. For now, the build will fail until Task 10 lands. To keep Task 2 self-contained, also stub the runtime type:

Create `crates/worker/src/wasm_runtime/scalar_runtime.rs` **stub** (filled in Task 10):

```rust
//! Placeholder — Task 10 replaces this with a real implementation.
use std::path::PathBuf;
use std::sync::Arc;

pub struct WasmScalarRuntime {
    _base_dir: PathBuf,
}

impl WasmScalarRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self { _base_dir: base_dir.into() }))
    }

    pub async fn apply(
        &self,
        _name_at_version: &str,
        _input: Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        anyhow::bail!("WasmScalarRuntime::apply is the Task 10 stub")
    }
}
```

Edit `crates/worker/src/wasm_runtime/mod.rs`. Append:

```rust
pub mod scalar_runtime;
pub use scalar_runtime::WasmScalarRuntime;
```

- [ ] **Step 7: Build**

Run: `cargo build -p worker`
Expected: clean (all operators are stubs that `bail!`, but compile).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(worker/transform): module scaffolding + stubs

transform::apply chains operators, enforces 'validate is terminal'
(only one operator may emit rejected rows). transform::derive_schema
composes per-operator pure functions. Operator dispatch module routes
to file-per-operator impls. WasmScalarRuntime stub so Task 2 can land
before Task 10; real implementation comes later.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Predicate parser + evaluator

**Files:**
- Modify: `crates/worker/src/transform/predicate.rs`

- [ ] **Step 1: Write the failing tests**

Replace the stub in `crates/worker/src/transform/predicate.rs` with:

```rust
//! Subset-SQL predicate parser + evaluator.
//! Grammar:
//!   <predicate> ::= <col> IS [NOT] NULL
//!                 | <col> = <literal>
//!                 | <col> IN ( <literal-list> )

use anyhow::{Context, Result, bail};
use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    IsNull(String),
    IsNotNull(String),
    Eq(String, Literal),
    In(String, Vec<Literal>),
}

pub fn parse(s: &str) -> Result<Predicate> {
    let toks = tokenize(s)?;
    let mut cursor = 0;
    let pred = parse_predicate(&toks, &mut cursor)?;
    if cursor != toks.len() {
        bail!("unexpected tokens after predicate at pos {cursor}");
    }
    Ok(pred)
}

#[derive(Debug, PartialEq)]
enum Tok {
    Ident(String),
    String(String),
    Int(i64),
    Eq,
    LParen,
    RParen,
    Comma,
    KwIs,
    KwNot,
    KwNull,
    KwTrue,
    KwFalse,
    KwIn,
}

fn tokenize(s: &str) -> Result<Vec<Tok>> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '=' {
            out.push(Tok::Eq);
            i += 1;
        } else if c == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if c == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if c == ',' {
            out.push(Tok::Comma);
            i += 1;
        } else if c == '\'' {
            // string literal
            let mut j = i + 1;
            let mut buf = String::new();
            while j < chars.len() && chars[j] != '\'' {
                buf.push(chars[j]);
                j += 1;
            }
            if j == chars.len() {
                bail!("unterminated string literal at pos {i}");
            }
            out.push(Tok::String(buf));
            i = j + 1;
        } else if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) {
            let mut j = i + 1;
            while j < chars.len() && (chars[j].is_ascii_digit()) {
                j += 1;
            }
            let n: i64 = s[i..j].parse().with_context(|| format!("invalid int at {i}"))?;
            out.push(Tok::Int(n));
            i = j;
        } else if c.is_alphabetic() || c == '_' || c == '"' {
            let (ident, next) = read_ident(&chars, i)?;
            i = next;
            match ident.to_ascii_uppercase().as_str() {
                "IS" => out.push(Tok::KwIs),
                "NOT" => out.push(Tok::KwNot),
                "NULL" => out.push(Tok::KwNull),
                "TRUE" => out.push(Tok::KwTrue),
                "FALSE" => out.push(Tok::KwFalse),
                "IN" => out.push(Tok::KwIn),
                _ => out.push(Tok::Ident(ident)),
            }
        } else {
            bail!("unexpected character '{c}' at pos {i}");
        }
    }
    Ok(out)
}

fn read_ident(chars: &[char], start: usize) -> Result<(String, usize)> {
    if chars[start] == '"' {
        let mut j = start + 1;
        let mut buf = String::new();
        while j < chars.len() && chars[j] != '"' {
            buf.push(chars[j]);
            j += 1;
        }
        if j == chars.len() {
            bail!("unterminated quoted identifier");
        }
        Ok((buf, j + 1))
    } else {
        let mut j = start;
        while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
            j += 1;
        }
        Ok((chars[start..j].iter().collect(), j))
    }
}

fn parse_predicate(toks: &[Tok], c: &mut usize) -> Result<Predicate> {
    let col = match toks.get(*c) {
        Some(Tok::Ident(n)) => n.clone(),
        other => bail!("expected column name, got {other:?}"),
    };
    *c += 1;
    match toks.get(*c) {
        Some(Tok::KwIs) => {
            *c += 1;
            if matches!(toks.get(*c), Some(Tok::KwNot)) {
                *c += 1;
                expect(toks, c, &Tok::KwNull)?;
                Ok(Predicate::IsNotNull(col))
            } else {
                expect(toks, c, &Tok::KwNull)?;
                Ok(Predicate::IsNull(col))
            }
        }
        Some(Tok::Eq) => {
            *c += 1;
            let lit = parse_literal(toks, c)?;
            Ok(Predicate::Eq(col, lit))
        }
        Some(Tok::KwIn) => {
            *c += 1;
            expect(toks, c, &Tok::LParen)?;
            let mut items = Vec::new();
            loop {
                items.push(parse_literal(toks, c)?);
                match toks.get(*c) {
                    Some(Tok::Comma) => *c += 1,
                    Some(Tok::RParen) => {
                        *c += 1;
                        break;
                    }
                    other => bail!("expected , or ) in IN list, got {other:?}"),
                }
            }
            Ok(Predicate::In(col, items))
        }
        other => bail!("unsupported operator after column, got {other:?}"),
    }
}

fn parse_literal(toks: &[Tok], c: &mut usize) -> Result<Literal> {
    let lit = match toks.get(*c) {
        Some(Tok::Int(n)) => Literal::Int(*n),
        Some(Tok::String(s)) => Literal::String(s.clone()),
        Some(Tok::KwTrue) => Literal::Bool(true),
        Some(Tok::KwFalse) => Literal::Bool(false),
        Some(Tok::KwNull) => Literal::Null,
        other => bail!("expected literal, got {other:?}"),
    };
    *c += 1;
    Ok(lit)
}

fn expect(toks: &[Tok], c: &mut usize, want: &Tok) -> Result<()> {
    if toks.get(*c) == Some(want) {
        *c += 1;
        Ok(())
    } else {
        bail!("expected {:?}, got {:?}", want, toks.get(*c))
    }
}

pub fn evaluate(p: &Predicate, batch: &RecordBatch) -> Result<BooleanArray> {
    let col = |name: &str| -> Result<&dyn Array> {
        batch
            .column_by_name(name)
            .map(|a| a.as_ref())
            .ok_or_else(|| anyhow::anyhow!("column '{name}' not in batch"))
    };
    match p {
        Predicate::IsNull(name) => {
            let a = col(name)?;
            Ok(BooleanArray::from((0..a.len()).map(|i| a.is_null(i)).collect::<Vec<_>>()))
        }
        Predicate::IsNotNull(name) => {
            let a = col(name)?;
            Ok(BooleanArray::from((0..a.len()).map(|i| !a.is_null(i)).collect::<Vec<_>>()))
        }
        Predicate::Eq(name, lit) => {
            let a = col(name)?;
            eq_column(a, lit)
        }
        Predicate::In(name, items) => {
            let a = col(name)?;
            let mut mask = vec![false; a.len()];
            for lit in items {
                let m = eq_column(a, lit)?;
                for (i, v) in m.iter().enumerate() {
                    mask[i] = mask[i] || v.unwrap_or(false);
                }
            }
            Ok(BooleanArray::from(mask))
        }
    }
}

fn eq_column(a: &dyn Array, lit: &Literal) -> Result<BooleanArray> {
    match lit {
        Literal::Int(n) => {
            let arr = a
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Eq Int expects Int64 column"))?;
            Ok(BooleanArray::from(
                (0..arr.len())
                    .map(|i| !arr.is_null(i) && arr.value(i) == *n)
                    .collect::<Vec<_>>(),
            ))
        }
        Literal::String(s) => {
            let arr = a
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Eq String expects Utf8 column"))?;
            Ok(BooleanArray::from(
                (0..arr.len())
                    .map(|i| !arr.is_null(i) && arr.value(i) == s.as_str())
                    .collect::<Vec<_>>(),
            ))
        }
        Literal::Null => Ok(BooleanArray::from(vec![false; a.len()])),
        Literal::Bool(_) => anyhow::bail!("Eq Bool against non-Bool column (Phase I.5: not supported)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Carol")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn parse_is_not_null() {
        assert_eq!(parse("email IS NOT NULL").unwrap(), Predicate::IsNotNull("email".into()));
    }

    #[test]
    fn parse_eq_int() {
        assert_eq!(parse("id = 42").unwrap(), Predicate::Eq("id".into(), Literal::Int(42)));
    }

    #[test]
    fn parse_eq_string() {
        assert_eq!(
            parse("status = 'active'").unwrap(),
            Predicate::Eq("status".into(), Literal::String("active".into()))
        );
    }

    #[test]
    fn parse_in_list() {
        assert_eq!(
            parse("status IN ('a', 'b', 'c')").unwrap(),
            Predicate::In(
                "status".into(),
                vec![
                    Literal::String("a".into()),
                    Literal::String("b".into()),
                    Literal::String("c".into()),
                ],
            )
        );
    }

    #[test]
    fn eval_is_not_null_on_name() {
        let b = batch();
        let mask = evaluate(&Predicate::IsNotNull("name".into()), &b).unwrap();
        let v: Vec<bool> = mask.iter().map(|o| o.unwrap()).collect();
        assert_eq!(v, vec![true, false, true]);
    }

    #[test]
    fn eval_eq_int() {
        let b = batch();
        let mask = evaluate(&Predicate::Eq("id".into(), Literal::Int(3)), &b).unwrap();
        let v: Vec<bool> = mask.iter().map(|o| o.unwrap()).collect();
        assert_eq!(v, vec![false, false, true]);
    }

    #[test]
    fn eval_in_list_strings() {
        let b = batch();
        let p = Predicate::In(
            "name".into(),
            vec![Literal::String("Alice".into()), Literal::String("Carol".into())],
        );
        let mask = evaluate(&p, &b).unwrap();
        let v: Vec<bool> = mask.iter().map(|o| o.unwrap()).collect();
        assert_eq!(v, vec![true, false, true]);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p worker transform::predicate`
Expected: 7 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/predicate): subset-SQL parser + evaluator

Grammar: <col> IS [NOT] NULL | <col> = <literal> | <col> IN (literals).
Tokenizer handles quoted-identifiers, single-quote strings, signed
ints, keywords case-insensitive. Evaluator returns BooleanArray over
RecordBatch, downcasting to Int64Array/StringArray for Eq. 7 tests
cover parse + eval paths.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Operator — `select`

**Files:**
- Modify: `crates/worker/src/transform/operators/select.rs`

- [ ] **Step 1: Write the test + impl**

Replace `crates/worker/src/transform/operators/select.rs`:

```rust
use anyhow::{Context, bail};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn apply(batch: RecordBatch, columns: &[String]) -> anyhow::Result<RecordBatch> {
    let out_schema = derive_schema(batch.schema().as_ref(), columns)?;
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|c| {
            batch
                .column_by_name(c)
                .cloned()
                .with_context(|| format!("select: column '{c}' not found"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::new(out_schema), arrays)?)
}

pub fn derive_schema(input: &Schema, columns: &[String]) -> anyhow::Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());
    for name in columns {
        let field = input
            .field_with_name(name)
            .with_context(|| format!("select: column '{name}' not in input schema"))?
            .clone();
        fields.push(field);
    }
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("A"), Some("B")])),
                Arc::new(StringArray::from(vec![Some("a@x"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn select_subset_reorders() {
        let cols = vec!["email".into(), "id".into()];
        let out = apply(batch(), &cols).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(0).name(), "email");
        assert_eq!(out.schema().field(1).name(), "id");
    }

    #[test]
    fn select_unknown_column_errors() {
        let cols = vec!["ghost".into()];
        assert!(apply(batch(), &cols).is_err());
    }

    #[test]
    fn derive_schema_matches_apply() {
        let input = batch();
        let cols = vec!["name".into(), "email".into()];
        let derived = derive_schema(input.schema().as_ref(), &cols).unwrap();
        let applied = apply(input, &cols).unwrap();
        assert_eq!(derived, *applied.schema().as_ref());
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p worker transform::operators::select`
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/operators): select

Subset + reorder columns; errors on missing column. Pure-function
derive_schema tested to match apply output.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Operator — `filter`

**Files:**
- Modify: `crates/worker/src/transform/operators/filter.rs`

- [ ] **Step 1: Write**

Replace `crates/worker/src/transform/operators/filter.rs`:

```rust
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;

use crate::transform::predicate;

pub fn apply(batch: RecordBatch, predicate_text: &str) -> anyhow::Result<RecordBatch> {
    let p = predicate::parse(predicate_text)?;
    let mask = predicate::evaluate(&p, &batch)?;
    let out = filter_record_batch(&batch, &mask)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![Some("a@x"), None, Some("b@x"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_is_not_null_keeps_two() {
        let out = apply(batch(), "email IS NOT NULL").unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn filter_eq_int_keeps_one() {
        let out = apply(batch(), "id = 3").unwrap();
        assert_eq!(out.num_rows(), 1);
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p worker transform::operators::filter`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/operators): filter

Uses the subset-SQL predicate parser + arrow::compute::filter_record_batch.
Schema unchanged (filter is a row op). Two tests: IS NOT NULL and = INT.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Operator — `mask`

**Files:**
- Modify: `crates/worker/src/transform/operators/mask.rs`

- [ ] **Step 1: Write**

Replace `crates/worker/src/transform/operators/mask.rs`:

```rust
use anyhow::{Context, anyhow, bail};
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::transform::MaskStrategy;
use std::sync::Arc;

pub fn apply(
    batch: RecordBatch,
    column: &str,
    strategy: &MaskStrategy,
) -> anyhow::Result<RecordBatch> {
    let input_schema = batch.schema();
    let (idx, field) = input_schema
        .column_with_name(column)
        .ok_or_else(|| anyhow!("mask: column '{column}' not in batch"))?;

    let source_array = batch.column(idx).clone();
    let utf8 = source_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("mask Phase I.5: only Utf8 columns are supported; '{column}' is {:?}", field.data_type()))?;

    let masked: StringArray = match strategy {
        MaskStrategy::Hash => {
            let mut b = StringBuilder::with_capacity(utf8.len(), utf8.len() * 64);
            for i in 0..utf8.len() {
                if utf8.is_null(i) {
                    b.append_null();
                } else {
                    let digest = blake3::hash(utf8.value(i).as_bytes());
                    b.append_value(digest.to_hex().to_string());
                }
            }
            b.finish()
        }
        MaskStrategy::Null => {
            if !field.is_nullable() {
                bail!("mask Null: column '{column}' is non-nullable");
            }
            let mut b = StringBuilder::with_capacity(utf8.len(), 0);
            for _ in 0..utf8.len() {
                b.append_null();
            }
            b.finish()
        }
        MaskStrategy::Redact { replacement } => {
            let default = "[REDACTED]".to_string();
            let r = replacement.as_ref().unwrap_or(&default);
            let mut b = StringBuilder::with_capacity(utf8.len(), utf8.len() * r.len());
            for i in 0..utf8.len() {
                if utf8.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(r);
                }
            }
            b.finish()
        }
    };

    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays[idx] = Arc::new(masked);

    // Schema may gain nullability if strategy is Null.
    let mut fields: Vec<Field> = input_schema.fields().iter().map(|f| (**f).clone()).collect();
    if matches!(strategy, MaskStrategy::Null) {
        fields[idx] = Field::new(column, fields[idx].data_type().clone(), true);
    }
    let out_schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(out_schema, arrays)?)
}

pub fn derive_schema(
    input: &Schema,
    column: &str,
    strategy: &MaskStrategy,
) -> anyhow::Result<Schema> {
    let (idx, field) = input
        .column_with_name(column)
        .with_context(|| format!("mask: column '{column}' not in input schema"))?;
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    if matches!(strategy, MaskStrategy::Null) {
        fields[idx] = Field::new(column, field.data_type().clone(), true);
    }
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a@x.com"), Some("b@x.com")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn hash_masks_preserve_schema_and_row_count() {
        let out = apply(batch(), "email", &MaskStrategy::Hash).unwrap();
        assert_eq!(out.num_rows(), 2);
        let a = out
            .column_by_name("email")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0).len(), 64); // BLAKE3 hex
        assert_ne!(a.value(0), "a@x.com");
    }

    #[test]
    fn redact_replaces_with_marker() {
        let out = apply(batch(), "email", &MaskStrategy::Redact { replacement: None }).unwrap();
        let a = out
            .column_by_name("email")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "[REDACTED]");
    }

    #[test]
    fn null_requires_nullable_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a@x"]))]).unwrap();
        assert!(apply(b, "email", &MaskStrategy::Null).is_err());
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p worker transform::operators::mask`
Expected: 3 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/operators): mask

Three strategies on Utf8 columns: Hash (BLAKE3 hex, 64 chars), Null
(requires nullable column; also upgrades derived schema to nullable),
Redact { replacement: default '[REDACTED]' }. Non-Utf8 columns error
with a clear message. 3 tests cover each strategy.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Operator — `add_column`

**Files:**
- Modify: `crates/worker/src/transform/operators/add_column.rs`

- [ ] **Step 1: Write**

Replace `crates/worker/src/transform/operators/add_column.rs`:

```rust
use anyhow::{anyhow, bail};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::transform::LiteralValue;
use std::sync::Arc;

pub fn apply(
    batch: RecordBatch,
    name: &str,
    value: &LiteralValue,
) -> anyhow::Result<RecordBatch> {
    if batch.schema().column_with_name(name).is_some() {
        bail!("add_column: column '{name}' already exists");
    }
    let n = batch.num_rows();
    let (new_field, new_array): (Field, ArrayRef) = match value {
        LiteralValue::Int(v) => (
            Field::new(name, DataType::Int64, false),
            Arc::new(Int64Array::from(vec![*v; n])),
        ),
        LiteralValue::Float(v) => (
            Field::new(name, DataType::Float64, false),
            Arc::new(Float64Array::from(vec![*v; n])),
        ),
        LiteralValue::String(v) => (
            Field::new(name, DataType::Utf8, false),
            Arc::new(StringArray::from(vec![v.as_str(); n])),
        ),
        LiteralValue::Bool(v) => (
            Field::new(name, DataType::Boolean, false),
            Arc::new(BooleanArray::from(vec![*v; n])),
        ),
        LiteralValue::Null => {
            return Err(anyhow!("add_column: LiteralValue::Null requires a declared type — Phase I.5 doesn't support it yet"));
        }
    };
    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| (**f).clone()).collect();
    fields.push(new_field);
    let out_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays.push(new_array);
    Ok(RecordBatch::try_new(out_schema, arrays)?)
}

pub fn derive_schema(
    input: &Schema,
    name: &str,
    value: &LiteralValue,
) -> anyhow::Result<Schema> {
    if input.column_with_name(name).is_some() {
        bail!("add_column: column '{name}' already exists");
    }
    let dt = match value {
        LiteralValue::Int(_) => DataType::Int64,
        LiteralValue::Float(_) => DataType::Float64,
        LiteralValue::String(_) => DataType::Utf8,
        LiteralValue::Bool(_) => DataType::Boolean,
        LiteralValue::Null => bail!("add_column Null literal not supported"),
    };
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(name, dt, false));
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn add_string_column() {
        let out = apply(batch(), "source", &LiteralValue::String("customers".into())).unwrap();
        assert_eq!(out.num_columns(), 2);
        let a = out
            .column_by_name("source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "customers");
        assert_eq!(a.value(2), "customers");
    }

    #[test]
    fn add_existing_column_errors() {
        assert!(apply(batch(), "id", &LiteralValue::Int(0)).is_err());
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p worker transform::operators::add_column`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/operators): add_column

Append a new column filled with a constant literal (Int/Float/String/
Bool). Non-nullable by construction. Errors on duplicate name. Null
literal intentionally unsupported in Phase I.5 (needs explicit type).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Operator — `validate` (with dead-letter split)

**Files:**
- Modify: `crates/worker/src/transform/operators/validate.rs`

- [ ] **Step 1: Write**

Replace `crates/worker/src/transform/operators/validate.rs`:

```rust
use anyhow::anyhow;
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use common_types::transform::ValidationRule;

pub fn apply(
    batch: RecordBatch,
    rules: &[ValidationRule],
) -> anyhow::Result<(RecordBatch, Option<RecordBatch>)> {
    let n = batch.num_rows();
    // Build a per-row "passes all rules" mask.
    let mut pass = vec![true; n];
    for rule in rules {
        match rule {
            ValidationRule::NotNull { column } => {
                let a = batch
                    .column_by_name(column)
                    .ok_or_else(|| anyhow!("validate: column '{column}' not in batch"))?;
                for i in 0..n {
                    if a.is_null(i) {
                        pass[i] = false;
                    }
                }
            }
        }
    }
    if pass.iter().all(|b| *b) {
        return Ok((batch, None));
    }
    let kept_mask = BooleanArray::from(pass.clone());
    let rejected_mask = BooleanArray::from(pass.iter().map(|b| !b).collect::<Vec<_>>());
    let kept = filter_record_batch(&batch, &kept_mask)?;
    let rejected = filter_record_batch(&batch, &rejected_mask)?;
    let rejected_opt = if rejected.num_rows() > 0 { Some(rejected) } else { None };
    Ok((kept, rejected_opt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), None])),
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), None, Some("D")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn not_null_id_splits_correctly() {
        let (kept, rejected) = apply(
            batch(),
            &[ValidationRule::NotNull { column: "id".into() }],
        )
        .unwrap();
        assert_eq!(kept.num_rows(), 2);
        assert_eq!(rejected.as_ref().unwrap().num_rows(), 2);
    }

    #[test]
    fn all_pass_means_no_rejected() {
        let (kept, rejected) = apply(
            batch(),
            &[ValidationRule::NotNull { column: "name".into() }],
        )
        .unwrap();
        assert_eq!(kept.num_rows(), 3);
        assert_eq!(rejected.unwrap().num_rows(), 1);
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p worker transform::operators::validate`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/operators): validate (not_null)

Row-level validation that splits the input batch into (kept, rejected)
via per-row boolean mask. Phase I.5 supports only NotNull rule; more
rules are deferred. Returns None for rejected when all rows pass.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Transform composition test

**Files:**
- Modify: `crates/worker/src/transform/mod.rs`

- [ ] **Step 1: Add composition tests to transform/mod.rs**

Append to the bottom of `crates/worker/src/transform/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use common_types::transform::{LiteralValue, MaskStrategy, Operator};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(StringArray::from(vec![Some("a@x"), None, Some("c@x")])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn filter_then_mask_chains_correctly() {
        let rt = crate::wasm_runtime::WasmScalarRuntime::new(
            std::env::temp_dir().join("etl-scalar-test"),
        )
        .unwrap();
        let ops = vec![
            Operator::Filter { predicate: "email IS NOT NULL".into() },
            Operator::Mask {
                column: "email".into(),
                strategy: MaskStrategy::Hash,
            },
        ];
        let out = apply(batch(), &ops, &rt).await.unwrap();
        assert_eq!(out.kept.num_rows(), 2);
        assert!(out.rejected.is_none());
        assert_eq!(out.per_operator.len(), 2);
        assert_eq!(out.per_operator[0].rows_in, 3);
        assert_eq!(out.per_operator[0].rows_out, 2);
        assert_eq!(out.per_operator[1].rows_out, 2);
    }

    #[test]
    fn derive_schema_chains_correctly() {
        let input = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        let ops = vec![
            Operator::Select { columns: vec!["email".into()] },
            Operator::AddColumn {
                name: "source".into(),
                value: LiteralValue::String("customers".into()),
            },
        ];
        let out = derive_schema(&input, &ops).unwrap();
        assert_eq!(out.fields().len(), 2);
        assert_eq!(out.field(0).name(), "email");
        assert_eq!(out.field(1).name(), "source");
    }
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p worker transform::tests`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(worker/transform): composition of apply + derive_schema

filter_then_mask_chains_correctly verifies per_operator metrics and
that filter reduces rows 3→2 while mask preserves count. Schema
derivation composed for select+add_column (input 2-col → output 2-col
with renamed+added column).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Scalar-UDF WIT + `WasmScalarRuntime`

**Files:**
- Create: `crates/connector-sdk/wit/scalar-udf.wit`
- Create: `crates/worker/src/wasm_runtime/scalar_bindings.rs`
- Modify: `crates/worker/src/wasm_runtime/scalar_runtime.rs` (replace the Task 2 stub)
- Modify: `crates/worker/src/wasm_runtime/mod.rs`

- [ ] **Step 1: WIT**

Create `crates/connector-sdk/wit/scalar-udf.wit`:

```wit
package platform:udf@0.1.0;

interface host {
    enum log-level { trace, debug, info, warn, error }
    log: func(level: log-level, message: string);
}

world scalar-udf {
    import host;
    /// Apply the UDF to each string in `input`; return same-length list.
    /// Deterministic (no wall-clock, randomness, or external state).
    export apply-scalar: func(input: list<string>) -> result<list<string>, string>;
}
```

- [ ] **Step 2: Host bindings**

Create `crates/worker/src/wasm_runtime/scalar_bindings.rs`:

```rust
//! Host-side Component Model bindings for scalar-udf.wit.

wasmtime::component::bindgen!({
    path: "../connector-sdk/wit",
    world: "scalar-udf",
    async: true,
});
```

- [ ] **Step 3: Runtime implementation**

Replace `crates/worker/src/wasm_runtime/scalar_runtime.rs`:

```rust
use anyhow::{Context, bail};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::component::{Component, Linker};
use wasmtime::Store;

use super::epoch::EpochTicker;
use super::host::HostState;
use super::scalar_bindings::ScalarUdf;

pub struct WasmScalarRuntime {
    engine: Arc<Engine>,
    linker: Linker<HostState>,
    cache: DashMap<String, Arc<Component>>,
    base_dir: PathBuf,
    ticker: Arc<EpochTicker>,
}

impl WasmScalarRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let engine = Arc::new(super::engine::build_engine()?);
        let ticker = EpochTicker::start(engine.clone());
        let mut linker: Linker<HostState> = Linker::new(&engine);
        super::scalar_bindings::platform::udf::host::add_to_linker(&mut linker, |s| s)
            .context("adding scalar UDF host.log to linker")?;
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
    pub fn ticker(&self) -> &Arc<EpochTicker> {
        &self.ticker
    }

    pub fn artifact_path(&self, name_at_version: &str) -> PathBuf {
        let mut p = self.base_dir.clone();
        p.push(name_at_version);
        p.push("component.cwasm");
        p
    }

    pub fn precompile_to(
        &self,
        wasm_path: &std::path::Path,
        out_path: &std::path::Path,
    ) -> anyhow::Result<()> {
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

    pub fn load(&self, name_at_version: &str) -> anyhow::Result<Arc<Component>> {
        if let Some(c) = self.cache.get(name_at_version) {
            return Ok(c.clone());
        }
        let path = self.artifact_path(name_at_version);
        if !path.exists() {
            bail!(
                "scalar UDF not found at {} — did you `platform connector build --kind scalar`?",
                path.display()
            );
        }
        let component = unsafe {
            Component::deserialize_file(&self.engine, &path)
                .with_context(|| format!("deserialize_file {}", path.display()))?
        };
        let arc = Arc::new(component);
        self.cache.insert(name_at_version.to_string(), arc.clone());
        Ok(arc)
    }

    pub async fn apply(
        &self,
        name_at_version: &str,
        input: Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        let component = self.load(name_at_version)?;
        let limits = super::Limits::default();
        let state = HostState::new(limits.clone());
        let mut store = Store::new(&self.engine, state);
        store.set_fuel(limits.fuel)?;
        store.set_epoch_deadline(limits.wall_time_secs);
        store.limiter(|s: &mut HostState| &mut s.memory_limiter);

        let bindings = ScalarUdf::instantiate_async(&mut store, &component, &self.linker)
            .await
            .context("instantiating scalar UDF component")?;
        let result = bindings
            .call_apply_scalar(&mut store, &input)
            .await
            .context("call_apply_scalar")?
            .map_err(|e| anyhow::anyhow!("scalar UDF error: {e}"))?;
        if result.len() != input.len() {
            bail!(
                "scalar UDF returned {} rows for {} inputs",
                result.len(),
                input.len()
            );
        }
        Ok(result)
    }
}
```

- [ ] **Step 4: Module registration**

Edit `crates/worker/src/wasm_runtime/mod.rs`. Append:

```rust
pub mod scalar_bindings;
```

Ensure `scalar_runtime` is already declared + re-exports `WasmScalarRuntime` (from Task 2 scaffolding).

- [ ] **Step 5: Build**

Run: `cargo build -p worker`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(worker/wasm_runtime): scalar UDF runtime + bindgen

New platform:udf/scalar-udf WIT world — imports only host.log
(tighter than source-connector, which also has http-fetch). Linker
only wires the host.log import, proving capability denial for
http/state/time/randomness at the link level.

WasmScalarRuntime mirrors the source runtime (Engine + ticker +
Component cache + precompile + load); its apply() instantiates async,
sets fuel/epoch/memory limits, calls apply-scalar, returns the result.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Reference guest — `upper-case-scalar`

**Files:**
- Create: `examples/upper-case-scalar/Cargo.toml`
- Create: `examples/upper-case-scalar/.cargo/config.toml`
- Create: `examples/upper-case-scalar/src/lib.rs`
- Create: `examples/upper-case-scalar/README.md`
- Modify: root `Cargo.toml` — add to `[workspace.exclude]`

- [ ] **Step 1: Exclude from workspace**

Edit root `Cargo.toml`:

```toml
exclude = ["examples/csv-source", "examples/upper-case-scalar"]
```

- [ ] **Step 2: Create crate**

Create `examples/upper-case-scalar/Cargo.toml`:

```toml
[package]
name = "upper-case-scalar"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.37"

[profile.release]
opt-level = "s"
lto = true
```

Create `examples/upper-case-scalar/.cargo/config.toml`:

```toml
[build]
target = "wasm32-wasip2"
```

Create `examples/upper-case-scalar/src/lib.rs`:

```rust
//! Reference scalar UDF: uppercase each input string.

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "scalar-udf",
});

use platform::udf::host::{log, LogLevel};

struct Component;

export!(Component);

impl Guest for Component {
    fn apply_scalar(input: Vec<String>) -> Result<Vec<String>, String> {
        log(LogLevel::Info, &format!("upper-case-scalar: {} rows", input.len()));
        Ok(input.into_iter().map(|s| s.to_uppercase()).collect())
    }
}
```

Create `examples/upper-case-scalar/README.md`:

```markdown
# upper-case-scalar — reference Phase I.5 WASM scalar UDF

Uppercases each input string. Exercises the `platform:udf/scalar-udf`
world with tight capabilities (log only — no http, no wall-clock, no
randomness, no state).

## Build

```bash
cd examples/upper-case-scalar
cargo build --release
# → target/wasm32-wasip2/release/upper_case_scalar.wasm

cargo run --bin platform -- connector build examples/upper-case-scalar --kind scalar
# → connectors/upper-case-scalar@0.1.0/component.cwasm
```
```

- [ ] **Step 3: Build**

Run: `cd examples/upper-case-scalar && cargo build --release`
Expected: produces `target/wasm32-wasip2/release/upper_case_scalar.wasm`.

- [ ] **Step 4: Commit**

```bash
cd /Users/satishbabariya/Desktop/etl
git add -A
git commit -m "feat(examples/upper-case-scalar): reference WASM scalar UDF

Rust guest targeting wasm32-wasip2 via wit-bindgen. Uppercases each
input string. Small — ~250 KB precompiled. Exercises the tighter
scalar-udf capability set (log only).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Operator — `wasm_scalar`

**Files:**
- Modify: `crates/worker/src/transform/operators/wasm_scalar.rs`

- [ ] **Step 1: Write the real impl**

Replace `crates/worker/src/transform/operators/wasm_scalar.rs`:

```rust
use anyhow::{Context, anyhow, bail};
use arrow::array::{ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::wasm_runtime::WasmScalarRuntime;

pub async fn apply(
    batch: RecordBatch,
    udf: &str,
    input_column: &str,
    output_column: &str,
    runtime: &Arc<WasmScalarRuntime>,
) -> anyhow::Result<RecordBatch> {
    if batch.schema().column_with_name(output_column).is_some() {
        bail!("wasm_scalar: output_column '{output_column}' already exists");
    }
    let (idx, field) = batch
        .schema()
        .column_with_name(input_column)
        .ok_or_else(|| anyhow!("wasm_scalar: input_column '{input_column}' not found"))?;
    let utf8 = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!(
            "wasm_scalar Phase I.5 only supports Utf8 input columns; '{input_column}' is {:?}",
            field.data_type()
        ))?;

    // Project nulls → empty strings in; record a null-mask to reapply after.
    let n = utf8.len();
    let mut inputs = Vec::with_capacity(n);
    let mut null_mask = vec![false; n];
    for i in 0..n {
        if utf8.is_null(i) {
            null_mask[i] = true;
            inputs.push(String::new());
        } else {
            inputs.push(utf8.value(i).to_string());
        }
    }

    let outputs = runtime
        .apply(udf, inputs)
        .await
        .with_context(|| format!("invoking scalar UDF '{udf}'"))?;

    if outputs.len() != n {
        bail!(
            "scalar UDF returned {} rows for {} inputs — UDF must preserve length",
            outputs.len(),
            n
        );
    }
    let mut b = StringBuilder::with_capacity(n, outputs.iter().map(|s| s.len()).sum());
    for i in 0..n {
        if null_mask[i] {
            b.append_null();
        } else {
            b.append_value(&outputs[i]);
        }
    }
    let arr = b.finish();

    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(output_column, DataType::Utf8, field.is_nullable()));
    let out_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays.push(Arc::new(arr));
    Ok(RecordBatch::try_new(out_schema, arrays)?)
}

pub fn derive_schema(
    input: &Schema,
    input_column: &str,
    output_column: &str,
) -> anyhow::Result<Schema> {
    if input.column_with_name(output_column).is_some() {
        bail!("wasm_scalar: output_column '{output_column}' already exists");
    }
    let (_, field) = input
        .column_with_name(input_column)
        .ok_or_else(|| anyhow!("wasm_scalar: input_column '{input_column}' not found"))?;
    let mut fields: Vec<Field> = input.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(output_column, DataType::Utf8, field.is_nullable()));
    Ok(Schema::new(fields))
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p worker`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(worker/transform/operators): wasm_scalar

Invokes a Phase I.5 scalar UDF via WasmScalarRuntime. Projects nulls
to empty strings for the UDF call, reapplies the null mask on output
(so UDFs don't need to handle null semantics). Validates length
preservation. Output column inherits nullability from input column.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: CLI `connector build --kind scalar`

**Files:**
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Extend ConnectorCmd::Build**

Edit `crates/cli/src/main.rs`. Update the `ConnectorCmd::Build` variant:

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
        /// Which runtime to precompile for: 'source' (default) or 'scalar'.
        #[arg(long, default_value = "source")]
        kind: String,
    },
}
```

Update the dispatch:

```rust
        Cmd::Connector {
            cmd: ConnectorCmd::Build { path, name, version, out, kind },
        } => connector_build(path, name, version, out, kind).await,
```

- [ ] **Step 2: Update `connector_build`**

Replace the `connector_build` function:

```rust
async fn connector_build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
    kind: String,
) -> anyhow::Result<()> {
    use std::path::PathBuf;
    use std::process::Command as StdCommand;

    let crate_dir = PathBuf::from(&path);
    let cargo_toml = crate_dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        anyhow::bail!("no Cargo.toml at {}", cargo_toml.display());
    }

    let toml_text = std::fs::read_to_string(&cargo_toml)?;
    let pkg_name = name.unwrap_or_else(|| {
        read_toml_value(&toml_text, "name").unwrap_or_else(|| "connector".into())
    });
    let pkg_version = version.unwrap_or_else(|| {
        read_toml_value(&toml_text, "version").unwrap_or_else(|| "0.1.0".into())
    });

    let status = StdCommand::new("cargo")
        .current_dir(&crate_dir)
        .args(["build", "--release"])
        .status()?;
    if !status.success() {
        anyhow::bail!("guest build failed");
    }

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

- [ ] **Step 3: Build and live-test**

Run: `cargo build -p cli`
Then: `cargo run --bin platform -- connector build examples/upper-case-scalar --kind scalar`
Expected: produces `./connectors/upper-case-scalar@0.1.0/component.cwasm`, prints path with `(scalar)` suffix.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(cli): platform connector build --kind source|scalar

Routes to WasmSourceRuntime or WasmScalarRuntime for precompilation.
Default 'source' keeps the Phase I.3 behavior. 'scalar' uses the new
tight-capability scalar-udf world.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: Wire transform into `SyncActivities::read_batch`

**Files:**
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/workflows/pipeline_run.rs`
- Modify: `crates/worker/src/main.rs`
- Modify: `crates/cli/src/main.rs`

- [ ] **Step 1: Extend `SyncActivities` with a scalar runtime**

Edit `crates/worker/src/activities/sync/mod.rs`. Update:

```rust
pub struct SyncActivities {
    pub catalog: Arc<Catalog>,
    pub wasm_runtime: Arc<WasmSourceRuntime>,
    pub scalar_runtime: Arc<crate::wasm_runtime::WasmScalarRuntime>,
}
```

And import at top:

```rust
use common_types::transform::{Operator, TransformSpec};
```

- [ ] **Step 2: Extend `ReadBatchInput` + `ReadBatchOutput`**

Edit `crates/worker/src/activities/sync/inputs.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub cursor: Option<CursorValue>,
    pub batch_size: usize,
    pub connector_ref: String,
    #[serde(default)]
    pub transform: Option<TransformSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadBatchOutput {
    pub batch_ipc_b64: String,
    pub rows: usize,
    pub new_cursor: Option<CursorValue>,
    pub is_final: bool,
    #[serde(default)]
    pub rejected_ipc_b64: Option<String>,
    #[serde(default)]
    pub rows_rejected: usize,
}
```

Add `use common_types::transform::TransformSpec;` at the top.

- [ ] **Step 3: Apply transform inside `read_batch` activity**

Edit `crates/worker/src/activities/sync/mod.rs`. Replace the body of `read_batch`:

```rust
    #[activity]
    pub async fn read_batch(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadBatchInput,
    ) -> Result<ReadBatchOutput, ActivityError> {
        let connector =
            build_source_connector(&input.connector_ref, Some(self.wasm_runtime.clone()))
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

        // Apply transforms (if any) in-memory.
        let (kept_batch, rejected_batch) = match &input.transform {
            Some(spec) if !spec.operators.is_empty() => {
                let tx = crate::transform::apply(
                    outcome.batch,
                    &spec.operators,
                    &self.scalar_runtime,
                )
                .await
                .map_err(to_retryable)?;
                tracing::info!(
                    per_operator = ?tx.per_operator,
                    rows_kept = tx.kept.num_rows(),
                    rows_rejected = tx.rejected.as_ref().map(|b| b.num_rows()).unwrap_or(0),
                    "transform complete"
                );
                (tx.kept, tx.rejected)
            }
            _ => (outcome.batch, None),
        };

        let rows = kept_batch.num_rows();
        let rows_rejected = rejected_batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        let b64 = encode_batch(&kept_batch).map_err(to_retryable)?;
        let rejected_b64 = rejected_batch
            .as_ref()
            .map(|b| encode_batch(b))
            .transpose()
            .map_err(to_retryable)?;

        Ok(ReadBatchOutput {
            batch_ipc_b64: b64,
            rows,
            new_cursor: outcome.new_cursor,
            is_final: outcome.is_final,
            rejected_ipc_b64: rejected_b64,
            rows_rejected,
        })
    }
```

- [ ] **Step 4: Pass transform through workflow**

Edit `crates/worker/src/workflows/pipeline_run.rs`. In `PipelineRunInput` and the workflow state, add:

```rust
    pub transform: Option<common_types::transform::TransformSpec>,
```

In `#[init]` and the `ctx.state` tuple, add `transform`. In the `ReadBatchInput` construction inside the workflow body:

```rust
            ReadBatchInput {
                source: spec.source.clone(),
                source_url: conn.url.clone(),
                cursor,
                batch_size: spec.batch_size,
                connector_ref: connector_ref.clone(),
                transform: transform.clone(),
            },
```

Also, schema_evolution should use the DERIVED schema post-transform. Modify the `discover_stream` path similarly (Task 15 covers).

- [ ] **Step 5: Construct scalar_runtime in main.rs**

Edit `crates/worker/src/main.rs`. Near the existing `WasmSourceRuntime::new`, add:

```rust
    let scalar_runtime = worker::wasm_runtime::WasmScalarRuntime::new(&wasm_base)?;
```

Pass it into `SyncActivities`:

```rust
    let sync = SyncActivities {
        catalog: catalog.clone(),
        wasm_runtime: wasm_runtime.clone(),
        scalar_runtime: scalar_runtime.clone(),
    };
```

- [ ] **Step 6: CLI pulls `transform` from spec**

Edit `crates/cli/src/main.rs`'s `pipeline_run`. After parsing `spec`:

```rust
    let transform: Option<common_types::transform::TransformSpec> = pipeline
        .spec
        .get("transform")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .or_else(|| spec.transform.clone());
```

Then include in `PipelineRunInput`:

```rust
    let input = PipelineRunInput {
        // ...existing fields...
        transform,
    };
```

- [ ] **Step 7: Build**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(worker): apply transform in read_batch; pass through workflow

SyncActivities now owns Arc<WasmScalarRuntime>. read_batch applies
the transform DAG in-memory after reading, emits rejected rows as
a second Arrow IPC base64 payload. ReadBatchOutput gains
rejected_ipc_b64 + rows_rejected. Workflow state + input threaded
through; backward-compat for pipelines without a transform field.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: Dead-letter routing in `load_batch` + threshold enforcement

**Files:**
- Modify: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/workflows/pipeline_run.rs`

- [ ] **Step 1: Extend `LoadBatchInput`**

Edit `crates/worker/src/activities/sync/inputs.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadBatchInput {
    pub destination: DestinationSpec,
    pub batch_ipc_b64: String,
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub batch_seq: u32,
    #[serde(default)]
    pub rejected_ipc_b64: Option<String>,
    #[serde(default)]
    pub dead_letter_threshold: f64,
    #[serde(default)]
    pub rows_rejected_so_far: usize,
    #[serde(default)]
    pub rows_total_so_far: usize,
}
```

- [ ] **Step 2: Write dead-letter in `load_batch`**

Edit `crates/worker/src/activities/sync/mod.rs`. Replace `load_batch` body:

```rust
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
            .load(&input.destination, load_id.clone(), batch)
            .await
            .map_err(to_retryable)?;

        // Dead-letter routing.
        if let Some(rej_b64) = input.rejected_ipc_b64 {
            let rej = decode_batch(&rej_b64).map_err(to_retryable)?;
            if rej.num_rows() > 0 {
                let dest = match &input.destination {
                    common_types::pipeline_spec::DestinationSpec::LocalParquet(s) => {
                        let mut p = std::path::PathBuf::from(&s.base_path);
                        p.push(load_id.pipeline_id.as_uuid().to_string());
                        p.push("dead-letter");
                        p.push(load_id.run_id.as_uuid().to_string());
                        std::fs::create_dir_all(&p)
                            .map_err(|e| to_retryable(anyhow::anyhow!("create dir: {e}")))?;
                        p.push(format!("batch-{:05}.parquet", input.batch_seq));
                        p
                    }
                };
                let file = std::fs::File::create(&dest)
                    .map_err(|e| to_retryable(anyhow::anyhow!("dead-letter create: {e}")))?;
                let props = parquet::file::properties::WriterProperties::builder().build();
                let mut writer = parquet::arrow::ArrowWriter::try_new(file, rej.schema(), Some(props))
                    .map_err(|e| to_retryable(anyhow::anyhow!("ArrowWriter: {e}")))?;
                writer.write(&rej)
                    .map_err(|e| to_retryable(anyhow::anyhow!("dead-letter write: {e}")))?;
                writer.close()
                    .map_err(|e| to_retryable(anyhow::anyhow!("dead-letter close: {e}")))?;
                tracing::info!(path = %dest.display(), rows = rej.num_rows(), "dead-letter batch written");
            }
        }

        // Threshold check (cumulative).
        if input.dead_letter_threshold > 0.0 && input.rows_total_so_far > 0 {
            let frac = input.rows_rejected_so_far as f64 / input.rows_total_so_far as f64;
            if frac > input.dead_letter_threshold {
                return Err(ActivityError::NonRetryable(
                    anyhow::anyhow!(
                        "dead-letter threshold exceeded: {:.4} > {:.4} (rejected {}/{} rows)",
                        frac,
                        input.dead_letter_threshold,
                        input.rows_rejected_so_far,
                        input.rows_total_so_far
                    )
                    .into(),
                ));
            }
        }

        Ok(LoadBatchOutput {
            rows_loaded: res.rows_loaded,
            bytes_written: res.bytes_written,
            path: res.path,
        })
    }
```

- [ ] **Step 3: Pass fields through workflow**

Edit `crates/worker/src/workflows/pipeline_run.rs`. Keep running totals in workflow state:

```rust
pub struct PipelineRunWorkflow {
    // existing fields...
    rows_total_so_far: u64,
    rows_rejected_so_far: u64,
}
```

In `#[init]`, initialize both to 0.

In the `run` loop, update totals after each `read_out`:

```rust
            ctx.state_mut(|s| {
                s.rows_total_so_far += read_out.rows as u64 + read_out.rows_rejected as u64;
                s.rows_rejected_so_far += read_out.rows_rejected as u64;
            });
```

Then construct `LoadBatchInput` with:

```rust
            LoadBatchInput {
                destination: spec.destination.clone(),
                batch_ipc_b64: read_out.batch_ipc_b64,
                pipeline_id,
                run_id,
                batch_seq,
                rejected_ipc_b64: read_out.rejected_ipc_b64,
                dead_letter_threshold: transform
                    .as_ref()
                    .map(|t| t.dead_letter_threshold)
                    .unwrap_or(0.0),
                rows_rejected_so_far: ctx.state(|s| s.rows_rejected_so_far) as usize,
                rows_total_so_far: ctx.state(|s| s.rows_total_so_far) as usize,
            },
```

- [ ] **Step 4: Build**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker): dead-letter routing + threshold enforcement

load_batch writes rejected rows as a separate Parquet file under
<base>/<pipeline>/dead-letter/<run>/batch-<seq>.parquet when
rejected_ipc_b64 is present. Workflow tracks cumulative
rows_total_so_far / rows_rejected_so_far; if the fraction exceeds
TransformSpec.dead_letter_threshold (default 1%), the activity fails
non-retryably and the run status becomes failed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 16: Schema-evolution tracks the DERIVED schema

**Files:**
- Modify: `crates/worker/src/activities/sync/inputs.rs`
- Modify: `crates/worker/src/activities/sync/mod.rs`
- Modify: `crates/worker/src/workflows/pipeline_run.rs`

- [ ] **Step 1: Extend `DiscoverInput` with transform**

Edit `crates/worker/src/activities/sync/inputs.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverInput {
    pub source: SourceSpec,
    pub source_url: String,
    pub connector_ref: String,
    pub tenant_id: Uuid,
    pub stream_name: String,
    pub pipeline_id: Uuid,
    pub cursor_column: String,
    pub cursor_kind: common_types::cursor::CursorKind,
    pub pk_columns: Vec<String>,
    pub evolution_policy: common_types::evolution::EvolutionPolicy,
    #[serde(default)]
    pub transform: Option<TransformSpec>,
}
```

- [ ] **Step 2: Derive schema inside `discover_stream`**

Edit `crates/worker/src/activities/sync/mod.rs`. Replace the body of `discover_stream` around the `record_and_resolve` call:

```rust
        let discovered_schema = connector
            .discover(
                &ConnectionConfig { url: input.source_url.clone() },
                &input.source,
            )
            .await
            .map_err(to_retryable)?;

        // Derive the post-transform schema — that's what lands at the destination.
        let final_schema: arrow::datatypes::SchemaRef = match &input.transform {
            Some(spec) if !spec.operators.is_empty() => {
                let derived = crate::transform::derive_schema(
                    discovered_schema.as_ref(),
                    &spec.operators,
                )
                .map_err(to_retryable)?;
                std::sync::Arc::new(derived)
            }
            _ => discovered_schema,
        };

        // ... existing Stream + record_and_resolve code, but pass final_schema ...
```

And change the `record_and_resolve` call to use `final_schema.clone()` instead of `schema.clone()`.

- [ ] **Step 3: Pass transform from workflow**

Edit `crates/worker/src/workflows/pipeline_run.rs`. Add `transform: transform.clone()` to the `DiscoverInput` construction.

- [ ] **Step 4: Build**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(worker): schema_evolution tracks post-transform schema

discover_stream derives the schema through transform::derive_schema
before calling record_and_resolve. The Schema entity stored in the
catalog now reflects what lands at the destination (with masks
preserved as types, new columns from add_column, etc.), not what the
source emitted. Backward-compat: pipelines without transform behave
identically — derive_schema is a no-op on empty operators list.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 17: End-to-end integration test — filter + mask

**Files:**
- Create: `tests/integration/tests/transforms_filter_mask.rs`
- Create: `examples/dsl/customers-with-transform.yaml`

- [ ] **Step 1: YAML example**

Create `examples/dsl/customers-with-transform.yaml`:

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: source-demo
spec:
  connector_ref: postgres@0.1.0
  config:
    url: postgres://etl:etl@localhost:5432/etl_source_demo
---
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: customers-masked
spec:
  source_connection: source-demo
  source:
    type: postgres
    schema: public
    table: customers
    cursor_column: updated_at
    cursor_kind: timestamp_tz
    pk_columns: [id]
  destination:
    type: local_parquet
    base_path: ./data
  batch_size: 10
  evolution_policy: propagate_additive
  transform:
    operators:
      - type: filter
        predicate: "email IS NOT NULL"
      - type: mask
        column: email
        strategy:
          kind: hash
      - type: add_column
        name: source_stream
        value: customers
```

- [ ] **Step 2: Integration test**

Create `tests/integration/tests/transforms_filter_mask.rs`:

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

async fn reseed_source() -> anyhow::Result<()> {
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());
    Ok(())
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
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn filter_and_mask_apply_end_to_end() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    reseed_source().await?;

    let tmp = tempfile::tempdir()?;
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
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 10,
        "evolution_policy": "propagate_additive",
        "transform": {
            "operators": [
                {"type":"filter","predicate":"email IS NOT NULL"},
                {"type":"mask","column":"email","strategy":{"kind":"hash"}},
                {"type":"add_column","name":"source_stream","value":"customers"}
            ]
        }
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "customers-masked".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(
        out.status.success(),
        "cli: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if std::time::Instant::now() > deadline {
            anyhow::bail!("timed out");
        }
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool())
                .await?;
        if let Some((s,)) = row {
            if RunStatus::parse(&s) == Some(RunStatus::Completed) { break; }
            if RunStatus::parse(&s) == Some(RunStatus::Failed) { anyhow::bail!("run failed"); }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    w.kill().await?;
    w.wait().await?;

    // Inspect Parquet: filter drops rows where email IS NULL (2 of 10 rows
    // have NULL email in the seed), mask replaces email with hash,
    // add_column appends source_stream = "customers".
    let mut total_rows = 0usize;
    let mut saw_hash = false;
    let mut saw_source_stream = false;
    let mut any_null_email = false;
    for entry in walkdir::WalkDir::new(tmp.path()).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            let f = std::fs::File::open(entry.path()).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
            for batch in reader {
                let b = batch.unwrap();
                total_rows += b.num_rows();

                let names: Vec<&str> = b.schema().fields().iter().map(|f| f.name().as_str()).collect();
                if names.contains(&"source_stream") {
                    saw_source_stream = true;
                }
                if let Some(email) = b.column_by_name("email") {
                    let arr = email.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    for i in 0..arr.len() {
                        if arr.is_null(i) { any_null_email = true; }
                        else if arr.value(i).len() == 64 && arr.value(i).chars().all(|c| c.is_ascii_hexdigit()) {
                            saw_hash = true;
                        }
                    }
                }
            }
        }
    }
    // 8 customers had non-null email (10 total, 2 NULL per seed).
    assert_eq!(total_rows, 8, "filter should drop 2 null-email rows");
    assert!(saw_hash, "mask should produce BLAKE3 hex (64 hex chars)");
    assert!(saw_source_stream, "add_column should append 'source_stream'");
    assert!(!any_null_email, "filter should have removed all null emails");

    Ok(())
}
```

- [ ] **Step 3: Run**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p integration-tests filter_and_mask_apply_end_to_end -- --ignored --nocapture`
Expected: 1 passed, ~60s.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(integration): filter + mask + add_column end-to-end

10-row Postgres seed (2 with NULL email). Pipeline filters to 8 rows,
masks email as BLAKE3 hex (64 chars), appends source_stream=customers.
Test asserts total_rows=8, every email is a hex hash, no null emails
leaked, and the appended column exists.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 18: Integration test — `validate` + dead-letter routing

**Files:**
- Create: `tests/integration/tests/transforms_dead_letter.rs`

- [ ] **Step 1: Write the test**

Create `tests/integration/tests/transforms_dead_letter.rs`:

```rust
use anyhow::Context;
use catalog::{Catalog, NewConnection, NewPipeline, RunStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
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

async fn run_sql(db: &str, sql: &str) -> anyhow::Result<()> {
    let mut child = Command::new("docker")
        .args(["exec", "-i", "etl-postgres", "psql", "-U", "etl", "-d", db])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()?;
    if let Some(mut s) = child.stdin.take() {
        s.write_all(sql.as_bytes()).await?;
        s.shutdown().await?;
    }
    let status = child.wait().await?;
    assert!(status.success(), "psql: {sql}");
    Ok(())
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
        .context("spawn worker")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(child)
}

#[tokio::test]
#[ignore = "requires docker stack + source demo; mutates source rows"]
async fn validate_not_null_routes_to_dead_letter() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    // Reset source + introduce 2 rows with NULL email by mutating the seed.
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());
    // 2 of 10 seed rows already have NULL email (Bob, Frank). Nothing to mutate.

    let tmp = tempfile::tempdir()?;
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

    // validate NotNull(email) will reject the 2 null-email rows (20% of 10);
    // dead_letter_threshold 0.3 to allow 20% without failing the run.
    let spec = json!({
        "source": {
            "type": "postgres", "schema": "public", "table": "customers",
            "cursor_column": "updated_at", "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"]
        },
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 10,
        "evolution_policy": "propagate_additive",
        "transform": {
            "operators": [
                {"type":"validate","rules":[{"rule":"not_null","column":"email"}]}
            ],
            "dead_letter_threshold": 0.3
        }
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "customers-validated".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;

    let out = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;
    assert!(out.status.success(), "cli: {}", String::from_utf8_lossy(&out.stderr));

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if std::time::Instant::now() > deadline { anyhow::bail!("timed out"); }
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool()).await?;
        if let Some((s,)) = row {
            if RunStatus::parse(&s) == Some(RunStatus::Completed) { break; }
            if RunStatus::parse(&s) == Some(RunStatus::Failed) {
                anyhow::bail!("run failed — threshold may have been exceeded; check logs");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    w.kill().await?;
    w.wait().await?;

    // Inspect: main output has 8 rows (10 minus 2 rejected).
    let mut main_rows = 0usize;
    let mut dead_letter_rows = 0usize;
    for entry in walkdir::WalkDir::new(tmp.path()).into_iter().flatten() {
        if entry.path().extension().and_then(|e| e.to_str()) != Some("parquet") { continue; }
        let is_dl = entry.path().components().any(|c| c.as_os_str() == "dead-letter");
        let f = std::fs::File::open(entry.path()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        for batch in reader {
            let b = batch.unwrap();
            if is_dl {
                dead_letter_rows += b.num_rows();
                // Dead-letter rows should preserve original email (null).
                let email = b.column_by_name("email").unwrap()
                    .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                for i in 0..email.len() {
                    assert!(email.is_null(i), "dead-letter row should have null email");
                }
            } else {
                main_rows += b.num_rows();
            }
        }
    }
    assert_eq!(main_rows, 8, "main output should contain 8 rows");
    assert_eq!(dead_letter_rows, 2, "dead-letter should contain 2 rows");

    // Cleanup is not needed — tempdir dropped.
    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack + source demo"]
async fn dead_letter_threshold_fails_run_when_exceeded() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["build", "--workspace"])
        .status()
        .await?;
    assert!(status.success());

    // Reseed; 2/10 rows already have null email (20%).
    let status = Command::new("bash")
        .current_dir(workspace_root())
        .arg("./scripts/seed-source-demo.sh")
        .status()
        .await?;
    assert!(status.success());

    let tmp = tempfile::tempdir()?;
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

    // 1% threshold against 20% actual rejection → fails.
    let spec = json!({
        "source": {
            "type": "postgres", "schema": "public", "table": "customers",
            "cursor_column": "updated_at", "cursor_kind": "timestamp_tz",
            "pk_columns": ["id"]
        },
        "destination": {"type":"local_parquet","base_path": tmp.path().to_string_lossy()},
        "batch_size": 10,
        "evolution_policy": "propagate_additive",
        "transform": {
            "operators": [
                {"type":"validate","rules":[{"rule":"not_null","column":"email"}]}
            ],
            "dead_letter_threshold": 0.01
        }
    });
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "customers-strict".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec,
        })
        .await?;

    let mut w = spawn_worker().await?;
    let _ = Command::new(cargo_bin("platform"))
        .args(["pipeline", "run", &pipe.to_string()])
        .env("DATABASE_URL", catalog_url())
        .env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
        .env("TEMPORAL_NAMESPACE", "default")
        .env("TEMPORAL_TASK_QUEUE", "pipeline-default")
        .current_dir(workspace_root())
        .output()
        .await?;

    // Wait for terminal status.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut final_status = String::new();
    loop {
        if std::time::Instant::now() > deadline { anyhow::bail!("timed out"); }
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM runs ORDER BY started_at DESC LIMIT 1")
                .fetch_optional(cat.pool()).await?;
        if let Some((s,)) = row {
            if s == "completed" || s == "failed" {
                final_status = s;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    w.kill().await?;
    w.wait().await?;

    assert_eq!(final_status, "failed", "run should fail when threshold exceeded");
    Ok(())
}
```

- [ ] **Step 2: Run**

Run: `DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog cargo test -p integration-tests transforms_dead_letter -- --ignored --nocapture`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(integration): validate → dead-letter end-to-end

Two tests: (1) 20% rejection with 30% threshold → run completes, 8
rows in main output, 2 rows in dead-letter with original null emails
preserved; (2) 20% rejection with 1% threshold → run fails via
NonRetryable error in load_batch.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 19: README + Phase completion log

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-04-23-phase-1-5-transformations.md` (this file)

- [ ] **Step 1: Update README Phase line**

Edit `README.md`. Replace the `Currently:` line with:

```markdown
Currently: **Phase I.5 — Transformation DAG (complete)**. Next: Phase I.6 — Postgres logical-replication CDC. See the roadmap spec for the four-era trajectory.

## Phase I.5 — Transformation demo

```bash
# 1. Reseed, apply the transform pipeline
bash scripts/seed-source-demo.sh
cargo run --bin platform -- apply -f examples/dsl/customers-with-transform.yaml

# 2. Run it
cargo run --bin worker &
cargo run --bin platform -- pipeline run <pipeline-id-from-apply>

# 3. Inspect: emails are BLAKE3-hashed, NULL-email rows dropped, source_stream column appended
duckdb -c "SELECT id, email, source_stream FROM read_parquet('data/**/*.parquet') LIMIT 5;"
```

WASM scalar UDF demo:

```bash
cargo run --bin platform -- connector build examples/upper-case-scalar --kind scalar
# Then reference it in a pipeline's transform:
#   - type: wasm_scalar
#     udf: upper-case-scalar@0.1.0
#     input_column: name
#     output_column: name_upper
```

Supported operators (Phase I.5): `select`, `filter`, `mask` (hash / null / redact), `add_column` (constant literals), `validate` (not_null rule with dead-letter routing), `wasm_scalar` (Utf8 columns). Remaining 6 operators deferred.
```

- [ ] **Step 2: Update crate map**

Edit `README.md` crate map to include:

```markdown
| `worker` | Temporal worker, PipelineRunWorkflow, Postgres connector, Parquet loader, WASM runtime (source + scalar), transform DAG, schema_evolution | I.1 → I.6 |
| `examples/upper-case-scalar` | Reference WASM scalar UDF (wasm32-wasip2) | I.5 |
```

- [ ] **Step 3: Append completion log**

Append to the bottom of the plan file (after the "Execution Handoff" section):

```markdown

---

## Phase I.5 Completion Log

Completed 2026-04-25 on branch `phase-1-5-transformations`, 19 commits.

- [x] Task 1 — TransformSpec + Operator + PipelineSpec.transform
- [x] Task 2 — transform module scaffolding + operator trait dispatch
- [x] Task 3 — Predicate parser + evaluator (7 tests)
- [x] Tasks 4–8 — Operators: select, filter, mask, add_column, validate (12 tests)
- [x] Task 9 — Composition tests
- [x] Task 10 — Scalar UDF WIT + WasmScalarRuntime
- [x] Task 11 — upper-case-scalar reference guest
- [x] Task 12 — wasm_scalar operator
- [x] Task 13 — CLI `connector build --kind source|scalar`
- [x] Task 14 — Transform wiring into SyncActivities
- [x] Task 15 — Dead-letter routing + threshold
- [x] Task 16 — Schema evolution tracks derived schema
- [x] Task 17 — filter+mask+add_column integration test
- [x] Task 18 — validate → dead-letter integration test (2 scenarios)
- [x] Task 19 — README + this log

### Exit criterion — MET

- 5 declarative operators implemented + WASM scalar UDF
- Each operator has a pure-function derive_schema tested in isolation
- Dead-letter files land at `<base>/<pipeline>/dead-letter/<run>/batch-<seq>.parquet` with original-schema preservation
- Post-transform schema flows into `record_and_resolve` — catalog's `schemas` table records what lands at the destination
- Backward-compat: pipelines without `transform` field behave identically to Phase I.4
- Phase I.2 / I.3 / I.4 integration tests still green

### Deviations

(Fill in as encountered during execution.)

### Handoff to Phase I.6

Phase I.6 (Postgres logical replication / CDC) adds:
- New `sync_mode: cdc` for streams, backed by replication slots
- Per-row `_cdc.op` / `_cdc.lsn` metadata columns
- CdcPipelineWorkflow (long-lived) + CdcSnapshotWorkflow (finite) — different Temporal shape from the current PipelineRunWorkflow
- Apply-change-stream loader pattern for Parquet (append + logical delete markers + compaction handoff)

The transform DAG built in I.5 is reusable on CDC output — each `_cdc.op = 'u'|'i'|'d'` row flows through the same `apply_transform` pipeline before landing in Parquet.
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: Phase I.5 README demo + completion log

README shows transform pipeline flow + WASM scalar UDF demo. Crate
map extended. Completion log scaffolded.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Appendix A — Troubleshooting

**`cargo test -p worker transform::composition::filter_then_mask_chains_correctly` fails with wasm_runtime errors.**
The composition test constructs a `WasmScalarRuntime` pointing at a tempdir; the scalar runtime requires `wasmtime` + `epoch ticker` to spin up. If this hangs, verify the Engine has `async_support + epoch_interruption + component_model` features (same as Phase I.3 source runtime).

**`filter` operator rejects `upper(name) = 'ALICE'`.**
Phase I.5 predicate grammar is intentionally minimal: `<col> IS [NOT] NULL | <col> = <literal> | <col> IN (...)`. Function calls and arithmetic are Phase IV (query engine). For `upper`, use a `wasm_scalar` operator that uppercases the column, then filter on the result.

**`mask` with `Hash` produces the same hex for duplicate inputs.**
Expected — hashing is deterministic (good for analytics joins on hashed columns). If you need salted or per-run unique hashes, a later `wasm_scalar` operator can apply a keyed hash; Phase I.5's `Hash` strategy is intentionally pure.

**Dead-letter file written but run still succeeds.**
Expected if `rows_rejected / rows_total` ≤ `dead_letter_threshold` (default 1%). Lower the threshold via `transform.dead_letter_threshold: 0.0` to fail on any rejection.

**Schema v2 recorded even though transform is unchanged.**
Post-transform schema is sensitive to the operator list. If you change the operator list without changing the source schema, you'll still see a new Schema version — the catalog tracks what lands at the destination, not the source.

## Appendix B — What's deferred to later phases

- Operators: `project`, `cast`, `rename`, dedupe, flatten, enrich-with-reference-data, full-feature validate (regex / range / format rules) — Phase I.5+
- Batch-crossing state (streaming dedupe, windowed aggregates) — Phase I.6 CDC / Phase IV
- Full-SQL predicate language (`CASE`, `COALESCE`, function calls) — Phase IV (query engine)
- Multi-input / fan-in / fan-out transforms — post-launch
- Aggregate operators (group_by, sum, count) — Phase IV
- WASM UDFs over non-Utf8 columns — Phase I.5+
- Column-level evolution overrides (ignore/freeze per field) — Phase I.6+
- Postgres-in-WASM — still deferred

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-23-phase-1-5-transformations.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**

---

## Phase I.5 Completion Log

Completed 2026-04-23 on branch `phase-1-5-transformations`.

- [x] Tasks 1-2 — `TransformSpec` + `Operator` tagged enum (Select, Filter, Mask, AddColumn, Validate, WasmScalar) + `LiteralValue` (Bool before Int for untagged serde) + 4 unit tests
- [x] Task 3 — `PipelineSpec` / `PipelineDslSpec` gain `#[serde(default)] transform: Option<TransformSpec>`
- [x] Tasks 4-6 — Subset-SQL predicate parser (`IS [NOT] NULL`, `= <literal>`, `IN (...)`), select/filter/mask/add_column operators (21 unit tests total)
- [x] Task 7 — `validate` operator: splits batch via BooleanArray masks into kept + rejected
- [x] Task 8 — `wasm_scalar` operator wired through a new `WasmScalarRuntime` (shared Engine + ticker + Component cache with `WasmSourceRuntime`)
- [x] Task 9 — `platform:udf/scalar-udf` WIT world (`log` imported; no http-fetch, no wall-clock, no randomness) under separate `connector-sdk/wit-scalar/` directory to keep bindgen per-world
- [x] Task 10 — Reference `examples/upper-case-scalar` guest (wasm32-wasip2)
- [x] Task 11 — CLI `connector build --kind source|scalar` dispatches to the right runtime for precompilation
- [x] Task 12 — `discover_stream` activity computes post-transform schema via `transform::derive_schema` before calling `record_and_resolve`
- [x] Task 13 — `read_batch` applies the transform and encodes kept + rejected IPC separately
- [x] Task 14 — `load_batch` writes dead-letter parquet and enforces cumulative `rows_rejected / rows_total` threshold (NonRetryable on exceed)
- [x] Task 14b — Workflow threads `transform` + running totals through DiscoverInput/ReadBatchInput/LoadBatchInput
- [x] Task 15 — Dead-letter path (`<base>/<pipeline_id>/dead-letter/<run_id>/batch-<seq>.parquet`) — folded into Task 14
- [x] Task 16 — Schema evolution tracks derived schema — folded into Task 12
- [x] Task 17 — `transforms_filter_mask_end_to_end` integration test
- [x] Task 18 — `transforms_dead_letter` integration test with two scenarios (under/over threshold); added `fail_run` activity + workflow error shim so Failed status is recorded
- [x] Task 19 — README section + this log

### Exit criterion — MET

- 6 of 10 declarative operators implemented MVP (select, filter, mask, add_column, validate, wasm_scalar). Deferred: project, cast, rename, dedupe, flatten.
- Each operator has a `derive_schema` method tested as a pure function (via unit tests in `crates/worker/src/transform/operators/`).
- Dead-letter parquet lands at the spec'd path, preserves original columns (verified by `transforms_dead_letter::validate_dead_letter_under_threshold_completes`), and is Parquet-readable.
- Phase I.4 schema-evolution flow continues: `discover_stream` computes derived schema first, then `record_and_resolve` stores it as the stream's current_schema_id.
- Backward-compat: existing pipelines without `transform` short-circuit the branch and behave identically (no regression in Phase I.2/I.3/I.4 integration tests).
- All 7 integration tests green (5 pre-existing + `transforms_filter_mask_end_to_end` + `transforms_dead_letter` × 2 scenarios).

### Deviations from the plan

- **Scalar-UDF WIT lives under `connector-sdk/wit-scalar/`, not `connector-sdk/wit/`.** `wit-bindgen` couldn't share a single directory with two world files. Separate directory isolates the worlds; the guest's `wit_bindgen::generate!` macro points at the new path.
- **`HostState` needs a second `Host` impl.** Each generated bindgen world has its own `host::Host` trait even when the interfaces are identical by name. Added a parallel `impl super::scalar_bindings::platform::udf::host::Host for HostState` alongside the existing connector one.
- **`arrow` Array trait imports.** `wasm_scalar.rs` needed explicit `use arrow::array::{Array, ArrayRef, StringArray, StringBuilder}` for `is_null()` / `value(i)` to resolve.
- **`batch.schema().column_with_name(...)` lifetime.** Temporary schema dropped while the downcast ref was still borrowed. Fixed by capturing `schema` into a local and reading `field.is_nullable()` / `field.data_type().clone()` into scalars before touching the array.
- **`LiteralValue` untagged ordering.** `Bool` variant must come before `Int`; otherwise serde matches `"true"` / `"false"` as Int first and fails.
- **`fail_run` wasn't in the plan** but turned out to be required for the over-threshold dead-letter test: without it, the workflow bubbles the NonRetryable up, but the `runs` row stays `running` forever. Added a `fail_run` activity and a Result-wrapping shim in `PipelineRunWorkflow::run` that invokes it on any error from the inner body.
- **`pipe.to_string()` vs `pipe.as_uuid().to_string()`.** IDs display with the `pipe-` prefix (Phase I.1 convention) but the loader writes to a UUID-only directory. Dead-letter test walked the wrong dir until it used `pipe.as_uuid().to_string()`.

### Handoff to Phase I.6

Phase I.6 (CDC / streaming) reuses:
- The transform DAG — CDC events land as `RecordBatch`es the same way batch reads do.
- The dead-letter path — CDC records that fail validation can land in the same subtree.
- `fail_run` — any error path in a streaming workflow now has a catalog-visible terminal state.

What Phase I.5 explicitly leaves open:
- Dedupe / flatten / project / cast / rename operators (stubs not checked in; add when a real pipeline needs them).
- Batch-crossing state for streaming dedupe — Phase I.6 CDC decides whether to put it in the transform layer or upstream.
- Full-SQL predicates (`CASE`, function calls, arithmetic) — Phase IV query engine.
