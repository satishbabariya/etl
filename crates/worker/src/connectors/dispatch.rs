//! Picks the right `SourceConnector` implementation from a `connector_ref`.
//!
//! - `"postgres@0.1.0"` → in-process Rust PostgresConnector
//! - `"wasm:<name>@<version>"` → WasmSourceConnector loading name@version
//! - `"wasm-cdc:<name>@<version>"` → same WasmSourceConnector, but the
//!   workflow it runs in (WasmCdcPipelineWorkflow) keeps polling
//!   indefinitely instead of stopping on empty batches. The component
//!   itself is identical; only the workflow shape differs.

use anyhow::{Context, bail};
use connector_sdk::SourceConnector;
use std::sync::Arc;

use crate::connectors::postgres::PostgresConnector;
use crate::wasm_runtime::{WasmSourceConnector, WasmSourceRuntime};

pub fn build_source_connector(
    connector_ref: &str,
    wasm_runtime: Option<Arc<WasmSourceRuntime>>,
) -> anyhow::Result<Box<dyn SourceConnector>> {
    let wasm_artifact = connector_ref
        .strip_prefix("wasm-cdc:")
        .or_else(|| connector_ref.strip_prefix("wasm:"));
    if let Some(rest) = wasm_artifact {
        let runtime = wasm_runtime
            .context("wasm connector requested but no WasmSourceRuntime provided")?;
        return Ok(Box::new(WasmSourceConnector::new(runtime, rest.to_string())));
    }
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
        drop(c);
    }

    #[test]
    fn unknown_ref_errors() {
        match build_source_connector("mystery@1.0", None) {
            Ok(_) => panic!("expected error for unknown ref"),
            Err(e) => assert!(e.to_string().contains("unknown connector_ref")),
        }
    }

    #[test]
    fn wasm_without_runtime_errors() {
        match build_source_connector("wasm:csv-source@0.1.0", None) {
            Ok(_) => panic!("expected error when runtime missing"),
            Err(e) => assert!(e.to_string().contains("no WasmSourceRuntime")),
        }
    }

    #[test]
    fn wasm_cdc_prefix_is_recognized() {
        match build_source_connector("wasm-cdc:mysql-cdc-rs@0.1.0", None) {
            Ok(_) => panic!("expected error when runtime missing"),
            Err(e) => assert!(
                e.to_string().contains("no WasmSourceRuntime"),
                "got: {e}"
            ),
        }
    }
}
