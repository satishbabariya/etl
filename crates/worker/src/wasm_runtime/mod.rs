//! WebAssembly Component Model runtime for source connectors (Phase I.3).
//!
//! See `docs/rfc/RFC-0005-wasm-runtime.md` and
//! `crates/connector-sdk/wit/source-connector.wit`.

pub mod engine;

pub use engine::build_engine;
