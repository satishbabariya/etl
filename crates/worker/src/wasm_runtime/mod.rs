//! WebAssembly Component Model runtime for source connectors (Phase I.3).

pub mod bindings;
pub mod engine;
pub mod epoch;
pub mod host;
pub mod limits;

pub use engine::build_engine;
pub use epoch::EpochTicker;
pub use host::HostState;
pub use limits::{Limits, MemoryCap};
