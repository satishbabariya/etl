//! Schema evolution: fingerprint → diff → policy.
pub mod diff;
pub mod fingerprint;
pub mod policy;

pub use diff::diff_schemas;
pub use fingerprint::fingerprint_schema;
pub use policy::{apply_policy, PolicyOutcome};
