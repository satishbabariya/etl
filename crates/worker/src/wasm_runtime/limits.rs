//! Resource limits applied per invocation. Fleshed out in Task 5.

#[derive(Clone, Debug)]
pub struct Limits {
    pub fuel: u64,
    pub memory_bytes: u64,
    pub wall_time_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        // RFC-5 Phase I.3 defaults.
        Self {
            fuel: 30_000_000_000,
            memory_bytes: 256 * 1024 * 1024,
            wall_time_secs: 60,
        }
    }
}
