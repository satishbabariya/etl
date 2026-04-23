//! Resource limits applied per invocation.

use wasmtime::ResourceLimiter;

#[derive(Clone, Debug)]
pub struct Limits {
    pub fuel: u64,
    pub memory_bytes: u64,
    pub wall_time_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            fuel: 30_000_000_000,
            memory_bytes: 256 * 1024 * 1024,
            wall_time_secs: 60,
        }
    }
}

/// Enforces `memory_bytes` by denying `memory_growing` past the cap.
pub struct MemoryCap {
    pub max_bytes: u64,
}

impl ResourceLimiter for MemoryCap {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok((desired as u64) <= self.max_bytes)
    }
    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok(desired <= 100_000)
    }
}
