//! Stub — Task 10 replaces this with the real scalar runtime.
use std::path::PathBuf;
use std::sync::Arc;

pub struct WasmScalarRuntime {
    _base_dir: PathBuf,
}

impl WasmScalarRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            _base_dir: base_dir.into(),
        }))
    }

    pub async fn apply(
        &self,
        _name_at_version: &str,
        _input: Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        anyhow::bail!("WasmScalarRuntime::apply stub — Task 10 replaces this")
    }
}
