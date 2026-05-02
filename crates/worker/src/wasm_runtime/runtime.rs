//! WasmSourceRuntime: owns the wasmtime Engine, the linker, and a cache of
//! loaded Components keyed by "<name>@<version>". Thread-safe (Arc-wrapped
//! by callers).

use anyhow::{Context, bail};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::component::{Component, Linker};

use super::epoch::EpochTicker;
use super::host::HostState;

pub struct WasmSourceRuntime {
    engine: Arc<Engine>,
    linker: Linker<HostState>,
    cache: DashMap<String, Arc<Component>>,
    base_dir: PathBuf,
    ticker: Arc<EpochTicker>,
}

impl WasmSourceRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let engine = Arc::new(super::engine::build_engine()?);
        let ticker = EpochTicker::start(engine.clone());

        let mut linker: Linker<HostState> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .context("adding WASI 0.2 imports to linker")?;
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
            .context("adding WASI 0.2 http imports to linker")?;
        super::bindings::platform::connector::host::add_to_linker::<
            _,
            wasmtime::component::HasSelf<HostState>,
        >(&mut linker, |s| s)
            .context("adding host.log / host.http-fetch to linker")?;
        super::bindings::platform::connector::db::add_to_linker::<
            _,
            wasmtime::component::HasSelf<HostState>,
        >(&mut linker, |s| s)
            .context("adding db.* (open/query/close/...) to linker")?;

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

    pub fn linker(&self) -> &Linker<HostState> {
        &self.linker
    }

    pub fn ticker(&self) -> &Arc<EpochTicker> {
        &self.ticker
    }

    /// Path where the .cwasm for `<name>@<version>` should live.
    pub fn artifact_path(&self, name_at_version: &str) -> PathBuf {
        let mut p = self.base_dir.clone();
        p.push(name_at_version);
        p.push("component.cwasm");
        p
    }

    /// Load (and cache) the precompiled Component for `<name>@<version>`.
    pub fn load(&self, name_at_version: &str) -> anyhow::Result<Arc<Component>> {
        if let Some(c) = self.cache.get(name_at_version) {
            return Ok(c.clone());
        }
        let path = self.artifact_path(name_at_version);
        if !path.exists() {
            bail!(
                "WASM connector artifact not found at {} — did you `platform connector build`?",
                path.display()
            );
        }
        // SAFETY: deserialize_file accepts only artifacts we control under
        // self.base_dir. A malformed file could trigger UB.
        let component = unsafe {
            Component::deserialize_file(&self.engine, &path)
                .with_context(|| format!("deserialize_file {}", path.display()))?
        };
        let arc = Arc::new(component);
        self.cache.insert(name_at_version.to_string(), arc.clone());
        Ok(arc)
    }

    /// Precompile a `.wasm` to a `.cwasm` and write it to `out_path`.
    pub fn precompile_to(&self, wasm_path: &Path, out_path: &Path) -> anyhow::Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smallest possible valid component: an empty world.
    const EMPTY_COMPONENT_WAT: &str = "(component)";

    #[test]
    fn runtime_loads_from_tempdir_and_precompiles() {
        let tmp = tempfile::tempdir().unwrap();
        let rt = WasmSourceRuntime::new(tmp.path()).unwrap();

        let wasm_bytes = wat::parse_str(EMPTY_COMPONENT_WAT).unwrap();
        let wasm_path = tmp.path().join("empty.wasm");
        std::fs::write(&wasm_path, &wasm_bytes).unwrap();

        let cwasm_path = rt.artifact_path("empty@0.1.0");
        rt.precompile_to(&wasm_path, &cwasm_path).unwrap();
        assert!(cwasm_path.exists());

        let _component = rt.load("empty@0.1.0").unwrap();
        let _again = rt.load("empty@0.1.0").unwrap(); // cache hit
    }
}
