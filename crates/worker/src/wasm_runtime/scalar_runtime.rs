use anyhow::{Context, bail};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

use super::epoch::EpochTicker;
use super::host::HostState;
use super::scalar_bindings::ScalarUdf;

pub struct WasmScalarRuntime {
    engine: Arc<Engine>,
    linker: Linker<HostState>,
    cache: DashMap<String, Arc<Component>>,
    base_dir: PathBuf,
    #[allow(dead_code)]
    ticker: Arc<EpochTicker>,
}

impl WasmScalarRuntime {
    pub fn new(base_dir: impl Into<PathBuf>) -> anyhow::Result<Arc<Self>> {
        let engine = Arc::new(super::engine::build_engine()?);
        let ticker = EpochTicker::start(engine.clone());
        let mut linker: Linker<HostState> = Linker::new(&engine);
        super::scalar_bindings::platform::udf::host::add_to_linker::<
            _,
            wasmtime::component::HasSelf<HostState>,
        >(&mut linker, |s| s)
            .context("adding scalar UDF host.log to linker")?;
        Ok(Arc::new(Self {
            engine,
            linker,
            cache: DashMap::new(),
            base_dir: base_dir.into(),
            ticker,
        }))
    }

    pub fn artifact_path(&self, name_at_version: &str) -> PathBuf {
        let mut p = self.base_dir.clone();
        p.push(name_at_version);
        p.push("component.cwasm");
        p
    }

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
