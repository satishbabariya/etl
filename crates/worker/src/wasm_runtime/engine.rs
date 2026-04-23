use anyhow::Context;
use wasmtime::{Config, Engine};

/// Build a wasmtime Engine configured for Phase I.3:
/// - Component Model enabled
/// - Async support (host functions can await)
/// - Fuel consumption (bounds CPU per invocation)
/// - Epoch interruption (bounds wall-time per invocation)
pub fn build_engine() -> anyhow::Result<Engine> {
    let mut config = Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    config.epoch_interruption(true);
    config.wasm_component_model(true);
    config.cranelift_opt_level(wasmtime::OptLevel::Speed);
    Engine::new(&config).context("building wasmtime Engine")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_builds() {
        let _engine = build_engine().expect("engine builds with fuel + epoch + component-model");
    }
}
