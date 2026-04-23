//! Adversarial tests for the WASM runtime resource limits.

use super::engine::build_engine;
use std::sync::Arc;
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

fn empty_host_state() -> super::HostState {
    super::HostState::new(super::Limits::default())
}

#[tokio::test]
async fn fuel_exhaustion_traps_guest() {
    let engine = Arc::new(build_engine().unwrap());
    let wat = include_str!("tests/infinite_loop.wat");
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let component = Component::new(&engine, &wasm_bytes).unwrap();
    let linker: Linker<super::HostState> = Linker::new(&engine);
    let mut store = Store::new(&engine, empty_host_state());
    // Tiny budget so the infinite loop trips instantly.
    store.set_fuel(10_000).unwrap();
    let instance = linker
        .instantiate_async(&mut store, &component)
        .await
        .unwrap();
    let spin = instance
        .get_typed_func::<(), ()>(&mut store, "spin")
        .unwrap();
    let err = spin.call_async(&mut store, ()).await.unwrap_err();
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("fuel") || msg.contains("trap"),
        "expected fuel/trap error, got: {msg}"
    );
}
