//! Adversarial tests for the WASM runtime resource limits.

use super::engine::build_engine;
use std::sync::Arc;
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

fn empty_host_state() -> super::HostState {
    super::HostState::new(super::Limits::default())
}

fn tight_memory_host_state(max_bytes: u64) -> super::HostState {
    let mut limits = super::Limits::default();
    limits.memory_bytes = max_bytes;
    super::HostState::new(limits)
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
    // Generous epoch deadline so interrupt doesn't fire (we're testing fuel).
    store.set_epoch_deadline(u64::MAX);
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

#[tokio::test]
async fn memory_cap_denies_large_growth() {
    let engine = Arc::new(build_engine().unwrap());
    let wat = include_str!("tests/memory_hog.wat");
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let component = Component::new(&engine, &wasm_bytes).unwrap();
    let linker: Linker<super::HostState> = Linker::new(&engine);
    // Tight cap: 2 pages = 128 KB. memory_hog tries to grow by 1024 pages (~64 MB).
    let mut store = Store::new(&engine, tight_memory_host_state(2 * 65536));
    store.set_fuel(1_000_000).unwrap();
    store.set_epoch_deadline(u64::MAX);
    store.limiter(|s: &mut super::HostState| &mut s.memory_limiter);
    let instance = linker
        .instantiate_async(&mut store, &component)
        .await
        .unwrap();
    let grow = instance
        .get_typed_func::<(), (i32,)>(&mut store, "grow")
        .unwrap();
    let (result,) = grow.call_async(&mut store, ()).await.unwrap();
    assert_eq!(result, -1, "memory.grow should return -1 when denied");
}

#[tokio::test]
async fn instantiation_fails_when_guest_imports_un_linked_function() {
    let engine = Arc::new(build_engine().unwrap());
    let wat = include_str!("tests/forbidden_import.wat");
    let wasm_bytes = wat::parse_str(wat).unwrap();
    // Component::new may even reject compilation if it can't resolve.
    // But typically compile succeeds and instantiate fails — either outcome
    // satisfies the "capability denied" commitment.
    match Component::new(&engine, &wasm_bytes) {
        Err(e) => {
            let msg = format!("{e:?}").to_lowercase();
            assert!(
                msg.contains("forbidden")
                    || msg.contains("unknown")
                    || msg.contains("unresolved")
                    || msg.contains("missing")
                    || msg.contains("import"),
                "expected import-related error, got: {msg}"
            );
            return;
        }
        Ok(component) => {
            let linker: Linker<super::HostState> = Linker::new(&engine);
            let mut store = Store::new(&engine, empty_host_state());
            store.set_fuel(1_000_000).unwrap();
            store.set_epoch_deadline(u64::MAX);
            let res = linker.instantiate_async(&mut store, &component).await;
            let err = match res {
                Ok(_) => panic!("expected instantiation to fail due to un-linked import"),
                Err(e) => e,
            };
            let msg = format!("{err:?}").to_lowercase();
            assert!(
                msg.contains("forbidden")
                    || msg.contains("unknown")
                    || msg.contains("unresolved")
                    || msg.contains("missing")
                    || msg.contains("import"),
                "expected capability-denial error mentioning the missing import, got: {msg}"
            );
        }
    }
}
