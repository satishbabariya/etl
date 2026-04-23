//! Background thread that ticks the wasmtime `Engine::increment_epoch()`
//! once per second. Each `Store` sets a deadline of `current + N` epochs
//! on construction; when the ticker crosses it, the guest traps with a
//! wall-time error.

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use wasmtime::Engine;

pub struct EpochTicker {
    engine: Arc<Engine>,
    _handle: thread::JoinHandle<()>,
}

impl EpochTicker {
    pub fn start(engine: Arc<Engine>) -> Arc<Self> {
        let engine_for_thread = engine.clone();
        let handle = thread::Builder::new()
            .name("wasm-epoch-ticker".into())
            .spawn(move || loop {
                thread::sleep(Duration::from_secs(1));
                engine_for_thread.increment_epoch();
            })
            .expect("spawning epoch ticker");
        Arc::new(Self {
            engine,
            _handle: handle,
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}
