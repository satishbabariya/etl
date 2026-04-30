//! Host-side Component Model bindings for source-connector.wit.

wasmtime::component::bindgen!({
    path: "../connector-sdk/wit",
    world: "source-connector",
    imports: { default: async | trappable },
    exports: { default: async },
});
