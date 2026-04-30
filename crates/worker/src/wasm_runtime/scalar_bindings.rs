//! Host-side Component Model bindings for scalar-udf.wit.

wasmtime::component::bindgen!({
    path: "../connector-sdk/wit-scalar",
    world: "scalar-udf",
    imports: { default: async | trappable },
    exports: { default: async },
});
