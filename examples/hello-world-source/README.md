# hello-world-source

Minimal WASM source connector — emits three constant rows on its first
`read_batch`, then EOF. Exercises every seam in the WIT world without
touching any external system.

## Schema

```text
id        Int64
greeting  Utf8
```

## Rows emitted

```text
(1, "hello")
(2, "world")
(3, "platform")
```

## Build

```bash
# 1. Compile the guest:
cd examples/hello-world-source
cargo build --release

# 2. Precompile for the host wasmtime:
cd ../..
cargo run --bin platform -- connector build examples/hello-world-source
# → ./connectors/hello-world-source@0.1.0/component.cwasm
```

## Use in a pipeline

```yaml
apiVersion: platform/v1
kind: Connection
metadata:
  name: hello-conn
spec:
  connector_ref: wasm:hello-world-source@0.1.0
  config: {}
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: hello-sync
spec:
  source:
    type: wasm
    config: {}
  destination:
    type: local_parquet
    base_path: ./data
  batch_size: 10
  evolution_policy: propagate_additive
```

## Where to go next

- `crates/connector-sdk/README.md` — full authoring tutorial
- `examples/csv-source/` — real file input, cursor iteration
