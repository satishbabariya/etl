# Connector SDK guide

Build a custom source connector for the ETL platform in 5 steps.

## 1. Scaffold

```bash
platform connector create my-source
cd my-source
```

This creates `Cargo.toml`, `README.md`, `wit/source-connector.wit`, and `src/lib.rs` with a stub implementation. The stub compiles and returns an empty Arrow batch — replace `discover()` and `read_batch()` with real code.

## 2. Implement `discover`

`discover` introspects your source and returns Arrow IPC schema bytes:

```rust
fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
    use arrow_schema::{DataType, Field, Schema};
    use arrow_ipc::writer::StreamWriter;
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| ConnectorError::Other(format!("schema writer: {e}")))?;
        w.finish()
            .map_err(|e| ConnectorError::Other(format!("schema finish: {e}")))?;
    }
    Ok(buf)
}
```

## 3. Implement `read_batch`

`read_batch` returns up to `batch_size` rows after `cursor`. Always return `is_final = true` when fewer than `batch_size` rows are available. The batch must encode the same schema `discover()` returned.

See `examples/hello-world-source/src/lib.rs` for a complete reference implementation that emits three fixed rows.

## 4. Test locally

```bash
platform connector test .
```

Runs `cargo build --release --target wasm32-wasip2` and `cargo test`. Both must succeed before `publish`. (If the wasm target isn't installed: `rustup target add wasm32-wasip2`.)

## 5. Publish

```bash
platform connector publish . --registry ./connectors
```

Produces `./connectors/my-source@0.1.0/component.cwasm` (precompiled wasmtime artifact) and a `manifest.yaml`:

```yaml
name: my-source
version: 0.1.0
kind: source
sdk_version: 0.1.0
sha256: <64-hex>
```

## Use the connector in a pipeline

Reference it from a `Connection` YAML:

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: my-source-conn
spec:
  connector_ref: wasm:my-source@0.1.0
  config:
    url: https://api.example.com
```

Set `ETL_CONNECTORS_DIR=./connectors` (default `./connectors`) so the worker finds the registry.

## SDK reference

`connector-sdk::SourceConnector` (Rust trait, host-side):

- `async fn discover(conn, source) -> SchemaRef` — return the source's Arrow schema.
- `async fn read_batch(conn, source, cursor, batch_size) -> ReadOutcome` — read after cursor.

`connector-sdk::test_harness::run_smoke` — author-side validation helper that exercises both methods against a fake source. Use it in your connector's integration tests:

```rust
let report = connector_sdk::test_harness::run_smoke(
    &my_connector, &conn, &source_spec, 100,
).await?;
assert!(report.batch_rows > 0);
```

## Future kinds

II.3.a only supports `source` connectors. `scalar` (transformation function) lands in II.3.b; `destination` (loader) in II.3.c.
