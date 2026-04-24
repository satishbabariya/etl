# connector-sdk — authoring source connectors

This crate defines the Rust-side `SourceConnector` trait and the WIT
interface for the Component Model. Phase I.3 established the surface;
Phase I.6 added an LSN cursor kind for CDC. Era I exit adds this
walkthrough.

Three ways to write a connector:

1. **Rust, in-process.** Implement the `SourceConnector` trait
   directly — fastest to iterate, lives inside the worker binary.
2. **Rust, WASM Component Model.** Implement the WIT interface,
   compile to `wasm32-wasip2`, precompile with `platform connector
   build`. Sandboxed, network-isolated, hot-swappable.
3. **Any language with wit-bindgen support.** Same WIT — use that
   language's wit-bindgen.

Era I exit ships the Rust-WASM path as stable. The other two compile
but carry fewer guarantees.

## The interface (`wit/source-connector.wit`)

Two functions, four records:

```wit
package platform:connector@0.1.0;

interface types {
  record connection-config { url: string }
  record source-config      { json: string }
  enum cursor-kind { int64, timestamp-tz, lsn }
  record cursor-value { kind: cursor-kind, value: string }
  record read-outcome {
    batch-ipc: list<u8>,
    rows: u32,
    new-cursor: option<cursor-value>,
    is-final: bool,
  }
  variant connector-error {
    invalid-config(string),
    transient(string),
    other(string),
  }
}

interface connector {
  use types.{...};
  discover: func(conn: connection-config, source: source-config)
    -> result<list<u8>, connector-error>;
  read-batch: func(conn: connection-config, source: source-config,
                    cursor: option<cursor-value>, batch-size: u32)
    -> result<read-outcome, connector-error>;
}

world source-connector {
  import platform:connector-host/host;
  export platform:connector/connector;
}
```

- `discover` returns Arrow IPC **schema** bytes (header-only, no rows).
- `read-batch` returns up to `batch-size` rows **strictly after**
  `cursor`, plus a new cursor of the *last* row and an `is-final`
  flag the host uses to stop iterating.
- The host imports a tiny `host` interface — currently `log(level,
  message)` and `http-fetch` (sandboxed). See
  `crates/worker/src/wasm_runtime/host.rs`.

## Write your first connector in three steps

### 1. Scaffolding

```bash
mkdir -p examples/my-connector/src examples/my-connector/.cargo
cd examples/my-connector
```

`Cargo.toml`:

```toml
[package]
name = "my-connector"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.37"
arrow-array = { version = "53", default-features = false }
arrow-schema = { version = "53", default-features = false }
arrow-ipc    = { version = "53", default-features = false }
arrow-data   = { version = "53", default-features = false }
arrow-buffer = { version = "53", default-features = false }

[profile.release]
opt-level = "s"
lto = true
strip = true
```

`.cargo/config.toml`:

```toml
[build]
target = "wasm32-wasip2"
```

### 2. The guest

`src/lib.rs`:

```rust
use std::sync::Arc;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit",
    world: "source-connector",
});

use platform::connector::host::{log, LogLevel};
use platform::connector::types::CursorKind;

struct Component;
export!(Component);

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| ConnectorError::Other(e.to_string()))?;
            w.finish().map_err(|e| ConnectorError::Other(e.to_string()))?;
        }
        Ok(buf)
    }

    fn read_batch(
        _conn: ConnectionConfig,
        _source: SourceConfig,
        _cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        log(LogLevel::Info, "my-connector read_batch");
        // ... build a RecordBatch, serialize with StreamWriter, return.
        Err(ConnectorError::Other("not yet implemented".into()))
    }
}
```

See `examples/hello-world-source/src/lib.rs` for the full
end-to-end version — ~60 lines.

### 3. Build + register

```bash
# From the workspace root:
cargo run --bin platform -- connector build examples/my-connector
# → ./connectors/my-connector@0.1.0/component.cwasm

# Reference it from a pipeline via connector_ref = wasm:my-connector@0.1.0
```

## Capabilities you have

- `host.log(level, message)` — structured log lines scoped to
  `guest=true`. Safe to call freely.
- `host.http-fetch(req)` — async HTTP GET/POST with allow-listed
  hosts. See `http-fetch.wit`.

## Capabilities you *don't* have

- No random, no wall-clock — for reproducibility.
- No filesystem. Your `source-config.json` is the only input.
- No direct network — use `http-fetch`.

For the tighter scalar-UDF capability set, see
`crates/connector-sdk/wit-scalar/scalar-udf.wit` — it drops
`http-fetch` entirely (scalar UDFs must be pure).

## Cursor kinds

| `cursor_kind` | Format of `cursor_value` | Typical use |
|---|---|---|
| `int64` | Decimal string, parsed as `i64` | Auto-increment IDs, row-index |
| `timestamp_tz` | RFC-3339 timestamp | Last-updated timestamps |
| `lsn` | Postgres LSN, e.g. `"16/B374D848"` | CDC streaming (host-only) |

Don't use `lsn` in a WASM connector — CDC lives in the host-side
Postgres connector. `discover` / `read_batch` in WASM see only
`int64` or `timestamp_tz`.

## References

- `examples/hello-world-source/` — 3 rows, smallest viable example
- `examples/csv-source/` — real file input, cursor iteration, error handling
- `crates/worker/src/wasm_runtime/` — host-side bindings + resource
  limits (fuel, memory cap, epoch interruption)
- RFC-5 (WASM runtime), RFC-6 (connector protocol) in `docs/rfc/`
