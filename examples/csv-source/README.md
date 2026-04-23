# csv-source — Phase I.3 reference WASM connector

Reads CSV text (inline in config) and emits row batches as Arrow IPC.

## Build

```bash
cd examples/csv-source
cargo build --release
# → target/wasm32-wasip2/release/csv_source.wasm
```

Then precompile via the platform CLI:

```bash
cargo run --bin platform -- connector build examples/csv-source
# → connectors/csv-source@0.1.0/component.cwasm
```

## Config

```json
{
  "csv_text": "id,name\n1,Alice\n2,Bob\n3,Carol\n",
  "has_header": true
}
```

Emits schema `[_row_index Int64, <csv_cols...> Utf8]`, cursor = row-index (Int64).
