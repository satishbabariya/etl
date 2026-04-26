//! Embedded text template materialized by `platform connector create`.
//! Format: a sequence of "===FILE: <path>===\n<content>\n" blocks.
//! The CLI splits on "===FILE:" and writes each block to disk.

pub const SOURCE_TEMPLATE: &str = r#"===FILE: Cargo.toml===
[package]
name = "{{NAME}}"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.37"
arrow-schema = { version = "53", default-features = false }
arrow-ipc = { version = "53", default-features = false }

[profile.release]
strip = true
opt-level = "s"
lto = true

===FILE: README.md===
# {{NAME}}

A custom source connector for the ETL platform.

## Build & publish

```bash
platform connector test .
platform connector publish . --registry ./connectors
```

The published artifact lands at `./connectors/{{NAME}}@<version>/component.cwasm`.

===FILE: wit/source-connector.wit===
package platform:connector@0.1.0;

interface types {
    enum cursor-kind { int64, timestamp-tz }

    record cursor-value {
        kind: cursor-kind,
        value: string,
    }

    record connection-config {
        url: string,
    }

    record source-config {
        json: string,
    }

    record read-outcome {
        batch-ipc: list<u8>,
        rows: u32,
        new-cursor: option<cursor-value>,
        is-final: bool,
    }

    variant connector-error {
        invalid-config(string),
        source-unavailable(string),
        schema-incompatible(string),
        other(string),
    }
}

interface host {
    enum log-level { trace, debug, info, warn, error }
    log: func(level: log-level, message: string);

    record http-request {
        method: string,
        url: string,
        headers: list<tuple<string, string>>,
        body: option<list<u8>>,
    }
    record http-response {
        status: u16,
        headers: list<tuple<string, string>>,
        body: list<u8>,
    }
    http-fetch: func(request: http-request) -> result<http-response, string>;
}

world source-connector {
    use types.{connection-config, source-config, cursor-value, read-outcome, connector-error};
    import host;
    export discover: func(conn: connection-config, source: source-config) -> result<list<u8>, connector-error>;
    export read-batch: func(
        conn: connection-config,
        source: source-config,
        cursor: option<cursor-value>,
        batch-size: u32,
    ) -> result<read-outcome, connector-error>;
}

===FILE: src/lib.rs===
//! {{NAME}} — source connector skeleton.
//!
//! Implement `discover()` and `read_batch()` to make this connector
//! useful. The stub returns errors so a fresh skeleton compiles
//! cleanly without doing any I/O.

wit_bindgen::generate!({
    path: "wit",
    world: "source-connector",
});

use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

struct Component;

export!(Component);

fn schema() -> Schema {
    // TODO: replace with your source's columns.
    Schema::new(vec![Field::new("id", DataType::Int64, false)])
}

fn ipc_schema_bytes(s: &Schema) -> Result<Vec<u8>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, s)?;
        w.finish()?;
    }
    Ok(buf)
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        let s = schema();
        ipc_schema_bytes(&s).map_err(|e| ConnectorError::Other(format!("ipc: {e}")))
    }

    fn read_batch(
        _conn: ConnectionConfig,
        _source: SourceConfig,
        _cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        // TODO: implement. Return rows after the cursor; set is_final
        // when fewer than batch_size rows are available.
        let s = schema();
        let batch_ipc = ipc_schema_bytes(&s)
            .map_err(|e| ConnectorError::Other(format!("ipc: {e}")))?;
        Ok(ReadOutcome {
            batch_ipc,
            rows: 0,
            new_cursor: None,
            is_final: true,
        })
    }
}
"#;

/// Materialize `SOURCE_TEMPLATE` into a new directory at `target_dir`,
/// substituting `{{NAME}}` for the supplied connector name.
pub fn materialize_source_template(
    target_dir: &std::path::Path,
    name: &str,
) -> anyhow::Result<()> {
    use std::fs;
    if target_dir.exists() {
        anyhow::bail!("{} already exists", target_dir.display());
    }
    fs::create_dir_all(target_dir)?;
    let body = SOURCE_TEMPLATE.replace("{{NAME}}", name);
    let mut current_path: Option<std::path::PathBuf> = None;
    let mut current_buf = String::new();
    let flush =
        |path: Option<&std::path::Path>, buf: &str| -> anyhow::Result<()> {
            if let Some(p) = path {
                let abs = target_dir.join(p);
                if let Some(parent) = abs.parent() {
                    fs::create_dir_all(parent)?;
                }
                let content = buf.trim_end_matches('\n').to_string() + "\n";
                fs::write(abs, content)?;
            }
            Ok(())
        };
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("===FILE: ") {
            let path = rest.trim_end_matches("===");
            flush(current_path.as_deref(), &current_buf)?;
            current_path = Some(std::path::PathBuf::from(path));
            current_buf.clear();
        } else if current_path.is_some() {
            current_buf.push_str(line);
            current_buf.push('\n');
        }
    }
    flush(current_path.as_deref(), &current_buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialize_creates_expected_files() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme-source");
        materialize_source_template(&target, "acme-source").unwrap();
        assert!(target.join("Cargo.toml").exists());
        assert!(target.join("README.md").exists());
        assert!(target.join("src/lib.rs").exists());
        let cargo = std::fs::read_to_string(target.join("Cargo.toml")).unwrap();
        assert!(cargo.contains("name = \"acme-source\""));
        let lib = std::fs::read_to_string(target.join("src/lib.rs")).unwrap();
        assert!(lib.contains("acme-source"));
        assert!(lib.contains("export!(Component);"));
    }

    #[test]
    fn materialize_refuses_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("acme");
        std::fs::create_dir_all(&target).unwrap();
        let err = materialize_source_template(&target, "acme").unwrap_err();
        assert!(format!("{err}").contains("already exists"));
    }
}
