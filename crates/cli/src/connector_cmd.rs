//! `platform connector` subcommand handlers — create / build / test / publish.

use anyhow::{Context, Result};
use std::path::PathBuf;

pub async fn create(name: String, kind: String, out_dir: Option<String>) -> Result<()> {
    if kind != "source" {
        anyhow::bail!(
            "kind '{kind}' not supported (II.3.a only supports 'source'; \
             scalar/destination land in II.3.b/c)"
        );
    }
    let target = match out_dir {
        Some(d) => PathBuf::from(d).join(&name),
        None => PathBuf::from(&name),
    };
    connector_sdk::templates::materialize_source_template(&target, &name)
        .with_context(|| format!("creating {}", target.display()))?;
    println!("created connector skeleton at {}", target.display());
    println!("next:");
    println!("  cd {}", target.display());
    println!("  # edit src/lib.rs to implement discover() and read_batch()");
    println!("  platform connector test .");
    println!("  platform connector publish . --registry ./connectors");
    Ok(())
}

pub async fn build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
    kind: String,
) -> Result<()> {
    use std::process::Command as StdCommand;

    let crate_dir = PathBuf::from(&path);
    let cargo_toml = crate_dir.join("Cargo.toml");
    if !cargo_toml.exists() {
        anyhow::bail!("no Cargo.toml at {}", cargo_toml.display());
    }

    let toml_text = std::fs::read_to_string(&cargo_toml)?;
    let pkg_name = name.unwrap_or_else(|| {
        read_toml_value(&toml_text, "name").unwrap_or_else(|| "connector".into())
    });
    let pkg_version = version.unwrap_or_else(|| {
        read_toml_value(&toml_text, "version").unwrap_or_else(|| "0.1.0".into())
    });

    let status = StdCommand::new("cargo")
        .current_dir(&crate_dir)
        .args(["build", "--release"])
        .status()?;
    if !status.success() {
        anyhow::bail!("guest build failed");
    }

    let wasm_name = format!("{}.wasm", pkg_name.replace('-', "_"));
    let wasm_path = crate_dir
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join(&wasm_name);
    if !wasm_path.exists() {
        anyhow::bail!(
            "expected artifact not found at {} — check the crate's [lib] crate-type and package name",
            wasm_path.display()
        );
    }

    let out_dir = PathBuf::from(&out);
    let target_name = format!("{}@{}", pkg_name, pkg_version);

    let out_path = match kind.as_str() {
        "source" => {
            let rt = worker::wasm_runtime::WasmSourceRuntime::new(&out_dir)?;
            let p = rt.artifact_path(&target_name);
            rt.precompile_to(&wasm_path, &p)?;
            p
        }
        "scalar" => {
            let rt = worker::wasm_runtime::WasmScalarRuntime::new(&out_dir)?;
            let p = rt.artifact_path(&target_name);
            rt.precompile_to(&wasm_path, &p)?;
            p
        }
        other => anyhow::bail!("unknown --kind: '{other}' (expected 'source' or 'scalar')"),
    };

    println!("built {} ({})", out_path.display(), kind);
    Ok(())
}

fn read_toml_value(text: &str, key: &str) -> Option<String> {
    let mut in_package = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_package = trimmed == "[package]";
            continue;
        }
        if !in_package {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix(&format!("{} = \"", key)) {
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
        }
    }
    None
}
