//! `platform connector` subcommand handlers — create / build / test / publish.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug)]
struct Manifest {
    name: String,
    version: String,
    kind: String,
    sdk_version: String,
    sha256: String,
}

pub async fn create(
    name: String,
    kind: String,
    lang: String,
    out_dir: Option<String>,
) -> Result<()> {
    if kind != "source" {
        anyhow::bail!(
            "kind '{kind}' not supported (II.3.a/b only support 'source')"
        );
    }
    let target = match out_dir {
        Some(d) => PathBuf::from(d).join(&name),
        None => PathBuf::from(&name),
    };
    match lang.as_str() {
        "rust" => {
            connector_sdk::templates::materialize_source_template(&target, &name)
                .with_context(|| format!("creating {}", target.display()))?;
            println!("created Rust connector skeleton at {}", target.display());
            println!("next:");
            println!("  cd {}", target.display());
            println!("  # edit src/lib.rs to implement discover() and read_batch()");
            println!("  platform connector test .");
            println!("  platform connector publish . --registry ./connectors");
        }
        "typescript" | "ts" => {
            connector_sdk::templates::materialize_source_template_typescript(&target, &name)
                .with_context(|| format!("creating {}", target.display()))?;
            println!("created TypeScript connector skeleton at {}", target.display());
            println!("next:");
            println!("  cd {}", target.display());
            println!("  npm install");
            println!("  # edit src/connector.ts to implement discover() and readBatch()");
            println!("  platform connector test .");
            println!("  platform connector publish . --registry ./connectors");
        }
        other => anyhow::bail!(
            "unknown --lang: '{other}' (expected 'rust' or 'typescript')"
        ),
    }
    Ok(())
}

pub async fn build(
    path: String,
    name: Option<String>,
    version: Option<String>,
    out: String,
    kind: String,
) -> Result<()> {
    use crate::connector_build::{
        detect_lang, read_package_json_name_version, ts_wasm_artifact, Lang,
    };
    use std::process::Command as StdCommand;

    let crate_dir = PathBuf::from(&path);
    let lang = detect_lang(&crate_dir)?;

    let (pkg_name, pkg_version, wasm_path) = match lang {
        Lang::Rust => {
            let cargo_toml = crate_dir.join("Cargo.toml");
            let toml_text = std::fs::read_to_string(&cargo_toml)?;
            let n = name.unwrap_or_else(|| {
                read_toml_value(&toml_text, "name").unwrap_or_else(|| "connector".into())
            });
            let v = version.unwrap_or_else(|| {
                read_toml_value(&toml_text, "version").unwrap_or_else(|| "0.1.0".into())
            });
            let status = StdCommand::new("cargo")
                .current_dir(&crate_dir)
                .args(["build", "--release"])
                .status()?;
            if !status.success() {
                anyhow::bail!("guest build failed");
            }
            let wasm_name = format!("{}.wasm", n.replace('-', "_"));
            let wp = crate_dir
                .join("target")
                .join("wasm32-wasip2")
                .join("release")
                .join(&wasm_name);
            if !wp.exists() {
                anyhow::bail!(
                    "expected artifact not found at {} — check the crate's [lib] crate-type and package name",
                    wp.display()
                );
            }
            (n, v, wp)
        }
        Lang::TypeScript => {
            let (n0, v0) = read_package_json_name_version(&crate_dir)?;
            let pkg_name = name.unwrap_or(n0);
            let pkg_version = version.unwrap_or(v0);
            if !crate_dir.join("node_modules").exists() {
                let status = StdCommand::new("npm")
                    .args(["install", "--no-audit", "--no-fund"])
                    .current_dir(&crate_dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("npm install failed");
                }
            }
            let status = StdCommand::new("npm")
                .args(["run", "build", "--silent"])
                .current_dir(&crate_dir)
                .status()?;
            if !status.success() {
                anyhow::bail!("npm run build (jco componentize) failed");
            }
            let wp = ts_wasm_artifact(&crate_dir);
            if !wp.exists() {
                anyhow::bail!(
                    "expected {} after npm run build but it's missing",
                    wp.display()
                );
            }
            (pkg_name, pkg_version, wp)
        }
    };

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

pub async fn test(path: String) -> Result<()> {
    use crate::connector_build::{detect_lang, ts_wasm_artifact, Lang};
    use std::process::Command as StdCommand;

    let path = PathBuf::from(&path);
    let lang = detect_lang(&path)?;
    match lang {
        Lang::Rust => {
            println!("[1/2] cargo build --release --target wasm32-wasip2");
            let status = StdCommand::new("cargo")
                .args(["build", "--release", "--target", "wasm32-wasip2"])
                .current_dir(&path)
                .status()
                .context("running cargo build")?;
            if !status.success() {
                anyhow::bail!("cargo build failed");
            }
            println!("[2/2] cargo test (host-side unit tests)");
            let status = StdCommand::new("cargo")
                .args(["test"])
                .current_dir(&path)
                .status()
                .context("running cargo test")?;
            if !status.success() {
                anyhow::bail!("cargo test failed");
            }
        }
        Lang::TypeScript => {
            if !path.join("node_modules").exists() {
                println!("[1/3] npm install");
                let status = StdCommand::new("npm")
                    .args(["install", "--no-audit", "--no-fund"])
                    .current_dir(&path)
                    .status()
                    .context("running npm install (is node/npm on PATH?)")?;
                if !status.success() {
                    anyhow::bail!("npm install failed");
                }
            } else {
                println!("[1/3] node_modules present (skipping npm install)");
            }
            println!("[2/3] npm test (vitest)");
            let status = StdCommand::new("npm")
                .args(["test", "--silent"])
                .current_dir(&path)
                .status()
                .context("running npm test")?;
            if !status.success() {
                anyhow::bail!("npm test failed");
            }
            println!("[3/3] npm run build (jco componentize)");
            let status = StdCommand::new("npm")
                .args(["run", "build", "--silent"])
                .current_dir(&path)
                .status()
                .context("running npm run build (jco componentize)")?;
            if !status.success() {
                anyhow::bail!("jco componentize failed");
            }
            let wasm = ts_wasm_artifact(&path);
            if !wasm.exists() {
                anyhow::bail!(
                    "expected {} after build but it's missing",
                    wasm.display()
                );
            }
        }
    }
    println!("connector test: ok");
    Ok(())
}

pub async fn publish(
    path: String,
    registry: String,
    version: Option<String>,
) -> Result<()> {
    use crate::connector_build::{detect_lang, read_package_json_name_version, Lang};

    let path = PathBuf::from(&path);
    let lang = detect_lang(&path)?;
    let (name, default_version) = match lang {
        Lang::Rust => {
            let cargo_toml = std::fs::read_to_string(path.join("Cargo.toml"))?;
            let n = read_toml_value(&cargo_toml, "name")
                .context("connector Cargo.toml missing a [package].name")?;
            let v = read_toml_value(&cargo_toml, "version").unwrap_or_else(|| "0.0.0".into());
            (n, v)
        }
        Lang::TypeScript => read_package_json_name_version(&path)?,
    };
    let final_version = version.unwrap_or(default_version);

    build(
        path.to_string_lossy().to_string(),
        Some(name.clone()),
        Some(final_version.clone()),
        registry.clone(),
        "source".to_string(),
    )
    .await?;

    let target_dir = PathBuf::from(&registry).join(format!("{name}@{final_version}"));
    let cwasm_path = target_dir.join("component.cwasm");
    if !cwasm_path.exists() {
        anyhow::bail!(
            "expected built artifact at {} but it's missing",
            cwasm_path.display()
        );
    }
    let bytes = std::fs::read(&cwasm_path)?;
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(&bytes);
    let hash_hex = hex::encode(h.finalize());
    let manifest = Manifest {
        name: name.clone(),
        version: final_version.clone(),
        kind: "source".into(),
        sdk_version: "0.1.0".into(),
        sha256: hash_hex.clone(),
    };
    let manifest_yaml = serde_yaml::to_string(&manifest)?;
    std::fs::write(target_dir.join("manifest.yaml"), manifest_yaml)?;
    println!(
        "published {}@{} → {} (sha256={})",
        name,
        final_version,
        target_dir.display(),
        &hash_hex[..16]
    );
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
