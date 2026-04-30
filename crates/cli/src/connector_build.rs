//! Language-aware helpers for `platform connector build/test/publish`.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lang {
    Rust,
    TypeScript,
}

pub fn detect_lang(path: &Path) -> Result<Lang> {
    let has_cargo = path.join("Cargo.toml").exists();
    let has_pkg = path.join("package.json").exists();
    match (has_cargo, has_pkg) {
        (true, false) => Ok(Lang::Rust),
        (false, true) => Ok(Lang::TypeScript),
        (true, true) => Err(anyhow!(
            "{} contains both Cargo.toml and package.json — ambiguous",
            path.display()
        )),
        (false, false) => Err(anyhow!(
            "{} is neither a cargo crate nor an npm package (no Cargo.toml or package.json)",
            path.display()
        )),
    }
}

pub fn read_package_json_name_version(path: &Path) -> Result<(String, String)> {
    let text = std::fs::read_to_string(path.join("package.json"))
        .with_context(|| format!("reading {}", path.join("package.json").display()))?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parsing {}", path.join("package.json").display()))?;
    let name = v
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow!("package.json: missing string field `name`"))?
        .to_string();
    let version = v
        .get("version")
        .and_then(|n| n.as_str())
        .unwrap_or("0.0.0")
        .to_string();
    Ok((name, version))
}

pub fn ts_wasm_artifact(path: &Path) -> PathBuf {
    path.join("dist").join("connector.wasm")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn detect_rust() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("Cargo.toml"), "[package]\nname=\"x\"\n").unwrap();
        assert_eq!(detect_lang(d.path()).unwrap(), Lang::Rust);
    }

    #[test]
    fn detect_ts() {
        let d = tempdir().unwrap();
        std::fs::write(
            d.path().join("package.json"),
            r#"{"name":"x","version":"0.1.0"}"#,
        )
        .unwrap();
        assert_eq!(detect_lang(d.path()).unwrap(), Lang::TypeScript);
    }

    #[test]
    fn detect_ambiguous() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("Cargo.toml"), "").unwrap();
        std::fs::write(d.path().join("package.json"), "{}").unwrap();
        let err = detect_lang(d.path()).unwrap_err();
        assert!(format!("{err}").contains("ambiguous"));
    }

    #[test]
    fn detect_neither() {
        let d = tempdir().unwrap();
        let err = detect_lang(d.path()).unwrap_err();
        assert!(format!("{err}").contains("neither"));
    }

    #[test]
    fn package_json_name_version_ok() {
        let d = tempdir().unwrap();
        std::fs::write(
            d.path().join("package.json"),
            r#"{"name":"acme","version":"1.2.3"}"#,
        )
        .unwrap();
        let (n, v) = read_package_json_name_version(d.path()).unwrap();
        assert_eq!(n, "acme");
        assert_eq!(v, "1.2.3");
    }

    #[test]
    fn package_json_missing_version_defaults() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("package.json"), r#"{"name":"acme"}"#).unwrap();
        let (n, v) = read_package_json_name_version(d.path()).unwrap();
        assert_eq!(n, "acme");
        assert_eq!(v, "0.0.0");
    }
}
