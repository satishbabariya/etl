use anyhow::{Context, Result};
use async_trait::async_trait;
use common_types::secrets::{PlaintextSecret, SecretRef};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use super::Secrets;

/// Reads from a JSON file: `{"<key>": "<plaintext>", ...}`.
/// File path defaults to `./.etl-secrets.json` and can be overridden
/// with `ETL_SECRETS_FILE`. Re-reads on every call (cheap, dev-only).
#[derive(Default)]
pub struct FileSecrets {
    path: PathBuf,
    cache: RwLock<HashMap<String, String>>,
}

impl FileSecrets {
    pub fn new() -> Self {
        let path = std::env::var("ETL_SECRETS_FILE")
            .unwrap_or_else(|_| ".etl-secrets.json".into())
            .into();
        Self::with_path(path)
    }

    pub fn with_path(path: PathBuf) -> Self {
        Self { path, cache: RwLock::new(HashMap::new()) }
    }

    fn load(&self) -> Result<HashMap<String, String>> {
        if !self.path.exists() {
            return Ok(HashMap::new());
        }
        let bytes = std::fs::read(&self.path)
            .with_context(|| format!("reading {}", self.path.display()))?;
        let map: HashMap<String, String> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} as JSON map", self.path.display()))?;
        Ok(map)
    }

    /// Write a key/value to the file. Used by `platform secret put`.
    pub fn put(&self, key: &str, value: &str) -> Result<()> {
        let mut map = self.load()?;
        map.insert(key.to_string(), value.to_string());
        let bytes = serde_json::to_vec_pretty(&map)?;
        std::fs::write(&self.path, bytes)
            .with_context(|| format!("writing {}", self.path.display()))?;
        if let Ok(mut c) = self.cache.write() {
            *c = map;
        }
        Ok(())
    }

    pub fn delete_key(&self, key: &str) -> Result<()> {
        let mut map = self.load()?;
        map.remove(key);
        let bytes = serde_json::to_vec_pretty(&map)?;
        std::fs::write(&self.path, bytes)
            .with_context(|| format!("writing {}", self.path.display()))?;
        if let Ok(mut c) = self.cache.write() {
            *c = map;
        }
        Ok(())
    }
}

#[async_trait]
impl Secrets for FileSecrets {
    async fn resolve(&self, r: &SecretRef) -> Result<PlaintextSecret> {
        let map = self.load()?;
        let v = map
            .get(&r.key)
            .with_context(|| {
                format!(
                    "file secret {} (key {}) not in {}",
                    r.name,
                    r.key,
                    self.path.display()
                )
            })?
            .clone();
        Ok(PlaintextSecret::new(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_types::ids::SecretId;
    use common_types::secrets::SecretBackendKind;

    fn r(name: &str, key: &str) -> SecretRef {
        SecretRef {
            secret_id: SecretId::new(),
            name: name.into(),
            backend: SecretBackendKind::File,
            key: key.into(),
        }
    }

    #[tokio::test]
    async fn file_put_then_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".etl-secrets.json");
        let fs = FileSecrets::with_path(path);
        fs.put("pg-url", "postgres://x").unwrap();
        let v = fs.resolve(&r("pg-url", "pg-url")).await.unwrap();
        assert_eq!(v.expose(), "postgres://x");
    }

    #[tokio::test]
    async fn file_missing_key_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".etl-secrets.json");
        let fs = FileSecrets::with_path(path);
        let err = fs.resolve(&r("nope", "nope")).await.unwrap_err();
        assert!(format!("{err}").contains("file secret nope"));
    }
}
