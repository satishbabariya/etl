use crate::secrets::SecretRef;
use serde::{Deserialize, Serialize};

/// Connection parameters for a connector.
///
/// Phase II.2.a: either an inline `url` (legacy plaintext, kept for
/// backward-compat with pre-secrets pipelines) OR a `url_secret`
/// pointing at a SecretRef. Resolver prefers `url_secret` when both set.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url_secret: Option<SecretRef>,
}

/// Redacts `url`: it is either a connection URL with a password or, for
/// some WASM connectors, a bare API key.
impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("url", &self.url.as_deref().map(crate::redact::redact_url))
            .field("url_secret", &self.url_secret)
            .finish()
    }
}

impl ConnectionConfig {
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            url_secret: None,
        }
    }

    pub fn from_secret(r: SecretRef) -> Self {
        Self {
            url: None,
            url_secret: Some(r),
        }
    }

    /// Borrow the plaintext URL after resolution. Panics if neither `url`
    /// nor a previously-resolved value is present — connectors must only
    /// be called with a resolved config.
    pub fn expect_url(&self) -> &str {
        self.url
            .as_deref()
            .expect("ConnectionConfig must be resolved (url Some) before passing to a connector")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SecretId;
    use crate::secrets::SecretBackendKind;

    #[test]
    fn legacy_url_roundtrips() {
        let c = ConnectionConfig::from_url("postgres://x");
        let j = serde_json::to_string(&c).unwrap();
        assert_eq!(j, r#"{"url":"postgres://x"}"#);
        let back: ConnectionConfig = serde_json::from_str(&j).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn url_secret_roundtrips() {
        let r = SecretRef {
            secret_id: SecretId::new(),
            name: "pg-url".into(),
            backend: SecretBackendKind::File,
            key: "pg-url".into(),
        };
        let c = ConnectionConfig::from_secret(r.clone());
        let j = serde_json::to_string(&c).unwrap();
        let back: ConnectionConfig = serde_json::from_str(&j).unwrap();
        assert_eq!(back.url, None);
        assert_eq!(back.url_secret, Some(r));
    }

    #[test]
    fn debug_redacts_url_password() {
        let c = ConnectionConfig::from_url("postgres://etl:hunter2@db.internal/app");
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2"), "got {dbg}");
        assert!(dbg.contains("db.internal"), "got {dbg}");
    }

    #[test]
    fn debug_redacts_non_url_secret() {
        // WASM connectors may carry a bare API key in `url`.
        let c = ConnectionConfig::from_url("sk_test_hunter2");
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2"), "got {dbg}");
    }
}
