//! Secret reference + plaintext wrapper (RFC-11).
//!
//! `SecretRef` is what the catalog stores — an opaque pointer.
//! `PlaintextSecret` wraps the resolved value and scrubs it on drop.
//! Plaintexts MUST never serialize or log; the type doesn't derive
//! `Serialize` and its custom `Debug` redacts.

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::ids::SecretId;

/// Backend that holds the plaintext value of a secret. Phase II.2.a
/// supports env-var and file backends. Phase II.2.b adds Vault.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretBackendKind {
    Env,
    File,
    Vault,
}

/// Opaque reference to a secret. Stored in catalog rows. Resolves at
/// runtime via the worker's `Secrets` backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    pub secret_id: SecretId,
    pub name: String,
    pub backend: SecretBackendKind,
    pub key: String,
}

/// Resolved plaintext. Zeroes on drop. Construct only from a backend
/// resolve(). Never derive Serialize.
pub struct PlaintextSecret(Zeroizing<String>);

impl PlaintextSecret {
    pub fn new(s: String) -> Self {
        Self(Zeroizing::new(s))
    }

    /// Borrow the plaintext for the duration of the call. Callers must
    /// not clone the &str past this scope.
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for PlaintextSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlaintextSecret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_plaintext() {
        let p = PlaintextSecret::new("super-secret-value".into());
        let dbg = format!("{p:?}");
        assert!(!dbg.contains("super-secret"));
        assert!(dbg.contains("<redacted>"));
    }

    #[test]
    fn expose_returns_plaintext_within_scope() {
        let p = PlaintextSecret::new("hello".into());
        assert_eq!(p.expose(), "hello");
    }

    #[test]
    fn secret_ref_roundtrips_json() {
        let r = SecretRef {
            secret_id: SecretId::new(),
            name: "pg-url".into(),
            backend: SecretBackendKind::File,
            key: "pg-url".into(),
        };
        let j = serde_json::to_string(&r).unwrap();
        let back: SecretRef = serde_json::from_str(&j).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn secret_backend_kind_serializes_snake_case() {
        let j = serde_json::to_string(&SecretBackendKind::File).unwrap();
        assert_eq!(j, "\"file\"");
    }
}
