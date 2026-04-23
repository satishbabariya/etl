use serde::{Deserialize, Serialize};

/// BLAKE3-hex fingerprint of a normalized Arrow schema.
/// 64 lowercase hex chars (256-bit).
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaFingerprint(String);

impl SchemaFingerprint {
    pub fn from_hex(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_hex(&self) -> &str {
        &self.0
    }
    pub fn into_hex(self) -> String {
        self.0
    }
}

impl std::fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_via_serde() {
        let f = SchemaFingerprint::from_hex(
            "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
        );
        let j = serde_json::to_string(&f).unwrap();
        assert_eq!(
            j,
            "\"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234\""
        );
        let back: SchemaFingerprint = serde_json::from_str(&j).unwrap();
        assert_eq!(back, f);
    }
}
