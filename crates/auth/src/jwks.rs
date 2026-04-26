use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rsa::traits::PublicKeyParts;
use rsa::RsaPublicKey;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Jwk {
    pub kty: String,
    pub alg: String,
    #[serde(rename = "use")]
    pub use_: String,
    pub kid: String,
    pub n: String,
    pub e: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwkSet {
    pub keys: Vec<Jwk>,
}

pub fn jwk_from_rsa_public(kid: &str, key: &RsaPublicKey) -> Jwk {
    let n_bytes = key.n().to_bytes_be();
    let e_bytes = key.e().to_bytes_be();
    Jwk {
        kty: "RSA".into(),
        alg: "RS256".into(),
        use_: "sig".into(),
        kid: kid.to_string(),
        n: URL_SAFE_NO_PAD.encode(n_bytes),
        e: URL_SAFE_NO_PAD.encode(e_bytes),
    }
}

pub fn jwks_from_keystore(ks: &crate::keystore::Keystore) -> Result<JwkSet> {
    let mut keys = Vec::new();
    for kid in ks.list_kids()? {
        let pubk = ks.load_public(&kid)?;
        keys.push(jwk_from_rsa_public(&kid, &pubk));
    }
    Ok(JwkSet { keys })
}

/// Remote JWKS source with a 10-minute in-memory cache.
pub struct RemoteJwks {
    url: String,
    client: reqwest::Client,
    cache: RwLock<Option<(JwkSet, Instant)>>,
    ttl: Duration,
}

impl RemoteJwks {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: reqwest::Client::new(),
            cache: RwLock::new(None),
            ttl: Duration::from_secs(600),
        }
    }

    pub async fn get(&self) -> Result<JwkSet> {
        {
            let g = self.cache.read().unwrap();
            if let Some((set, t)) = g.as_ref() {
                if t.elapsed() < self.ttl {
                    return Ok(set.clone());
                }
            }
        }
        let resp = self.client.get(&self.url).send().await?.error_for_status()?;
        let set: JwkSet = resp.json().await.context("parsing JWKS JSON")?;
        *self.cache.write().unwrap() = Some((set.clone(), Instant::now()));
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keystore::Keystore;

    #[test]
    fn keystore_emits_jwks_with_modulus_and_exponent() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        assert_eq!(set.keys.len(), 1);
        let jwk = &set.keys[0];
        assert_eq!(jwk.kid, kid);
        assert_eq!(jwk.kty, "RSA");
        assert_eq!(jwk.alg, "RS256");
        assert_eq!(jwk.use_, "sig");
        assert_eq!(jwk.e, "AQAB");
        assert!(jwk.n.len() > 100);
    }
}
