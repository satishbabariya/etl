//! Snowflake key-pair JWT (SQL API v2, KEYPAIR_JWT token type).
//!
//! iss = `<ACCOUNT>.<USER>.SHA256:<b64(sha256(der(pubkey)))>`,
//! sub = `<ACCOUNT>.<USER>`; account/user uppercased, region suffix
//! stripped from the account identifier; RS256, 55-minute expiry.

use anyhow::Context;
use base64::Engine;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use sha2::{Digest, Sha256};

pub fn public_key_fingerprint(private_key_pem: &str) -> anyhow::Result<String> {
    let key = rsa::RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .context("parsing PKCS#8 private key")?;
    let der = key
        .to_public_key()
        .to_public_key_der()
        .context("encoding public key DER")?;
    let hash = Sha256::digest(der.as_bytes());
    Ok(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD.encode(hash)
    ))
}

#[derive(serde::Serialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: i64,
    exp: i64,
}

pub struct KeyPairAuth {
    issuer: String,
    subject: String,
    key: jsonwebtoken::EncodingKey,
}

impl KeyPairAuth {
    pub fn from_pem(account: &str, user: &str, pem: &str) -> anyhow::Result<Self> {
        // Account identifier: uppercase, strip any region suffix after '.'.
        let account = account.split('.').next().unwrap_or(account).to_uppercase();
        let user = user.to_uppercase();
        let fp = public_key_fingerprint(pem)?;
        Ok(Self {
            issuer: format!("{account}.{user}.{fp}"),
            subject: format!("{account}.{user}"),
            key: jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
                .context("loading RSA key for JWT signing")?,
        })
    }

    pub fn token(&self) -> anyhow::Result<String> {
        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            iss: self.issuer.clone(),
            sub: self.subject.clone(),
            iat: now,
            exp: now + 55 * 60,
        };
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &self.key,
        )
        .context("signing snowflake JWT")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn test_pem() -> String {
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rand::thread_rng();
        let key = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    #[test]
    fn fingerprint_is_stable_sha256() {
        let pem = test_pem();
        let fp1 = public_key_fingerprint(&pem).unwrap();
        let fp2 = public_key_fingerprint(&pem).unwrap();
        assert_eq!(fp1, fp2);
        assert!(fp1.starts_with("SHA256:"));
    }

    #[test]
    fn token_claims_shape() {
        let pem = test_pem();
        let auth = KeyPairAuth::from_pem("xy12345.eu-central-1", "loader_user", &pem).unwrap();
        let tok = auth.token().unwrap();
        // decode without verification to inspect claims
        let payload_b64 = tok.split('.').nth(1).unwrap();
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload_b64)
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(v["sub"], "XY12345.LOADER_USER");
        assert!(v["iss"]
            .as_str()
            .unwrap()
            .starts_with("XY12345.LOADER_USER.SHA256:"));
        assert!(v["exp"].as_i64().unwrap() > v["iat"].as_i64().unwrap());
    }
}
