use chrono::{Duration, Utc};
use common_types::auth::Role;
use common_types::ids::{PrincipalId, TenantId};
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::jwks::{Jwk, JwkSet, RemoteJwks};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub tenant_id: String,
    pub role: Role,
    pub exp: i64,
    pub iat: i64,
    pub iss: String,
    pub aud: String,
    pub jti: String,
}

#[derive(Clone, Copy, Debug)]
pub struct Principal {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub role: Role,
    pub jti: Uuid,
}

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("invalid JWT: {0}")]
    InvalidJwt(String),
    #[error("malformed sub claim: {0}")]
    BadSub(String),
    #[error("malformed tenant claim: {0}")]
    BadTenant(String),
    #[error("malformed jti claim: {0}")]
    BadJti(String),
    #[error("missing kid in JWT header")]
    MissingKid,
    #[error("kid {0} not found in JWKS")]
    UnknownKid(String),
    #[error("JWKS fetch failed: {0}")]
    JwksFetch(String),
}

/// Issuer side: holds a private key + metadata. Phase II.2.c keeps
/// HS256 alive for the dev seam; production uses RS256.
pub enum JwtIssuer {
    Hs256 {
        key: EncodingKey,
        ttl: i64,
        iss: String,
        aud: String,
    },
    Rs256 {
        key: EncodingKey,
        kid: String,
        ttl: i64,
        iss: String,
        aud: String,
    },
}

impl JwtIssuer {
    pub fn hs256(secret: &[u8], ttl: i64, iss: impl Into<String>, aud: impl Into<String>) -> Self {
        Self::Hs256 {
            key: EncodingKey::from_secret(secret),
            ttl,
            iss: iss.into(),
            aud: aud.into(),
        }
    }

    pub fn rs256_pem(
        private_pem: &str,
        kid: impl Into<String>,
        ttl: i64,
        iss: impl Into<String>,
        aud: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let key = EncodingKey::from_rsa_pem(private_pem.as_bytes())?;
        Ok(Self::Rs256 {
            key,
            kid: kid.into(),
            ttl,
            iss: iss.into(),
            aud: aud.into(),
        })
    }

    pub fn issue(
        &self,
        principal_id: PrincipalId,
        tenant_id: TenantId,
        role: Role,
    ) -> anyhow::Result<String> {
        let now = Utc::now();
        let jti = Uuid::now_v7();
        match self {
            Self::Hs256 { key, ttl, iss, aud } => {
                let claims = Claims {
                    sub: principal_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    role,
                    iat: now.timestamp(),
                    exp: (now + Duration::seconds(*ttl)).timestamp(),
                    iss: iss.clone(),
                    aud: aud.clone(),
                    jti: jti.to_string(),
                };
                Ok(encode(&Header::new(Algorithm::HS256), &claims, key)?)
            }
            Self::Rs256 { key, kid, ttl, iss, aud } => {
                let mut header = Header::new(Algorithm::RS256);
                header.kid = Some(kid.clone());
                let claims = Claims {
                    sub: principal_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    role,
                    iat: now.timestamp(),
                    exp: (now + Duration::seconds(*ttl)).timestamp(),
                    iss: iss.clone(),
                    aud: aud.clone(),
                    jti: jti.to_string(),
                };
                Ok(encode(&header, &claims, key)?)
            }
        }
    }
}

pub enum VerifierKeySource {
    Hs256(Vec<u8>),
    Jwks(RemoteJwks),
    JwksInline(JwkSet),
}

pub struct JwtVerifier {
    source: VerifierKeySource,
    expected_iss: Option<String>,
    expected_aud: Option<String>,
}

impl JwtVerifier {
    pub fn hs256(secret: &[u8]) -> Self {
        Self {
            source: VerifierKeySource::Hs256(secret.to_vec()),
            expected_iss: None,
            expected_aud: None,
        }
    }

    pub fn jwks_url(url: impl Into<String>) -> Self {
        Self {
            source: VerifierKeySource::Jwks(RemoteJwks::new(url)),
            expected_iss: None,
            expected_aud: None,
        }
    }

    pub fn jwks_inline(set: JwkSet) -> Self {
        Self {
            source: VerifierKeySource::JwksInline(set),
            expected_iss: None,
            expected_aud: None,
        }
    }

    pub fn with_issuer(mut self, iss: impl Into<String>) -> Self {
        self.expected_iss = Some(iss.into());
        self
    }
    pub fn with_audience(mut self, aud: impl Into<String>) -> Self {
        self.expected_aud = Some(aud.into());
        self
    }

    fn lookup_jwk<'a>(&self, set: &'a JwkSet, kid: &str) -> Option<&'a Jwk> {
        set.keys.iter().find(|k| k.kid == kid)
    }

    fn jwk_to_decoding_key(jwk: &Jwk) -> Result<DecodingKey, AuthError> {
        DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
            .map_err(|e| AuthError::InvalidJwt(format!("rsa components: {e}")))
    }

    pub async fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        let header = decode_header(token)
            .map_err(|e| AuthError::InvalidJwt(format!("header: {e}")))?;

        let (key, alg) = match &self.source {
            VerifierKeySource::Hs256(secret) => {
                (DecodingKey::from_secret(secret), Algorithm::HS256)
            }
            VerifierKeySource::Jwks(remote) => {
                let kid = header.kid.as_ref().ok_or(AuthError::MissingKid)?;
                let set = remote
                    .get()
                    .await
                    .map_err(|e| AuthError::JwksFetch(e.to_string()))?;
                let jwk = self
                    .lookup_jwk(&set, kid)
                    .ok_or_else(|| AuthError::UnknownKid(kid.clone()))?;
                (Self::jwk_to_decoding_key(jwk)?, Algorithm::RS256)
            }
            VerifierKeySource::JwksInline(set) => {
                let kid = header.kid.as_ref().ok_or(AuthError::MissingKid)?;
                let jwk = self
                    .lookup_jwk(set, kid)
                    .ok_or_else(|| AuthError::UnknownKid(kid.clone()))?;
                (Self::jwk_to_decoding_key(jwk)?, Algorithm::RS256)
            }
        };

        let mut validation = Validation::new(alg);
        if let Some(iss) = &self.expected_iss {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = &self.expected_aud {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        let data = decode::<Claims>(token, &key, &validation)
            .map_err(|e| AuthError::InvalidJwt(e.to_string()))?;
        let principal_id = data
            .claims
            .sub
            .parse::<PrincipalId>()
            .map_err(|e| AuthError::BadSub(format!("{e:?}")))?;
        let tenant_id = data
            .claims
            .tenant_id
            .parse::<TenantId>()
            .map_err(|e| AuthError::BadTenant(format!("{e:?}")))?;
        let jti = Uuid::parse_str(&data.claims.jti)
            .map_err(|e| AuthError::BadJti(format!("{e:?}")))?;
        Ok(Principal {
            principal_id,
            tenant_id,
            role: data.claims.role,
            jti,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwks::jwks_from_keystore;
    use crate::keystore::Keystore;
    use serial_test::serial;

    fn fake() -> (PrincipalId, TenantId) {
        (PrincipalId::new(), TenantId::new())
    }

    #[tokio::test]
    #[serial(env_master_key)]
    async fn rs256_round_trip_via_inline_jwks() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let private_pem = ks.private_pem(&kid).unwrap();
        let issuer = JwtIssuer::rs256_pem(
            &private_pem,
            &kid,
            3600,
            "https://etl.local",
            "etl-platform",
        )
        .unwrap();
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Operator).unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        let v = JwtVerifier::jwks_inline(set)
            .with_issuer("https://etl.local")
            .with_audience("etl-platform");
        let p = v.verify(&token).await.unwrap();
        assert_eq!(p.principal_id, pid);
        assert_eq!(p.tenant_id, tid);
        assert_eq!(p.role, Role::Operator);
    }

    #[tokio::test]
    #[serial(env_master_key)]
    async fn wrong_audience_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        let issuer = JwtIssuer::rs256_pem(
            &ks.private_pem(&kid).unwrap(),
            &kid,
            3600,
            "i",
            "good-aud",
        )
        .unwrap();
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Viewer).unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        let err = JwtVerifier::jwks_inline(set)
            .with_audience("wrong-aud")
            .verify(&token)
            .await
            .unwrap_err();
        let msg = format!("{err}").to_lowercase();
        assert!(msg.contains("audience") || msg.contains("aud"), "got: {msg}");
    }

    #[tokio::test]
    #[serial(env_master_key)]
    async fn unknown_kid_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let _kid = ks.init().unwrap();
        let issuer = JwtIssuer::rs256_pem(
            &ks.private_pem(&_kid).unwrap(),
            "fabricated-kid",
            3600,
            "i",
            "a",
        )
        .unwrap();
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Admin).unwrap();
        let set = jwks_from_keystore(&ks).unwrap();
        let err = JwtVerifier::jwks_inline(set).verify(&token).await.unwrap_err();
        assert!(matches!(err, AuthError::UnknownKid(_)));
    }

    #[tokio::test]
    async fn hs256_back_compat_round_trips() {
        let issuer = JwtIssuer::hs256(
            b"dev-secret-dev-secret-dev-secret",
            3600,
            "i",
            "etl-platform",
        );
        let (pid, tid) = fake();
        let token = issuer.issue(pid, tid, Role::Operator).unwrap();
        let v = JwtVerifier::hs256(b"dev-secret-dev-secret-dev-secret")
            .with_audience("etl-platform");
        let p = v.verify(&token).await.unwrap();
        assert_eq!(p.principal_id, pid);
    }
}
