use chrono::{Duration, Utc};
use common_types::auth::Role;
use common_types::ids::{PrincipalId, TenantId};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub tenant_id: String,
    pub role: Role,
    pub exp: i64,
    pub iat: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct Principal {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub role: Role,
}

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("invalid JWT: {0}")]
    InvalidJwt(String),
    #[error("malformed sub claim: {0}")]
    BadSub(String),
    #[error("malformed tenant claim: {0}")]
    BadTenant(String),
}

pub struct JwtIssuer {
    key: EncodingKey,
    ttl_seconds: i64,
}

impl JwtIssuer {
    pub fn new(secret: &[u8], ttl_seconds: i64) -> Self {
        Self { key: EncodingKey::from_secret(secret), ttl_seconds }
    }

    pub fn issue(&self, p: Principal) -> anyhow::Result<String> {
        let now = Utc::now();
        let claims = Claims {
            sub: p.principal_id.to_string(),
            tenant_id: p.tenant_id.to_string(),
            role: p.role,
            iat: now.timestamp(),
            exp: (now + Duration::seconds(self.ttl_seconds)).timestamp(),
        };
        let token = encode(&Header::default(), &claims, &self.key)?;
        Ok(token)
    }
}

pub struct JwtVerifier {
    key: DecodingKey,
}

impl JwtVerifier {
    pub fn new(secret: &[u8]) -> Self {
        Self { key: DecodingKey::from_secret(secret) }
    }

    pub fn verify(&self, token: &str) -> Result<Principal, AuthError> {
        let data = decode::<Claims>(token, &self.key, &Validation::default())
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
        Ok(Principal {
            principal_id,
            tenant_id,
            role: data.claims.role,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_principal() -> Principal {
        Principal {
            principal_id: PrincipalId::new(),
            tenant_id: TenantId::new(),
            role: Role::Operator,
        }
    }

    #[test]
    fn issue_then_verify_roundtrips() {
        let secret = b"test-secret-test-secret-test-secret";
        let p = fake_principal();
        let token = JwtIssuer::new(secret, 3600).issue(p).unwrap();
        let back = JwtVerifier::new(secret).verify(&token).unwrap();
        assert_eq!(back.principal_id, p.principal_id);
        assert_eq!(back.tenant_id, p.tenant_id);
        assert_eq!(back.role, p.role);
    }

    #[test]
    fn wrong_secret_rejects() {
        let token = JwtIssuer::new(b"a-secret-a-secret", 3600)
            .issue(fake_principal())
            .unwrap();
        assert!(JwtVerifier::new(b"different-secret-different")
            .verify(&token)
            .is_err());
    }

    #[test]
    fn expired_token_rejects() {
        // jsonwebtoken's default Validation has 60s leeway; pick a ttl
        // far enough in the past to clear it.
        let token = JwtIssuer::new(b"k-k-k-k-k-k-k-k-k-k", -3600)
            .issue(fake_principal())
            .unwrap();
        let err = JwtVerifier::new(b"k-k-k-k-k-k-k-k-k-k")
            .verify(&token)
            .unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("expired"));
    }
}
