//! Refresh tokens — plaintext format `<token_id_uuid>.<secret>`.
//! The catalog stores `argon2(secret)`; on use we look up by token_id,
//! verify the secret, then DELETE the row and issue a new pair.

use anyhow::{Context, Result};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Duration, Utc};
use rand::RngCore;
use uuid::Uuid;

const REFRESH_TTL_DAYS: i64 = 30;

pub fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn hash(secret: &str) -> Result<String> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    let h = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("argon2: {e}"))?
        .to_string();
    Ok(h)
}

pub fn verify(secret: &str, hashed: &str) -> bool {
    let parsed = match PasswordHash::new(hashed) {
        Ok(p) => p,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(secret.as_bytes(), &parsed)
        .is_ok()
}

pub fn parse_plaintext(token: &str) -> Result<(Uuid, &str)> {
    let (id_str, secret) = token.split_once('.').context("malformed refresh token")?;
    let id = Uuid::parse_str(id_str)?;
    Ok((id, secret))
}

pub fn format_plaintext(token_id: Uuid, secret: &str) -> String {
    format!("{token_id}.{secret}")
}

pub fn mint() -> Result<(String, String, DateTime<Utc>)> {
    let secret = random_secret();
    let h = hash(&secret)?;
    let exp = Utc::now() + Duration::days(REFRESH_TTL_DAYS);
    Ok((secret, h, exp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_roundtrips() {
        let s = "super-secret-refresh-secret-blob";
        let h = hash(s).unwrap();
        assert!(verify(s, &h));
        assert!(!verify("wrong", &h));
    }

    #[test]
    fn parse_format_roundtrips() {
        let id = Uuid::now_v7();
        let s = "abc";
        let formatted = format_plaintext(id, s);
        let (back_id, back_s) = parse_plaintext(&formatted).unwrap();
        assert_eq!(back_id, id);
        assert_eq!(back_s, s);
    }

    #[test]
    fn random_secrets_are_unique() {
        let a = random_secret();
        let b = random_secret();
        assert_ne!(a, b);
    }
}
