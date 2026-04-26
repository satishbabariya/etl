//! Envelope encryption: ETL_MASTER_KEY (32-byte hex string) is the
//! XChaCha20-Poly1305 key. Each `seal()` generates a fresh 24-byte
//! nonce. On-disk format: `nonce(24) || aead_output`.

use anyhow::{anyhow, Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;

pub const NONCE_LEN: usize = 24;
pub const KEY_LEN: usize = 32;

pub fn parse_master_key(hex_str: &str) -> Result<[u8; KEY_LEN]> {
    let bytes = hex::decode(hex_str.trim()).context("ETL_MASTER_KEY must be hex")?;
    if bytes.len() != KEY_LEN {
        return Err(anyhow!(
            "ETL_MASTER_KEY must decode to {} bytes (got {})",
            KEY_LEN,
            bytes.len()
        ));
    }
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub fn master_key_from_env() -> Result<[u8; KEY_LEN]> {
    let s = std::env::var("ETL_MASTER_KEY").context(
        "ETL_MASTER_KEY env var not set — provide a 32-byte hex string \
         (`openssl rand -hex 32` to generate)",
    )?;
    parse_master_key(&s)
}

pub fn seal(plaintext: &[u8], key: &[u8; KEY_LEN]) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(key.into());
    let mut nonce = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let xn = XNonce::from_slice(&nonce);
    let ct = cipher
        .encrypt(xn, plaintext)
        .map_err(|e| anyhow!("seal: aead encrypt: {e}"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn unseal(blob: &[u8], key: &[u8; KEY_LEN]) -> Result<Vec<u8>> {
    if blob.len() < NONCE_LEN + 16 {
        return Err(anyhow!("sealed blob too short ({} bytes)", blob.len()));
    }
    let cipher = XChaCha20Poly1305::new(key.into());
    let xn = XNonce::from_slice(&blob[..NONCE_LEN]);
    let ct = &blob[NONCE_LEN..];
    cipher
        .decrypt(xn, ct)
        .map_err(|e| anyhow!("unseal: aead decrypt failed (wrong master key?): {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for i in 0..32 {
            k[i] = i as u8;
        }
        k
    }

    #[test]
    fn parse_master_key_accepts_64_hex_chars() {
        let s = "00".repeat(32);
        let k = parse_master_key(&s).unwrap();
        assert_eq!(k, [0u8; 32]);
    }

    #[test]
    fn parse_master_key_rejects_wrong_length() {
        let err = parse_master_key("aabb").unwrap_err();
        assert!(format!("{err}").contains("32 bytes"));
    }

    #[test]
    fn parse_master_key_rejects_non_hex() {
        let err = parse_master_key("zzzz").unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("hex"));
    }

    #[test]
    fn seal_then_unseal_roundtrips() {
        let k = key();
        let plaintext = b"-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANB...";
        let sealed = seal(plaintext, &k).unwrap();
        assert_ne!(&sealed[..], &plaintext[..]);
        let back = unseal(&sealed, &k).unwrap();
        assert_eq!(back, plaintext);
    }

    #[test]
    fn unseal_with_wrong_key_fails() {
        let k = key();
        let mut wrong = key();
        wrong[0] ^= 0xff;
        let sealed = seal(b"data", &k).unwrap();
        assert!(unseal(&sealed, &wrong).is_err());
    }

    #[test]
    fn unseal_short_blob_errors() {
        let k = key();
        assert!(unseal(b"too-short", &k).is_err());
    }

    #[test]
    fn each_seal_uses_fresh_nonce() {
        let k = key();
        let a = seal(b"x", &k).unwrap();
        let b = seal(b"x", &k).unwrap();
        assert_ne!(a, b);
    }
}
