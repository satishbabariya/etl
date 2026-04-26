use anyhow::{Context, Result};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::path::{Path, PathBuf};

use crate::sealed;

/// On-disk key state: each kid is its own subdirectory containing
/// `private.pem` and `public.pem`. The active kid (the one used for
/// signing) is recorded in `active.txt`.
pub struct Keystore {
    root: PathBuf,
}

impl Keystore {
    pub fn open(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Generate a new 2048-bit RSA keypair, write under
    /// `<root>/<kid>/{private.{pem,enc}, public.pem}`, and mark active.
    /// Sealed iff `ETL_MASTER_KEY` is set; otherwise writes plaintext
    /// `private.pem` (legacy / dev seam).
    pub fn init(&self) -> Result<String> {
        std::fs::create_dir_all(&self.root)?;
        let kid = uuid::Uuid::now_v7().simple().to_string();
        let dir = self.root.join(&kid);
        std::fs::create_dir_all(&dir)?;
        let mut rng = rand::thread_rng();
        let private = RsaPrivateKey::new(&mut rng, 2048)
            .context("generating RSA-2048 keypair")?;
        let public = RsaPublicKey::from(&private);
        let pkcs8 = private.to_pkcs8_pem(LineEnding::LF)?.to_string();
        let spki = public.to_public_key_pem(LineEnding::LF)?;
        if let Ok(master_hex) = std::env::var("ETL_MASTER_KEY") {
            let key = sealed::parse_master_key(&master_hex)?;
            let blob = sealed::seal(pkcs8.as_bytes(), &key)?;
            std::fs::write(dir.join("private.enc"), blob)?;
        } else {
            std::fs::write(dir.join("private.pem"), pkcs8)?;
        }
        std::fs::write(dir.join("public.pem"), spki)?;
        std::fs::write(self.root.join("active.txt"), &kid)?;
        Ok(kid)
    }

    /// Seal an existing unsealed keystore in place. Encrypts each kid's
    /// `private.pem` to `private.enc` and removes the plaintext.
    pub fn seal_in_place(&self) -> Result<usize> {
        let key = sealed::master_key_from_env()?;
        let mut count = 0;
        for kid in self.list_kids()? {
            let dir = self.root.join(&kid);
            let pem_path = dir.join("private.pem");
            let enc_path = dir.join("private.enc");
            if enc_path.exists() {
                continue;
            }
            if !pem_path.exists() {
                continue;
            }
            let pem = std::fs::read(&pem_path)?;
            let blob = sealed::seal(&pem, &key)?;
            std::fs::write(&enc_path, blob)?;
            std::fs::remove_file(&pem_path)?;
            count += 1;
        }
        Ok(count)
    }

    pub fn active_kid(&self) -> Result<String> {
        let s = std::fs::read_to_string(self.root.join("active.txt"))
            .context("reading active.txt — run 'etl-auth init-issuer'?")?;
        Ok(s.trim().to_string())
    }

    pub fn list_kids(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// Read the private key as plaintext PEM. Prefers `private.enc`
    /// (sealed) over `private.pem` (legacy).
    pub fn private_pem(&self, kid: &str) -> Result<String> {
        let dir = self.root.join(kid);
        let enc = dir.join("private.enc");
        if enc.exists() {
            let key = sealed::master_key_from_env()
                .context("private.enc requires ETL_MASTER_KEY")?;
            let blob = std::fs::read(&enc)?;
            let pem = sealed::unseal(&blob, &key)?;
            return String::from_utf8(pem).context("decrypted private key is not UTF-8");
        }
        Ok(std::fs::read_to_string(dir.join("private.pem"))?)
    }

    pub fn public_pem(&self, kid: &str) -> Result<String> {
        Ok(std::fs::read_to_string(self.root.join(kid).join("public.pem"))?)
    }

    pub fn load_private(&self, kid: &str) -> Result<RsaPrivateKey> {
        let pem = self.private_pem(kid)?;
        Ok(RsaPrivateKey::from_pkcs8_pem(&pem)?)
    }

    pub fn load_public(&self, kid: &str) -> Result<RsaPublicKey> {
        let pem = self.public_pem(kid)?;
        Ok(RsaPublicKey::from_public_key_pem(&pem)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn run_with_env<F: FnOnce()>(key: &str, val: Option<&str>, f: F) {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var(key).ok();
        match val {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        f();
        match prev {
            Some(p) => std::env::set_var(key, p),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn init_writes_keypair_and_marks_active() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        run_with_env("ETL_MASTER_KEY", None, || {
            let kid = ks.init().unwrap();
            assert!(dir.path().join(&kid).join("private.pem").exists());
            assert!(dir.path().join(&kid).join("public.pem").exists());
            assert_eq!(ks.active_kid().unwrap(), kid);
            let kids = ks.list_kids().unwrap();
            assert_eq!(kids, vec![kid.clone()]);
            let _priv = ks.load_private(&kid).unwrap();
            let _pub = ks.load_public(&kid).unwrap();
        });
    }

    #[test]
    fn init_writes_enc_when_master_key_set() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        run_with_env("ETL_MASTER_KEY", Some(&"00".repeat(32)), || {
            let kid = ks.init().unwrap();
            assert!(dir.path().join(&kid).join("private.enc").exists());
            assert!(!dir.path().join(&kid).join("private.pem").exists());
            let pem = ks.private_pem(&kid).unwrap();
            assert!(pem.contains("BEGIN PRIVATE KEY"));
        });
    }

    #[test]
    fn seal_in_place_upgrades_pem_to_enc() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        run_with_env("ETL_MASTER_KEY", None, || {
            ks.init().unwrap();
        });
        run_with_env("ETL_MASTER_KEY", Some(&"11".repeat(32)), || {
            let count = ks.seal_in_place().unwrap();
            assert_eq!(count, 1);
            let kid = ks.active_kid().unwrap();
            assert!(dir.path().join(&kid).join("private.enc").exists());
            assert!(!dir.path().join(&kid).join("private.pem").exists());
            let pem = ks.private_pem(&kid).unwrap();
            assert!(pem.contains("BEGIN PRIVATE KEY"));
        });
    }
}
