use anyhow::{Context, Result};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::path::{Path, PathBuf};

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
    /// `<root>/<kid>/{private.pem, public.pem}`, and mark it active.
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
        std::fs::write(dir.join("private.pem"), pkcs8)?;
        std::fs::write(dir.join("public.pem"), spki)?;
        std::fs::write(self.root.join("active.txt"), &kid)?;
        Ok(kid)
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

    pub fn private_pem(&self, kid: &str) -> Result<String> {
        Ok(std::fs::read_to_string(self.root.join(kid).join("private.pem"))?)
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

    #[test]
    fn init_writes_keypair_and_marks_active() {
        let dir = tempfile::tempdir().unwrap();
        let ks = Keystore::open(dir.path().to_path_buf());
        let kid = ks.init().unwrap();
        assert!(dir.path().join(&kid).join("private.pem").exists());
        assert!(dir.path().join(&kid).join("public.pem").exists());
        assert_eq!(ks.active_kid().unwrap(), kid);
        let kids = ks.list_kids().unwrap();
        assert_eq!(kids, vec![kid.clone()]);
        let _priv = ks.load_private(&kid).unwrap();
        let _pub = ks.load_public(&kid).unwrap();
    }
}
