use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use chrono::{DateTime, Utc};
use common_types::ids::{PrincipalId, TenantId};

#[derive(Debug, Clone)]
pub struct Principal {
    pub principal_id: PrincipalId,
    pub tenant_id: TenantId,
    pub name: String,
    pub role: String,
    pub created_at: DateTime<Utc>,
}

pub struct NewPrincipal {
    pub tenant_id: TenantId,
    pub name: String,
    pub password: String,
    pub role: String,
}

pub fn hash_password(plaintext: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut rand::thread_rng());
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(plaintext.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("argon2 hash: {e}"))?
        .to_string();
    Ok(hash)
}

pub fn verify_password(plaintext: &str, hashed: &str) -> bool {
    let parsed = match PasswordHash::new(hashed) {
        Ok(p) => p,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(plaintext.as_bytes(), &parsed)
        .is_ok()
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewPrincipal,
) -> sqlx::Result<PrincipalId> {
    let id = PrincipalId::new();
    let hash = hash_password(&new.password).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query(
        "INSERT INTO principals (principal_id, tenant_id, name, password_hash, role) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(&new.name)
    .bind(&hash)
    .bind(&new.role)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<Option<(Principal, String)>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT principal_id, tenant_id, name, password_hash, role, created_at \
             FROM principals WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(pid, tid, n, hash, role, ts)| {
        (
            Principal {
                principal_id: PrincipalId::from_uuid_unchecked(pid),
                tenant_id: TenantId::from_uuid_unchecked(tid),
                name: n,
                role,
                created_at: ts,
            },
            hash,
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_roundtrips() {
        let h = hash_password("hunter2").unwrap();
        assert!(verify_password("hunter2", &h));
        assert!(!verify_password("wrong", &h));
    }
}
