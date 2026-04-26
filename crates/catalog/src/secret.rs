//! CRUD for the secrets table. Free functions take a transaction;
//! public Catalog methods wrap with TenantContext-scoped tx.

use chrono::{DateTime, Utc};
use common_types::ids::{SecretId, TenantId};
use common_types::secrets::{SecretBackendKind, SecretRef};

#[derive(Debug, Clone)]
pub struct Secret {
    pub secret_id: SecretId,
    pub tenant_id: TenantId,
    pub name: String,
    pub backend: SecretBackendKind,
    pub key: String,
    pub created_at: DateTime<Utc>,
}

pub struct NewSecret {
    pub tenant_id: TenantId,
    pub name: String,
    pub backend: SecretBackendKind,
    pub key: String,
}

fn backend_to_str(b: SecretBackendKind) -> &'static str {
    match b {
        SecretBackendKind::Env => "env",
        SecretBackendKind::File => "file",
    }
}

fn parse_backend(s: &str) -> SecretBackendKind {
    match s {
        "env" => SecretBackendKind::Env,
        "file" => SecretBackendKind::File,
        other => panic!("unknown secret backend in DB: {other}"),
    }
}

pub async fn create(
    conn: &mut sqlx::PgConnection,
    new: NewSecret,
) -> sqlx::Result<SecretId> {
    let id = SecretId::new();
    sqlx::query(
        "INSERT INTO secrets (secret_id, tenant_id, name, backend, key) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id.as_uuid())
    .bind(new.tenant_id.as_uuid())
    .bind(&new.name)
    .bind(backend_to_str(new.backend))
    .bind(&new.key)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    name: &str,
) -> sqlx::Result<Option<Secret>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT secret_id, tenant_id, name, backend, key, created_at \
             FROM secrets WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(row.map(|(sid, tid, name, backend, key, created_at)| Secret {
        secret_id: SecretId::from_uuid_unchecked(sid),
        tenant_id: TenantId::from_uuid_unchecked(tid),
        name,
        backend: parse_backend(&backend),
        key,
        created_at,
    }))
}

pub async fn list(conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Secret>> {
    let rows: Vec<(uuid::Uuid, uuid::Uuid, String, String, String, DateTime<Utc>)> =
        sqlx::query_as(
            "SELECT secret_id, tenant_id, name, backend, key, created_at \
             FROM secrets ORDER BY name",
        )
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(sid, tid, name, backend, key, created_at)| Secret {
            secret_id: SecretId::from_uuid_unchecked(sid),
            tenant_id: TenantId::from_uuid_unchecked(tid),
            name,
            backend: parse_backend(&backend),
            key,
            created_at,
        })
        .collect())
}

pub async fn delete(
    conn: &mut sqlx::PgConnection,
    id: SecretId,
) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM secrets WHERE secret_id = $1")
        .bind(id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub fn to_ref(s: &Secret) -> SecretRef {
    SecretRef {
        secret_id: s.secret_id,
        name: s.name.clone(),
        backend: s.backend,
        key: s.key.clone(),
    }
}
