use chrono::{DateTime, Utc};
use common_types::ids::{TenantId, WorkspaceId};

#[derive(Debug, Clone)]
pub struct Workspace {
    pub workspace_id: WorkspaceId,
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

pub async fn ensure_default(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<WorkspaceId> {
    if let Some(existing) = get_by_name(conn, tenant_id, "default").await? {
        return Ok(existing.workspace_id);
    }
    let id = WorkspaceId::new();
    sqlx::query(
        "INSERT INTO workspaces (workspace_id, tenant_id, name) VALUES ($1, $2, 'default') \
         ON CONFLICT (tenant_id, name) DO NOTHING",
    )
    .bind(id.as_uuid())
    .bind(tenant_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(get_by_name(conn, tenant_id, "default")
        .await?
        .expect("inserted or conflicted row must exist")
        .workspace_id)
}

pub async fn get_by_name(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    name: &str,
) -> sqlx::Result<Option<Workspace>> {
    let row: Option<(uuid::Uuid, uuid::Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT workspace_id, tenant_id, name, created_at \
         FROM workspaces WHERE tenant_id = $1 AND name = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(w, t, name, ts)| Workspace {
        workspace_id: WorkspaceId::from_uuid_unchecked(w),
        tenant_id: TenantId::from_uuid_unchecked(t),
        name,
        created_at: ts,
    }))
}
