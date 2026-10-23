//! Phase II.1.a — Postgres RLS adversarial test.
//!
//! Verifies the migration-0006 policy at the SQL layer, before any
//! catalog API refactor: connect as the non-superuser `etl_app` role,
//! seed two tenants' worth of pipelines via the admin (`etl`)
//! connection, then for each tenant scope assert that:
//!
//! 1. SELECT under `app.tenant_id = A` returns only A's pipelines (not
//!    B's), even if you ask for B's id by-id.
//! 2. UPDATE under `app.tenant_id = B` against tenant-A's pipeline
//!    affects zero rows.
//! 3. INSERT under `app.tenant_id = A` rejecting a row whose
//!    tenant_id is B fails the WITH CHECK predicate.
//!
//! This is the foundation; Phase II.1.b adds TenantContext threading
//! through the app code so production paths flow through the same
//! policy.

use anyhow::Context;
use sqlx::{PgPool, Row};
use uuid::Uuid;

fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}

async fn reset(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "TRUNCATE cdc_slots, runs, stream_state, schemas, streams, pipelines, \
                  connections, workspaces, tenants CASCADE",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_tenant(admin: &PgPool, name: &str) -> anyhow::Result<(Uuid, Uuid, Uuid)> {
    let tenant_id = Uuid::now_v7();
    let conn_id = Uuid::now_v7();
    let pipe_id = Uuid::now_v7();
    let workspace_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants(tenant_id, name) VALUES ($1, $2)")
        .bind(tenant_id)
        .bind(name)
        .execute(admin)
        .await?;
    sqlx::query("INSERT INTO workspaces(workspace_id, tenant_id, name) VALUES ($1,$2,'default')")
        .bind(workspace_id)
        .bind(tenant_id)
        .execute(admin)
        .await?;
    sqlx::query(
        "INSERT INTO connections(connection_id, tenant_id, workspace_id, name, connector_ref, config) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(conn_id)
    .bind(tenant_id)
    .bind(workspace_id)
    .bind(format!("{name}-conn"))
    .bind("postgres@0.1.0")
    .bind(serde_json::json!({"url":"postgres://x"}))
    .execute(admin)
    .await?;
    sqlx::query(
        "INSERT INTO pipelines(pipeline_id, tenant_id, workspace_id, name, source_conn_id, spec) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(pipe_id)
    .bind(tenant_id)
    .bind(workspace_id)
    .bind(format!("{name}-pipe"))
    .bind(conn_id)
    .bind(serde_json::json!({}))
    .execute(admin)
    .await?;
    Ok((tenant_id, conn_id, pipe_id))
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn cross_tenant_reads_blocked() -> anyhow::Result<()> {
    let admin = PgPool::connect(&admin_url()).await?;
    reset(&admin).await?;
    let (a, _, pipe_a) = seed_tenant(&admin, "acme").await?;
    let (b, _, pipe_b) = seed_tenant(&admin, "globex").await?;

    let app = PgPool::connect(&app_url()).await?;

    // Tenant A: should see pipe_a, not pipe_b.
    let mut tx = app.begin().await?;
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{a}'"))
        .execute(&mut *tx)
        .await?;
    let visible: Vec<Uuid> = sqlx::query("SELECT pipeline_id FROM pipelines")
        .map(|r: sqlx::postgres::PgRow| r.get::<Uuid, _>("pipeline_id"))
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(visible, vec![pipe_a], "tenant A leaked rows: {visible:?}");

    // Tenant A explicit lookup of B's pipeline by id: returns nothing.
    let mut tx = app.begin().await?;
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{a}'"))
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query("SELECT pipeline_id FROM pipelines WHERE pipeline_id = $1")
        .bind(pipe_b)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    assert!(row.is_none(), "tenant A read tenant B's pipeline by id");

    // Tenant B sees only its own.
    let mut tx = app.begin().await?;
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{b}'"))
        .execute(&mut *tx)
        .await?;
    let visible_b: Vec<Uuid> = sqlx::query("SELECT pipeline_id FROM pipelines")
        .map(|r: sqlx::postgres::PgRow| r.get::<Uuid, _>("pipeline_id"))
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(visible_b, vec![pipe_b]);

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn cross_tenant_updates_blocked() -> anyhow::Result<()> {
    let admin = PgPool::connect(&admin_url()).await?;
    reset(&admin).await?;
    let (_, _, pipe_a) = seed_tenant(&admin, "acme").await?;
    let (b, _, _) = seed_tenant(&admin, "globex").await?;

    let app = PgPool::connect(&app_url()).await?;

    // Tenant B tries to UPDATE A's pipeline. Affects 0 rows.
    let mut tx = app.begin().await?;
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{b}'"))
        .execute(&mut *tx)
        .await?;
    let result = sqlx::query("UPDATE pipelines SET name = 'hijacked' WHERE pipeline_id = $1")
        .bind(pipe_a)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(result.rows_affected(), 0, "RLS allowed cross-tenant UPDATE");

    // Confirm A's pipeline name is unchanged (admin can read).
    let unchanged: String = sqlx::query("SELECT name FROM pipelines WHERE pipeline_id = $1")
        .bind(pipe_a)
        .map(|r: sqlx::postgres::PgRow| r.get::<String, _>("name"))
        .fetch_one(&admin)
        .await?;
    assert_eq!(unchanged, "acme-pipe");

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn insert_with_wrong_tenant_id_rejected() -> anyhow::Result<()> {
    let admin = PgPool::connect(&admin_url()).await?;
    reset(&admin).await?;
    let (a, _, _) = seed_tenant(&admin, "acme").await?;
    let (b, _, _) = seed_tenant(&admin, "globex").await?;
    // Need a workspace owned by tenant B for the FK; it already exists.
    let ws_b: Uuid = sqlx::query("SELECT workspace_id FROM workspaces WHERE tenant_id = $1")
        .bind(b)
        .map(|r: sqlx::postgres::PgRow| r.get::<Uuid, _>("workspace_id"))
        .fetch_one(&admin)
        .await?;

    let app = PgPool::connect(&app_url()).await?;

    // Authenticated as A, tries to INSERT a row whose tenant_id = B. WITH CHECK rejects.
    let mut tx = app.begin().await?;
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{a}'"))
        .execute(&mut *tx)
        .await?;
    let res = sqlx::query(
        "INSERT INTO connections(connection_id, tenant_id, workspace_id, name, connector_ref, config) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(Uuid::now_v7())
    .bind(b)
    .bind(ws_b)
    .bind("smuggled")
    .bind("postgres@0.1.0")
    .bind(serde_json::json!({}))
    .execute(&mut *tx)
    .await;
    let err = res
        .err()
        .context("INSERT unexpectedly succeeded — RLS WITH CHECK is not enforced")?;
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("row-level security") || msg.to_lowercase().contains("policy"),
        "unexpected error: {msg}"
    );

    Ok(())
}

// Migration 0018: RLS fails closed. Earlier tests in this file seed via the
// superuser and don't depend on migration order; these do.
async fn migrated_admin() -> anyhow::Result<PgPool> {
    catalog::Catalog::connect(&admin_url())
        .await?
        .migrate()
        .await?;
    let admin = PgPool::connect(&admin_url()).await?;
    reset(&admin).await?;
    Ok(admin)
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn unscoped_session_sees_no_tenant_rows() -> anyhow::Result<()> {
    let admin = migrated_admin().await?;
    let (a, _, _) = seed_tenant(&admin, "acme").await?;
    seed_tenant(&admin, "globex").await?;
    let ws_a: Uuid = sqlx::query("SELECT workspace_id FROM workspaces WHERE tenant_id = $1")
        .bind(a)
        .map(|r: sqlx::postgres::PgRow| r.get::<Uuid, _>("workspace_id"))
        .fetch_one(&admin)
        .await?;

    let app = PgPool::connect(&app_url()).await?;

    // No app.tenant_id and no app.admin: nothing is visible.
    let pipelines: i64 = sqlx::query_scalar("SELECT count(*) FROM pipelines")
        .fetch_one(&app)
        .await?;
    assert_eq!(pipelines, 0, "unscoped session saw pipelines");
    let tenants: i64 = sqlx::query_scalar("SELECT count(*) FROM tenants")
        .fetch_one(&app)
        .await?;
    assert_eq!(tenants, 0, "unscoped session saw tenants");

    // An empty tenant id (the old admin-mode spelling) is not admin either.
    let mut tx = app.begin().await?;
    sqlx::query("SET LOCAL app.tenant_id = ''")
        .execute(&mut *tx)
        .await?;
    let pipelines: i64 = sqlx::query_scalar("SELECT count(*) FROM pipelines")
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(pipelines, 0, "empty app.tenant_id still bypasses RLS");

    // ...and it cannot write.
    let res = sqlx::query(
        "INSERT INTO connections(connection_id, tenant_id, workspace_id, name, connector_ref, config) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(Uuid::now_v7())
    .bind(a)
    .bind(ws_a)
    .bind("unscoped")
    .bind("postgres@0.1.0")
    .bind(serde_json::json!({}))
    .execute(&app)
    .await;
    assert!(res.is_err(), "unscoped session inserted a tenant row");

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn explicit_admin_flag_sees_all_tenants() -> anyhow::Result<()> {
    let admin = migrated_admin().await?;
    seed_tenant(&admin, "acme").await?;
    seed_tenant(&admin, "globex").await?;

    let app = PgPool::connect(&app_url()).await?;
    let mut tx = app.begin().await?;
    sqlx::query("SET LOCAL app.admin = 'on'")
        .execute(&mut *tx)
        .await?;
    let pipelines: i64 = sqlx::query_scalar("SELECT count(*) FROM pipelines")
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(pipelines, 2);

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn cdc_snapshots_are_tenant_isolated() -> anyhow::Result<()> {
    let admin = migrated_admin().await?;
    let (a, _, pipe_a) = seed_tenant(&admin, "acme").await?;
    let (b, _, pipe_b) = seed_tenant(&admin, "globex").await?;
    for (tenant, pipe) in [(a, pipe_a), (b, pipe_b)] {
        sqlx::query(
            "INSERT INTO cdc_snapshots(pipeline_id, tenant_id, captured_position) VALUES ($1,$2,'0/0')",
        )
        .bind(pipe)
        .bind(tenant)
        .execute(&admin)
        .await?;
    }

    let app = PgPool::connect(&app_url()).await?;
    let mut tx = app.begin().await?;
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{a}'"))
        .execute(&mut *tx)
        .await?;
    let visible: Vec<Uuid> = sqlx::query_scalar("SELECT pipeline_id FROM cdc_snapshots")
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    assert_eq!(
        visible,
        vec![pipe_a],
        "tenant A saw another tenant's snapshot"
    );

    Ok(())
}

// The worker writes metering and audit rows over its etl_app pool. Under
// fail-closed RLS those writes must scope themselves to the row's tenant.
#[tokio::test]
#[ignore = "requires docker stack"]
async fn etl_app_metering_and_audit_writes_scope_themselves() -> anyhow::Result<()> {
    use metering::MeteringSink;
    let admin = migrated_admin().await?;
    let (a, _, pipe_a) = seed_tenant(&admin, "acme").await?;
    let tenant = common_types::ids::TenantId::from_uuid_unchecked(a);

    let app = catalog::Catalog::connect_app(&app_url()).await?;
    let sink = metering::CatalogMeteringSink::new(app.pool().clone());
    sink.emit(&metering::MeteringEvent::new(
        tenant,
        Some(common_types::ids::PipelineId::from_uuid_unchecked(pipe_a)),
        None,
        metering::BillableMetric::RowsRead,
        7,
        metering::MeteringSource::Read,
    ))
    .await?;

    app.audit_write(&audit::AuditRow {
        tenant_id: Some(tenant),
        principal_id: None,
        jti: None,
        event: audit::AuditEvent::SecretRead,
        target: Some("rls-test".into()),
        occurred_at: chrono::Utc::now(),
        payload: serde_json::json!({}),
    })
    .await?;
    let tail = app.audit_tail(tenant, 10).await?;
    assert_eq!(tail.len(), 1, "audit_tail as etl_app: {tail:?}");

    let metered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM metering_events WHERE tenant_id = $1")
            .bind(a)
            .fetch_one(&admin)
            .await?;
    assert_eq!(metered, 1);

    Ok(())
}
