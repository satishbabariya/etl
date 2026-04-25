//! Phase II.1.b: cross-tenant isolation via the Catalog API.
//!
//! Where Phase II.1.a's `rls_cross_tenant.rs` exercised RLS via raw
//! SQL, this test goes through the public `Catalog::*` methods —
//! pinning the TenantContext threading. Two tenants each create a
//! pipeline. Each tenant's context cannot see the other's row, even
//! when querying by id.

use catalog::{Catalog, NewConnection, NewPipeline};
use common_types::ids::TenantContext;
use serde_json::json;

fn admin_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}
fn app_url() -> String {
    std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into())
}

#[tokio::test]
#[ignore = "requires docker stack"]
async fn cross_tenant_api_reads_blocked() -> anyhow::Result<()> {
    let admin = Catalog::connect(&admin_url()).await?;
    admin.migrate().await?;
    admin.truncate_all_for_tests().await?;
    let tenant_a = admin.create_tenant("acme").await?;
    let tenant_b = admin.create_tenant("globex").await?;

    let cat = Catalog::connect_app(&app_url()).await?;
    let conn_a = cat
        .create_connection(NewConnection {
            tenant_id: tenant_a,
            name: "a".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({"url":"postgres://x"}),
        })
        .await?;
    let pipe_a = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_a,
            name: "pa".into(),
            source_conn_id: conn_a,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;
    let conn_b = cat
        .create_connection(NewConnection {
            tenant_id: tenant_b,
            name: "b".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({"url":"postgres://y"}),
        })
        .await?;
    let pipe_b = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant_b,
            name: "pb".into(),
            source_conn_id: conn_b,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await?;

    let ctx_a = TenantContext::new(tenant_a);
    let ctx_b = TenantContext::new(tenant_b);

    // Cross-tenant reads of pipelines: invisible.
    assert!(
        cat.get_pipeline(ctx_a, pipe_b).await?.is_none(),
        "tenant A read tenant B's pipeline"
    );
    assert!(
        cat.get_pipeline(ctx_b, pipe_a).await?.is_none(),
        "tenant B read tenant A's pipeline"
    );

    // Cross-tenant reads of connections: invisible.
    assert!(cat.get_connection(ctx_a, conn_b).await?.is_none());
    assert!(cat.get_connection(ctx_b, conn_a).await?.is_none());

    // Sanity: each tenant sees its own.
    assert!(cat.get_pipeline(ctx_a, pipe_a).await?.is_some());
    assert!(cat.get_pipeline(ctx_b, pipe_b).await?.is_some());
    assert!(cat.get_connection(ctx_a, conn_a).await?.is_some());
    assert!(cat.get_connection(ctx_b, conn_b).await?.is_some());

    Ok(())
}
