use catalog::{Catalog, NewConnection, NewPipeline, NewRun, RunStatus};
use common_types::cursor::{CursorKind, CursorValue};
use common_types::ids::RunId;
use serde_json::json;

async fn test_catalog() -> Catalog {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into());
    let cat = Catalog::connect(&url).await.unwrap();
    cat.migrate().await.unwrap();
    cat.truncate_all_for_tests().await.unwrap();
    cat
}

#[tokio::test]
async fn tenant_insert_and_get() {
    let cat = test_catalog().await;
    let id = cat.create_tenant("acme").await.unwrap();
    let t = cat.get_tenant(id).await.unwrap().unwrap();
    assert_eq!(t.name, "acme");
}

#[tokio::test]
async fn connection_insert_scoped_to_tenant() {
    let cat = test_catalog().await;
    let tenant = cat.create_tenant("acme").await.unwrap();
    let conn = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "main-pg".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({"host": "localhost", "port": 5432, "database": "src"}),
        })
        .await
        .unwrap();
    let got = cat.get_connection(conn).await.unwrap().unwrap();
    assert_eq!(got.name, "main-pg");
    assert_eq!(got.tenant_id, tenant);
}

#[tokio::test]
async fn pipeline_run_lifecycle() {
    let cat = test_catalog().await;
    let tenant = cat.create_tenant("acme").await.unwrap();
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await
        .unwrap();
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "demo".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await
        .unwrap();
    let run = cat
        .create_run(NewRun {
            run_id: RunId::new(),
            tenant_id: tenant,
            pipeline_id: pipe,
            trigger: "manual".into(),
            temporal_workflow_id: Some("wf-abc".into()),
        })
        .await
        .unwrap();
    cat.mark_run_completed(run).await.unwrap();
    let got = cat.get_run(run).await.unwrap().unwrap();
    assert_eq!(got.status, RunStatus::Completed);
    assert!(got.completed_at.is_some());
}

#[tokio::test]
async fn stream_state_upsert_then_get() {
    let cat = test_catalog().await;
    let tenant = cat.create_tenant("acme").await.unwrap();
    let src = cat
        .create_connection(NewConnection {
            tenant_id: tenant,
            name: "src".into(),
            connector_ref: "postgres@0.1.0".into(),
            config: json!({}),
        })
        .await
        .unwrap();
    let pipe = cat
        .create_pipeline(NewPipeline {
            tenant_id: tenant,
            name: "demo".into(),
            source_conn_id: src,
            dest_conn_id: None,
            spec: json!({}),
        })
        .await
        .unwrap();

    assert!(cat.get_stream_state(pipe, "customers").await.unwrap().is_none());

    cat.upsert_stream_state(
        pipe,
        "customers",
        Some(CursorValue {
            kind: CursorKind::TimestampTz,
            value: "2026-04-22T11:00:00Z".into(),
        }),
        None,
    )
    .await
    .unwrap();

    let got = cat.get_stream_state(pipe, "customers").await.unwrap().unwrap();
    assert_eq!(got.cursor.as_ref().unwrap().value, "2026-04-22T11:00:00Z");
    assert_eq!(got.cursor.as_ref().unwrap().kind, CursorKind::TimestampTz);

    cat.upsert_stream_state(
        pipe,
        "customers",
        Some(CursorValue {
            kind: CursorKind::TimestampTz,
            value: "2026-04-23T10:00:00Z".into(),
        }),
        None,
    )
    .await
    .unwrap();
    let got2 = cat.get_stream_state(pipe, "customers").await.unwrap().unwrap();
    assert_eq!(got2.cursor.as_ref().unwrap().value, "2026-04-23T10:00:00Z");
}
