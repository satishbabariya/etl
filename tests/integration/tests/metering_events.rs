//! Integration tests for the metering foundation (phase-2-6a).
//!
//! Requires docker-compose `postgres` service. Skipped on unreachable DB.

use catalog::Catalog;
use chrono::Utc;
use common_types::ids::{PipelineId, RunId, TenantId};
use metering::{
    BillableMetric, BufferedSink, CatalogMeteringSink, MeteringEvent, MeteringSink,
    MeteringSource,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

fn catalog_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn setup() -> Option<(Catalog, PgPool)> {
    let url = catalog_url();
    let cat = match Catalog::connect(&url).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP metering_events test: cannot reach {url}: {e}");
            return None;
        }
    };
    cat.migrate().await.expect("migrate");
    cat.truncate_all_for_tests().await.expect("truncate");
    let pool = cat.pool().clone();
    Some((cat, pool))
}

async fn seed_tenant(pool: &PgPool, name: &str) -> Uuid {
    let tid = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
        .bind(tid)
        .bind(name)
        .execute(pool)
        .await
        .expect("insert tenant");
    tid
}

fn make_event(tid_uuid: Uuid, metric: BillableMetric, value: i64) -> MeteringEvent {
    MeteringEvent {
        event_id: Uuid::now_v7(),
        tenant_id: TenantId::from_uuid_unchecked(tid_uuid),
        pipeline_id: Some(PipelineId::new()),
        run_id: Some(RunId::new()),
        metric,
        value,
        timestamp: Utc::now(),
        source: MeteringSource::Read,
    }
}

#[tokio::test]
async fn emit_writes_a_row_to_metering_events() {
    let Some((_, pool)) = setup().await else { return };
    let tid = seed_tenant(&pool, "tenant-a").await;
    let sink = CatalogMeteringSink::new(pool.clone());

    sink.emit(&make_event(tid, BillableMetric::RowsRead, 100))
        .await
        .expect("emit");

    let count: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT FROM metering_events WHERE tenant_id = $1",
    )
    .bind(tid)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn multiple_emits_sum_to_correct_total() {
    let Some((_, pool)) = setup().await else { return };
    let tid = seed_tenant(&pool, "tenant-b").await;
    let sink = CatalogMeteringSink::new(pool.clone());

    for v in [10i64, 20, 30] {
        sink.emit(&make_event(tid, BillableMetric::BytesRead, v))
            .await
            .expect("emit");
    }

    let total: i64 = sqlx::query(
        "SELECT COALESCE(SUM(value), 0)::BIGINT FROM metering_events \
         WHERE tenant_id = $1 AND metric = 'bytes_read'",
    )
    .bind(tid)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(total, 60);
}

#[tokio::test]
async fn rls_tenant_cannot_see_other_tenants_events() {
    // RLS only applies to non-superuser roles. `etl` is the admin role
    // (BYPASSRLS); the RLS check must connect as `etl_app`.
    let Some((_, admin_pool)) = setup().await else { return };
    let tid_a = seed_tenant(&admin_pool, "tenant-rls-a").await;
    let tid_b = seed_tenant(&admin_pool, "tenant-rls-b").await;

    let sink = CatalogMeteringSink::new(admin_pool.clone());
    sink.emit(&make_event(tid_a, BillableMetric::RowsRead, 1))
        .await
        .expect("emit a");
    sink.emit(&make_event(tid_b, BillableMetric::RowsRead, 1))
        .await
        .expect("emit b");

    let app_url = std::env::var("DATABASE_URL_APP")
        .unwrap_or_else(|_| "postgres://etl_app:etl_app@localhost:5432/etl_catalog".into());
    let app_pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&app_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SKIP RLS test: etl_app unreachable: {e}");
            return;
        }
    };

    let mut tx = app_pool.begin().await.unwrap();
    sqlx::query(&format!("SET LOCAL app.tenant_id = '{}'", tid_a))
        .execute(&mut *tx)
        .await
        .unwrap();
    let count: i64 = sqlx::query("SELECT COUNT(*)::BIGINT FROM metering_events")
        .fetch_one(&mut *tx)
        .await
        .unwrap()
        .get(0);
    tx.rollback().await.unwrap();

    assert_eq!(count, 1, "tenant A must see only its own event, not tenant B's");
}

#[tokio::test]
async fn duplicate_event_id_is_silently_ignored() {
    let Some((_, pool)) = setup().await else { return };
    let tid = seed_tenant(&pool, "tenant-idem").await;
    let sink = CatalogMeteringSink::new(pool.clone());

    let ev = make_event(tid, BillableMetric::RowsWritten, 50);
    sink.emit(&ev).await.expect("first emit");
    sink.emit(&ev).await.expect("second emit (duplicate)");

    let count: i64 = sqlx::query(
        "SELECT COUNT(*)::BIGINT FROM metering_events WHERE event_id = $1",
    )
    .bind(ev.event_id)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    assert_eq!(count, 1, "duplicate event_id must produce exactly one row");
}

#[tokio::test]
async fn buffered_sink_captures_and_drains() {
    let sink = BufferedSink::new();
    let tid = Uuid::now_v7();
    for m in [BillableMetric::RowsRead, BillableMetric::BytesRead] {
        sink.emit(&make_event(tid, m, 1)).await.unwrap();
    }
    assert_eq!(sink.drain().len(), 2);
    assert_eq!(sink.drain().len(), 0, "second drain must be empty");
}
