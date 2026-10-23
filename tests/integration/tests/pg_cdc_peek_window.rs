//! Native PG CDC reads must not consume the slot: a window is released
//! only when the caller advances to its `new_position`, after the window
//! is durably written. Otherwise a failure between read and write loses
//! the changes (the slot has already moved past them).
//!
//! Requires the docker-compose `postgres` service (postgres:16, wal_level=logical).
//! Skipped with a message if the database is unreachable.

use sqlx::postgres::PgPoolOptions;
use worker::connectors::postgres::cdc::{slot, stream, CdcEvent};

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn connect() -> Option<sqlx::PgPool> {
    let url = test_url();
    match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("SKIP pg_cdc_peek_window: cannot reach {url}: {e}");
            None
        }
    }
}

/// Fresh table + publication + pgoutput slot. Returns (table, publication, slot).
async fn setup(pool: &sqlx::PgPool) -> (String, String, String) {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let (table, publication, slot_name) = (
        format!("_etl_peek_{id}"),
        format!("etl_peek_pub_{id}"),
        format!("etl_peek_slot_{id}"),
    );
    sqlx::query(&format!(
        "CREATE TABLE public.{table} (id bigint primary key, v text)"
    ))
    .execute(pool)
    .await
    .expect("create table");
    sqlx::query(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE public.{table}"
    ))
    .execute(pool)
    .await
    .expect("create publication");
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(&slot_name)
        .execute(pool)
        .await
        .expect("create slot");
    (table, publication, slot_name)
}

async fn teardown(pool: &sqlx::PgPool, table: &str, publication: &str, slot_name: &str) {
    let _ = sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(slot_name)
        .execute(pool)
        .await;
    let _ = sqlx::query(&format!("DROP PUBLICATION IF EXISTS {publication}"))
        .execute(pool)
        .await;
    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS public.{table}"))
        .execute(pool)
        .await;
}

async fn insert(pool: &sqlx::PgPool, table: &str, id: i64) {
    sqlx::query(&format!("INSERT INTO public.{table} VALUES ($1, 'x')"))
        .bind(id)
        .execute(pool)
        .await
        .expect("insert");
}

async fn read(publication: &str, slot_name: &str) -> stream::WindowOutput {
    stream::read_window(
        &test_url(),
        slot_name,
        publication,
        None,
        1000,
        Default::default(),
    )
    .await
    .expect("read_window")
}

fn inserts(out: &stream::WindowOutput) -> usize {
    out.events
        .iter()
        .filter(|e| matches!(e, CdcEvent::Insert { .. }))
        .count()
}

#[tokio::test]
async fn read_window_does_not_consume_the_slot() {
    let Some(pool) = connect().await else { return };
    let (table, publication, slot_name) = setup(&pool).await;
    insert(&pool, &table, 1).await;
    insert(&pool, &table, 2).await;

    let before = slot::confirmed_flush_lsn(&test_url(), &slot_name)
        .await
        .unwrap();
    let first = read(&publication, &slot_name).await;
    // A retry (e.g. the destination write failed) must see the same changes.
    let retry = read(&publication, &slot_name).await;
    let after = slot::confirmed_flush_lsn(&test_url(), &slot_name)
        .await
        .unwrap();

    teardown(&pool, &table, &publication, &slot_name).await;
    assert_eq!(inserts(&first), 2);
    assert_eq!(
        inserts(&retry),
        2,
        "re-read after a failed write lost changes"
    );
    assert_eq!(before, after, "reading moved the slot");
}

#[tokio::test]
async fn advancing_to_new_position_releases_exactly_the_window() {
    let Some(pool) = connect().await else { return };
    let (table, publication, slot_name) = setup(&pool).await;
    insert(&pool, &table, 1).await;

    let first = read(&publication, &slot_name).await;
    let target = common_types::cursor::lsn_to_string(first.new_position.unwrap());
    let confirmed = slot::advance_slot(&test_url(), &slot_name, &target)
        .await
        .unwrap();
    let drained = read(&publication, &slot_name).await;
    insert(&pool, &table, 2).await;
    let next = read(&publication, &slot_name).await;

    teardown(&pool, &table, &publication, &slot_name).await;
    assert_eq!(inserts(&first), 1);
    assert_eq!(confirmed, target);
    assert!(drained.is_empty, "advanced window was read again");
    assert_eq!(inserts(&next), 1, "later change missing or duplicated");
}
