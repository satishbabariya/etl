//! Integration tests for pg_replication_slot_advance via the slot.rs helper.
//!
//! Requires the docker-compose `postgres` service (postgres:16, wal_level=logical).
//! Skipped with a message if the database is unreachable.

use sqlx::postgres::PgPoolOptions;
use worker::connectors::postgres::cdc::slot;

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

async fn connect() -> Option<sqlx::PgPool> {
    let url = test_url();
    match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("SKIP pg_cdc_advance_slot: cannot reach {url}: {e}");
            None
        }
    }
}

fn slot_name() -> String {
    format!("etl_advance_test_{}", uuid::Uuid::new_v4().simple())
}

async fn create_slot(pool: &sqlx::PgPool, name: &str) {
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(name)
        .execute(pool)
        .await
        .expect("create test slot");
}

async fn drop_slot(pool: &sqlx::PgPool, name: &str) {
    let _ = sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(name)
        .execute(pool)
        .await;
}

async fn current_wal_lsn(pool: &sqlx::PgPool) -> String {
    let (lsn,): (String,) =
        sqlx::query_as("SELECT pg_current_wal_lsn()::text")
            .fetch_one(pool)
            .await
            .expect("pg_current_wal_lsn");
    lsn
}

async fn ensure_scratch(pool: &sqlx::PgPool) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _etl_advance_test_scratch \
         (id bigserial primary key, v text)",
    )
    .execute(pool)
    .await
    .expect("create scratch");
}

#[tokio::test]
async fn advance_slot_moves_confirmed_flush_lsn() {
    let Some(pool) = connect().await else { return };
    let sname = slot_name();
    create_slot(&pool, &sname).await;

    ensure_scratch(&pool).await;
    sqlx::query("INSERT INTO _etl_advance_test_scratch (v) VALUES ('x')")
        .execute(&pool)
        .await
        .expect("insert");

    let target = current_wal_lsn(&pool).await;
    let url = test_url();
    let confirmed = slot::advance_slot(&url, &sname, &target)
        .await
        .expect("advance_slot should succeed");

    let (advanced,): (bool,) =
        sqlx::query_as("SELECT $1::pg_lsn >= $2::pg_lsn")
            .bind(&confirmed)
            .bind(&target)
            .fetch_one(&pool)
            .await
            .expect("lsn compare");
    assert!(advanced, "confirmed_flush_lsn {confirmed} should be >= target {target}");

    drop_slot(&pool, &sname).await;
}

#[tokio::test]
async fn advance_slot_is_idempotent() {
    let Some(pool) = connect().await else { return };
    let sname = slot_name();
    create_slot(&pool, &sname).await;

    ensure_scratch(&pool).await;
    sqlx::query("INSERT INTO _etl_advance_test_scratch (v) VALUES ('y')")
        .execute(&pool)
        .await
        .expect("insert");

    let target = current_wal_lsn(&pool).await;
    let url = test_url();

    let first = slot::advance_slot(&url, &sname, &target)
        .await
        .expect("first advance");
    let second = slot::advance_slot(&url, &sname, &target)
        .await
        .expect("second advance (idempotent)");

    assert_eq!(first, second, "idempotent calls must return same lsn");
    drop_slot(&pool, &sname).await;
}

#[tokio::test]
async fn advance_slot_with_older_lsn_errors_and_does_not_regress() {
    // PG 11+ errors on attempts to advance backwards (with a "minimum is X"
    // message). That's safer than silent no-op — slot is protected from
    // regression, and Temporal retries can surface workflow-state bugs.
    let Some(pool) = connect().await else { return };
    let sname = slot_name();
    create_slot(&pool, &sname).await;

    ensure_scratch(&pool).await;
    sqlx::query("INSERT INTO _etl_advance_test_scratch (v) VALUES ('z')")
        .execute(&pool)
        .await
        .expect("insert");

    let first_target = current_wal_lsn(&pool).await;
    let url = test_url();

    let after_first = slot::advance_slot(&url, &sname, &first_target)
        .await
        .expect("first advance");

    // Backward-advance must error (PG returns "cannot advance ... minimum is X").
    // Use {err:#} to flatten the anyhow chain so we see the underlying PG message.
    let err = slot::advance_slot(&url, &sname, "0/1").await.unwrap_err();
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("cannot advance") || msg.contains("minimum"),
        "expected PG backward-advance rejection, got: {err:#}"
    );

    // confirmed_flush_lsn must not regress — re-query to confirm.
    let (current,): (Option<String>,) = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(&sname)
    .fetch_one(&pool)
    .await
    .expect("re-query slot");
    let current = current.expect("confirmed_flush_lsn null");
    let (no_regression,): (bool,) =
        sqlx::query_as("SELECT $1::pg_lsn >= $2::pg_lsn")
            .bind(&current)
            .bind(&after_first)
            .fetch_one(&pool)
            .await
            .expect("lsn compare");
    assert!(
        no_regression,
        "lsn must not regress: after_first={after_first} current={current}"
    );

    drop_slot(&pool, &sname).await;
}

#[tokio::test]
async fn advance_slot_errors_on_nonexistent_slot() {
    let Some(_pool) = connect().await else { return };
    let url = test_url();
    let err = slot::advance_slot(&url, "etl_does_not_exist_xyz", "0/1")
        .await
        .unwrap_err();
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("pg_replication_slot_advance")
            || msg.contains("slot")
            || msg.contains("exist")
            || msg.contains("does not exist"),
        "unexpected error message: {err:#}"
    );
}
