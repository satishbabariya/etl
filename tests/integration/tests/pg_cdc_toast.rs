//! An UPDATE that leaves a TOASTed column untouched sends that column as
//! "unchanged" in pgoutput's new tuple, with no value. Decoded as NULL, a
//! loader applying the update would overwrite the stored value. With
//! REPLICA IDENTITY FULL the old tuple carries the value, so the decoder
//! must use it.
//!
//! Requires the docker-compose `postgres` service (postgres:16, wal_level=logical).
//! Skipped with a message if the database is unreachable.

use sqlx::postgres::PgPoolOptions;
use worker::connectors::postgres::cdc::{stream, CdcEvent};

fn test_url() -> String {
    std::env::var("ETL_INTEGRATION_PG_URL")
        .unwrap_or_else(|_| "postgres://etl:etl@localhost:5432/etl_catalog".into())
}

/// Returns the decoded new-tuple `body` of the single UPDATE after
/// touching only `n` on a row whose `body` is TOASTed out of line.
async fn updated_body(identity: &str) -> Option<Option<String>> {
    let url = test_url();
    let pool = match PgPoolOptions::new().max_connections(2).connect(&url).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("SKIP pg_cdc_toast: cannot reach {url}: {e}");
            return None;
        }
    };
    let id = uuid::Uuid::new_v4().simple().to_string();
    let (table, publication, slot_name) = (
        format!("_etl_toast_{id}"),
        format!("etl_toast_pub_{id}"),
        format!("etl_toast_slot_{id}"),
    );
    for stmt in [
        format!("CREATE TABLE public.{table} (id bigint primary key, n int, body text)"),
        format!("ALTER TABLE public.{table} REPLICA IDENTITY {identity}"),
        format!("CREATE PUBLICATION {publication} FOR TABLE public.{table}"),
        // ~64 KB of md5 hex: incompressible enough to be stored out of line.
        format!(
            "INSERT INTO public.{table} SELECT 1, 0, string_agg(md5(g::text), '') \
             FROM generate_series(1, 2000) g"
        ),
    ] {
        sqlx::query(&stmt).execute(&pool).await.expect(&stmt);
    }
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(&slot_name)
        .execute(&pool)
        .await
        .expect("create slot");
    sqlx::query(&format!("UPDATE public.{table} SET n = 1 WHERE id = 1"))
        .execute(&pool)
        .await
        .expect("update");
    let (stored,): (String,) = sqlx::query_as(&format!("SELECT body FROM public.{table}"))
        .fetch_one(&pool)
        .await
        .unwrap();

    let out = stream::read_window(
        &url,
        &slot_name,
        &publication,
        None,
        1000,
        Default::default(),
    )
    .await
    .expect("read_window");

    let _ = sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(&slot_name)
        .execute(&pool)
        .await;
    let _ = sqlx::query(&format!("DROP PUBLICATION {publication}"))
        .execute(&pool)
        .await;
    let _ = sqlx::query(&format!("DROP TABLE public.{table}"))
        .execute(&pool)
        .await;

    let row = out
        .events
        .iter()
        .find_map(|e| match e {
            CdcEvent::Update { row, .. } => Some(row.clone()),
            _ => None,
        })
        .expect("one UPDATE event");
    let body = row[2].clone();
    if let Some(b) = &body {
        assert_eq!(b, &stored, "decoded body differs from the stored value");
    }
    Some(body)
}

#[tokio::test]
async fn full_identity_update_keeps_unchanged_toast_value() {
    let Some(body) = updated_body("FULL").await else {
        return;
    };
    assert!(body.is_some(), "unchanged TOAST column decoded as NULL");
}

#[tokio::test]
async fn default_identity_update_has_no_value_for_unchanged_toast() {
    // Documents the remaining gap: without the old tuple there is nothing
    // to fill from (carrying "unchanged" to the loaders is a follow-up).
    let Some(body) = updated_body("DEFAULT").await else {
        return;
    };
    assert!(body.is_none());
}
