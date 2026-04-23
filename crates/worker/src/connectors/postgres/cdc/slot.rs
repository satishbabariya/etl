use anyhow::Context;
use sqlx::{Connection, PgConnection};

pub struct EnsureResult {
    pub slot_name: String,
    pub publication_name: String,
    pub consistent_point: String,
    pub created: bool,
}

pub async fn ensure_publication(
    conn_url: &str,
    publication: &str,
    table: &str,
) -> anyhow::Result<()> {
    let mut c = PgConnection::connect(conn_url).await?;
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
    )
    .bind(publication)
    .fetch_one(&mut c)
    .await?;
    if !exists.0 {
        // table is expected as "schema.table"; split + quote parts.
        let (schema, tbl) = table
            .split_once('.')
            .ok_or_else(|| anyhow::anyhow!("table must be schema.table, got {table}"))?;
        let stmt = format!(
            "CREATE PUBLICATION \"{publication}\" FOR TABLE \"{schema}\".\"{tbl}\""
        );
        sqlx::query(&stmt).execute(&mut c).await.context("CREATE PUBLICATION")?;
    }
    Ok(())
}

pub async fn ensure_slot(conn_url: &str, slot: &str) -> anyhow::Result<EnsureResult> {
    let mut c = PgConnection::connect(conn_url).await?;
    let existing: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_optional(&mut c)
    .await?;
    if let Some((lsn,)) = existing {
        return Ok(EnsureResult {
            slot_name: slot.to_string(),
            publication_name: String::new(),
            consistent_point: lsn.unwrap_or_else(|| "0/0".to_string()),
            created: false,
        });
    }
    let row: (String, String) = sqlx::query_as(
        "SELECT slot_name, lsn::text \
         FROM pg_create_logical_replication_slot($1, 'pgoutput')",
    )
    .bind(slot)
    .fetch_one(&mut c)
    .await
    .context("pg_create_logical_replication_slot")?;
    Ok(EnsureResult {
        slot_name: row.0,
        publication_name: String::new(),
        consistent_point: row.1,
        created: true,
    })
}

pub async fn release_slot(conn_url: &str, slot: &str) -> anyhow::Result<()> {
    let mut c = PgConnection::connect(conn_url).await?;
    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(slot)
        .execute(&mut c)
        .await
        .context("pg_drop_replication_slot")?;
    Ok(())
}

pub async fn slot_lag_bytes(conn_url: &str, slot: &str) -> anyhow::Result<i64> {
    let mut c = PgConnection::connect(conn_url).await?;
    let (lag,): (i64,) = sqlx::query_as(
        "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot)
    .fetch_one(&mut c)
    .await?;
    Ok(lag)
}
