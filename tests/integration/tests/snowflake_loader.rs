//! SnowflakeLoader against a REAL Snowflake account. Skips unless the
//! SNOWFLAKE_TEST_* env vars are set (RFC-9: destination tests run
//! against real accounts, not mocks). Uses a throwaway table per run.
//!
//! Required env:
//!   SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PRIVATE_KEY
//!   (PEM content), SNOWFLAKE_TEST_WAREHOUSE, SNOWFLAKE_TEST_DATABASE,
//!   SNOWFLAKE_TEST_SCHEMA

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common_types::pipeline_spec::{DestinationSpec, SnowflakeDestinationSpec};
use loader_sdk::{DestinationLoader, LoadId};
use std::sync::Arc;
use worker::loaders::snowflake::SnowflakeLoader;

fn real_spec(table: &str, pk: Vec<String>) -> Option<DestinationSpec> {
    let get = |k: &str| std::env::var(k).ok();
    let account = get("SNOWFLAKE_TEST_ACCOUNT")?;
    let user = get("SNOWFLAKE_TEST_USER")?;
    let key = get("SNOWFLAKE_TEST_PRIVATE_KEY")?;
    let warehouse = get("SNOWFLAKE_TEST_WAREHOUSE")?;
    let database = get("SNOWFLAKE_TEST_DATABASE")?;
    let schema = get("SNOWFLAKE_TEST_SCHEMA")?;
    std::env::set_var("SNOWFLAKE_TEST_KEY_MATERIAL", key);
    Some(DestinationSpec::Snowflake(SnowflakeDestinationSpec {
        account,
        user,
        private_key_env: "SNOWFLAKE_TEST_KEY_MATERIAL".into(),
        warehouse,
        database,
        schema,
        table: table.into(),
        role: None,
        pk_columns: pk,
    }))
}

fn batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn load_id(seq: u32) -> LoadId {
    LoadId {
        tenant_id: common_types::ids::TenantId::from_uuid_unchecked(uuid::Uuid::new_v4()),
        pipeline_id: common_types::ids::PipelineId::from_uuid_unchecked(uuid::Uuid::new_v4()),
        run_id: common_types::ids::RunId::from_uuid_unchecked(uuid::Uuid::new_v4()),
        batch_seq: seq,
        stream_name: String::new(),
    }
}

#[tokio::test]
#[ignore = "requires a real Snowflake account via SNOWFLAKE_TEST_* env vars"]
async fn snowflake_append_upsert_idempotent_roundtrip() -> anyhow::Result<()> {
    let table = format!("etl_test_{}", uuid::Uuid::new_v4().simple());
    let Some(spec) = real_spec(&table, vec!["id".into()]) else {
        eprintln!("SKIP: SNOWFLAKE_TEST_* env not set");
        return Ok(());
    };

    // Load 1: initial upsert. Same LoadId reused for the replay check.
    let id0 = load_id(0);
    let r1 = SnowflakeLoader
        .load(&spec, id0.clone(), batch(vec![1, 2], vec!["a", "b"]))
        .await?;
    assert_eq!(r1.rows_loaded, 2);

    // Replay same LoadId: must be a no-op.
    let r2 = SnowflakeLoader
        .load(&spec, id0, batch(vec![1, 2], vec!["a", "b"]))
        .await?;
    assert_eq!(r2.rows_loaded, 0, "same LoadId must short-circuit");

    // Load 2: upsert overwrites row 2, inserts row 3.
    let r3 = SnowflakeLoader
        .load(&spec, load_id(1), batch(vec![2, 3], vec!["B", "c"]))
        .await?;
    assert_eq!(r3.rows_loaded, 2);

    // Verify final state via the loader's own client, then clean up.
    if let DestinationSpec::Snowflake(s) = &spec {
        let client = worker::loaders::snowflake::client::SnowflakeClient::from_spec(s)?;
        let body = client
            .execute(&format!(
                "SELECT \"id\", \"name\" FROM \"{}\".\"{}\".\"{}\" ORDER BY \"id\"",
                s.database, s.schema, table
            ))
            .await?;
        let rows = body["data"].as_array().unwrap();
        assert_eq!(rows.len(), 3, "expected rows 1,2,3; got {rows:?}");
        assert_eq!(rows[1][1], "B", "row 2 must reflect the upsert");
        client
            .execute(&format!(
                "DROP TABLE IF EXISTS \"{}\".\"{}\".\"{}\"",
                s.database, s.schema, table
            ))
            .await?;
        client
            .execute(&format!(
                "DELETE FROM \"{}\".\"{}\".\"_ETL_LOADED_BATCHES\" WHERE STREAM_NAME = ''",
                s.database, s.schema
            ))
            .await
            .ok();
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real Snowflake account via SNOWFLAKE_TEST_* env vars"]
async fn snowflake_cdc_applies_last_change_per_key() -> anyhow::Result<()> {
    let table = format!("etl_test_cdc_{}", uuid::Uuid::new_v4().simple());
    let Some(spec) = real_spec(&table, vec!["id".into()]) else {
        eprintln!("SKIP: SNOWFLAKE_TEST_* env not set");
        return Ok(());
    };
    let cdc = |rows: Vec<(i64, Option<&str>, &str)>| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_cdc.op", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };

    // Snapshot rows 1..3, then a streaming batch touching each key twice.
    SnowflakeLoader
        .load(
            &spec,
            load_id(10),
            cdc(vec![
                (1, Some("a"), "s"),
                (2, Some("b"), "s"),
                (3, Some("c"), "s"),
            ]),
        )
        .await?;
    let r = SnowflakeLoader
        .load(
            &spec,
            load_id(11),
            cdc(vec![
                (1, Some("a1"), "u"),
                (1, Some("a2"), "u"), // 1 → a2
                (2, None, "d"),       // 2 → gone
                (3, None, "d"),
                (3, Some("c2"), "i"), // 3 → back as c2
                (4, Some("d"), "i"),
                (4, None, "d"), // 4 → never lands
            ]),
        )
        .await?;
    assert_eq!(r.rows_loaded, 7);

    if let DestinationSpec::Snowflake(s) = &spec {
        let client = worker::loaders::snowflake::client::SnowflakeClient::from_spec(s)?;
        let body = client
            .execute(&format!(
                "SELECT \"id\", \"name\" FROM \"{}\".\"{}\".\"{}\" ORDER BY \"id\"",
                s.database, s.schema, table
            ))
            .await?;
        let rows = body["data"].as_array().unwrap();
        client
            .execute(&format!(
                "DROP TABLE IF EXISTS \"{}\".\"{}\".\"{}\"",
                s.database, s.schema, table
            ))
            .await?;
        assert_eq!(rows.len(), 2, "expected rows 1 and 3; got {rows:?}");
        assert_eq!(
            (rows[0][0].as_str(), rows[0][1].as_str()),
            (Some("1"), Some("a2"))
        );
        assert_eq!(
            (rows[1][0].as_str(), rows[1][1].as_str()),
            (Some("3"), Some("c2"))
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real Snowflake account via SNOWFLAKE_TEST_* env vars"]
async fn snowflake_cdc_schema_change_adds_column() -> anyhow::Result<()> {
    let table = format!("etl_test_evo_{}", uuid::Uuid::new_v4().simple());
    let Some(spec) = real_spec(&table, vec!["id".into()]) else {
        eprintln!("SKIP: SNOWFLAKE_TEST_* env not set");
        return Ok(());
    };
    let op_batch = |cols: Vec<(&str, Vec<Option<&str>>)>, ids: Vec<i64>, ops: Vec<&str>| {
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        let mut arrays: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(ids))];
        for (name, vals) in cols {
            fields.push(Field::new(name, DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(vals)));
        }
        fields.push(Field::new("_cdc.op", DataType::Utf8, false));
        arrays.push(Arc::new(StringArray::from(ops)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    };

    SnowflakeLoader
        .load(
            &spec,
            load_id(20),
            op_batch(vec![("name", vec![Some("a")])], vec![1], vec!["s"]),
        )
        .await?;
    // Source added `email`: a `c` event, then a row using it.
    SnowflakeLoader
        .load(
            &spec,
            load_id(21),
            op_batch(
                vec![
                    ("name", vec![None, Some("b")]),
                    ("email", vec![None, Some("b@x")]),
                ],
                vec![0, 2],
                vec!["c", "i"],
            ),
        )
        .await?;

    if let DestinationSpec::Snowflake(s) = &spec {
        let client = worker::loaders::snowflake::client::SnowflakeClient::from_spec(s)?;
        let body = client
            .execute(&format!(
                "SELECT \"id\", \"email\" FROM \"{}\".\"{}\".\"{}\" ORDER BY \"id\"",
                s.database, s.schema, table
            ))
            .await?;
        let rows = body["data"].as_array().unwrap().clone();
        client
            .execute(&format!(
                "DROP TABLE IF EXISTS \"{}\".\"{}\".\"{}\"",
                s.database, s.schema, table
            ))
            .await?;
        assert_eq!(rows.len(), 2, "got {rows:?}");
        assert!(
            rows[0][1].is_null(),
            "existing row gets NULL email: {rows:?}"
        );
        assert_eq!(rows[1][1].as_str(), Some("b@x"));
    }
    Ok(())
}
