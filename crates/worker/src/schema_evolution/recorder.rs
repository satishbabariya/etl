use anyhow::{Context, bail};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use catalog::Catalog;
use common_types::evolution::EvolutionPolicy;
use common_types::ids::StreamId;

use super::diff::diff_schemas;
use super::fingerprint::fingerprint_schema;
use super::policy::{apply_policy, PolicyOutcome};

pub struct ResolvedSchema {
    pub schema: SchemaRef,
    pub schema_id: common_types::ids::SchemaId,
    pub created_new_version: bool,
}

pub async fn record_and_resolve(
    catalog: &Catalog,
    tenant_id: common_types::ids::TenantId,
    stream_id: StreamId,
    policy: EvolutionPolicy,
    incoming: SchemaRef,
) -> anyhow::Result<ResolvedSchema> {
    let fp = fingerprint_schema(&incoming);

    let existing = catalog.get_latest_schema(stream_id).await?;

    if let Some(ref prev) = existing {
        if prev.fingerprint == fp {
            let schema = decode_arrow_schema(&prev.arrow_schema_json)?;
            return Ok(ResolvedSchema {
                schema,
                schema_id: prev.schema_id,
                created_new_version: false,
            });
        }
    }

    let changes = match existing.as_ref() {
        None => Vec::new(),
        Some(prev) => {
            let old = decode_arrow_schema(&prev.arrow_schema_json)?;
            diff_schemas(&old, &incoming)
        }
    };

    match apply_policy(policy, &changes) {
        PolicyOutcome::Reject { reason } => bail!("schema evolution rejected: {reason}"),
        PolicyOutcome::RetainOld => {
            let prev = existing.expect("RetainOld only makes sense when prior exists");
            let schema = decode_arrow_schema(&prev.arrow_schema_json)?;
            return Ok(ResolvedSchema {
                schema,
                schema_id: prev.schema_id,
                created_new_version: false,
            });
        }
        PolicyOutcome::NoOp | PolicyOutcome::Accept => {
            // fall through
        }
    }

    let arrow_json = encode_schema_ipc(&incoming)?;
    let parent = existing.as_ref().map(|p| p.schema_id);
    let new_id = catalog
        .insert_schema(catalog::schema::NewSchema {
            tenant_id,
            stream_id,
            parent_schema_id: parent,
            fingerprint: fp,
            arrow_schema_json: arrow_json,
            change_summary: changes,
        })
        .await
        .context("inserting schema")?;
    catalog
        .set_stream_current_schema(stream_id, new_id)
        .await
        .context("updating streams.current_schema_id")?;

    Ok(ResolvedSchema {
        schema: incoming,
        schema_id: new_id,
        created_new_version: true,
    })
}

/// Encode an Arrow schema as `{"ipc_b64": "<base64>"}` JSON.
/// Arrow 53's Schema doesn't impl Serialize, so we round-trip through
/// IPC bytes (same format we use for batch transport).
fn encode_schema_ipc(schema: &SchemaRef) -> anyhow::Result<serde_json::Value> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema.as_ref())
            .context("StreamWriter::try_new for schema encoding")?;
        w.finish().context("StreamWriter::finish")?;
    }
    Ok(serde_json::json!({ "ipc_b64": BASE64.encode(&buf) }))
}

fn decode_arrow_schema(v: &serde_json::Value) -> anyhow::Result<SchemaRef> {
    let b64 = v
        .get("ipc_b64")
        .and_then(|x| x.as_str())
        .context("arrow_schema_json is missing 'ipc_b64' field")?;
    let bytes = BASE64.decode(b64).context("base64 decode")?;
    let reader = StreamReader::try_new(&*bytes, None)
        .context("StreamReader::try_new for schema decoding")?;
    Ok(reader.schema())
}
