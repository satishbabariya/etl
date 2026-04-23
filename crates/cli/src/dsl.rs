//! Parse YAML resource files and apply them to the catalog idempotently.

use anyhow::{Context, bail};
use catalog::Catalog;
use common_types::dsl::{
    ConnectionSpec, Metadata, PipelineDslSpec, ResourceEnvelope, ResourceKind,
};
use common_types::ids::TenantId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct ParsedFile {
    pub path: PathBuf,
    pub resources: Vec<ResourceEnvelope>,
}

#[derive(Debug, Default)]
pub struct ApplyReport {
    pub connections_created: usize,
    pub connections_updated: usize,
    pub connections_unchanged: usize,
    pub pipelines_created: usize,
    pub pipelines_updated: usize,
    pub pipelines_unchanged: usize,
}

pub fn load_path(path: &Path) -> anyhow::Result<Vec<ParsedFile>> {
    let mut out = Vec::new();
    if path.is_file() {
        out.push(parse_file(path)?);
    } else if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            let ext = p.extension().and_then(|e| e.to_str());
            if ext == Some("yaml") || ext == Some("yml") {
                out.push(parse_file(&p)?);
            }
        }
    } else {
        bail!("path not found: {}", path.display());
    }
    Ok(out)
}

fn parse_file(path: &Path) -> anyhow::Result<ParsedFile> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let mut resources = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(&text) {
        let env: ResourceEnvelope = serde::Deserialize::deserialize(doc)
            .with_context(|| format!("parsing YAML doc in {}", path.display()))?;
        if env.api_version != "platform.etl/v0" {
            bail!(
                "unsupported apiVersion '{}' in {}",
                env.api_version,
                path.display()
            );
        }
        resources.push(env);
    }
    Ok(ParsedFile {
        path: path.to_path_buf(),
        resources,
    })
}

pub async fn apply(
    catalog: &Catalog,
    tenant_id: TenantId,
    files: &[ParsedFile],
) -> anyhow::Result<ApplyReport> {
    let mut report = ApplyReport::default();
    catalog.ensure_default_workspace(tenant_id).await?;

    let mut connections: HashMap<String, ConnectionSpec> = HashMap::new();
    let mut pipelines: HashMap<String, (Metadata, PipelineDslSpec)> = HashMap::new();

    for file in files {
        for env in &file.resources {
            match env.kind {
                ResourceKind::Connection => {
                    let spec: ConnectionSpec = serde_json::from_value(env.spec.clone())
                        .with_context(|| format!("parsing Connection spec for {}", env.metadata.name))?;
                    connections.insert(env.metadata.name.clone(), spec);
                }
                ResourceKind::Pipeline => {
                    let spec: PipelineDslSpec = serde_json::from_value(env.spec.clone())
                        .with_context(|| format!("parsing Pipeline spec for {}", env.metadata.name))?;
                    pipelines.insert(env.metadata.name.clone(), (env.metadata.clone(), spec));
                }
            }
        }
    }

    let mut conn_name_to_id = HashMap::new();
    for (name, spec) in &connections {
        let (id, action) = upsert_connection(catalog, tenant_id, name, spec).await?;
        conn_name_to_id.insert(name.clone(), id);
        match action {
            UpsertAction::Created => report.connections_created += 1,
            UpsertAction::Updated => report.connections_updated += 1,
            UpsertAction::Unchanged => report.connections_unchanged += 1,
        }
    }

    for (name, (_meta, spec)) in &pipelines {
        let src_id = conn_name_to_id
            .get(&spec.source_connection)
            .copied()
            .with_context(|| {
                format!(
                    "pipeline '{name}' references connection '{}' which was not applied",
                    spec.source_connection
                )
            })?;
        let action = upsert_pipeline(catalog, tenant_id, name, src_id, spec).await?;
        match action {
            UpsertAction::Created => report.pipelines_created += 1,
            UpsertAction::Updated => report.pipelines_updated += 1,
            UpsertAction::Unchanged => report.pipelines_unchanged += 1,
        }
    }

    Ok(report)
}

#[derive(Debug, Clone, Copy)]
enum UpsertAction {
    Created,
    Updated,
    Unchanged,
}

async fn upsert_connection(
    catalog: &Catalog,
    tenant_id: TenantId,
    name: &str,
    spec: &ConnectionSpec,
) -> anyhow::Result<(common_types::ids::ConnectionId, UpsertAction)> {
    let existing: Option<(uuid::Uuid, String, serde_json::Value)> = sqlx::query_as(
        "SELECT connection_id, connector_ref, config FROM connections \
         WHERE tenant_id = $1 AND name = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(name)
    .fetch_optional(catalog.pool())
    .await?;

    if let Some((cid, cur_ref, cur_config)) = existing {
        if cur_ref == spec.connector_ref && cur_config == spec.config {
            return Ok((
                common_types::ids::ConnectionId::from_uuid_unchecked(cid),
                UpsertAction::Unchanged,
            ));
        }
        sqlx::query(
            "UPDATE connections SET connector_ref = $1, config = $2, updated_at = NOW() \
             WHERE connection_id = $3",
        )
        .bind(&spec.connector_ref)
        .bind(&spec.config)
        .bind(cid)
        .execute(catalog.pool())
        .await?;
        return Ok((
            common_types::ids::ConnectionId::from_uuid_unchecked(cid),
            UpsertAction::Updated,
        ));
    }
    let id = catalog
        .create_connection(catalog::NewConnection {
            tenant_id,
            name: name.to_string(),
            connector_ref: spec.connector_ref.clone(),
            config: spec.config.clone(),
        })
        .await?;
    Ok((id, UpsertAction::Created))
}

async fn upsert_pipeline(
    catalog: &Catalog,
    tenant_id: TenantId,
    name: &str,
    source_conn_id: common_types::ids::ConnectionId,
    spec: &PipelineDslSpec,
) -> anyhow::Result<UpsertAction> {
    let spec_json = serde_json::json!({
        "source": spec.source,
        "destination": spec.destination,
        "batch_size": spec.batch_size,
        "evolution_policy": spec.evolution_policy,
    });

    let existing: Option<(uuid::Uuid, uuid::Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT pipeline_id, source_conn_id, spec FROM pipelines \
         WHERE tenant_id = $1 AND name = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(name)
    .fetch_optional(catalog.pool())
    .await?;

    if let Some((pid, cur_src, cur_spec)) = existing {
        if cur_src == source_conn_id.as_uuid() && cur_spec == spec_json {
            return Ok(UpsertAction::Unchanged);
        }
        sqlx::query(
            "UPDATE pipelines SET source_conn_id = $1, spec = $2, updated_at = NOW() \
             WHERE pipeline_id = $3",
        )
        .bind(source_conn_id.as_uuid())
        .bind(&spec_json)
        .bind(pid)
        .execute(catalog.pool())
        .await?;
        return Ok(UpsertAction::Updated);
    }
    catalog
        .create_pipeline(catalog::NewPipeline {
            tenant_id,
            name: name.to_string(),
            source_conn_id,
            dest_conn_id: None,
            spec: spec_json,
        })
        .await?;
    Ok(UpsertAction::Created)
}

#[derive(Debug, Clone)]
pub enum DiffRow {
    Create { kind: ResourceKind, name: String },
    Update { kind: ResourceKind, name: String, fields: Vec<String> },
    Unchanged { kind: ResourceKind, name: String },
}

pub async fn diff(
    catalog: &Catalog,
    tenant_id: TenantId,
    files: &[ParsedFile],
) -> anyhow::Result<Vec<DiffRow>> {
    let mut out = Vec::new();
    for file in files {
        for env in &file.resources {
            match env.kind {
                ResourceKind::Connection => {
                    let spec: ConnectionSpec = serde_json::from_value(env.spec.clone())?;
                    let existing: Option<(uuid::Uuid, String, serde_json::Value)> = sqlx::query_as(
                        "SELECT connection_id, connector_ref, config FROM connections \
                         WHERE tenant_id = $1 AND name = $2",
                    )
                    .bind(tenant_id.as_uuid())
                    .bind(&env.metadata.name)
                    .fetch_optional(catalog.pool())
                    .await?;
                    match existing {
                        None => out.push(DiffRow::Create {
                            kind: ResourceKind::Connection,
                            name: env.metadata.name.clone(),
                        }),
                        Some((_id, cur_ref, cur_cfg)) => {
                            let mut fields = Vec::new();
                            if cur_ref != spec.connector_ref { fields.push("connector_ref".into()); }
                            if cur_cfg != spec.config { fields.push("config".into()); }
                            if fields.is_empty() {
                                out.push(DiffRow::Unchanged {
                                    kind: ResourceKind::Connection,
                                    name: env.metadata.name.clone(),
                                });
                            } else {
                                out.push(DiffRow::Update {
                                    kind: ResourceKind::Connection,
                                    name: env.metadata.name.clone(),
                                    fields,
                                });
                            }
                        }
                    }
                }
                ResourceKind::Pipeline => {
                    let spec: PipelineDslSpec = serde_json::from_value(env.spec.clone())?;
                    let existing: Option<serde_json::Value> = sqlx::query_scalar(
                        "SELECT spec FROM pipelines WHERE tenant_id = $1 AND name = $2",
                    )
                    .bind(tenant_id.as_uuid())
                    .bind(&env.metadata.name)
                    .fetch_optional(catalog.pool())
                    .await?;
                    let new_spec = serde_json::json!({
                        "source": spec.source,
                        "destination": spec.destination,
                        "batch_size": spec.batch_size,
                        "evolution_policy": spec.evolution_policy,
                    });
                    match existing {
                        None => out.push(DiffRow::Create {
                            kind: ResourceKind::Pipeline,
                            name: env.metadata.name.clone(),
                        }),
                        Some(cur) => {
                            if cur == new_spec {
                                out.push(DiffRow::Unchanged {
                                    kind: ResourceKind::Pipeline,
                                    name: env.metadata.name.clone(),
                                });
                            } else {
                                let fields = diff_json_fields(&cur, &new_spec);
                                out.push(DiffRow::Update {
                                    kind: ResourceKind::Pipeline,
                                    name: env.metadata.name.clone(),
                                    fields,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

fn diff_json_fields(a: &serde_json::Value, b: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let (Some(ao), Some(bo)) = (a.as_object(), b.as_object()) {
        for k in ao.keys().chain(bo.keys()).collect::<std::collections::BTreeSet<_>>() {
            if ao.get(k) != bo.get(k) {
                out.push(k.clone());
            }
        }
    }
    out
}
