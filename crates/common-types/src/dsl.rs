use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::evolution::EvolutionPolicy;
use crate::pipeline_spec::{DestinationSpec, SourceSpec};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceEnvelope {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: ResourceKind,
    pub metadata: Metadata,
    pub spec: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceKind {
    Connection,
    Pipeline,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionSpec {
    pub connector_ref: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineDslSpec {
    /// Reference by `metadata.name` of a Connection resource.
    pub source_connection: String,
    pub source: SourceSpec,
    pub destination: DestinationSpec,
    pub batch_size: usize,
    #[serde(default)]
    pub evolution_policy: EvolutionPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_envelope_roundtrips_via_yaml() {
        let yaml = r#"
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: source-demo
spec:
  connector_ref: postgres@0.1.0
  config:
    url: postgres://etl:etl@localhost:5432/etl_source_demo
"#;
        let env: ResourceEnvelope = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(env.api_version, "platform.etl/v0");
        assert_eq!(env.kind, ResourceKind::Connection);
        assert_eq!(env.metadata.name, "source-demo");
        let spec: ConnectionSpec = serde_json::from_value(env.spec.clone()).unwrap();
        assert_eq!(spec.connector_ref, "postgres@0.1.0");
    }
}
