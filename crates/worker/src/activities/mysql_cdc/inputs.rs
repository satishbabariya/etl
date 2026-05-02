use common_types::connection_config::ConnectionConfig;
use common_types::pipeline_spec::DestinationSpec;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyMysqlConfigInput {
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureStartGtidInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureStartGtidOutput {
    /// Serialized GTID set; "" if MySQL has no GTID history.
    pub gtid_set: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverMysqlSchemaInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverMysqlSchemaOutput {
    /// Arrow schema serialized as JSON. Persisted in catalog and replayed
    /// to subsequent read_window calls so they don't requery the source.
    pub schema_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlReadWindowInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub batch_seq: u32,
    pub source_conn: ConnectionConfig,
    pub server_id: u32,
    pub schema: String,
    pub table: String,
    pub start_gtid: String,
    pub max_events: u32,
    pub schema_json: String,
    pub heartbeat_secs: u32,
    pub destination: DestinationSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlReadWindowOutput {
    pub rows: u32,
    pub new_gtid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotChunkInput {
    pub pipeline_id: Uuid,
    pub run_id: Uuid,
    pub tenant_id: Uuid,
    #[serde(default)]
    pub principal_id: Uuid,
    #[serde(default)]
    pub jti: Uuid,
    pub batch_seq: u32,
    pub source_conn: ConnectionConfig,
    pub schema: String,
    pub table: String,
    pub pk_column: String,
    pub last_pk: Option<i64>,
    pub batch_size: u32,
    pub schema_json: String,
    pub captured_gtid: String,
    pub destination: DestinationSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MysqlSnapshotChunkOutput {
    pub rows: u32,
    pub last_pk: Option<i64>,
    pub is_final: bool,
}
