//! WasmSourceConnector: a `SourceConnector` implementation that dispatches
//! to a WASM Component Model guest.

use anyhow::{Context, bail};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_types::connection_config::ConnectionConfig;
use common_types::cursor::{CursorKind, CursorValue};
use common_types::pipeline_spec::SourceSpec;
use connector_sdk::{ReadOutcome, SourceConnector};
use std::sync::Arc;
use wasmtime::Store;

use super::HostState;
use super::bindings::SourceConnector as SourceConnectorBindings;
use super::bindings::platform::connector::types as wit_types;
use super::runtime::WasmSourceRuntime;

pub struct WasmSourceConnector {
    runtime: Arc<WasmSourceRuntime>,
    name_at_version: String,
}

impl WasmSourceConnector {
    pub fn new(runtime: Arc<WasmSourceRuntime>, name_at_version: impl Into<String>) -> Self {
        Self {
            runtime,
            name_at_version: name_at_version.into(),
        }
    }

    fn wasm_source_json(source: &SourceSpec) -> anyhow::Result<String> {
        match source {
            SourceSpec::Wasm(spec) => Ok(serde_json::to_string(&spec.config)?),
            SourceSpec::Postgres(_) => bail!(
                "WasmSourceConnector only handles SourceSpec::Wasm; got SourceSpec::Postgres"
            ),
        }
    }

    async fn new_store(&self) -> Store<HostState> {
        let limits = super::Limits::default();
        let state = HostState::new(limits.clone());
        let mut store = Store::new(self.runtime.engine(), state);
        store.set_fuel(limits.fuel).expect("fuel enabled in Engine");
        store.set_epoch_deadline(limits.wall_time_secs);
        store.limiter(|s: &mut HostState| &mut s.memory_limiter);
        store
    }
}

#[async_trait]
impl SourceConnector for WasmSourceConnector {
    async fn discover(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
    ) -> anyhow::Result<arrow::datatypes::SchemaRef> {
        let component = self.runtime.load(&self.name_at_version)?;
        let mut store = self.new_store().await;
        let bindings = SourceConnectorBindings::instantiate_async(
            &mut store,
            &component,
            self.runtime.linker(),
        )
        .await
        .context("instantiating component")?;

        let wit_conn = wit_types::ConnectionConfig {
            url: conn.url.clone(),
        };
        let wit_source = wit_types::SourceConfig {
            json: Self::wasm_source_json(source)?,
        };

        let schema_bytes = bindings
            .call_discover(&mut store, &wit_conn, &wit_source)
            .await
            .context("call_discover")?
            .map_err(|e| anyhow::anyhow!("guest error: {e:?}"))?;

        let reader = StreamReader::try_new(&*schema_bytes, None)
            .context("parsing schema bytes as Arrow IPC stream header")?;
        Ok(reader.schema())
    }

    async fn read_batch(
        &self,
        conn: &ConnectionConfig,
        source: &SourceSpec,
        cursor: Option<CursorValue>,
        batch_size: usize,
    ) -> anyhow::Result<ReadOutcome> {
        let component = self.runtime.load(&self.name_at_version)?;
        let mut store = self.new_store().await;
        let bindings = SourceConnectorBindings::instantiate_async(
            &mut store,
            &component,
            self.runtime.linker(),
        )
        .await
        .context("instantiating component")?;

        let wit_conn = wit_types::ConnectionConfig {
            url: conn.url.clone(),
        };
        let wit_source = wit_types::SourceConfig {
            json: Self::wasm_source_json(source)?,
        };
        let wit_cursor = cursor.map(|c| wit_types::CursorValue {
            kind: match c.kind {
                CursorKind::Int64 => wit_types::CursorKind::Int64,
                CursorKind::TimestampTz => wit_types::CursorKind::TimestampTz,
            },
            value: c.value,
        });

        let outcome = bindings
            .call_read_batch(
                &mut store,
                &wit_conn,
                &wit_source,
                wit_cursor.as_ref(),
                batch_size as u32,
            )
            .await
            .context("call_read_batch")?
            .map_err(|e| anyhow::anyhow!("guest error: {e:?}"))?;

        let batch = if outcome.batch_ipc.is_empty() {
            let schema = Arc::new(arrow::datatypes::Schema::empty());
            RecordBatch::new_empty(schema)
        } else {
            let mut reader = StreamReader::try_new(&*outcome.batch_ipc, None)
                .context("parsing batch as Arrow IPC")?;
            reader
                .next()
                .context("guest returned non-empty batch_ipc but no batches")?
                .context("decoding batch")?
        };

        let new_cursor = outcome.new_cursor.map(|c| CursorValue {
            kind: match c.kind {
                wit_types::CursorKind::Int64 => CursorKind::Int64,
                wit_types::CursorKind::TimestampTz => CursorKind::TimestampTz,
            },
            value: c.value,
        });

        Ok(ReadOutcome {
            batch,
            new_cursor,
            is_final: outcome.is_final,
        })
    }
}
