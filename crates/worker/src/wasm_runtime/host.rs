use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};

use super::bindings::platform::connector::host;
use super::bindings::platform::connector::host::{HttpRequest, HttpResponse, LogLevel};

/// Per-invocation host state.
pub struct HostState {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    pub http: reqwest::Client,
    pub limits: super::limits::Limits,
}

impl HostState {
    pub fn new(limits: super::limits::Limits) -> Self {
        // Deliberately no filesystem preopens, no network, no env-vars.
        // Phase I.3 connectors get only what we explicitly linked below.
        let wasi = WasiCtxBuilder::new().build();
        Self {
            wasi,
            table: ResourceTable::new(),
            http: reqwest::Client::builder()
                .user_agent("etl-platform/0.1")
                .build()
                .expect("reqwest client"),
            limits,
        }
    }
}

impl WasiView for HostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

#[wasmtime::component::__internal::async_trait]
impl host::Host for HostState {
    async fn log(&mut self, level: LogLevel, message: String) {
        match level {
            LogLevel::Trace => tracing::trace!(guest = true, "{}", message),
            LogLevel::Debug => tracing::debug!(guest = true, "{}", message),
            LogLevel::Info => tracing::info!(guest = true, "{}", message),
            LogLevel::Warn => tracing::warn!(guest = true, "{}", message),
            LogLevel::Error => tracing::error!(guest = true, "{}", message),
        }
    }

    async fn http_fetch(&mut self, request: HttpRequest) -> Result<HttpResponse, String> {
        let method = request
            .method
            .parse::<reqwest::Method>()
            .map_err(|e| format!("bad method {}: {e}", request.method))?;
        let mut req = self.http.request(method, &request.url);
        for (k, v) in &request.headers {
            req = req.header(k, v);
        }
        if let Some(body) = request.body {
            req = req.body(body);
        }
        let resp = req.send().await.map_err(|e| format!("send: {e}"))?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("read body: {e}"))?
            .to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}
