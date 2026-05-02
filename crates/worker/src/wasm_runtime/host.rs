use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

use super::bindings::platform::connector::host;
use super::bindings::platform::connector::host::{HttpRequest, HttpResponse, LogLevel};

/// Per-invocation host state.
pub struct HostState {
    pub wasi: WasiCtx,
    pub wasi_http: WasiHttpCtx,
    pub table: ResourceTable,
    pub http: reqwest::Client,
    pub limits: super::limits::Limits,
    pub memory_limiter: super::limits::MemoryCap,
    pub db: super::db_host::DbHostState,
}

impl HostState {
    pub fn new(limits: super::limits::Limits) -> Self {
        let wasi = WasiCtxBuilder::new().build();
        let memory_limiter = super::limits::MemoryCap {
            max_bytes: limits.memory_bytes,
        };
        Self {
            wasi,
            wasi_http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            http: reqwest::Client::builder()
                .user_agent("etl-platform/0.1")
                .build()
                .expect("reqwest client"),
            limits,
            memory_limiter,
            db: super::db_host::DbHostState::new(),
        }
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for HostState {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl super::scalar_bindings::platform::udf::host::Host for HostState {
    async fn log(
        &mut self,
        level: super::scalar_bindings::platform::udf::host::LogLevel,
        message: String,
    ) -> wasmtime::Result<()> {
        use super::scalar_bindings::platform::udf::host::LogLevel as L;
        match level {
            L::Trace => tracing::trace!(guest = true, udf = true, "{}", message),
            L::Debug => tracing::debug!(guest = true, udf = true, "{}", message),
            L::Info => tracing::info!(guest = true, udf = true, "{}", message),
            L::Warn => tracing::warn!(guest = true, udf = true, "{}", message),
            L::Error => tracing::error!(guest = true, udf = true, "{}", message),
        }
        Ok(())
    }
}

impl host::Host for HostState {
    async fn log(&mut self, level: LogLevel, message: String) -> wasmtime::Result<()> {
        match level {
            LogLevel::Trace => tracing::trace!(guest = true, "{}", message),
            LogLevel::Debug => tracing::debug!(guest = true, "{}", message),
            LogLevel::Info => tracing::info!(guest = true, "{}", message),
            LogLevel::Warn => tracing::warn!(guest = true, "{}", message),
            LogLevel::Error => tracing::error!(guest = true, "{}", message),
        }
        Ok(())
    }

    async fn http_fetch(
        &mut self,
        request: HttpRequest,
    ) -> wasmtime::Result<Result<HttpResponse, String>> {
        let method = match request.method.parse::<reqwest::Method>() {
            Ok(m) => m,
            Err(e) => return Ok(Err(format!("bad method {}: {e}", request.method))),
        };
        let mut req = self.http.request(method, &request.url);
        for (k, v) in &request.headers {
            req = req.header(k, v);
        }
        if let Some(body) = request.body {
            req = req.body(body);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Ok(Err(format!("send: {e}"))),
        };
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = match resp.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => return Ok(Err(format!("read body: {e}"))),
        };
        Ok(Ok(HttpResponse {
            status,
            headers,
            body,
        }))
    }
}
