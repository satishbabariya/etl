//! stripe-source — Stripe /v1/customers as an ETL source.
//!
//! ConnectionConfig.url carries the Stripe secret API key
//! (`sk_test_...` or `sk_live_...`). SourceConfig.json optionally
//! carries `{"base_url":"https://api.stripe.com","limit":100,
//! "max_429_retries":3}`; defaults applied when absent.

pub mod parse;
pub mod request;

wit_bindgen::generate!({
    path: "wit",
    world: "source-connector",
});

use platform::connector::host::{http_fetch, log, HttpRequest, LogLevel};
use platform::connector::types::CursorKind;
use serde::Deserialize;

struct Component;
export!(Component);

#[derive(Deserialize, Default)]
struct StripeSourceCfg {
    #[serde(default = "default_base_url")]
    base_url: String,
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default = "default_max_429_retries")]
    max_429_retries: u32,
}
fn default_base_url() -> String { "https://api.stripe.com".into() }
fn default_limit() -> u32 { 100 }
fn default_max_429_retries() -> u32 { 3 }

fn parse_source_cfg(json: &str) -> StripeSourceCfg {
    if json.trim().is_empty() {
        return StripeSourceCfg {
            base_url: default_base_url(),
            limit: default_limit(),
            max_429_retries: default_max_429_retries(),
        };
    }
    serde_json::from_str(json).unwrap_or_else(|_| StripeSourceCfg {
        base_url: default_base_url(),
        limit: default_limit(),
        max_429_retries: default_max_429_retries(),
    })
}

fn fetch_with_retry(req: &HttpRequest, max_retries: u32) -> Result<Vec<u8>, String> {
    let mut attempt = 0u32;
    loop {
        let resp = http_fetch(req)?;
        if resp.status == 429 && attempt < max_retries {
            log(
                LogLevel::Warn,
                &format!("stripe-source: 429 rate-limit, retry {}/{}", attempt + 1, max_retries),
            );
            attempt += 1;
            continue;
        }
        if resp.status >= 200 && resp.status < 300 {
            return Ok(resp.body);
        }
        return Err(format!(
            "stripe HTTP {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        ));
    }
}

impl Guest for Component {
    fn discover(_conn: ConnectionConfig, _source: SourceConfig) -> Result<Vec<u8>, ConnectorError> {
        parse::schema_ipc_bytes()
            .map_err(|e| ConnectorError::Other(format!("schema ipc: {e}")))
    }

    fn read_batch(
        conn: ConnectionConfig,
        source: SourceConfig,
        cursor: Option<CursorValue>,
        _batch_size: u32,
    ) -> Result<ReadOutcome, ConnectorError> {
        let cfg = parse_source_cfg(&source.json);
        let starting_after = cursor.as_ref().map(|c| c.value.as_str());
        let req = request::build_list_customers(&conn.url, cfg.limit, starting_after, &cfg.base_url);
        let http_req = HttpRequest {
            method: "GET".into(),
            url: req.url.clone(),
            headers: req.headers,
            body: None,
        };
        let body = fetch_with_retry(&http_req, cfg.max_429_retries)
            .map_err(|e| ConnectorError::SourceUnavailable(e))?;
        let page = parse::parse_page(&body)
            .map_err(|e| ConnectorError::Other(e))?;
        let new_cursor = page.last_id.map(|id| CursorValue {
            kind: CursorKind::Int64,
            value: id,
        });
        Ok(ReadOutcome {
            batch_ipc: page.batch_ipc,
            rows: page.rows,
            new_cursor,
            is_final: !page.has_more,
        })
    }
}
