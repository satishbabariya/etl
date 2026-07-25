//! Minimal Snowflake SQL API v2 client (statements endpoint only).
//!
//! Base URL defaults to `https://<account>.snowflakecomputing.com`;
//! `ETL_SNOWFLAKE_BASE_URL` overrides it (integration-test seam) and
//! `with_base_url` overrides it programmatically (unit-test seam).
//! Async statement execution (HTTP 202) is polled until terminal.

use anyhow::{bail, Context};
use common_types::pipeline_spec::SnowflakeDestinationSpec;

use super::jwt::KeyPairAuth;

pub struct SnowflakeClient {
    http: reqwest::Client,
    base_url: String,
    auth: KeyPairAuth,
    warehouse: String,
    database: String,
    schema: String,
    role: Option<String>,
}

pub fn rowset_len(body: &serde_json::Value) -> usize {
    body.get("data")
        .and_then(|d| d.as_array())
        .map(|a| a.len())
        .unwrap_or(0)
}

impl SnowflakeClient {
    pub fn from_spec(spec: &SnowflakeDestinationSpec) -> anyhow::Result<Self> {
        let pem = std::env::var(&spec.private_key_env).with_context(|| {
            format!("env var {} (private_key_env) not set", spec.private_key_env)
        })?;
        let base_url = std::env::var("ETL_SNOWFLAKE_BASE_URL").unwrap_or_else(|_| {
            format!("https://{}.snowflakecomputing.com", spec.account.to_lowercase())
        });
        Ok(Self {
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            auth: KeyPairAuth::from_pem(&spec.account, &spec.user, &pem)?,
            warehouse: spec.warehouse.clone(),
            database: spec.database.clone(),
            schema: spec.schema.clone(),
            role: spec.role.clone(),
        })
    }

    pub fn with_base_url(mut self, url: &str) -> Self {
        self.base_url = url.trim_end_matches('/').to_string();
        self
    }

    async fn post_statement(
        &self,
        statement: &str,
        multi_count: Option<usize>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut body = serde_json::json!({
            "statement": statement,
            "timeout": 60,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        });
        if let Some(role) = &self.role {
            body["role"] = serde_json::json!(role);
        }
        if let Some(n) = multi_count {
            body["parameters"] = serde_json::json!({"MULTI_STATEMENT_COUNT": n.to_string()});
        }
        let token = self.auth.token()?;
        let resp = self
            .http
            .post(format!("{}/api/v2/statements", self.base_url))
            .bearer_auth(&token)
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .context("snowflake POST /statements")?;
        self.handle_response(resp, &token).await
    }

    async fn handle_response(
        &self,
        resp: reqwest::Response,
        token: &str,
    ) -> anyhow::Result<serde_json::Value> {
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        if status.as_u16() == 202 {
            let handle = body["statementHandle"]
                .as_str()
                .context("202 without statementHandle")?
                .to_string();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                if std::time::Instant::now() > deadline {
                    bail!("snowflake statement {handle} still running after 60s");
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let r = self
                    .http
                    .get(format!("{}/api/v2/statements/{handle}", self.base_url))
                    .bearer_auth(token)
                    .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
                    .send()
                    .await
                    .context("snowflake GET /statements/<handle>")?;
                if r.status().as_u16() == 202 {
                    continue;
                }
                let s = r.status();
                let b: serde_json::Value = r.json().await.unwrap_or_default();
                if s.is_success() {
                    return Ok(b);
                }
                bail!(
                    "snowflake statement failed ({s}): {}",
                    b["message"].as_str().unwrap_or("<no message>")
                );
            }
        }
        if status.is_success() {
            return Ok(body);
        }
        bail!(
            "snowflake request failed ({status}): {}",
            body["message"].as_str().unwrap_or("<no message>")
        );
    }

    pub async fn execute(&self, sql: &str) -> anyhow::Result<serde_json::Value> {
        self.post_statement(sql, None).await
    }

    pub async fn execute_multi(&self, statements: &[String]) -> anyhow::Result<()> {
        let joined = statements.join(";\n");
        self.post_statement(&joined, Some(statements.len()))
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_pem() -> String {
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rand::thread_rng();
        let key = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    fn spec(key_env: &str) -> common_types::pipeline_spec::SnowflakeDestinationSpec {
        std::env::set_var(key_env, test_pem());
        common_types::pipeline_spec::SnowflakeDestinationSpec {
            account: "xy12345".into(),
            user: "u".into(),
            private_key_env: key_env.into(),
            warehouse: "W".into(),
            database: "DB".into(),
            schema: "RAW".into(),
            table: "t".into(),
            role: None,
            pk_columns: vec![],
        }
    }

    #[tokio::test]
    async fn execute_posts_statement_with_jwt_headers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v2/statements"))
            .and(header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [["1"]], "statementHandle": "h1"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = SnowflakeClient::from_spec(&spec("SNOWTEST_KEY_A"))
            .unwrap()
            .with_base_url(&server.uri());
        let body = client.execute("SELECT 1").await.unwrap();
        assert_eq!(rowset_len(&body), 1);
    }

    #[tokio::test]
    async fn execute_polls_on_202() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v2/statements"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_json(serde_json::json!({"statementHandle": "abc"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v2/statements/abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []})))
            .mount(&server)
            .await;
        let client = SnowflakeClient::from_spec(&spec("SNOWTEST_KEY_B"))
            .unwrap()
            .with_base_url(&server.uri());
        let body = client.execute("SELECT 1").await.unwrap();
        assert_eq!(rowset_len(&body), 0);
    }

    #[tokio::test]
    async fn non_2xx_surfaces_snowflake_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v2/statements"))
            .respond_with(
                ResponseTemplate::new(422)
                    .set_body_json(serde_json::json!({"message": "SQL compilation error"})),
            )
            .mount(&server)
            .await;
        let client = SnowflakeClient::from_spec(&spec("SNOWTEST_KEY_C"))
            .unwrap()
            .with_base_url(&server.uri());
        let err = client.execute("BROKEN").await.unwrap_err().to_string();
        assert!(err.contains("SQL compilation error"), "got: {err}");
    }
}
