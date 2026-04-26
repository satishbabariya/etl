//! HTTP client for the `etl-auth` issuer.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct LoginReq<'a> {
    name: &'a str,
    password: &'a str,
}

#[derive(Serialize)]
struct RefreshReq<'a> {
    refresh_token: &'a str,
}

#[derive(Serialize)]
struct LogoutReq<'a> {
    refresh_token: &'a str,
}

#[derive(Deserialize, Debug)]
pub struct LoginResp {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
}

pub fn issuer_url() -> String {
    std::env::var("ETL_AUTH_ISSUER").unwrap_or_else(|_| "http://localhost:8400".into())
}

pub async fn login(name: &str, password: &str) -> Result<LoginResp> {
    let url = format!("{}/auth/login", issuer_url());
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&LoginReq { name, password })
        .send()
        .await
        .with_context(|| format!("POST {url}"))?
        .error_for_status()
        .context("login request rejected")?;
    Ok(resp.json().await?)
}

pub async fn refresh(refresh_token: &str) -> Result<LoginResp> {
    let url = format!("{}/auth/refresh", issuer_url());
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&RefreshReq { refresh_token })
        .send()
        .await?
        .error_for_status()
        .context("refresh request rejected")?;
    Ok(resp.json().await?)
}

pub async fn logout(refresh_token: &str) -> Result<()> {
    let url = format!("{}/auth/logout", issuer_url());
    reqwest::Client::new()
        .post(&url)
        .json(&LogoutReq { refresh_token })
        .send()
        .await?;
    Ok(())
}
