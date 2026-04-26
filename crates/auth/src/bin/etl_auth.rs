use anyhow::{Context, Result};
use auth::jwks::jwks_from_keystore;
use auth::jwt::JwtIssuer;
use auth::keystore::Keystore;
use auth::refresh;
use axum::{extract::State, http::StatusCode, response::Json, routing::{get, post}, Router};
use catalog::{Catalog, NewRefreshToken};
use chrono::Utc;
use clap::{Parser, Subcommand};
use common_types::auth::Role;
use common_types::ids::TenantContext;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "etl-auth", about = "ETL platform auth issuer (Phase II.2.c)")]
struct Cli {
    /// Keystore root, default ~/.etl/auth-keys
    #[arg(long, env = "ETL_AUTH_KEYS_DIR")]
    keys_dir: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a new RSA-2048 keypair and mark it active.
    InitIssuer,
    /// Print the current JWKS to stdout.
    ShowJwks,
    /// Generate a new keypair, mark it active; old keys remain in JWKS for verification.
    RotateKey,
    /// Encrypt every key in the keystore in place using ETL_MASTER_KEY.
    /// Removes the plaintext files. Idempotent for already-sealed keys.
    SealKeys {
        /// Required confirmation flag — sealing is irreversible without
        /// the master key.
        #[arg(long)]
        confirm: bool,
    },
    /// Run the issuer HTTP server (login + refresh + JWKS + healthz/readyz).
    Serve {
        #[arg(long, default_value = "0.0.0.0:8400")]
        bind: String,
        #[arg(long, env = "ETL_AUTH_ISSUER", default_value = "http://localhost:8400")]
        issuer_url: String,
        #[arg(long, env = "ETL_AUTH_AUDIENCE", default_value = "etl-platform")]
        audience: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        #[arg(long, env = "ETL_AUDIT_RETENTION_DAYS", default_value_t = 365)]
        audit_retention_days: i64,
    },
    /// Revoke an access token by its jti.
    Revoke {
        jti: String,
        #[arg(long)]
        tenant: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

fn keys_dir(arg: Option<PathBuf>) -> PathBuf {
    arg.unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".etl/auth-keys")
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let ks = Keystore::open(keys_dir(cli.keys_dir));
    match cli.cmd {
        Cmd::InitIssuer => {
            let kid = ks.init()?;
            println!("created keypair {kid} under {}", ks.root().display());
            let set = jwks_from_keystore(&ks)?;
            println!("{}", serde_json::to_string_pretty(&set)?);
            Ok(())
        }
        Cmd::ShowJwks => {
            let set = jwks_from_keystore(&ks)?;
            println!("{}", serde_json::to_string_pretty(&set)?);
            Ok(())
        }
        Cmd::RotateKey => {
            let kid = ks.init()?;
            println!("rotated to new active kid {kid} (old keys retained for verification)");
            Ok(())
        }
        Cmd::SealKeys { confirm } => {
            if !confirm {
                anyhow::bail!(
                    "seal-keys is destructive (removes plaintext). Re-run with --confirm."
                );
            }
            let n = ks.seal_in_place()?;
            println!("sealed {n} keypair(s) under {}", ks.root().display());
            Ok(())
        }
        Cmd::Serve {
            bind,
            issuer_url,
            audience,
            database_url,
            audit_retention_days,
        } => serve(ks, bind, issuer_url, audience, database_url, audit_retention_days).await,
        Cmd::Revoke {
            jti,
            tenant,
            database_url,
        } => revoke(jti, tenant, database_url).await,
    }
}

#[derive(Clone)]
struct AppState {
    keystore: Arc<Keystore>,
    catalog: Arc<Catalog>,
    issuer_url: String,
    audience: String,
}

async fn serve(
    ks: Keystore,
    bind: String,
    issuer_url: String,
    audience: String,
    database_url: String,
    audit_retention_days: i64,
) -> Result<()> {
    let cat = Arc::new(Catalog::connect(&database_url).await?);
    cat.migrate().await?;
    let state = AppState {
        keystore: Arc::new(ks),
        catalog: cat.clone(),
        issuer_url,
        audience,
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let job_handles =
        auth::jobs::spawn_all(cat.clone(), audit_retention_days, shutdown_rx.clone());
    let app = Router::new()
        .route("/.well-known/jwks.json", get(jwks_endpoint))
        .route("/auth/login", post(login_endpoint))
        .route("/auth/refresh", post(refresh_endpoint))
        .route("/auth/logout", post(logout_endpoint))
        .route("/healthz", get(|| async { axum::http::StatusCode::OK }))
        .route("/readyz", get(readyz_endpoint))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(%bind, "etl-auth issuer serving");
    let server = axum::serve(listener, app);
    let result = tokio::select! {
        r = server => r.map_err(anyhow::Error::from),
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received, shutting down");
            Ok(())
        }
    };
    let _ = shutdown_tx.send(true);
    for h in job_handles {
        let _ = h.await;
    }
    result
}

async fn readyz_endpoint(State(s): State<AppState>) -> axum::http::StatusCode {
    match sqlx::query("SELECT 1").execute(s.catalog.pool()).await {
        Ok(_) => axum::http::StatusCode::OK,
        Err(_) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn jwks_endpoint(State(s): State<AppState>) -> Json<auth::jwks::JwkSet> {
    let set = jwks_from_keystore(&s.keystore)
        .unwrap_or_else(|_| auth::jwks::JwkSet { keys: vec![] });
    Json(set)
}

#[derive(Deserialize)]
struct LoginReq {
    name: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResp {
    access_token: String,
    refresh_token: String,
    expires_in: i64,
}

const ACCESS_TTL_SECS: i64 = 15 * 60;

fn issue_pair(
    s: &AppState,
    p: &catalog::principal::Principal,
    role: Role,
) -> std::result::Result<LoginResp, (StatusCode, String)> {
    let kid = s
        .keystore
        .active_kid()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let private_pem = s
        .keystore
        .private_pem(&kid)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let issuer = JwtIssuer::rs256_pem(
        &private_pem,
        &kid,
        ACCESS_TTL_SECS,
        &s.issuer_url,
        &s.audience,
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let access = issuer
        .issue(p.principal_id, p.tenant_id, role)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(LoginResp {
        access_token: access,
        refresh_token: String::new(),
        expires_in: ACCESS_TTL_SECS,
    })
}

async fn login_endpoint(
    State(s): State<AppState>,
    Json(req): Json<LoginReq>,
) -> std::result::Result<Json<LoginResp>, (StatusCode, String)> {
    let row = s
        .catalog
        .principal_get_by_name(&req.name)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let (principal, hash) = match row {
        Some(r) => r,
        None => {
            audit_record(
                &s.catalog,
                None,
                None,
                None,
                audit::AuditEvent::AuthLoginFailed,
                Some(req.name.clone()),
                serde_json::json!({"reason": "no_such_principal"}),
            )
            .await;
            return Err((StatusCode::UNAUTHORIZED, "invalid login".into()));
        }
    };
    if !catalog::principal::verify_password(&req.password, &hash) {
        audit_record(
            &s.catalog,
            Some(principal.tenant_id),
            None,
            None,
            audit::AuditEvent::AuthLoginFailed,
            Some(req.name.clone()),
            serde_json::json!({"reason": "wrong_password"}),
        )
        .await;
        return Err((StatusCode::UNAUTHORIZED, "invalid login".into()));
    }
    let role: Role = serde_json::from_str(&format!("\"{}\"", principal.role))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad role".into()))?;
    let mut resp = issue_pair(&s, &principal, role)?;
    audit_record(
        &s.catalog,
        Some(principal.tenant_id),
        Some(principal.principal_id),
        None,
        audit::AuditEvent::AuthLogin,
        Some(principal.name.clone()),
        serde_json::json!({}),
    )
    .await;
    let (secret, hash, exp) = refresh::mint()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let token_id = s
        .catalog
        .refresh_create(
            TenantContext::authed(principal.tenant_id, principal.principal_id, role),
            NewRefreshToken {
                tenant_id: principal.tenant_id,
                principal_id: principal.principal_id,
                hash,
                expires_at: exp,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    resp.refresh_token = refresh::format_plaintext(token_id, &secret);
    Ok(Json(resp))
}

async fn audit_record(
    cat: &Catalog,
    tenant_id: Option<common_types::ids::TenantId>,
    principal_id: Option<common_types::ids::PrincipalId>,
    jti: Option<uuid::Uuid>,
    event: audit::AuditEvent,
    target: Option<String>,
    payload: serde_json::Value,
) {
    let row = audit::AuditRow {
        tenant_id,
        principal_id,
        jti,
        event,
        target,
        occurred_at: chrono::Utc::now(),
        payload,
    };
    if let Err(e) = cat.audit_write(&row).await {
        tracing::warn!(error = %e, "audit_write failed");
    }
}

#[derive(Deserialize)]
struct RefreshReq {
    refresh_token: String,
}

async fn refresh_endpoint(
    State(s): State<AppState>,
    Json(req): Json<RefreshReq>,
) -> std::result::Result<Json<LoginResp>, (StatusCode, String)> {
    let (token_id, secret) = refresh::parse_plaintext(&req.refresh_token)
        .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid refresh token".into()))?;
    let row = s
        .catalog
        .refresh_get(token_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::UNAUTHORIZED, "refresh token unknown or expired".into()))?;
    if !refresh::verify(secret, &row.hash) {
        return Err((StatusCode::UNAUTHORIZED, "invalid refresh token".into()));
    }
    // Rotate-on-use: delete the consumed row.
    s.catalog
        .refresh_delete(token_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Re-load principal to recover role.
    let (p, _) = s
        .catalog
        .principal_get_by_id(row.principal_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::UNAUTHORIZED, "principal gone".into()))?;
    let role: Role = serde_json::from_str(&format!("\"{}\"", p.role))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "bad role".into()))?;
    let mut resp = issue_pair(&s, &p, role)?;
    let (new_secret, new_hash, new_exp) = refresh::mint()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let new_id = s
        .catalog
        .refresh_create(
            TenantContext::authed(p.tenant_id, p.principal_id, role),
            NewRefreshToken {
                tenant_id: p.tenant_id,
                principal_id: p.principal_id,
                hash: new_hash,
                expires_at: new_exp,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    resp.refresh_token = refresh::format_plaintext(new_id, &new_secret);
    audit_record(
        &s.catalog,
        Some(p.tenant_id),
        Some(p.principal_id),
        None,
        audit::AuditEvent::AuthRefresh,
        Some(p.name.clone()),
        serde_json::json!({}),
    )
    .await;
    Ok(Json(resp))
}

#[derive(Deserialize)]
struct LogoutReq {
    refresh_token: String,
}

async fn logout_endpoint(
    State(s): State<AppState>,
    Json(req): Json<LogoutReq>,
) -> std::result::Result<StatusCode, (StatusCode, String)> {
    if let Ok((token_id, _)) = refresh::parse_plaintext(&req.refresh_token) {
        let _ = s.catalog.refresh_delete(token_id).await;
    }
    audit_record(
        &s.catalog,
        None,
        None,
        None,
        audit::AuditEvent::AuthLogout,
        None,
        serde_json::json!({}),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn revoke(jti: String, tenant: String, database_url: String) -> Result<()> {
    let cat = Catalog::connect(&database_url).await?;
    cat.migrate().await?;
    let t = cat
        .get_tenant_by_name(&tenant)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tenant {tenant} not found"))?;
    let jti_uuid = uuid::Uuid::parse_str(&jti)?;
    let exp = Utc::now() + chrono::Duration::days(1);
    cat.revoke_insert(TenantContext::new(t.tenant_id), jti_uuid, exp)
        .await?;
    audit_record(
        &cat,
        Some(t.tenant_id),
        None,
        None,
        audit::AuditEvent::TokenRevoke,
        Some(jti.clone()),
        serde_json::json!({}),
    )
    .await;
    println!("revoked jti={jti} for tenant={tenant}");
    Ok(())
}
