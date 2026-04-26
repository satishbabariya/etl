//! Canonical bytes are hand-written (not serde_json::to_string) so the
//! hash is stable across serde versions, key orderings, and feature
//! flags. Format:
//!
//!   tenant_id_bytes(16) OR 16 zero bytes (system-scoped)
//!     || principal_id_bytes(16) OR 16 zero bytes
//!     || jti_bytes(16) OR 16 zero bytes
//!     || action_len_be4 || action_utf8
//!     || target_len_be4 || target_utf8       (target_len = 0 if None)
//!     || occurred_at_unix_micros_be8
//!     || payload_len_be4 || payload_canonical_json_utf8

use chrono::{DateTime, Utc};
use common_types::ids::{PrincipalId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEvent {
    TenantCreate,
    TenantSuspend,
    TenantResume,
    TenantTerminate,
    PrincipalCreate,
    SecretCreate,
    SecretDelete,
    SecretRead,
    ConnectionApply,
    PipelineApply,
    AuthLogin,
    AuthLoginFailed,
    AuthRefresh,
    AuthLogout,
    TokenRevoke,
    TenantOverride,
}

impl AuditEvent {
    pub fn as_action_str(self) -> &'static str {
        match self {
            Self::TenantCreate => "TENANT_CREATE",
            Self::TenantSuspend => "TENANT_SUSPEND",
            Self::TenantResume => "TENANT_RESUME",
            Self::TenantTerminate => "TENANT_TERMINATE",
            Self::PrincipalCreate => "PRINCIPAL_CREATE",
            Self::SecretCreate => "SECRET_CREATE",
            Self::SecretDelete => "SECRET_DELETE",
            Self::SecretRead => "SECRET_READ",
            Self::ConnectionApply => "CONNECTION_APPLY",
            Self::PipelineApply => "PIPELINE_APPLY",
            Self::AuthLogin => "AUTH_LOGIN",
            Self::AuthLoginFailed => "AUTH_LOGIN_FAILED",
            Self::AuthRefresh => "AUTH_REFRESH",
            Self::AuthLogout => "AUTH_LOGOUT",
            Self::TokenRevoke => "TOKEN_REVOKE",
            Self::TenantOverride => "TENANT_OVERRIDE",
        }
    }
}

#[derive(Clone, Debug)]
pub struct AuditRow {
    pub tenant_id: Option<TenantId>,
    pub principal_id: Option<PrincipalId>,
    pub jti: Option<Uuid>,
    pub event: AuditEvent,
    pub target: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

impl AuditRow {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(
            self.tenant_id
                .map(|t| *t.as_uuid().as_bytes())
                .unwrap_or([0u8; 16])
                .as_slice(),
        );
        out.extend_from_slice(
            self.principal_id
                .map(|p| *p.as_uuid().as_bytes())
                .unwrap_or([0u8; 16])
                .as_slice(),
        );
        out.extend_from_slice(
            self.jti
                .map(|j| *j.as_bytes())
                .unwrap_or([0u8; 16])
                .as_slice(),
        );
        let action = self.event.as_action_str();
        out.extend_from_slice(&(action.len() as u32).to_be_bytes());
        out.extend_from_slice(action.as_bytes());
        let target_bytes = self.target.as_deref().unwrap_or("").as_bytes();
        out.extend_from_slice(&(target_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(target_bytes);
        out.extend_from_slice(&self.occurred_at.timestamp_micros().to_be_bytes());
        let payload_bytes = canon_bytes(&self.payload);
        out.extend_from_slice(&(payload_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload_bytes);
        out
    }
}

/// Sort object keys alphabetically and emit JSON. Stable across runs.
pub fn canon_bytes(v: &Value) -> Vec<u8> {
    fn walk(v: &Value, out: &mut Vec<u8>) {
        match v {
            Value::Null => out.extend_from_slice(b"null"),
            Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
            Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
            Value::String(s) => {
                out.push(b'"');
                for ch in s.chars() {
                    match ch {
                        '"' => out.extend_from_slice(b"\\\""),
                        '\\' => out.extend_from_slice(b"\\\\"),
                        '\n' => out.extend_from_slice(b"\\n"),
                        '\r' => out.extend_from_slice(b"\\r"),
                        '\t' => out.extend_from_slice(b"\\t"),
                        c if (c as u32) < 0x20 => {
                            out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes())
                        }
                        c => {
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                        }
                    }
                }
                out.push(b'"');
            }
            Value::Array(a) => {
                out.push(b'[');
                for (i, e) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    walk(e, out);
                }
                out.push(b']');
            }
            Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                out.push(b'{');
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    walk(&Value::String((*k).clone()), out);
                    out.push(b':');
                    walk(&m[*k], out);
                }
                out.push(b'}');
            }
        }
    }
    let mut out = Vec::new();
    walk(v, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canon_sorts_object_keys() {
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_eq!(canon_bytes(&a), canon_bytes(&b));
        assert_eq!(String::from_utf8(canon_bytes(&a)).unwrap(), r#"{"a":2,"b":1}"#);
    }

    #[test]
    fn canon_handles_nested_objects_and_strings() {
        let v = json!({"name": "with \"quote\"", "kids": [3, 1, 2]});
        let bytes = canon_bytes(&v);
        let s = String::from_utf8(bytes).unwrap();
        assert_eq!(s, r#"{"kids":[3,1,2],"name":"with \"quote\""}"#);
    }

    #[test]
    fn row_canonical_bytes_are_deterministic() {
        let row1 = AuditRow {
            tenant_id: Some(TenantId::from_uuid_unchecked(
                Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            )),
            principal_id: None,
            jti: None,
            event: AuditEvent::SecretCreate,
            target: Some("pg-url".into()),
            occurred_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            payload: json!({"backend": "file", "key": "pg-url"}),
        };
        let row2 = row1.clone();
        assert_eq!(row1.canonical_bytes(), row2.canonical_bytes());
    }
}
