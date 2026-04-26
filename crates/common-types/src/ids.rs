use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a new identifier (UUIDv7 — time-ordered).
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Construct from an existing UUID. Name is explicit so callers
            /// cannot silently fabricate identities.
            pub fn from_uuid_unchecked(u: Uuid) -> Self {
                Self(u)
            }

            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}-{}", $prefix, self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let expected_prefix = concat!($prefix, "-");
                let rest = s
                    .strip_prefix(expected_prefix)
                    .ok_or(IdParseError::WrongPrefix)?;
                let uuid = Uuid::parse_str(rest).map_err(|_| IdParseError::BadUuid)?;
                Ok(Self(uuid))
            }
        }
    };
}

#[derive(thiserror::Error, Debug)]
pub enum IdParseError {
    #[error("identifier has the wrong prefix for its type")]
    WrongPrefix,
    #[error("identifier tail is not a valid UUID")]
    BadUuid,
}

define_id!(TenantId, "ten");

/// Identity carried through every cross-component call. Phase II.2.b
/// extends from a bare TenantId to also carry the authenticated
/// principal + role; admin paths and tests construct via `new` which
/// leaves them None.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantContext {
    pub tenant_id: TenantId,
    pub principal_id: Option<PrincipalId>,
    pub role: Option<crate::auth::Role>,
}

impl TenantContext {
    /// Authenticated context — built from a verified JWT.
    pub fn authed(
        tenant_id: TenantId,
        principal_id: PrincipalId,
        role: crate::auth::Role,
    ) -> Self {
        Self {
            tenant_id,
            principal_id: Some(principal_id),
            role: Some(role),
        }
    }

    /// Tenant-only context for admin paths and tests.
    pub fn new(tenant_id: TenantId) -> Self {
        Self { tenant_id, principal_id: None, role: None }
    }
}

define_id!(ConnectionId, "conn");
define_id!(PipelineId, "pipe");
define_id!(RunId, "run");
define_id!(WorkspaceId, "ws");
define_id!(StreamId, "stream");
define_id!(SchemaId, "sch");
define_id!(SecretId, "sec");
define_id!(PrincipalId, "prn");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_roundtrip() {
        let t = TenantId::new();
        let s = t.to_string();
        assert!(s.starts_with("ten-"));
        let parsed: TenantId = s.parse().unwrap();
        assert_eq!(t, parsed);
    }

    #[test]
    fn wrong_prefix_rejected() {
        let t = TenantId::new();
        let s = t.to_string().replace("ten-", "pipe-");
        let err = s.parse::<TenantId>().unwrap_err();
        assert!(matches!(err, IdParseError::WrongPrefix));
    }

    #[test]
    fn serde_roundtrip_is_bare_uuid() {
        let t = TenantId::new();
        let j = serde_json::to_string(&t).unwrap();
        // With #[serde(transparent)], the JSON form is just the UUID string.
        assert!(j.starts_with('"') && j.ends_with('"'));
        let back: TenantId = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn id_types_are_distinct() {
        // Compile-time test: PipelineId and TenantId are not cross-assignable.
        let _t = TenantId::new();
        let _p = PipelineId::new();
    }
}
