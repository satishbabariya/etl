//! Phase II.2.b — JWT issue/verify + RBAC role re-export.
//!
//! `Role` and `Action` live in `common-types::auth` to avoid a
//! `common-types → auth` cycle (TenantContext carries Role).

pub mod jwt;

pub use common_types::auth::{Action, Role};
pub use jwt::{AuthError, Claims, JwtIssuer, JwtVerifier, Principal};
