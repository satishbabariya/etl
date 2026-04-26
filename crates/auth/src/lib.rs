//! Phase II.2.b — JWT issue/verify + RBAC role re-export.
//!
//! `Role` and `Action` live in `common-types::auth` to avoid a
//! `common-types → auth` cycle (TenantContext carries Role).

pub mod jobs;
pub mod jwks;
pub mod jwt;
pub mod keystore;
pub mod refresh;
pub mod sealed;

pub use common_types::auth::{Action, Role};
pub use jwt::{AuthError, Claims, JwtIssuer, JwtVerifier, Principal};
