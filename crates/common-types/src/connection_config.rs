use serde::{Deserialize, Serialize};

/// Connection parameters for a connector. Phase I.2: a single URL.
/// Phase II.2 (RFC-11) splits this into a reference + resolved secret.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub url: String,
}
