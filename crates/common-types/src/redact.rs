//! Redaction for connection strings that end up in errors and logs.
//!
//! Errors are logged and recorded in Temporal failure payloads, which
//! persist in workflow history, so anything formatted into one must be
//! scrubbed first.

/// Placeholder for values that can't be redacted piecemeal.
pub const REDACTED: &str = "<redacted>";

/// Return `raw` with the userinfo password and any `password=` query
/// parameter replaced by `***`:
/// `postgres://user:secret@host/db` → `postgres://user:***@host/db`.
///
/// Anything that isn't a hierarchical URL (key=value DSNs, bare API
/// keys) becomes [`REDACTED`] wholesale rather than being guessed at.
pub fn redact_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw) else {
        return REDACTED.into();
    };
    // An unencoded `/` in the password ends the authority early and
    // leaves the rest of it (and the `@`) in the path.
    if url.cannot_be_a_base() || !url.has_host() || url[url::Position::AfterPort..].contains('@') {
        return REDACTED.into();
    }
    if url.password().is_some() {
        let _ = url.set_password(Some("***"));
    }
    if url
        .query_pairs()
        .any(|(k, _)| k.eq_ignore_ascii_case("password"))
    {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                let v = if k.eq_ignore_ascii_case("password") {
                    "***".into()
                } else {
                    v.into_owned()
                };
                (k.into_owned(), v)
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_userinfo_password() {
        assert_eq!(
            redact_url("postgres://user:secret@host/db"),
            "postgres://user:***@host/db"
        );
    }

    #[test]
    fn keeps_host_port_db_and_params() {
        let r = redact_url("postgres://etl:hunter2@db.internal:5433/etl_dest?sslmode=require");
        assert!(!r.contains("hunter2"), "got {r}");
        assert_eq!(
            r,
            "postgres://etl:***@db.internal:5433/etl_dest?sslmode=require"
        );
    }

    #[test]
    fn redacts_password_query_param() {
        let r = redact_url("postgres://etl@host/db?password=hunter2&sslmode=disable");
        assert!(!r.contains("hunter2"), "got {r}");
        assert!(r.contains("sslmode=disable"), "got {r}");
    }

    #[test]
    fn redacts_percent_encoded_password() {
        let r = redact_url("mysql://root:p%40ss%2Fword@host:3306/app");
        assert!(!r.contains("p%40ss"), "got {r}");
        assert_eq!(r, "mysql://root:***@host:3306/app");
    }

    #[test]
    fn leaves_url_without_password_unchanged() {
        assert_eq!(
            redact_url("postgres://etl@localhost:5432/etl"),
            "postgres://etl@localhost:5432/etl"
        );
    }

    #[test]
    fn unencoded_slash_in_password_is_fully_redacted() {
        // `/` ends the authority, so the parser sees host `u`, port `12`
        // and a path holding the rest of the password.
        let r = redact_url("postgres://u:12/secret@host/db");
        assert!(!r.contains("secret"), "got {r}");
    }

    #[test]
    fn non_url_input_is_fully_redacted() {
        // key=value DSNs and bare API keys (WASM connectors carry e.g. a
        // Stripe key in ConnectionConfig.url) aren't URLs.
        for raw in [
            "host=db user=etl password=hunter2",
            "sk_test_hunter2",
            "user:hunter2",
        ] {
            assert_eq!(redact_url(raw), REDACTED, "input {raw}");
        }
    }
}
