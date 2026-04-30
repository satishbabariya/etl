//! Pure HTTP-request builder for Stripe /v1/customers list calls.

pub struct StripeRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

pub fn build_list_customers(
    api_key: &str,
    limit: u32,
    starting_after: Option<&str>,
    base_url: &str,
) -> StripeRequest {
    let mut url = format!("{base_url}/v1/customers?limit={limit}");
    if let Some(after) = starting_after {
        url.push_str("&starting_after=");
        url.push_str(after);
    }
    let headers = vec![
        ("Authorization".into(), format!("Bearer {api_key}")),
        ("Stripe-Version".into(), "2024-04-10".into()),
    ];
    StripeRequest { url, headers }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_page_url() {
        let r = build_list_customers("sk_test_x", 100, None, "https://api.stripe.com");
        assert_eq!(r.url, "https://api.stripe.com/v1/customers?limit=100");
    }

    #[test]
    fn paginated_url() {
        let r = build_list_customers(
            "sk_test_x",
            50,
            Some("cus_42"),
            "https://api.stripe.com",
        );
        assert_eq!(
            r.url,
            "https://api.stripe.com/v1/customers?limit=50&starting_after=cus_42"
        );
    }

    #[test]
    fn auth_header_uses_bearer() {
        let r = build_list_customers("sk_test_secret", 1, None, "https://api.stripe.com");
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer sk_test_secret"));
    }

    #[test]
    fn stripe_version_pinned() {
        let r = build_list_customers("k", 1, None, "https://api.stripe.com");
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Stripe-Version" && v == "2024-04-10"));
    }
}
