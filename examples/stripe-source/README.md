# stripe-source

Stripe `/v1/customers` source connector for the ETL platform.

## Schema

| column  | type      | nullable |
|---------|-----------|----------|
| id      | utf8      | no       |
| email   | utf8      | yes      |
| name    | utf8      | yes      |
| created | int64 (unix-seconds) | no |

## Connection

```yaml
apiVersion: platform.etl/v0
kind: Connection
metadata:
  name: stripe-prod
spec:
  connector_ref: wasm:stripe-source@0.1.0
  config:
    # Use a SecretRef in production; plaintext shown here for demo.
    url: sk_test_xxxxxxxxxxxxxxxxxxxxxxxx
```

## Pipeline

```yaml
apiVersion: platform.etl/v0
kind: Pipeline
metadata:
  name: stripe-customers-sync
spec:
  source_connection: stripe-prod
  source:
    type: wasm
    config:
      limit: 100
  destination:
    type: local_parquet
    base_path: ./data/stripe
  batch_size: 100
  evolution_policy: propagate_additive
```

## Source-config knobs

```json
{
  "base_url": "https://api.stripe.com",
  "limit": 100,
  "max_429_retries": 3
}
```

All fields optional with defaults shown.

## Build & publish

```bash
platform connector test .
platform connector publish . --registry ./connectors
```

## Behavior

- Pagination: Stripe `starting_after=<last_id>` cursor.
- Auth: `Authorization: Bearer <api_key>` (URL field of the Connection).
- Rate-limit: HTTP 429 → exponential backoff up to `max_429_retries` (default 3).
- Cursor: returns the last row's `id` so successive runs resume from there.
- `is_final = true` when Stripe responds with `has_more: false`.
