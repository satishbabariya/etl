# stripe-source-ts

TypeScript port of `examples/stripe-source` — Stripe `/v1/customers` source connector built via the II.3.b TS SDK.

## Schema

| column  | type      | nullable |
|---------|-----------|----------|
| id      | utf8      | no       |
| email   | utf8      | yes      |
| name    | utf8      | yes      |
| created | int64 (unix-seconds) | no |

## Build & publish

```bash
npm install
platform connector test .
platform connector publish . --registry ./connectors
```

The published artifact lands at `./connectors/stripe-source-ts@0.1.0/component.cwasm`.

## Source-config knobs

```json
{ "base_url": "https://api.stripe.com", "limit": 100, "max_429_retries": 3 }
```

## Behavior

Identical to the Rust connector. Bundle size ~16 MB (componentize-js embeds StarlingMonkey). Functional behavior, schema, and host imports match `examples/stripe-source/` byte-for-byte from the platform's perspective.
