# stripe-source

A custom source connector for the ETL platform.

## Build & publish

```bash
platform connector test .
platform connector publish . --registry ./connectors
```

The published artifact lands at `./connectors/stripe-source@<version>/component.cwasm`.
