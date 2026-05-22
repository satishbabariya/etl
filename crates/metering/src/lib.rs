//! Metering foundation — RFC-17 §"Metering Events".
//!
//! MVP scope: emit `MeteringEvent` rows to the catalog Postgres DB.
//! Kafka pipeline, aggregation, quota enforcement, and cost observability
//! are explicitly deferred.

pub mod event;
pub mod sink;

pub use event::{BillableMetric, MeteringEvent, MeteringSource};
pub use sink::{BufferedSink, CatalogMeteringSink, MeteringSink};
