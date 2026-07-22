//! Runtime-neutral Kafka broker and cluster RPC foundations.
//!
//! The crate begins with durable protocol and policy vocabulary while its
//! runtime-neutral execution boundaries are built in verified milestones.

mod api;

pub use api::{Delivery, RequestResponsePair, TrafficClass};
pub use kafka_driver_core::{CallId, Moment};
