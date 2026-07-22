//! Runtime-neutral Kafka broker and cluster RPC foundations.
//!
//! The crate begins with durable protocol and policy vocabulary while its
//! runtime-neutral execution boundaries are built in verified milestones.

mod api;
mod completion;
mod config;
mod reactor;
#[allow(
    dead_code,
    reason = "M3 locks typed response dispatch before the M4 reactor adapter consumes it"
)]
mod response;

pub use api::{
    Call, CancellationRequest, CompletionError, Delivery, Driver, DriverBuilder,
    RequestResponsePair, SubmitError, TrafficClass,
};
pub use config::DriverLimits;
pub use kafka_driver_core::{CallId, Moment};
pub use reactor::{Reactor, TurnOutcome, WakeHandle};
