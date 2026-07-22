//! Runtime-neutral Kafka broker and cluster RPC foundations.
//!
//! The crate begins with durable protocol and policy vocabulary while its
//! runtime-neutral execution boundaries are built in verified milestones.

mod api;
mod completion;
mod config;
mod reactor;
mod request;
mod response;

pub use api::{
    Call, CancellationRequest, CompletionError, Delivery, Driver, DriverBuildError, DriverBuilder,
    RequestError, RequestResponsePair, ResponseCloseReason, SubmitError, TrafficClass,
};
pub use config::DriverLimits;
pub use kafka_driver_core::{CallId, Moment};
pub use kafka_wire_core::ApiVersion;
pub use reactor::{Reactor, ReactorError, TurnOutcome, WakeHandle};
