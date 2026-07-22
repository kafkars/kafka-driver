//! Runtime-neutral Kafka broker and cluster RPC foundations.
//!
//! The crate begins with durable protocol and policy vocabulary while its
//! runtime-neutral execution boundaries are built in verified milestones.

mod api;
mod authentication;
mod completion;
mod config;
mod metadata;
mod negotiation;
mod reactor;
mod request;
mod response;

pub use api::{
    Call, CancellationRequest, CompletionError, Delivery, Driver, DriverBuildError, DriverBuilder,
    RequestError, RequestResponsePair, ResponseCloseReason, SubmitError, TrafficClass,
};
#[cfg(feature = "tls-rustls")]
pub use config::TlsClientConfig;
pub use config::{DriverLimits, MetadataLimits, ResolverLimits, SaslConfig, SaslConfigError};
pub use kafka_driver_core::{
    BootstrapError, BootstrapLimits, BootstrapSet, BrokerDirectoryLimits, BrokerEndpoint, CallId,
    HostName, HostNameError, Moment,
};
pub use kafka_wire_core::{ApiKey, ApiVersion};
pub use reactor::{Reactor, ReactorError, TurnOutcome, WakeHandle};
