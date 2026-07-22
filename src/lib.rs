//! Runtime-neutral Kafka broker and cluster RPC foundations.
//!
//! The crate begins with durable protocol and policy vocabulary while its
//! runtime-neutral execution boundaries are built in verified milestones.

mod api;
mod authentication;
mod completion;
mod config;
mod coordinator;
mod host;
mod metadata;
mod negotiation;
mod reactor;
mod request;
mod response;

pub use api::{
    Call, CompletionError, Delivery, Driver, DriverBuildError, DriverBuilder,
    InvalidationDisposition, RequestError, RequestResponsePair, ResponseCloseReason, Route,
    RouteReceipt, RoutedCall, RoutedOutcome, SubmitError, TrafficClass,
};
pub use config::{
    CoordinatorLimits, DriverLimits, MetadataLimits, ResolverLimits, SaslConfig, SaslConfigError,
    ScramProofLimits,
};
#[cfg(feature = "tls-rustls")]
pub use config::{TlsClientConfig, TlsClientPolicy};
pub use host::{DriverHost, DriverHostError};
pub use kafka_driver_core::{
    AuthenticationFailure, BootstrapError, BootstrapLimits, BootstrapSet, BrokerDirectoryLimits,
    BrokerEndpoint, BrokerRoute, CallFailure, CallId, CloseReason as ConnectionCloseReason,
    CoordinatorKey, CoordinatorKeyError, CoordinatorKind, CoordinatorRoute, HostName,
    HostNameError, Moment, NegotiationFailure, PartitionId, PartitionIdError, PartitionRoute,
    TopicName, TopicNameError, TransportFailure,
};
pub use kafka_wire_core::{ApiKey, ApiVersion};
pub use reactor::{Reactor, ReactorError, TurnOutcome, WakeHandle};
