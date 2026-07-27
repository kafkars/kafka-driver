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
mod observation;
mod reactor;
mod request;
mod response;

pub use api::{
    AvailableTopicPartition, BootstrapSnapshot, BrokerLaneLoadSnapshot, BrokerLanePhase,
    BrokerLaneSnapshot, Call, CallCounters, CallLatencySnapshot, CompletionError, Delivery, Driver,
    DriverBuildError, DriverBuilder, DriverSnapshot, FailureCounters, InvalidationDisposition,
    InvalidationSubmitError, LatencyMetric, MailboxSnapshot, RequestError, RequestOptions,
    RequestResponsePair, ResponseCloseReason, Route, RouteFailureToken, RouteKind, RoutedCall,
    RoutedOutcome, SeedSnapshot, SnapshotError, SubmitError, TopicView, TopicViewError,
    TrafficClass, WriteQueueSnapshot,
};
pub use config::{
    ControllerWaitingLimits, CoordinatorLimits, DriverLimits, MetadataLimits, ResolverLimits,
    SaslConfig, SaslConfigError, ScramProofLimits,
};
#[cfg(feature = "tls-rustls")]
pub use config::{TlsClientConfig, TlsClientPolicy};
pub use host::{DriverHost, DriverHostError};
pub use kafka_driver_core::{
    AuthenticationFailure, AuthenticationFailureDisposition, BootstrapError, BootstrapLimits,
    BootstrapSet, BrokerDirectoryLimits, BrokerEndpoint, BrokerRoute, BrokerState, CallFailure,
    CallId, CloseReason as ConnectionCloseReason, ConnectionPhase, CoordinatorKey,
    CoordinatorKeyError, CoordinatorKind, CoordinatorRoute, HostName, HostNameError,
    MetadataGeneration, MetadataRevision, Moment, NegotiationFailure, PartitionId,
    PartitionIdError, PartitionRoute, TopicName, TopicNameError, TransportFailure,
};
pub use kafka_wire_core::{ApiKey, ApiVersion};
pub use reactor::{Reactor, ReactorError, TurnOutcome, WakeHandle};
