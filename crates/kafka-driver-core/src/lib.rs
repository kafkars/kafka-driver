//! Deterministic vocabulary and state machines for kafka-driver.
//!
//! This crate owns Kafka policy but has no operating-system, synchronization,
//! reactor, or transport capabilities.

mod authentication;
mod bootstrap;
mod broker;
mod broker_resolution;
mod capability;
mod connection;
mod coordinator;
mod delivery;
mod directory;
mod endpoint;
mod endpoint_dialer;
mod identity;
mod metadata;
mod resolution;
mod time;

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod endpoint_test;
#[cfg(test)]
mod time_test;

pub use authentication::{
    AuthenticationAttempt, AuthenticationDisposition, AuthenticationEffect, AuthenticationFailure,
    AuthenticationFailureDisposition, AuthenticationInput, AuthenticationLimits,
    AuthenticationMachine, AuthenticationPhase, AuthenticationPolicy, AuthenticationRound,
    AuthenticationState, AuthenticationTransition, ExchangeOutcome, SaslMechanism, SaslProtocol,
};
pub use bootstrap::{
    BootstrapCursor, BootstrapDisposition, BootstrapEffect, BootstrapError, BootstrapInput,
    BootstrapLimits, BootstrapMachine, BootstrapRetryDisposition, BootstrapRetryEffect,
    BootstrapRetryError, BootstrapRetryInput, BootstrapRetryMachine, BootstrapRetryState,
    BootstrapRetryTransition, BootstrapSet, BootstrapState, BootstrapTransition,
};
pub use broker::{
    BackoffPolicy, BackoffPolicyError, BrokerCloseReason, BrokerDisposition, BrokerEffect,
    BrokerInput, BrokerMachine, BrokerPhase, BrokerState, BrokerTransition, JitterSample,
    ReconnectSchedule, RetryOrdinal,
};
pub use broker_resolution::{
    BrokerResolutionDisposition, BrokerResolutionEffect, BrokerResolutionInput,
    BrokerResolutionMachine, BrokerResolutionState, BrokerResolutionTransition,
};
pub use capability::{CapabilityError, NegotiatedApi, NegotiatedCapabilities};
pub use connection::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput, ConnectionInputKind,
    ConnectionLimits, ConnectionMachine, ConnectionMachineError, ConnectionPhase, ConnectionState,
    ConnectionTransition, CorrelationId, IdentityKind, NegotiationAttempt, NegotiationFailure,
    PendingCall, PendingPhase, ResponseFault, TransitionDisposition, TransitionRecord,
    TransitionSequence, TransportFailure,
};
pub use coordinator::{
    CoordinatorDisposition, CoordinatorEffect, CoordinatorInput, CoordinatorMachine,
    CoordinatorRoute, CoordinatorState, CoordinatorTransition,
};
pub use delivery::Delivery;
pub use directory::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryError, BrokerDirectoryLimits,
    BrokerRoute, BrokerRouteError,
};
pub use endpoint::{BrokerEndpoint, HostName, HostNameError, IpAddress, ResolvedAddress};
pub use endpoint_dialer::{
    EndpointDialer, EndpointDialerEffect, EndpointDialerInput, EndpointDialerTransition,
};
pub use identity::{
    BrokerId, BrokerIdError, CallId, ConnectionEpoch, CoordinatorEpoch, CoordinatorKey,
    CoordinatorKeyError, CoordinatorKind, EffectId, LeaderEpoch, LeaderEpochError,
    MetadataGeneration, OperationId, PartitionId, PartitionIdError, TimerId, TopicName,
    TopicNameError, TransportId,
};
pub use kafka_wire_core::{ApiKey, ApiVersion};
pub use metadata::{
    MetadataDisposition, MetadataEffect, MetadataInput, MetadataMachine, MetadataQuery,
    MetadataQueryLimits, MetadataSnapshot, MetadataSnapshotError, MetadataState,
    MetadataTransition, PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet,
    PartitionLeaderSetError, PartitionRoute,
};
pub use resolution::{
    DnsFailure, DnsOutcome, DnsRequest, ResolutionLimits, ResolvedAddressSet,
    ResolvedAddressSetError,
};
pub use time::Moment;
