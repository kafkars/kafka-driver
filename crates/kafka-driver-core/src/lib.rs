//! Deterministic vocabulary and state machines for kafka-driver.
//!
//! This crate owns Kafka policy but has no operating-system, synchronization,
//! reactor, or transport capabilities.

mod authentication;
mod broker;
mod capability;
mod connection;
mod delivery;
mod directory;
mod endpoint;
mod identity;
mod time;

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod endpoint_test;
#[cfg(test)]
mod time_test;

pub use authentication::{
    AuthenticationAttempt, AuthenticationDisposition, AuthenticationEffect, AuthenticationFailure,
    AuthenticationInput, AuthenticationLimits, AuthenticationMachine, AuthenticationPhase,
    AuthenticationPolicy, AuthenticationRound, AuthenticationState, AuthenticationTransition,
    ExchangeOutcome, SaslMechanism, SaslProtocol,
};
pub use broker::{
    BackoffPolicy, BackoffPolicyError, BrokerCloseReason, BrokerDisposition, BrokerEffect,
    BrokerInput, BrokerMachine, BrokerPhase, BrokerState, BrokerTransition, JitterSample,
    ReconnectSchedule, RetryOrdinal,
};
pub use capability::{CapabilityError, NegotiatedApi, NegotiatedCapabilities};
pub use connection::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput, ConnectionInputKind,
    ConnectionLimits, ConnectionMachine, ConnectionMachineError, ConnectionPhase, ConnectionState,
    ConnectionTransition, CorrelationId, IdentityKind, NegotiationAttempt, NegotiationFailure,
    PendingCall, PendingPhase, ResponseFault, TransitionDisposition, TransitionRecord,
    TransitionSequence, TransportFailure,
};
pub use delivery::Delivery;
pub use directory::{
    BrokerDirectory, BrokerDirectoryEntry, BrokerDirectoryError, BrokerDirectoryLimits,
    BrokerRoute, BrokerRouteError,
};
pub use endpoint::{BrokerEndpoint, HostName, HostNameError, IpAddress, ResolvedAddress};
pub use identity::{
    BrokerId, BrokerIdError, CallId, ConnectionEpoch, EffectId, MetadataGeneration, OperationId,
    TimerId, TransportId,
};
pub use kafka_wire_core::{ApiKey, ApiVersion};
pub use time::Moment;
