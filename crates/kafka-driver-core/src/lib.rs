//! Deterministic vocabulary and state machines for kafka-driver.
//!
//! This crate owns Kafka policy but has no operating-system, synchronization,
//! reactor, or transport capabilities.

mod broker;
mod capability;
mod connection;
mod delivery;
mod identity;
mod time;

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod time_test;

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
pub use identity::{CallId, ConnectionEpoch, EffectId, OperationId, TimerId, TransportId};
pub use time::Moment;
