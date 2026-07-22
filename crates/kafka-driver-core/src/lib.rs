//! Deterministic vocabulary and state machines for kafka-driver.
//!
//! This crate owns Kafka policy but has no operating-system, synchronization,
//! reactor, or transport capabilities.

mod connection;
mod delivery;
mod identity;
mod time;

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod time_test;

pub use connection::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput, ConnectionInputKind,
    ConnectionLimits, ConnectionMachine, ConnectionMachineError, ConnectionPhase, ConnectionState,
    ConnectionTransition, CorrelationId, IdentityKind, PendingCall, PendingPhase,
    TransitionDisposition, TransitionRecord, TransitionSequence, TransportFailure,
};
pub use delivery::Delivery;
pub use identity::{CallId, ConnectionEpoch, EffectId, OperationId, TimerId, TransportId};
pub use time::Moment;
