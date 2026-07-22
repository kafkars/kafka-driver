//! Deterministic vocabulary and state machines for kafka-driver.
//!
//! This crate owns Kafka policy but has no operating-system, synchronization,
//! reactor, or transport capabilities.

mod delivery;
mod identity;
mod time;

#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod time_test;

pub use delivery::Delivery;
pub use identity::{CallId, ConnectionEpoch, EffectId, OperationId, TimerId, TransportId};
pub use time::Moment;
