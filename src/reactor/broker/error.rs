//! Internal invariant failures while adapting broker effects to OS resources.

use std::{fmt, io};

use kafka_driver_core::{ConnectionEffect, ConnectionMachineError};

/// Why the single-broker adapter could not preserve its machine contract.
#[derive(Debug)]
pub(in crate::reactor) enum BrokerError {
    /// A broker-local effect or transport identity source was exhausted.
    IdentityExhausted,
    /// A deterministic machine invariant rejected an adapter input.
    Machine(ConnectionMachineError),
    /// A transition emitted work that is invalid at the current adapter seam.
    UnexpectedEffect(ConnectionEffect),
    /// A transition omitted work required by the current adapter seam.
    MissingEffect,
    /// An opened resource could not be deregistered after its terminal outcome.
    ResourceClose(io::Error),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityExhausted => formatter.write_str("broker identity space is exhausted"),
            Self::Machine(error) => {
                write!(formatter, "connection machine rejected input: {error:?}")
            }
            Self::UnexpectedEffect(effect) => {
                write!(formatter, "unexpected connection effect: {effect:?}")
            }
            Self::MissingEffect => formatter.write_str("required connection effect was missing"),
            Self::ResourceClose(_) => formatter.write_str("failed to close broker transport"),
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ResourceClose(source) => Some(source),
            Self::IdentityExhausted
            | Self::Machine(_)
            | Self::UnexpectedEffect(_)
            | Self::MissingEffect => None,
        }
    }
}

impl From<ConnectionMachineError> for BrokerError {
    fn from(source: ConnectionMachineError) -> Self {
        Self::Machine(source)
    }
}
