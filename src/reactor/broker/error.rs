//! Internal invariant failures while adapting broker effects to OS resources.

use std::{fmt, io};

use kafka_driver_core::{CallId, ConnectionEffect, ConnectionMachineError};

use crate::response::{ResponseDispatchError, ResponseFailError};

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
    /// An effect named request ownership not carried by the current exchange.
    RequestOwnership { expected: CallId, observed: CallId },
    /// A machine failure contradicted typed FIFO response ownership.
    ResponseFailure(ResponseFailError),
    /// A machine completion contradicted typed FIFO response ownership.
    ResponseDispatch(ResponseDispatchError),
    /// A registered resource could not change its readiness interests.
    ResourceInterest(io::Error),
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
            Self::RequestOwnership { expected, observed } => write!(
                formatter,
                "request ownership names call {observed:?}; effect names {expected:?}"
            ),
            Self::ResponseFailure(error) => error.fmt(formatter),
            Self::ResponseDispatch(error) => error.fmt(formatter),
            Self::ResourceInterest(_) => {
                formatter.write_str("failed to update broker readiness interests")
            }
            Self::ResourceClose(_) => formatter.write_str("failed to close broker transport"),
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ResponseFailure(source) => Some(source),
            Self::ResponseDispatch(source) => Some(source),
            Self::ResourceInterest(source) | Self::ResourceClose(source) => Some(source),
            Self::IdentityExhausted
            | Self::Machine(_)
            | Self::UnexpectedEffect(_)
            | Self::MissingEffect
            | Self::RequestOwnership { .. } => None,
        }
    }
}

impl From<ResponseFailError> for BrokerError {
    fn from(source: ResponseFailError) -> Self {
        Self::ResponseFailure(source)
    }
}

impl From<ResponseDispatchError> for BrokerError {
    fn from(source: ResponseDispatchError) -> Self {
        Self::ResponseDispatch(source)
    }
}

impl From<ConnectionMachineError> for BrokerError {
    fn from(source: ConnectionMachineError) -> Self {
        Self::Machine(source)
    }
}
