//! Sanitized invariant failures from interpreting deterministic metadata effects.

use std::{error::Error, fmt};

use crate::reactor::BrokerRpcError;

#[derive(Debug)]
pub(in crate::reactor) enum MetadataOwnerError {
    CallIdentityExhausted,
    OperationIdentityExhausted,
    GenerationExhausted,
    UnexpectedEffect,
    Broker(BrokerRpcError),
}

impl fmt::Display for MetadataOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CallIdentityExhausted => {
                formatter.write_str("metadata call identity space is exhausted")
            }
            Self::OperationIdentityExhausted => {
                formatter.write_str("metadata operation identity space is exhausted")
            }
            Self::GenerationExhausted => {
                formatter.write_str("metadata generation space is exhausted")
            }
            Self::UnexpectedEffect => {
                formatter.write_str("metadata machine emitted an effect without an owner")
            }
            Self::Broker(_) => formatter.write_str("metadata broker submission failed"),
        }
    }
}

impl Error for MetadataOwnerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Broker(source) => Some(source),
            Self::CallIdentityExhausted
            | Self::OperationIdentityExhausted
            | Self::GenerationExhausted
            | Self::UnexpectedEffect => None,
        }
    }
}
