//! Sanitized invariant failures from coordinator effect interpretation.

use std::{error::Error, fmt};

use crate::reactor::BrokerRpcError;

#[derive(Debug)]
pub(in crate::reactor) enum CoordinatorOwnerError {
    CallIdentityExhausted,
    OperationIdentityExhausted,
    EpochExhausted,
    UnexpectedEffect,
    Broker(BrokerRpcError),
}

impl fmt::Display for CoordinatorOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CallIdentityExhausted => formatter.write_str("coordinator call IDs exhausted"),
            Self::OperationIdentityExhausted => {
                formatter.write_str("coordinator operation IDs exhausted")
            }
            Self::EpochExhausted => formatter.write_str("coordinator epochs exhausted"),
            Self::UnexpectedEffect => formatter.write_str("coordinator effect has no owner"),
            Self::Broker(_) => formatter.write_str("coordinator broker submission failed"),
        }
    }
}

impl Error for CoordinatorOwnerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Broker(source) => Some(source),
            Self::CallIdentityExhausted
            | Self::OperationIdentityExhausted
            | Self::EpochExhausted
            | Self::UnexpectedEffect => None,
        }
    }
}
