//! Sanitized Kafka framing failures for authentication bootstrap exchanges.

use std::{error::Error, fmt};

use kafka_driver_core::{AuthenticationFailure, CorrelationId};
use kafka_wire_core::{DecodeError, EncodeError};

/// Why authentication bytes could not form a trusted Kafka exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AuthenticationExchangeError {
    /// A generated request could not fit or encode.
    Encode(EncodeError),
    /// A generated response could not be decoded completely.
    Decode(DecodeError),
    /// A response did not echo the exchange correlation identity.
    Correlation {
        expected: CorrelationId,
        observed: CorrelationId,
    },
}

impl AuthenticationExchangeError {
    pub(crate) fn failure(&self) -> AuthenticationFailure {
        match self {
            Self::Encode(
                EncodeError::FrameLimitExceeded { .. }
                | EncodeError::FrameTooLarge { .. }
                | EncodeError::LengthOverflow { .. },
            ) => AuthenticationFailure::PolicyLimitExceeded,
            Self::Encode(_) | Self::Decode(_) | Self::Correlation { .. } => {
                AuthenticationFailure::Malformed
            }
        }
    }
}

impl fmt::Display for AuthenticationExchangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(source) => write!(formatter, "authentication encode failed: {source}"),
            Self::Decode(source) => write!(formatter, "authentication decode failed: {source}"),
            Self::Correlation { expected, observed } => write!(
                formatter,
                "authentication correlation {observed:?} does not match {expected:?}"
            ),
        }
    }
}

impl Error for AuthenticationExchangeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(source) => Some(source),
            Self::Decode(source) => Some(source),
            Self::Correlation { .. } => None,
        }
    }
}

impl From<EncodeError> for AuthenticationExchangeError {
    fn from(source: EncodeError) -> Self {
        Self::Encode(source)
    }
}

impl From<DecodeError> for AuthenticationExchangeError {
    fn from(source: DecodeError) -> Self {
        Self::Decode(source)
    }
}
