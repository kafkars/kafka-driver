//! Framing failures for one machine-owned `ApiVersions` exchange.

use std::{error::Error, fmt};

use kafka_driver_core::{CorrelationId, NegotiationFailure};
use kafka_wire_core::{DecodeError, EncodeError};

/// Why initial `ApiVersions` bytes could not form a trusted response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NegotiationExchangeError {
    /// The bootstrap request or its generated header policy could not encode.
    Encode(EncodeError),
    /// The response header or body could not be decoded completely.
    Decode(DecodeError),
    /// The response did not echo the bootstrap correlation identity.
    Correlation {
        expected: CorrelationId,
        observed: CorrelationId,
    },
}

impl NegotiationExchangeError {
    pub(crate) fn failure(&self) -> NegotiationFailure {
        match self {
            Self::Encode(EncodeError::FrameLimitExceeded { .. }) => NegotiationFailure::Capacity,
            Self::Encode(_) | Self::Decode(_) | Self::Correlation { .. } => {
                NegotiationFailure::Malformed
            }
        }
    }
}

impl fmt::Display for NegotiationExchangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(source) => write!(formatter, "ApiVersions encode failed: {source}"),
            Self::Decode(source) => write!(formatter, "ApiVersions decode failed: {source}"),
            Self::Correlation { expected, observed } => write!(
                formatter,
                "ApiVersions correlation {observed:?} does not match {expected:?}"
            ),
        }
    }
}

impl Error for NegotiationExchangeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(source) => Some(source),
            Self::Decode(source) => Some(source),
            Self::Correlation { .. } => None,
        }
    }
}

impl From<EncodeError> for NegotiationExchangeError {
    fn from(source: EncodeError) -> Self {
        Self::Encode(source)
    }
}

impl From<DecodeError> for NegotiationExchangeError {
    fn from(source: DecodeError) -> Self {
        Self::Decode(source)
    }
}
