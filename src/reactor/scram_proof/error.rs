//! Ownership-preserving SCRAM proof worker admission failures.

use std::fmt;

use super::ScramProofRequest;

#[derive(Debug)]
pub(in crate::reactor) enum ScramProofSubmitError {
    Full(Box<ScramProofRequest>),
    Closed(Box<ScramProofRequest>),
}

impl ScramProofSubmitError {
    pub(in crate::reactor) const fn failure(&self) -> kafka_driver_core::AuthenticationFailure {
        match self {
            Self::Full(_) => kafka_driver_core::AuthenticationFailure::Capacity,
            Self::Closed(_) => kafka_driver_core::AuthenticationFailure::Protocol,
        }
    }

    pub(in crate::reactor) fn into_request(self) -> ScramProofRequest {
        match self {
            Self::Full(request) | Self::Closed(request) => *request,
        }
    }
}

impl fmt::Display for ScramProofSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("SCRAM proof request capacity reached"),
            Self::Closed(_) => formatter.write_str("SCRAM proof worker is closed"),
        }
    }
}

impl std::error::Error for ScramProofSubmitError {}
