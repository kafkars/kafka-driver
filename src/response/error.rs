//! Atomic admission, header inspection, and verified-dispatch failures.

use std::{error::Error, fmt};

use kafka_driver_core::{CallId, CorrelationId};
use kafka_driver_transport::FrameBody;
use kafka_wire_core::{ApiVersion, DecodeError};

use super::{CompletionDisposition, RequestError, ResponseEnvelope};

/// Why a typed response slot could not enter the bounded FIFO registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseAdmissionError {
    /// Pending slot capacity was reached.
    CapacityReached { limit: usize },
    /// A pending slot already owns the call identity.
    CallInUse { call_id: CallId },
    /// A pending slot already owns the correlation identity.
    CorrelationInUse { correlation_id: CorrelationId },
    /// The request or response message does not support the chosen version.
    UnsupportedVersion {
        message: &'static str,
        version: ApiVersion,
    },
}

impl fmt::Display for ResponseAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityReached { limit } => {
                write!(formatter, "typed response capacity {limit} reached")
            }
            Self::CallInUse { call_id } => write!(formatter, "call {call_id:?} is already pending"),
            Self::CorrelationInUse { correlation_id } => {
                write!(
                    formatter,
                    "correlation {correlation_id:?} is already pending"
                )
            }
            Self::UnsupportedVersion { message, version } => {
                write!(formatter, "{message} does not support version {version}")
            }
        }
    }
}

impl Error for ResponseAdmissionError {}

/// Why a complete frame could not become a policy-visible response envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResponseInspectError {
    /// No typed response slot awaits a frame.
    NoPendingResponse { frame: FrameBody },
    /// The front slot's generated header version could not decode the frame.
    HeaderDecode {
        error: DecodeError,
        frame: FrameBody,
    },
}

impl fmt::Display for ResponseInspectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPendingResponse { .. } => {
                formatter.write_str("response frame has no pending typed slot")
            }
            Self::HeaderDecode { error, .. } => {
                write!(formatter, "Kafka response header decode failed: {error}")
            }
        }
    }
}

impl Error for ResponseInspectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::HeaderDecode { error, .. } => Some(error),
            Self::NoPendingResponse { .. } => None,
        }
    }
}

/// Why machine-approved response completion could not settle the FIFO front.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResponseDispatchError {
    /// No typed response slot remains for a machine-approved envelope.
    NoPendingResponse { envelope: ResponseEnvelope },
    /// The machine effect did not name the registry and envelope FIFO front.
    VerificationMismatch {
        expected_call: CallId,
        expected_correlation: CorrelationId,
        approved_call: CallId,
        approved_correlation: CorrelationId,
        observed_correlation: CorrelationId,
        envelope: ResponseEnvelope,
    },
    /// The generated response body decoder rejected verified bytes.
    BodyDecode {
        call_id: CallId,
        error: DecodeError,
        completion: CompletionDisposition,
    },
}

impl fmt::Display for ResponseDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPendingResponse { .. } => {
                formatter.write_str("machine approved a response with no pending typed slot")
            }
            Self::VerificationMismatch { .. } => {
                formatter.write_str("machine-approved response does not match typed registry front")
            }
            Self::BodyDecode { error, .. } => {
                write!(formatter, "Kafka response body decode failed: {error}")
            }
        }
    }
}

impl Error for ResponseDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BodyDecode { error, .. } => Some(error),
            Self::NoPendingResponse { .. } | Self::VerificationMismatch { .. } => None,
        }
    }
}

/// Why a machine-approved call failure could not settle the FIFO front.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResponseFailError {
    /// No typed response slot remains for the named call.
    NoPendingResponse {
        call_id: CallId,
        failure: RequestError,
    },
    /// The machine failure did not name the registry FIFO front.
    VerificationMismatch {
        expected_call: CallId,
        failed_call: CallId,
        failure: RequestError,
    },
}

impl fmt::Display for ResponseFailError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPendingResponse { .. } => {
                formatter.write_str("machine failed a call with no pending typed slot")
            }
            Self::VerificationMismatch { .. } => {
                formatter.write_str("machine-failed call does not match typed registry front")
            }
        }
    }
}

impl Error for ResponseFailError {}
