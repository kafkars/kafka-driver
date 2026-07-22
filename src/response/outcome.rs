//! Typed completion observations and sanitized connection-close failures.

use std::{error::Error, fmt};

use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire_core::DecodeError;

/// Why a registered typed response could not complete successfully.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResponseFailure {
    /// The verified response body was malformed or unsupported.
    Decode(DecodeError),
    /// The connection ended before this response completed.
    ConnectionClosed(ResponseCloseReason),
}

impl fmt::Display for ResponseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(formatter, "Kafka response decode failed: {error}"),
            Self::ConnectionClosed(reason) => {
                write!(formatter, "connection closed before response: {reason}")
            }
        }
    }
}

impl Error for ResponseFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::ConnectionClosed(_) => None,
        }
    }
}

/// Sanitized reason all remaining typed response slots were failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseCloseReason {
    /// The transport ended or failed.
    TransportClosed,
    /// Connection policy rejected peer behavior.
    ProtocolFault,
    /// Driver shutdown abandoned outstanding calls.
    Shutdown,
}

impl fmt::Display for ResponseCloseReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TransportClosed => formatter.write_str("transport closed"),
            Self::ProtocolFault => formatter.write_str("protocol fault"),
            Self::Shutdown => formatter.write_str("driver shutdown"),
        }
    }
}

/// Whether a terminal value reached the still-live public call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionDisposition {
    /// The call still owned its completion receiver.
    Delivered,
    /// The caller had already abandoned its receiver.
    ReceiverAbandoned,
}

/// Successful verified response dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResponseDispatch {
    pub(crate) call_id: CallId,
    pub(crate) correlation_id: CorrelationId,
    pub(crate) completion: CompletionDisposition,
}

/// Aggregate result of failing every remaining typed response slot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FailedResponses {
    /// Slots removed from the registry.
    pub(crate) total: usize,
    /// Slots whose public receivers had already been abandoned.
    pub(crate) abandoned: usize,
}
