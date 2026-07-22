//! Typed completion observations and sanitized connection-close failures.

use std::{error::Error, fmt};

use kafka_driver_core::{CallFailure, CallId, CorrelationId, Delivery, DnsFailure};
use kafka_wire_core::{ApiKey, ApiVersion, DecodeError, EncodeError};

/// Why one generated request could not complete successfully.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestError {
    /// The generated request could not be encoded at the selected version.
    Encode(EncodeError),
    /// The verified response body was malformed or unsupported.
    Decode(DecodeError),
    /// The generated request or response does not support the selected version.
    UnsupportedVersion {
        /// Generated message that rejected the version.
        message: &'static str,
        /// Requested API version.
        version: ApiVersion,
    },
    /// The broker and driver share no usable version of this API.
    ApiUnavailable {
        /// Generated Kafka API key requested by the call.
        api_key: ApiKey,
    },
    /// The typed FIFO response registry reached its explicit capacity.
    ResponseCapacityReached {
        /// Configured pending response maximum.
        limit: usize,
    },
    /// Driver-owned call or correlation identity unexpectedly conflicted.
    IdentityConflict,
    /// The relative timeout could not fit in the driver clock domain.
    DeadlineOverflow,
    /// The semantic destination is unavailable in the current metadata generation.
    RouteUnavailable,
    /// A bounded semantic-route waiting queue cannot retain this call.
    RouteCapacityReached {
        /// Maximum retained calls for this route owner.
        call_limit: usize,
        /// Maximum retained request bytes for this route owner.
        byte_limit: usize,
    },
    /// The bounded distinct Metadata query queue cannot admit this topic lookup.
    MetadataQueryCapacityReached {
        /// Maximum distinct queries retained behind the active Metadata RPC.
        limit: usize,
    },
    /// The bounded coordinator-key registry cannot retain another key.
    CoordinatorCapacityReached {
        /// Maximum coordinator machines retained by this shard.
        limit: usize,
    },
    /// Advertised broker name resolution failed without endpoint details.
    NameResolutionFailed {
        /// Sanitized resolver failure category.
        failure: DnsFailure,
    },
    /// Deterministic connection policy rejected or failed the call.
    Rejected {
        /// Connection-policy reason.
        failure: CallFailure,
        /// Whether the broker may have received the request.
        delivery: Delivery,
    },
    /// The connection ended before this response completed.
    ConnectionClosed(ResponseCloseReason),
}

impl fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(error) => write!(formatter, "Kafka request encode failed: {error}"),
            Self::Decode(error) => write!(formatter, "Kafka response decode failed: {error}"),
            Self::UnsupportedVersion { message, version } => {
                write!(formatter, "{message} does not support version {version}")
            }
            Self::ApiUnavailable { api_key } => {
                write!(formatter, "Kafka API {api_key} has no negotiated version")
            }
            Self::ResponseCapacityReached { limit } => {
                write!(formatter, "typed response capacity {limit} reached")
            }
            Self::IdentityConflict => {
                formatter.write_str("driver-owned request identity unexpectedly conflicted")
            }
            Self::DeadlineOverflow => {
                formatter.write_str("request deadline exceeds the driver clock domain")
            }
            Self::RouteUnavailable => formatter.write_str("semantic Kafka route is unavailable"),
            Self::RouteCapacityReached {
                call_limit,
                byte_limit,
            } => write!(
                formatter,
                "route wait capacity reached ({call_limit} calls, {byte_limit} bytes)"
            ),
            Self::MetadataQueryCapacityReached { limit } => {
                write!(formatter, "metadata query capacity {limit} reached")
            }
            Self::CoordinatorCapacityReached { limit } => {
                write!(formatter, "coordinator key capacity {limit} reached")
            }
            Self::NameResolutionFailed { failure } => {
                write!(formatter, "broker name resolution failed: {failure:?}")
            }
            Self::Rejected { failure, delivery } => {
                write!(
                    formatter,
                    "connection rejected request ({delivery:?}): {failure:?}"
                )
            }
            Self::ConnectionClosed(reason) => {
                write!(formatter, "connection closed before response: {reason}")
            }
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::UnsupportedVersion { .. }
            | Self::ApiUnavailable { .. }
            | Self::ResponseCapacityReached { .. }
            | Self::IdentityConflict
            | Self::DeadlineOverflow
            | Self::RouteUnavailable
            | Self::RouteCapacityReached { .. }
            | Self::MetadataQueryCapacityReached { .. }
            | Self::CoordinatorCapacityReached { .. }
            | Self::NameResolutionFailed { .. }
            | Self::Rejected { .. }
            | Self::ConnectionClosed(_) => None,
        }
    }
}

pub(crate) type ResponseFailure = RequestError;

/// Sanitized reason all remaining typed response slots were failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseCloseReason {
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
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FailedResponses {
    /// Slots removed from the registry.
    pub(crate) total: usize,
    /// Slots whose public receivers had already been abandoned.
    pub(crate) abandoned: usize,
}
