//! Human-readable diagnostics for typed request completion failures.

use std::{error::Error, fmt};

use super::{RequestError, ResponseCloseReason};

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
            Self::VersionLimitUnavailable {
                api_key,
                maximum,
                negotiated_minimum,
            } => write!(
                formatter,
                "Kafka API {api_key} requires version {negotiated_minimum} or newer, above request maximum {maximum}"
            ),
            Self::VersionFloorUnavailable {
                api_key,
                minimum,
                negotiated_maximum,
            } => write!(
                formatter,
                "Kafka API {api_key} is available only through version {negotiated_maximum}, below request minimum {minimum}"
            ),
            Self::VersionBoundsInvalid {
                api_key,
                minimum,
                maximum,
            } => write!(
                formatter,
                "Kafka API {api_key} request minimum {minimum} exceeds request maximum {maximum}"
            ),
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
            Self::NameResolutionCapacityReached { limit } => {
                write!(
                    formatter,
                    "name-resolution ownership capacity {limit} reached"
                )
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
            | Self::VersionLimitUnavailable { .. }
            | Self::VersionFloorUnavailable { .. }
            | Self::VersionBoundsInvalid { .. }
            | Self::ResponseCapacityReached { .. }
            | Self::IdentityConflict
            | Self::DeadlineOverflow
            | Self::RouteUnavailable
            | Self::RouteCapacityReached { .. }
            | Self::MetadataQueryCapacityReached { .. }
            | Self::CoordinatorCapacityReached { .. }
            | Self::NameResolutionCapacityReached { .. }
            | Self::NameResolutionFailed { .. }
            | Self::Rejected { .. }
            | Self::ConnectionClosed(_) => None,
        }
    }
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
