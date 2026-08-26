//! Typed completion observations and sanitized connection-close failures.

use kafka_driver_core::{CallFailure, Delivery, DnsFailure};
#[cfg(test)]
use kafka_driver_core::{CallId, CorrelationId};
use kafka_wire_core::{ApiKey, ApiVersion, DecodeError, EncodeError};

/// Why one generated request could not complete successfully.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
    /// This request's version ceiling excludes the negotiated overlap.
    VersionLimitUnavailable {
        /// Generated Kafka API key requested by the call.
        api_key: ApiKey,
        /// Greatest version the caller permits for this request.
        maximum: ApiVersion,
        /// Lowest version mutually supported by the broker and driver.
        negotiated_minimum: ApiVersion,
    },
    /// This request's version floor excludes the negotiated overlap.
    VersionFloorUnavailable {
        /// Generated Kafka API key requested by the call.
        api_key: ApiKey,
        /// Least version the caller permits for this request.
        minimum: ApiVersion,
        /// Greatest version mutually supported by the broker and driver.
        negotiated_maximum: ApiVersion,
    },
    /// This request supplied a minimum above its maximum.
    VersionBoundsInvalid {
        /// Generated Kafka API key requested by the call.
        api_key: ApiKey,
        /// Least version the caller permits for this request.
        minimum: ApiVersion,
        /// Greatest version the caller permits for this request.
        maximum: ApiVersion,
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
    /// The bounded DNS ownership pool cannot start another route resolution.
    NameResolutionCapacityReached {
        /// Maximum outstanding DNS effects owned by this shard.
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

#[cfg(test)]
pub(crate) type ResponseFailure = RequestError;

/// Sanitized reason all remaining typed response slots were failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseCloseReason {
    /// The transport ended or failed.
    TransportClosed,
    /// Connection policy rejected peer behavior.
    ProtocolFault,
    /// Driver shutdown abandoned outstanding calls.
    Shutdown,
}

/// Whether a terminal value reached the still-live public call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionDisposition {
    /// The call still owned its completion receiver.
    Delivered,
    /// The caller had already abandoned its receiver.
    ReceiverAbandoned,
}

#[cfg(test)]
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
