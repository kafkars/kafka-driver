//! Authoritative delivery certainty for every terminal request failure.

use kafka_driver_core::Delivery;

use super::RequestError;

impl RequestError {
    /// Returns whether the broker may have received the failed request.
    ///
    /// The result is conservative: only failures proven to occur before writer
    /// ownership return [`Delivery::NotSent`].
    pub const fn delivery(&self) -> Delivery {
        match self {
            Self::Rejected { delivery, .. } => *delivery,
            Self::Decode(_) | Self::ConnectionClosed(_) => Delivery::PossiblySent,
            Self::Encode(_)
            | Self::UnsupportedVersion { .. }
            | Self::ApiUnavailable { .. }
            | Self::VersionLimitUnavailable { .. }
            | Self::ResponseCapacityReached { .. }
            | Self::IdentityConflict
            | Self::DeadlineOverflow
            | Self::RouteUnavailable
            | Self::RouteCapacityReached { .. }
            | Self::MetadataQueryCapacityReached { .. }
            | Self::CoordinatorCapacityReached { .. }
            | Self::NameResolutionCapacityReached { .. }
            | Self::NameResolutionFailed { .. } => Delivery::NotSent,
        }
    }
}
