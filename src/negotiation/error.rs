//! Sanitized failures from validating and intersecting an API advertisement.

use std::{error::Error, fmt};

use kafka_driver_core::CapabilityError;
use kafka_wire_core::ApiKey;

/// Why a broker advertisement could not produce connection capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NegotiationError {
    /// The `ApiVersions` response itself reported a broker error.
    BrokerRejected { error_code: i16 },
    /// The peer advertised more APIs than this connection permits.
    AdvertisementCapacity { observed: usize, limit: usize },
    /// One advertised inclusive range had its bounds reversed.
    InvalidRange {
        api_key: ApiKey,
        min_version: i16,
        max_version: i16,
    },
    /// One API key appeared more than once in the advertisement.
    DuplicateApi { api_key: ApiKey },
    /// The locally supported overlap violated its canonical bounded owner.
    Capability(CapabilityError),
}

impl NegotiationError {
    pub(crate) const fn failure(&self) -> kafka_driver_core::NegotiationFailure {
        match self {
            Self::BrokerRejected { .. } => kafka_driver_core::NegotiationFailure::Broker,
            Self::AdvertisementCapacity { .. } | Self::Capability(_) => {
                kafka_driver_core::NegotiationFailure::Capacity
            }
            Self::InvalidRange { .. } | Self::DuplicateApi { .. } => {
                kafka_driver_core::NegotiationFailure::Malformed
            }
        }
    }
}

impl fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BrokerRejected { error_code } => {
                write!(
                    formatter,
                    "ApiVersions failed with broker error {error_code}"
                )
            }
            Self::AdvertisementCapacity { observed, limit } => write!(
                formatter,
                "broker advertised {observed} APIs, exceeding limit {limit}"
            ),
            Self::InvalidRange {
                api_key,
                min_version,
                max_version,
            } => write!(
                formatter,
                "broker advertised invalid API {api_key} range {min_version}-{max_version}"
            ),
            Self::DuplicateApi { api_key } => {
                write!(formatter, "broker advertised API {api_key} more than once")
            }
            Self::Capability(source) => source.fmt(formatter),
        }
    }
}

impl Error for NegotiationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Capability(source) => Some(source),
            Self::BrokerRejected { .. }
            | Self::AdvertisementCapacity { .. }
            | Self::InvalidRange { .. }
            | Self::DuplicateApi { .. } => None,
        }
    }
}

impl From<CapabilityError> for NegotiationError {
    fn from(source: CapabilityError) -> Self {
        Self::Capability(source)
    }
}
