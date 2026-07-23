//! Per-request selection within one broker-and-driver version overlap.

use kafka_driver_core::NegotiatedApi;
use kafka_wire_core::ApiVersion;

use crate::RequestError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VersionSelection {
    Highest,
    AtMost(ApiVersion),
}

impl VersionSelection {
    pub(super) const fn from_maximum(maximum: Option<ApiVersion>) -> Self {
        match maximum {
            Some(maximum) => Self::AtMost(maximum),
            None => Self::Highest,
        }
    }

    pub(super) const fn select(
        self,
        negotiated: NegotiatedApi,
    ) -> Result<ApiVersion, RequestError> {
        match self {
            Self::Highest => Ok(negotiated.version()),
            Self::AtMost(maximum) => match negotiated.highest_at_most(maximum) {
                Some(version) => Ok(version),
                None => Err(limit_unavailable(negotiated, maximum)),
            },
        }
    }
}

const fn limit_unavailable(negotiated: NegotiatedApi, maximum: ApiVersion) -> RequestError {
    RequestError::VersionLimitUnavailable {
        api_key: negotiated.api_key(),
        maximum,
        negotiated_minimum: negotiated.versions().min(),
    }
}
