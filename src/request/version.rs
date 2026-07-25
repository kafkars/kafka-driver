//! Per-request selection within one broker-and-driver version overlap.

use kafka_driver_core::NegotiatedApi;
use kafka_wire_core::ApiVersion;

use crate::RequestError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VersionSelection {
    Highest,
    AtLeast(ApiVersion),
    AtMost(ApiVersion),
    Within {
        minimum: ApiVersion,
        maximum: ApiVersion,
    },
}

impl VersionSelection {
    pub(super) const fn from_bounds(
        minimum: Option<ApiVersion>,
        maximum: Option<ApiVersion>,
    ) -> Self {
        match (minimum, maximum) {
            (None, None) => Self::Highest,
            (Some(minimum), None) => Self::AtLeast(minimum),
            (None, Some(maximum)) => Self::AtMost(maximum),
            (Some(minimum), Some(maximum)) => Self::Within { minimum, maximum },
        }
    }

    pub(super) const fn select(
        self,
        negotiated: NegotiatedApi,
    ) -> Result<ApiVersion, RequestError> {
        match self {
            Self::Highest => Ok(negotiated.version()),
            Self::AtLeast(minimum) => select_minimum(negotiated, minimum),
            Self::AtMost(maximum) => select_maximum(negotiated, maximum),
            Self::Within { minimum, maximum } => select_within(negotiated, minimum, maximum),
        }
    }
}

const fn select_minimum(
    negotiated: NegotiatedApi,
    minimum: ApiVersion,
) -> Result<ApiVersion, RequestError> {
    if negotiated.version().value() < minimum.value() {
        return Err(floor_unavailable(negotiated, minimum));
    }
    Ok(negotiated.version())
}

const fn select_maximum(
    negotiated: NegotiatedApi,
    maximum: ApiVersion,
) -> Result<ApiVersion, RequestError> {
    match negotiated.highest_at_most(maximum) {
        Some(version) => Ok(version),
        None => Err(limit_unavailable(negotiated, maximum)),
    }
}

const fn select_within(
    negotiated: NegotiatedApi,
    minimum: ApiVersion,
    maximum: ApiVersion,
) -> Result<ApiVersion, RequestError> {
    if minimum.value() > maximum.value() {
        return Err(RequestError::VersionBoundsInvalid {
            api_key: negotiated.api_key(),
            minimum,
            maximum,
        });
    }
    let selected = match select_maximum(negotiated, maximum) {
        Ok(selected) => selected,
        Err(failure) => return Err(failure),
    };
    if selected.value() < minimum.value() {
        return Err(floor_unavailable(negotiated, minimum));
    }
    Ok(selected)
}

const fn limit_unavailable(negotiated: NegotiatedApi, maximum: ApiVersion) -> RequestError {
    RequestError::VersionLimitUnavailable {
        api_key: negotiated.api_key(),
        maximum,
        negotiated_minimum: negotiated.versions().min(),
    }
}

const fn floor_unavailable(negotiated: NegotiatedApi, minimum: ApiVersion) -> RequestError {
    RequestError::VersionFloorUnavailable {
        api_key: negotiated.api_key(),
        minimum,
        negotiated_maximum: negotiated.versions().max(),
    }
}
