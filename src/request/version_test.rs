//! Scenarios for request-owned selection within a negotiated version range.

use kafka_driver_core::NegotiatedApi;
use kafka_wire::PRODUCE_API_DESCRIPTOR;
use kafka_wire_core::{ApiVersion, VersionRange};

use crate::RequestError;

use super::VersionSelection;

#[test]
fn produce_v12_ceiling_selects_v12_from_a_newer_broker_overlap() {
    let negotiated =
        NegotiatedApi::with_range(PRODUCE_API_DESCRIPTOR.api_key, VersionRange::new(3, 13));

    assert_eq!(
        VersionSelection::AtMost(ApiVersion::new(12)).select(negotiated),
        Ok(ApiVersion::new(12))
    );
}

#[test]
fn version_floor_accepts_the_highest_version_when_the_overlap_reaches_it() {
    let negotiated =
        NegotiatedApi::with_range(PRODUCE_API_DESCRIPTOR.api_key, VersionRange::new(3, 13));

    assert_eq!(
        VersionSelection::AtLeast(ApiVersion::new(12)).select(negotiated),
        Ok(ApiVersion::new(13))
    );
}

#[test]
fn version_floor_above_the_negotiated_maximum_is_rejected_before_encoding() {
    let negotiated =
        NegotiatedApi::with_range(PRODUCE_API_DESCRIPTOR.api_key, VersionRange::new(3, 11));

    assert_eq!(
        VersionSelection::AtLeast(ApiVersion::new(12)).select(negotiated),
        Err(RequestError::VersionFloorUnavailable {
            api_key: PRODUCE_API_DESCRIPTOR.api_key,
            minimum: ApiVersion::new(12),
            negotiated_maximum: ApiVersion::new(11),
        })
    );
}

#[test]
fn bounded_window_selects_its_highest_negotiated_member() {
    let negotiated =
        NegotiatedApi::with_range(PRODUCE_API_DESCRIPTOR.api_key, VersionRange::new(3, 13));

    assert_eq!(
        VersionSelection::Within {
            minimum: ApiVersion::new(6),
            maximum: ApiVersion::new(12),
        }
        .select(negotiated),
        Ok(ApiVersion::new(12))
    );
}

#[test]
fn reversed_request_bounds_are_rejected_before_encoding() {
    let negotiated =
        NegotiatedApi::with_range(PRODUCE_API_DESCRIPTOR.api_key, VersionRange::new(3, 13));

    assert_eq!(
        VersionSelection::Within {
            minimum: ApiVersion::new(12),
            maximum: ApiVersion::new(9),
        }
        .select(negotiated),
        Err(RequestError::VersionBoundsInvalid {
            api_key: PRODUCE_API_DESCRIPTOR.api_key,
            minimum: ApiVersion::new(12),
            maximum: ApiVersion::new(9),
        })
    );
}

#[test]
fn ceiling_below_the_negotiated_minimum_is_rejected_before_encoding() {
    let negotiated =
        NegotiatedApi::with_range(PRODUCE_API_DESCRIPTOR.api_key, VersionRange::new(3, 13));

    assert_eq!(
        VersionSelection::AtMost(ApiVersion::new(2)).select(negotiated),
        Err(RequestError::VersionLimitUnavailable {
            api_key: PRODUCE_API_DESCRIPTOR.api_key,
            maximum: ApiVersion::new(2),
            negotiated_minimum: ApiVersion::new(3),
        })
    );
}
