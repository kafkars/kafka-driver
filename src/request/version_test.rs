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
