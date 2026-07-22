//! Scenarios for stable generated-version intersection and malformed advertisements.

use std::num::NonZeroUsize;

use kafka_driver_core::{CapabilityError, NegotiatedApi};
use kafka_wire::{
    API_VERSIONS_API_DESCRIPTOR, ApiVersionsResponse, INIT_PRODUCER_ID_API_DESCRIPTOR,
    PRODUCE_API_DESCRIPTOR, api_versions_response::ApiVersion as AdvertisedApi,
};
use kafka_wire_core::{ApiKey, ApiVersion};

use super::{NegotiationError, NegotiationLimits, negotiate};

#[test]
fn given_broker_ranges_when_negotiated_then_highest_stable_overlap_is_selected() {
    // Given
    let response = response([
        advertised(PRODUCE_API_DESCRIPTOR.api_key.value(), 3, 8),
        advertised(INIT_PRODUCER_ID_API_DESCRIPTOR.api_key.value(), 0, 6),
        advertised(9_000, 0, 4),
    ]);

    // When
    let Ok(capabilities) = negotiate(response, limits(8, 8)) else {
        panic!("valid broker ranges must negotiate");
    };

    // Then
    assert_eq!(
        capabilities.version(PRODUCE_API_DESCRIPTOR.api_key),
        Some(ApiVersion::new(8))
    );
    assert_eq!(
        capabilities.version(INIT_PRODUCER_ID_API_DESCRIPTOR.api_key),
        INIT_PRODUCER_ID_API_DESCRIPTOR.latest_stable_version()
    );
    assert_eq!(capabilities.version(ApiKey::new(9_000)), None);
}

#[test]
fn given_no_version_overlap_when_negotiated_then_the_api_is_absent() {
    // Given
    let local_min = API_VERSIONS_API_DESCRIPTOR.supported_versions.min().value();
    let response = response([advertised(
        API_VERSIONS_API_DESCRIPTOR.api_key.value(),
        local_min - 2,
        local_min - 1,
    )]);

    // When
    let Ok(capabilities) = negotiate(response, limits(1, 1)) else {
        panic!("a valid non-overlapping range is not malformed");
    };

    // Then
    assert!(capabilities.is_empty());
}

#[test]
fn given_a_reversed_range_when_negotiated_then_the_api_is_rejected() {
    // Given
    let api_key = PRODUCE_API_DESCRIPTOR.api_key;
    let response = response([advertised(api_key.value(), 8, 3)]);

    // When
    let result = negotiate(response, limits(1, 1));

    // Then
    assert_eq!(
        result,
        Err(NegotiationError::InvalidRange {
            api_key,
            min_version: 8,
            max_version: 3,
        })
    );
}

#[test]
fn given_duplicate_unsorted_keys_when_negotiated_then_the_key_is_rejected() {
    // Given
    let api_key = PRODUCE_API_DESCRIPTOR.api_key;
    let response = response([
        advertised(9_000, 0, 1),
        advertised(api_key.value(), 0, 1),
        advertised(api_key.value(), 2, 3),
    ]);

    // When
    let result = negotiate(response, limits(3, 3));

    // Then
    assert_eq!(result, Err(NegotiationError::DuplicateApi { api_key }));
}

#[test]
fn given_a_broker_error_when_negotiated_then_no_advertisement_is_accepted() {
    // Given
    let mut response = response([advertised(9_000, 0, 1)]);
    response.error_code = 35;

    // When
    let result = negotiate(response, limits(1, 1));

    // Then
    assert_eq!(
        result,
        Err(NegotiationError::BrokerRejected { error_code: 35 })
    );
}

#[test]
fn given_an_oversized_advertisement_when_negotiated_then_count_is_rejected() {
    // Given
    let response = response([
        advertised(
            PRODUCE_API_DESCRIPTOR.api_key.value(),
            PRODUCE_API_DESCRIPTOR.supported_versions.min().value(),
            PRODUCE_API_DESCRIPTOR
                .latest_stable_version()
                .map_or(0, ApiVersion::value),
        ),
        advertised(API_VERSIONS_API_DESCRIPTOR.api_key.value(), 0, 1),
    ]);

    // When
    let result = negotiate(response, limits(1, 1));

    // Then
    assert_eq!(
        result,
        Err(NegotiationError::AdvertisementCapacity {
            observed: 2,
            limit: 1,
        })
    );
}

#[test]
fn given_more_overlap_than_retained_capacity_then_the_exact_api_is_rejected() {
    // Given
    let response = response([
        advertised(
            PRODUCE_API_DESCRIPTOR.api_key.value(),
            PRODUCE_API_DESCRIPTOR.supported_versions.min().value(),
            PRODUCE_API_DESCRIPTOR
                .latest_stable_version()
                .map_or(0, ApiVersion::value),
        ),
        advertised(API_VERSIONS_API_DESCRIPTOR.api_key.value(), 0, 1),
    ]);

    // When
    let result = negotiate(response, limits(2, 1));

    // Then
    let rejected = NegotiatedApi::new(API_VERSIONS_API_DESCRIPTOR.api_key, ApiVersion::new(1));
    assert_eq!(
        result,
        Err(NegotiationError::Capability(
            CapabilityError::CapacityReached { limit: 1, rejected }
        ))
    );
}

fn response(apis: impl IntoIterator<Item = AdvertisedApi>) -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.api_keys.extend(apis);
    response
}

fn advertised(api_key: i16, min_version: i16, max_version: i16) -> AdvertisedApi {
    let mut api = AdvertisedApi::default();
    api.api_key = api_key;
    api.min_version = min_version;
    api.max_version = max_version;
    api
}

fn limits(advertised: usize, negotiated: usize) -> NegotiationLimits {
    NegotiationLimits::new(nonzero(advertised), nonzero(negotiated))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
