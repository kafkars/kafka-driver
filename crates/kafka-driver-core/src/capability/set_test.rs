//! Scenarios for bounded canonical capability ownership and lookup.

use std::num::NonZeroUsize;

use kafka_wire_core::{ApiKey, ApiVersion, VersionRange};

use super::{CapabilityError, NegotiatedApi, NegotiatedCapabilities};

#[test]
fn given_sorted_entries_when_built_then_versions_are_found_by_api_key() {
    // Given
    let entries = [api(1, 3), api(18, 4), api(67, 1)];

    // When
    let Ok(capabilities) = NegotiatedCapabilities::try_from_iter(entries, capacity(3)) else {
        panic!("sorted entries within capacity must be retained");
    };

    // Then
    assert_eq!(capabilities.len(), 3);
    assert_eq!(capabilities.version(ApiKey::new(18)), Some(version(4)));
    assert_eq!(capabilities.version(ApiKey::new(19)), None);
    assert_eq!(capabilities.iter().collect::<Vec<_>>(), entries);
}

#[test]
fn given_a_negotiated_range_when_capped_then_the_highest_usable_version_is_selected() {
    // Given
    let ranged = NegotiatedApi::with_range(ApiKey::new(18), VersionRange::new(2, 4));
    let Ok(capabilities) = NegotiatedCapabilities::try_from_iter([ranged], capacity(1)) else {
        panic!("one negotiated range must fit");
    };

    // When / Then
    assert_eq!(capabilities.api(ApiKey::new(18)), Some(ranged));
    assert_eq!(ranged.highest_at_most(version(3)), Some(version(3)));
    assert_eq!(ranged.highest_at_most(version(5)), Some(version(4)));
    assert_eq!(ranged.highest_at_most(version(1)), None);
}

#[test]
fn given_capacity_reached_when_an_entry_is_admitted_then_it_is_returned() {
    // Given
    let rejected = api(18, 4);

    // When
    let result = NegotiatedCapabilities::try_from_iter([api(1, 3), rejected], capacity(1));

    // Then
    assert_eq!(
        result,
        Err(CapabilityError::CapacityReached { limit: 1, rejected })
    );
}

#[test]
fn given_duplicate_keys_when_built_then_order_is_rejected() {
    // Given
    let previous = api(18, 3);
    let rejected = api(18, 4);

    // When
    let result = NegotiatedCapabilities::try_from_iter([previous, rejected], capacity(2));

    // Then
    assert_eq!(
        result,
        Err(CapabilityError::NonAscending { previous, rejected })
    );
}

#[test]
fn given_regressing_keys_when_built_then_order_is_rejected() {
    // Given
    let previous = api(18, 3);
    let rejected = api(1, 4);

    // When
    let result = NegotiatedCapabilities::try_from_iter([previous, rejected], capacity(2));

    // Then
    assert_eq!(
        result,
        Err(CapabilityError::NonAscending { previous, rejected })
    );
}

#[test]
fn given_no_overlap_when_built_then_an_empty_set_is_valid() {
    // Given / When
    let Ok(capabilities) = NegotiatedCapabilities::try_from_iter([], capacity(1)) else {
        panic!("an empty negotiated set is valid");
    };

    // Then
    assert!(capabilities.is_empty());
}

const fn api(key: i16, version: i16) -> NegotiatedApi {
    NegotiatedApi::new(ApiKey::new(key), ApiVersion::new(version))
}

const fn version(value: i16) -> ApiVersion {
    ApiVersion::new(value)
}

fn capacity(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test capacity must be nonzero"))
}
