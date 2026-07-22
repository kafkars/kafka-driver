//! Scenarios for legacy, batched, transaction, and share coordinator requests.

use kafka_driver_core::{CoordinatorKey, CoordinatorKind};
use kafka_wire_core::ApiVersion;

use super::{CoordinatorBuildError, find_coordinator_request};

#[test]
fn legacy_group_request_uses_the_single_key_field() {
    let request = find_coordinator_request(&key(CoordinatorKind::Group), version(0))
        .unwrap_or_else(|error| panic!("valid group request rejected: {error}"));

    assert_eq!(request.key.as_str(), "orders");
    assert_eq!(request.key_type, 0);
    assert!(request.coordinator_keys.is_empty());
}

#[test]
fn transaction_requires_key_type_support_and_uses_type_one() {
    assert!(matches!(
        find_coordinator_request(&key(CoordinatorKind::Transaction), version(0)),
        Err(CoordinatorBuildError::UnsupportedKind { .. })
    ));

    let request = find_coordinator_request(&key(CoordinatorKind::Transaction), version(1))
        .unwrap_or_else(|error| panic!("valid transaction request rejected: {error}"));

    assert_eq!(request.key.as_str(), "orders");
    assert_eq!(request.key_type, 1);
}

#[test]
fn batched_version_carries_exactly_one_key_and_share_requires_version_six() {
    let group = find_coordinator_request(&key(CoordinatorKind::Group), version(4))
        .unwrap_or_else(|error| panic!("valid batched request rejected: {error}"));

    assert!(group.key.is_empty());
    assert_eq!(group.coordinator_keys.len(), 1);
    assert_eq!(group.coordinator_keys[0].as_str(), "orders");
    assert!(matches!(
        find_coordinator_request(&key(CoordinatorKind::Share), version(5)),
        Err(CoordinatorBuildError::UnsupportedKind { .. })
    ));
    let share = find_coordinator_request(&key(CoordinatorKind::Share), version(6))
        .unwrap_or_else(|error| panic!("valid share request rejected: {error}"));
    assert_eq!(share.key_type, 2);
}

fn key(kind: CoordinatorKind) -> CoordinatorKey {
    CoordinatorKey::new(kind, "orders")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"))
}

const fn version(value: i16) -> ApiVersion {
    ApiVersion::new(value)
}
