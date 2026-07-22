//! Scenarios for ordered metadata-operation allocation and terminal exhaustion.

use super::identity::MetadataOperationIds;

#[test]
fn identities_are_monotonic_and_exhaust_without_wrapping() {
    let mut ids = MetadataOperationIds::starting_at(u64::MAX - 1);

    assert_eq!(
        ids.reserve().map(kafka_driver_core::OperationId::get),
        Some(u64::MAX - 1)
    );
    assert_eq!(
        ids.reserve().map(kafka_driver_core::OperationId::get),
        Some(u64::MAX)
    );
    assert_eq!(ids.reserve(), None);
}
