//! Scenarios for atomic broker-local identity reservations and exhaustion.

use kafka_driver_core::{EffectId, TimerId, TransportId};

use super::identity::BrokerIds;

#[test]
fn given_fresh_sources_when_work_is_reserved_then_each_domain_advances_independently() {
    // Given
    let mut ids = BrokerIds::new();

    // When
    let Some(open) = ids.reserve_open() else {
        panic!("fresh open identities must exist");
    };
    let Some(submission) = ids.reserve_submission() else {
        panic!("fresh submission identities must exist");
    };

    // Then
    assert_eq!(open.effect_id, EffectId::from_raw(1));
    assert_eq!(open.transport_id, TransportId::from_raw(1));
    assert_eq!(submission.write_effect, EffectId::from_raw(2));
    assert_eq!(submission.deadline_timer, TimerId::from_raw(1));
}

#[test]
fn given_an_exhausted_timer_when_submission_is_reserved_then_effect_is_not_consumed() {
    // Given
    let mut ids = BrokerIds::for_test(Some(7), None, Some(11));

    // When
    let submission = ids.reserve_submission();
    let Some(open) = ids.reserve_open() else {
        panic!("failed pair must preserve effect identity");
    };

    // Then
    assert!(submission.is_none());
    assert_eq!(open.effect_id, EffectId::from_raw(7));
    assert_eq!(open.transport_id, TransportId::from_raw(11));
}

#[test]
fn given_last_identities_when_reserved_then_maximum_values_are_issued_once() {
    // Given
    let mut ids = BrokerIds::for_test(Some(u64::MAX), Some(u64::MAX), Some(u64::MAX));

    // When
    let Some(submission) = ids.reserve_submission() else {
        panic!("maximum submission identities must remain valid");
    };
    let next_submission = ids.reserve_submission();
    let open = ids.reserve_open();

    // Then
    assert_eq!(submission.write_effect, EffectId::from_raw(u64::MAX));
    assert_eq!(submission.deadline_timer, TimerId::from_raw(u64::MAX));
    assert!(next_submission.is_none());
    assert!(open.is_none());
}
