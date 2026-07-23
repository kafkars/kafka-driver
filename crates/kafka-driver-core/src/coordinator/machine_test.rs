//! Scenarios for coordinator discovery coalescing, refresh, and stale fencing.

use std::num::NonZeroU16;

use crate::{
    BrokerEndpoint, BrokerId, CoordinatorDisposition, CoordinatorEffect, CoordinatorEpoch,
    CoordinatorFollowup, CoordinatorInput, CoordinatorKey, CoordinatorKind, CoordinatorMachine,
    CoordinatorRoute, CoordinatorState, EvidenceStamp, HostName, OperationId, OutcomeStamp,
};

#[test]
fn first_resolution_starts_one_exact_discovery_and_duplicates_coalesce() {
    let mut machine = coordinator_machine();

    let first = machine.apply(resolve(1));
    let duplicate = machine.apply(resolve(2));

    assert_eq!(
        first.effects(),
        [CoordinatorEffect::Find {
            operation_id: operation(1),
            key: key(),
            epoch: CoordinatorEpoch::from_raw(1),
        }]
    );
    assert_eq!(duplicate.disposition(), CoordinatorDisposition::Coalesced);
}

#[test]
fn queued_refresh_starts_a_new_epoch_after_success() {
    let mut machine = coordinator_machine();
    let _ = machine.apply(resolve(1));
    let queued = machine.apply(refresh(2));

    let succeeded = machine.apply(success(1, 1, 7, 3));

    assert_eq!(queued.disposition(), CoordinatorDisposition::RefreshQueued);
    assert_eq!(
        succeeded.effects(),
        [CoordinatorEffect::Find {
            operation_id: operation(3),
            key: key(),
            epoch: CoordinatorEpoch::from_raw(2),
        }]
    );
}

#[test]
fn stale_result_cannot_replace_current_discovery_ownership() {
    let mut machine = ready_machine();
    let current = machine
        .current()
        .cloned()
        .unwrap_or_else(|| panic!("route"));
    let _ = machine.apply(refresh(2));

    let stale = machine.apply(success(1, 1, 9, 3));

    assert_eq!(stale.disposition(), CoordinatorDisposition::IgnoredStale);
    assert_eq!(machine.current(), Some(&current));
    assert!(matches!(
        machine.state(),
        CoordinatorState::Discovering { operation_id, .. } if *operation_id == operation(2)
    ));
}

#[test]
fn current_operation_with_the_wrong_epoch_is_ignored() {
    let mut machine = coordinator_machine();
    let _ = machine.apply(resolve(1));

    let stale = machine.apply(success(1, 2, 7, 2));

    assert_eq!(stale.disposition(), CoordinatorDisposition::IgnoredStale);
    assert!(machine.current().is_none());
    assert!(matches!(
        machine.state(),
        CoordinatorState::Discovering { target_epoch, .. }
            if *target_epoch == CoordinatorEpoch::from_raw(1)
    ));
}

#[test]
fn exact_route_invalidation_refreshes_and_stale_route_is_ignored() {
    let mut machine = ready_machine();
    let current = machine
        .current()
        .cloned()
        .unwrap_or_else(|| panic!("route"));
    let mut other = coordinator_machine();
    let _ = other.apply(resolve(8));
    let _ = other.apply(success(8, 1, 9, 9));
    let stale_route = other.current().cloned().unwrap_or_else(|| panic!("route"));

    let stale = machine.apply(CoordinatorInput::Invalidate {
        route: stale_route,
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(2),
    });
    let refreshed = machine.apply(CoordinatorInput::Invalidate {
        route: current,
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(3),
    });

    assert_eq!(stale.disposition(), CoordinatorDisposition::IgnoredStale);
    assert!(matches!(
        refreshed.effects(),
        [CoordinatorEffect::Find { operation_id, epoch, .. }]
            if *operation_id == operation(3) && *epoch == CoordinatorEpoch::from_raw(2)
    ));
    assert!(machine.current().is_none());
}

#[test]
fn invalidation_during_active_discovery_withdraws_route_and_requires_followup() {
    let mut machine = ready_machine();
    let failed = machine
        .current()
        .cloned()
        .unwrap_or_else(|| panic!("ready route"));
    let _ = machine.apply(refresh(2));

    let invalidated = machine.apply(CoordinatorInput::Invalidate {
        route: failed,
        observed_at: OutcomeStamp::ORIGIN,
        operation_id: operation(3),
    });

    assert_eq!(
        invalidated.disposition(),
        CoordinatorDisposition::RefreshQueued
    );
    assert!(machine.current().is_none());
    assert!(matches!(
        machine.state(),
        CoordinatorState::Discovering {
            followup: Some(CoordinatorFollowup::Revocation),
            ..
        }
    ));

    let active = machine.apply(success(2, 2, 8, 4));
    assert_eq!(
        active.effects(),
        [CoordinatorEffect::Find {
            operation_id: operation(4),
            key: key(),
            epoch: CoordinatorEpoch::from_raw(3),
        }]
    );
    assert!(machine.current().is_none());

    let followup = machine.apply(success(4, 3, 9, 5));
    assert!(followup.effects().is_empty());
    assert_eq!(
        machine.current().map(CoordinatorRoute::epoch),
        Some(CoordinatorEpoch::from_raw(3))
    );
}

#[test]
fn failed_refresh_retains_route_and_does_not_consume_an_epoch() {
    let mut machine = ready_machine();
    let current = machine
        .current()
        .cloned()
        .unwrap_or_else(|| panic!("route"));
    let _ = machine.apply(refresh(2));
    let failed = machine.apply(CoordinatorInput::DiscoveryFailed {
        operation_id: operation(2),
        epoch: CoordinatorEpoch::from_raw(2),
        followup_operation_id: operation(3),
    });

    let retried = machine.apply(refresh(4));

    assert_eq!(failed.disposition(), CoordinatorDisposition::Applied);
    assert_eq!(machine.current(), Some(&current));
    assert!(matches!(
        retried.effects(),
        [CoordinatorEffect::Find { epoch, .. }]
            if *epoch == CoordinatorEpoch::from_raw(2)
    ));
}

fn ready_machine() -> CoordinatorMachine {
    let mut machine = coordinator_machine();
    let _ = machine.apply(resolve(1));
    let installed = machine.apply(success(1, 1, 7, 2));
    assert_eq!(installed.disposition(), CoordinatorDisposition::Applied);
    machine
}

fn coordinator_machine() -> CoordinatorMachine {
    CoordinatorMachine::new(key())
}

fn resolve(raw_operation: u64) -> CoordinatorInput {
    CoordinatorInput::Resolve {
        operation_id: operation(raw_operation),
    }
}

fn refresh(raw_operation: u64) -> CoordinatorInput {
    CoordinatorInput::Refresh {
        operation_id: operation(raw_operation),
    }
}

fn success(
    raw_operation: u64,
    raw_epoch: u64,
    raw_broker: i32,
    raw_followup: u64,
) -> CoordinatorInput {
    CoordinatorInput::DiscoverySucceeded {
        operation_id: operation(raw_operation),
        epoch: CoordinatorEpoch::from_raw(raw_epoch),
        broker_id: BrokerId::new(raw_broker)
            .unwrap_or_else(|error| panic!("valid broker rejected: {error}")),
        endpoint: endpoint(raw_broker),
        evidence: EvidenceStamp::ORIGIN,
        followup_operation_id: operation(raw_followup),
    }
}

fn key() -> CoordinatorKey {
    CoordinatorKey::new(CoordinatorKind::Group, "orders")
        .unwrap_or_else(|error| panic!("valid key rejected: {error}"))
}

fn endpoint(raw_broker: i32) -> BrokerEndpoint {
    let host = HostName::new(format!("broker-{raw_broker}.test"))
        .unwrap_or_else(|error| panic!("valid host rejected: {error}"));
    BrokerEndpoint::new(host, port())
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}

fn port() -> NonZeroU16 {
    NonZeroU16::new(9_092).unwrap_or_else(|| panic!("test port must be nonzero"))
}
