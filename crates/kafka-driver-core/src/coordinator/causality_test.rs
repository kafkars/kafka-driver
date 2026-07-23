//! Causal invalidation scenarios across completed pre-failure discoveries.

use std::num::NonZeroU16;

use crate::{
    BrokerEndpoint, BrokerId, CoordinatorEpoch, CoordinatorInput, CoordinatorKey, CoordinatorKind,
    CoordinatorMachine, EvidenceStamp, HostName, OperationId, OutcomeStamp,
};

use super::{CoordinatorDisposition, CoordinatorEffect};

#[test]
fn discovery_completed_before_failure_cannot_satisfy_invalidation() {
    let mut machine = ready_machine(1);
    let failed = route(&machine);
    refresh(&mut machine, 2, 2);

    let invalidated = machine.apply(CoordinatorInput::Invalidate {
        route: failed,
        observed_at: OutcomeStamp::from_raw(3),
        operation_id: operation(3),
    });

    assert_eq!(invalidated.disposition(), CoordinatorDisposition::Applied);
    assert!(matches!(
        invalidated.effects(),
        [CoordinatorEffect::Find {
            epoch,
            ..
        }] if *epoch == CoordinatorEpoch::from_raw(3)
    ));
    assert!(machine.current().is_none());
}

#[test]
fn discovery_started_after_failure_is_already_sufficient_evidence() {
    let mut machine = ready_machine(1);
    let failed = route(&machine);
    refresh(&mut machine, 2, 4);

    let invalidated = machine.apply(CoordinatorInput::Invalidate {
        route: failed,
        observed_at: OutcomeStamp::from_raw(3),
        operation_id: operation(3),
    });

    assert_eq!(
        invalidated.disposition(),
        CoordinatorDisposition::IgnoredStale
    );
    assert!(machine.current().is_some());
}

#[test]
fn topology_withdrawal_does_not_masquerade_as_a_broker_failure_observation() {
    let mut machine = ready_machine(5);
    let route = machine
        .current()
        .cloned()
        .unwrap_or_else(|| panic!("ready route"));

    let withdrawn = machine.apply(CoordinatorInput::Withdraw {
        route,
        operation_id: operation(2),
    });

    assert_eq!(withdrawn.disposition(), CoordinatorDisposition::Applied);
    assert!(machine.current().is_none());
}

fn ready_machine(raw_evidence: u64) -> CoordinatorMachine {
    let mut machine = CoordinatorMachine::new(key());
    let _ = machine.apply(CoordinatorInput::Resolve {
        operation_id: operation(1),
    });
    let installed = machine.apply(success(1, 1, raw_evidence, 2));
    assert!(installed.effects().is_empty());
    machine
}

fn refresh(machine: &mut CoordinatorMachine, raw_operation: u64, raw_evidence: u64) {
    let _ = machine.apply(CoordinatorInput::Refresh {
        operation_id: operation(raw_operation),
    });
    let installed = machine.apply(success(
        raw_operation,
        raw_operation,
        raw_evidence,
        raw_operation + 1,
    ));
    assert!(installed.effects().is_empty());
}

fn success(
    raw_operation: u64,
    raw_epoch: u64,
    raw_evidence: u64,
    raw_followup: u64,
) -> CoordinatorInput {
    CoordinatorInput::DiscoverySucceeded {
        operation_id: operation(raw_operation),
        epoch: CoordinatorEpoch::from_raw(raw_epoch),
        broker_id: broker_id(),
        endpoint: endpoint(),
        evidence: EvidenceStamp::from_raw(raw_evidence),
        followup_operation_id: operation(raw_followup),
    }
}

fn route(machine: &CoordinatorMachine) -> crate::CoordinatorRoute {
    machine
        .current()
        .cloned()
        .unwrap_or_else(|| panic!("coordinator route"))
}

fn key() -> CoordinatorKey {
    CoordinatorKey::new(CoordinatorKind::Group, "orders")
        .unwrap_or_else(|error| panic!("valid key: {error}"))
}

fn endpoint() -> BrokerEndpoint {
    BrokerEndpoint::new(
        HostName::new("broker.test").unwrap_or_else(|error| panic!("valid host: {error}")),
        NonZeroU16::new(9092).unwrap_or_else(|| panic!("nonzero port")),
    )
}

fn broker_id() -> BrokerId {
    BrokerId::new(1).unwrap_or_else(|error| panic!("valid broker ID: {error}"))
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}
