//! Scenarios for bounded transient discovery retry and stale-result fencing.

use std::num::NonZeroU16;

use crate::{
    BrokerEndpoint, BrokerId, CoordinatorDisposition, CoordinatorEffect, CoordinatorEpoch,
    CoordinatorInput, CoordinatorKey, CoordinatorKind, CoordinatorMachine, CoordinatorState,
    EvidenceStamp, HostName, Moment, OperationId,
};

#[test]
fn retry_wait_is_positive_and_due_work_uses_a_fresh_identity() {
    let mut machine = machine();
    let _ = machine.apply(resolve(1));

    let rejected = machine.apply(rejected(1, 1, 10, 90));

    let at = Moment::from_nanos(100_000_010);
    assert_eq!(rejected.effects(), [wait(1, 1, at)]);
    assert!(matches!(
        machine.state(),
        CoordinatorState::Retrying {
            operation_id,
            target_epoch,
            retries: 1,
            at: deadline,
            ..
        } if *operation_id == operation(1)
            && *target_epoch == CoordinatorEpoch::from_raw(1)
            && *deadline == at
    ));
    assert_eq!(
        machine.apply(resolve(2)).disposition(),
        CoordinatorDisposition::Coalesced
    );

    let early = machine.apply(elapsed(1, 1, at.as_nanos() - 1, 3));
    assert_eq!(early.effects(), [wait(1, 1, at)]);
    let stale = machine.apply(elapsed(9, 1, at.as_nanos(), 4));
    assert_eq!(stale.disposition(), CoordinatorDisposition::IgnoredStale);

    let due = machine.apply(elapsed(1, 1, at.as_nanos(), 5));
    assert_eq!(
        due.effects(),
        [CoordinatorEffect::Find {
            operation_id: operation(5),
            key: key(),
            epoch: CoordinatorEpoch::from_raw(1),
        }]
    );
    assert!(matches!(
        machine.state(),
        CoordinatorState::Discovering {
            operation_id,
            retries: 1,
            ..
        } if *operation_id == operation(5)
    ));

    let stale_result = machine.apply(success(1, 1, 7));
    assert_eq!(
        stale_result.disposition(),
        CoordinatorDisposition::IgnoredStale
    );
    assert!(machine.current().is_none());
    let installed = machine.apply(success(5, 1, 8));
    assert_eq!(installed.disposition(), CoordinatorDisposition::Applied);
    assert_eq!(
        machine.current().map(|route| route.broker_id().get()),
        Some(7)
    );
}

#[test]
fn ninth_consecutive_rejection_exhausts_retry_without_a_spin() {
    let mut machine = machine();
    let _ = machine.apply(resolve(1));
    let mut active = 1_u64;
    let mut now = 0_u64;

    for retry in 1_u64..=8 {
        let rejected = machine.apply(rejected(active, 1, now, 100 + retry));
        let [CoordinatorEffect::WaitUntil { at, .. }] = rejected.effects() else {
            panic!("retry {retry} must own one wait");
        };
        assert!(at.as_nanos() > now);
        let next = retry + 1;
        let due = machine.apply(elapsed(active, 1, at.as_nanos(), next));
        assert!(matches!(
            due.effects(),
            [CoordinatorEffect::Find { operation_id, .. }] if *operation_id == operation(next)
        ));
        active = next;
        now = at.as_nanos();
    }

    let exhausted = machine.apply(rejected(active, 1, now, 999));

    assert!(exhausted.effects().is_empty());
    assert!(matches!(machine.state(), CoordinatorState::Unknown { .. }));
}

#[test]
fn terminal_abandonment_during_retry_makes_the_owned_wake_stale() {
    let mut machine = machine();
    let _ = machine.apply(resolve(1));
    let rejected = machine.apply(rejected(1, 1, 0, 2));
    let [CoordinatorEffect::WaitUntil { at, .. }] = rejected.effects() else {
        panic!("transient rejection must wait");
    };

    let abandoned = machine.apply(CoordinatorInput::DiscoveryFailed {
        operation_id: operation(1),
        epoch: CoordinatorEpoch::from_raw(1),
        followup_operation_id: operation(3),
    });
    let stale_wake = machine.apply(elapsed(1, 1, at.as_nanos(), 4));

    assert!(abandoned.effects().is_empty());
    assert!(matches!(machine.state(), CoordinatorState::Unknown { .. }));
    assert_eq!(
        stale_wake.disposition(),
        CoordinatorDisposition::IgnoredStale
    );
}

fn machine() -> CoordinatorMachine {
    CoordinatorMachine::new(key())
}

fn resolve(raw: u64) -> CoordinatorInput {
    CoordinatorInput::Resolve {
        operation_id: operation(raw),
    }
}

fn rejected(raw: u64, epoch: u64, now: u64, followup: u64) -> CoordinatorInput {
    CoordinatorInput::DiscoveryRejected {
        operation_id: operation(raw),
        epoch: CoordinatorEpoch::from_raw(epoch),
        now: Moment::from_nanos(now),
        followup_operation_id: operation(followup),
    }
}

fn elapsed(raw: u64, epoch: u64, now: u64, retry: u64) -> CoordinatorInput {
    CoordinatorInput::RetryElapsed {
        operation_id: operation(raw),
        epoch: CoordinatorEpoch::from_raw(epoch),
        now: Moment::from_nanos(now),
        retry_operation_id: operation(retry),
    }
}

fn success(raw: u64, epoch: u64, followup: u64) -> CoordinatorInput {
    CoordinatorInput::DiscoverySucceeded {
        operation_id: operation(raw),
        epoch: CoordinatorEpoch::from_raw(epoch),
        broker_id: BrokerId::new(7).unwrap_or_else(|error| panic!("broker ID: {error}")),
        endpoint: BrokerEndpoint::new(
            HostName::new("broker.test").unwrap_or_else(|error| panic!("host: {error}")),
            NonZeroU16::new(9_092).unwrap_or_else(|| panic!("nonzero port")),
        ),
        evidence: EvidenceStamp::ORIGIN,
        followup_operation_id: operation(followup),
    }
}

fn wait(raw: u64, epoch: u64, at: Moment) -> CoordinatorEffect {
    CoordinatorEffect::WaitUntil {
        operation_id: operation(raw),
        epoch: CoordinatorEpoch::from_raw(epoch),
        at,
    }
}

fn key() -> CoordinatorKey {
    CoordinatorKey::new(CoordinatorKind::Group, "orders")
        .unwrap_or_else(|error| panic!("coordinator key: {error}"))
}

const fn operation(raw: u64) -> OperationId {
    OperationId::from_raw(raw)
}
