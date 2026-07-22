//! Given/When/Then scenarios for broker-owned reconnect generations.

use std::time::Duration;

use crate::{AuthenticationFailure, ConnectionEpoch, Moment, TimerId};

use super::{
    BackoffPolicy, BrokerCloseReason, BrokerDisposition, BrokerEffect, BrokerInput, BrokerMachine,
    BrokerPhase, BrokerState, JitterSample, ReconnectSchedule,
};

const EPOCH_1: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const EPOCH_2: ConnectionEpoch = ConnectionEpoch::from_raw(2);
const TIMER_1: TimerId = TimerId::from_raw(11);

#[test]
fn failed_initial_connection_schedules_a_bounded_fresh_epoch() {
    // Given
    let mut machine = connecting_machine();

    // When
    let transition = machine.apply(failed(EPOCH_1, TIMER_1, 1_000, 0));

    // Then
    assert_eq!(
        transition.effects(),
        &[BrokerEffect::ScheduleReconnect {
            failed_epoch: EPOCH_1,
            timer_id: TIMER_1,
            at: Moment::from_nanos(1_050),
        }]
    );
    assert!(matches!(
        machine.state(),
        BrokerState::Backoff {
            failed_epoch: EPOCH_1,
            next_epoch: EPOCH_2,
            ..
        }
    ));
}

#[test]
fn authentication_rejection_closes_without_authorizing_reconnect() {
    // Given
    let mut machine = connecting_machine();

    // When
    let transition = machine.apply(BrokerInput::ConnectionRejected {
        epoch: EPOCH_1,
        failure: AuthenticationFailure::Rejected,
    });

    // Then
    assert!(transition.effects().is_empty());
    assert_eq!(
        machine.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::AuthenticationFailed(AuthenticationFailure::Rejected),
        }
    );
}

#[test]
fn reconnect_timer_is_identity_fenced_and_early_delivery_is_rescheduled() {
    // Given
    let mut machine = backoff_machine();

    // When / Then: stale identity cannot advance the generation.
    let stale = machine.apply(BrokerInput::ReconnectElapsed {
        failed_epoch: EPOCH_2,
        timer_id: TIMER_1,
        now: Moment::from_nanos(2_000),
    });
    assert_eq!(stale.disposition(), BrokerDisposition::IgnoredStale);
    assert_eq!(machine.state().phase(), BrokerPhase::Backoff);

    // When / Then: early delivery retains the exact owned deadline.
    let early = machine.apply(BrokerInput::ReconnectElapsed {
        failed_epoch: EPOCH_1,
        timer_id: TIMER_1,
        now: Moment::from_nanos(1_049),
    });
    assert_eq!(
        early.effects(),
        &[BrokerEffect::ScheduleReconnect {
            failed_epoch: EPOCH_1,
            timer_id: TIMER_1,
            at: Moment::from_nanos(1_050),
        }]
    );

    // When / Then: the due timer opens only the preselected fresh epoch.
    let due = machine.apply(BrokerInput::ReconnectElapsed {
        failed_epoch: EPOCH_1,
        timer_id: TIMER_1,
        now: Moment::from_nanos(1_050),
    });
    assert_eq!(
        due.effects(),
        &[BrokerEffect::OpenConnection { epoch: EPOCH_2 }]
    );
}

#[test]
fn becoming_available_resets_the_failure_streak() {
    // Given
    let mut machine = backoff_machine();
    apply(
        &mut machine,
        BrokerInput::ReconnectElapsed {
            failed_epoch: EPOCH_1,
            timer_id: TIMER_1,
            now: Moment::from_nanos(1_050),
        },
    );
    apply(
        &mut machine,
        BrokerInput::ConnectionReady { epoch: EPOCH_2 },
    );

    // When
    let transition = machine.apply(failed(EPOCH_2, TimerId::from_raw(12), 2_000, 0));

    // Then: first-retry floor is 50ns again, not the second-retry floor.
    assert_eq!(
        transition.effects(),
        &[BrokerEffect::ScheduleReconnect {
            failed_epoch: EPOCH_2,
            timer_id: TimerId::from_raw(12),
            at: Moment::from_nanos(2_050),
        }]
    );
}

#[test]
fn drain_cancels_backoff_and_makes_a_late_timer_stale() {
    // Given
    let mut machine = backoff_machine();

    // When
    let drain = machine.apply(BrokerInput::BeginDrain);

    // Then
    assert_eq!(
        drain.effects(),
        &[BrokerEffect::CancelReconnect { timer_id: TIMER_1 }]
    );
    assert_eq!(
        machine.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::Requested,
        }
    );
    let late = machine.apply(BrokerInput::ReconnectElapsed {
        failed_epoch: EPOCH_1,
        timer_id: TIMER_1,
        now: Moment::from_nanos(2_000),
    });
    assert_eq!(late.disposition(), BrokerDisposition::IgnoredStale);
}

#[test]
fn available_drain_waits_for_the_matching_connection_child() {
    // Given
    let mut machine = connecting_machine();
    apply(
        &mut machine,
        BrokerInput::ConnectionReady { epoch: EPOCH_1 },
    );

    // When
    let drain = machine.apply(BrokerInput::BeginDrain);

    // Then
    assert_eq!(
        drain.effects(),
        &[BrokerEffect::DrainConnection { epoch: EPOCH_1 }]
    );
    assert_eq!(machine.state().phase(), BrokerPhase::Draining);
    assert_eq!(
        machine
            .apply(BrokerInput::ConnectionDrained { epoch: EPOCH_2 })
            .disposition(),
        BrokerDisposition::IgnoredStale
    );
    apply(
        &mut machine,
        BrokerInput::ConnectionDrained { epoch: EPOCH_1 },
    );
    assert_eq!(machine.state().phase(), BrokerPhase::Closed);
}

#[test]
fn exhausted_epoch_space_closes_without_scheduling_external_work() {
    let mut machine = BrokerMachine::new(ConnectionEpoch::from_raw(u64::MAX), policy());
    apply(&mut machine, BrokerInput::Start);

    let transition = machine.apply(failed(ConnectionEpoch::from_raw(u64::MAX), TIMER_1, 0, 0));

    assert!(transition.effects().is_empty());
    assert_eq!(
        machine.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::EpochExhausted,
        }
    );
}

fn connecting_machine() -> BrokerMachine {
    let mut machine = BrokerMachine::new(EPOCH_1, policy());
    let start = machine.apply(BrokerInput::Start);
    assert_eq!(
        start.effects(),
        &[BrokerEffect::OpenConnection { epoch: EPOCH_1 }]
    );
    machine
}

fn backoff_machine() -> BrokerMachine {
    let mut machine = connecting_machine();
    apply(&mut machine, failed(EPOCH_1, TIMER_1, 1_000, 0));
    machine
}

fn failed(epoch: ConnectionEpoch, timer_id: TimerId, now: u64, jitter: u64) -> BrokerInput {
    BrokerInput::ConnectionFailed {
        epoch,
        reconnect: ReconnectSchedule::new(
            timer_id,
            Moment::from_nanos(now),
            JitterSample::from_raw(jitter),
        ),
    }
}

fn policy() -> BackoffPolicy {
    BackoffPolicy::try_new(Duration::from_nanos(100), Duration::from_nanos(1_000))
        .unwrap_or_else(|error| panic!("test backoff policy must be valid: {error}"))
}

fn apply(machine: &mut BrokerMachine, input: BrokerInput) {
    let transition = machine.apply(input);
    assert_eq!(transition.disposition(), BrokerDisposition::Applied);
}
