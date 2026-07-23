//! Scenarios for suspending reconnect until endpoint refresh is causally newer.

use std::time::Duration;

use crate::{ConnectionEpoch, Moment, TimerId};

use super::{
    BackoffPolicy, BrokerDisposition, BrokerEffect, BrokerInput, BrokerMachine, BrokerPhase,
    BrokerState, JitterSample, ReconnectSchedule,
};

const EPOCH_1: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const EPOCH_2: ConnectionEpoch = ConnectionEpoch::from_raw(2);
const TIMER_1: TimerId = TimerId::from_raw(11);

#[test]
fn endpoint_exhaustion_reserves_the_next_epoch_without_scheduling_reconnect() {
    let mut machine = connecting_machine();

    let exhausted = machine.apply(exhausted(EPOCH_1, 1_000));

    assert!(exhausted.effects().is_empty());
    assert!(matches!(
        machine.state(),
        BrokerState::Refreshing {
            failed_epoch: EPOCH_1,
            next_epoch: EPOCH_2,
            timer_id: TIMER_1,
            deadline,
            ..
        } if deadline == Moment::from_nanos(1_050)
    ));
}

#[test]
fn only_matching_refresh_waits_for_the_original_reconnect_deadline() {
    let mut machine = connecting_machine();
    apply(&mut machine, exhausted(EPOCH_1, 1_000));

    let stale = machine.apply(BrokerInput::EndpointRefreshed {
        failed_epoch: EPOCH_2,
        now: Moment::from_nanos(1_049),
    });
    assert_eq!(stale.disposition(), BrokerDisposition::IgnoredStale);
    assert_eq!(machine.state().phase(), BrokerPhase::Refreshing);

    let refreshed = machine.apply(BrokerInput::EndpointRefreshed {
        failed_epoch: EPOCH_1,
        now: Moment::from_nanos(1_049),
    });
    assert_eq!(
        refreshed.effects(),
        &[BrokerEffect::ScheduleReconnect {
            failed_epoch: EPOCH_1,
            timer_id: TIMER_1,
            at: Moment::from_nanos(1_050),
        }]
    );
    assert_eq!(machine.state().phase(), BrokerPhase::Backoff);

    let elapsed = machine.apply(BrokerInput::ReconnectElapsed {
        failed_epoch: EPOCH_1,
        timer_id: TIMER_1,
        now: Moment::from_nanos(1_050),
    });
    assert_eq!(
        elapsed.effects(),
        &[BrokerEffect::OpenConnection { epoch: EPOCH_2 }]
    );
    assert_eq!(machine.state().phase(), BrokerPhase::Connecting);
}

#[test]
fn refresh_after_the_original_deadline_opens_without_scheduling_a_stale_timer() {
    let mut machine = connecting_machine();
    apply(&mut machine, exhausted(EPOCH_1, 1_000));

    let refreshed = machine.apply(BrokerInput::EndpointRefreshed {
        failed_epoch: EPOCH_1,
        now: Moment::from_nanos(1_051),
    });

    assert_eq!(
        refreshed.effects(),
        &[BrokerEffect::OpenConnection { epoch: EPOCH_2 }]
    );
    assert_eq!(machine.state().phase(), BrokerPhase::Connecting);
}

#[test]
fn shutdown_closes_a_suspended_refresh_without_external_work() {
    let mut machine = connecting_machine();
    apply(&mut machine, exhausted(EPOCH_1, 1_000));

    let drain = machine.apply(BrokerInput::BeginDrain);

    assert!(drain.effects().is_empty());
    assert_eq!(machine.state().phase(), BrokerPhase::Closed);
}

fn connecting_machine() -> BrokerMachine {
    let mut machine = BrokerMachine::new(EPOCH_1, policy());
    apply(&mut machine, BrokerInput::Start);
    machine
}

fn policy() -> BackoffPolicy {
    BackoffPolicy::try_new(Duration::from_nanos(100), Duration::from_nanos(1_000))
        .unwrap_or_else(|error| panic!("test backoff policy must be valid: {error}"))
}

fn exhausted(epoch: ConnectionEpoch, now: u64) -> BrokerInput {
    BrokerInput::EndpointExhausted {
        epoch,
        reconnect: ReconnectSchedule::new(
            TIMER_1,
            Moment::from_nanos(now),
            JitterSample::from_raw(0),
        ),
    }
}

fn apply(machine: &mut BrokerMachine, input: BrokerInput) {
    let transition = machine.apply(input);
    assert_eq!(transition.disposition(), BrokerDisposition::Applied);
}
