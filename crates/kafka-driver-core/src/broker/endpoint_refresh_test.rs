//! Deterministic endpoint-refresh failure, backoff, and terminal scenarios.

use std::time::Duration;

use crate::{ConnectionEpoch, DnsFailure, Moment, TimerId};

use super::{
    AddressRefreshState, BackoffPolicy, BrokerCloseReason, BrokerDisposition, BrokerEffect,
    BrokerInput, BrokerMachine, BrokerState, EndpointRefreshSchedule, JitterSample,
    ReconnectSchedule,
};

const FAILED_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const RECONNECT_TIMER: TimerId = TimerId::from_raw(11);
const REFRESH_TIMER: TimerId = TimerId::from_raw(12);

#[test]
fn temporary_failure_waits_for_a_capped_jittered_retry_deadline() {
    let mut machine = resolving_machine();

    let failed = machine.apply(BrokerInput::EndpointRefreshFailed {
        failed_epoch: FAILED_EPOCH,
        failure: DnsFailure::Temporary,
        retry: refresh_schedule(REFRESH_TIMER, 2_000),
    });

    assert_eq!(
        failed.effects(),
        [BrokerEffect::ScheduleEndpointRefreshRetry {
            failed_epoch: FAILED_EPOCH,
            timer_id: REFRESH_TIMER,
            at: Moment::from_nanos(2_050),
        }]
    );
    assert!(matches!(
        machine.state(),
        BrokerState::Refreshing {
            refresh: AddressRefreshState::Backoff {
                retry,
                deadline,
                ..
            },
            ..
        } if retry.get() == 1 && deadline == Moment::from_nanos(2_050)
    ));

    let due = machine.apply(BrokerInput::EndpointRefreshRetryElapsed {
        failed_epoch: FAILED_EPOCH,
        timer_id: REFRESH_TIMER,
        now: Moment::from_nanos(2_050),
    });

    assert!(due.effects().is_empty());
    assert!(matches!(
        machine.state(),
        BrokerState::Refreshing {
            refresh: AddressRefreshState::Pending {
                last_retry: Some(retry),
            },
            ..
        } if retry.get() == 1
    ));
}

#[test]
fn retry_timer_is_identity_fenced_and_cannot_fire_before_its_deadline() {
    let mut machine = resolving_machine();
    apply(
        &mut machine,
        BrokerInput::EndpointRefreshFailed {
            failed_epoch: FAILED_EPOCH,
            failure: DnsFailure::Temporary,
            retry: refresh_schedule(REFRESH_TIMER, 2_000),
        },
    );

    let stale = machine.apply(BrokerInput::EndpointRefreshRetryElapsed {
        failed_epoch: FAILED_EPOCH,
        timer_id: TimerId::from_raw(99),
        now: Moment::from_nanos(2_050),
    });
    assert_eq!(stale.disposition(), BrokerDisposition::IgnoredStale);

    let early = machine.apply(BrokerInput::EndpointRefreshRetryElapsed {
        failed_epoch: FAILED_EPOCH,
        timer_id: REFRESH_TIMER,
        now: Moment::from_nanos(2_049),
    });
    assert_eq!(
        early.effects(),
        [BrokerEffect::ScheduleEndpointRefreshRetry {
            failed_epoch: FAILED_EPOCH,
            timer_id: REFRESH_TIMER,
            at: Moment::from_nanos(2_050),
        }]
    );
    assert!(matches!(
        machine.state(),
        BrokerState::Refreshing {
            refresh: AddressRefreshState::Backoff { .. },
            ..
        }
    ));
}

#[test]
fn refresh_failure_classification_distinguishes_retryable_names_from_unusable_answers() {
    let mut missing = resolving_machine();
    let retryable = missing.apply(BrokerInput::EndpointRefreshFailed {
        failed_epoch: FAILED_EPOCH,
        failure: DnsFailure::NameNotFound,
        retry: refresh_schedule(REFRESH_TIMER, 2_000),
    });
    assert!(matches!(
        retryable.effects(),
        [BrokerEffect::ScheduleEndpointRefreshRetry { .. }]
    ));

    let mut unusable = resolving_machine();
    let terminal = unusable.apply(BrokerInput::EndpointRefreshFailed {
        failed_epoch: FAILED_EPOCH,
        failure: DnsFailure::NoUsableAddress,
        retry: refresh_schedule(REFRESH_TIMER, 2_000),
    });

    assert!(terminal.effects().is_empty());
    assert_eq!(
        unusable.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::EndpointResolutionFailed(DnsFailure::NoUsableAddress),
        }
    );
}

#[test]
fn shutdown_cancels_an_owned_endpoint_refresh_timer() {
    let mut machine = resolving_machine();
    apply(
        &mut machine,
        BrokerInput::EndpointRefreshFailed {
            failed_epoch: FAILED_EPOCH,
            failure: DnsFailure::Temporary,
            retry: refresh_schedule(REFRESH_TIMER, 2_000),
        },
    );

    let shutdown = machine.apply(BrokerInput::BeginDrain);

    assert_eq!(
        shutdown.effects(),
        [BrokerEffect::CancelEndpointRefreshRetry {
            timer_id: REFRESH_TIMER,
        }]
    );
    assert_eq!(
        machine.state(),
        BrokerState::Closed {
            reason: BrokerCloseReason::Requested,
        }
    );
}

fn resolving_machine() -> BrokerMachine {
    let mut machine = BrokerMachine::new(FAILED_EPOCH, policy());
    apply(&mut machine, BrokerInput::Start);
    apply(
        &mut machine,
        BrokerInput::EndpointExhausted {
            epoch: FAILED_EPOCH,
            reconnect: ReconnectSchedule::new(
                RECONNECT_TIMER,
                Moment::from_nanos(1_000),
                JitterSample::from_raw(0),
            ),
        },
    );
    apply(
        &mut machine,
        BrokerInput::EndpointRefreshStarted {
            failed_epoch: FAILED_EPOCH,
        },
    );
    machine
}

fn refresh_schedule(timer_id: TimerId, now: u64) -> EndpointRefreshSchedule {
    EndpointRefreshSchedule::new(timer_id, Moment::from_nanos(now), JitterSample::from_raw(0))
}

fn policy() -> BackoffPolicy {
    BackoffPolicy::try_new(Duration::from_nanos(100), Duration::from_nanos(1_000))
        .unwrap_or_else(|error| panic!("valid test backoff: {error}"))
}

fn apply(machine: &mut BrokerMachine, input: BrokerInput) {
    let transition = machine.apply(input);
    assert_eq!(transition.disposition(), BrokerDisposition::Applied);
}
