//! Scenarios for jittered bootstrap retry waiting, escalation, and reset.

use std::time::Duration;

use crate::{BackoffPolicy, JitterSample, Moment, RetryOrdinal};

use super::{
    BootstrapRetryEffect, BootstrapRetryInput, BootstrapRetryMachine, BootstrapRetryState,
};

#[test]
fn exhausted_pass_waits_for_a_bounded_jittered_deadline() {
    let mut retry = machine();

    let transition = retry
        .apply(BootstrapRetryInput::Exhausted {
            now: moment(1_000),
            jitter: JitterSample::from_raw(49),
        })
        .unwrap_or_else(|error| panic!("schedule retry: {error}"));

    assert_eq!(
        transition.effects(),
        [BootstrapRetryEffect::WaitUntil { at: moment(1_099) }]
    );
    assert_eq!(retry.deadline(), Some(moment(1_099)));
}

#[test]
fn early_wake_retains_deadline_and_due_wake_restarts_once() {
    let mut retry = waiting_machine();

    let early = retry
        .apply(BootstrapRetryInput::Elapsed { now: moment(98) })
        .unwrap_or_else(|error| panic!("observe early wake: {error}"));
    let due = retry
        .apply(BootstrapRetryInput::Elapsed { now: moment(99) })
        .unwrap_or_else(|error| panic!("observe due wake: {error}"));
    let duplicate = retry
        .apply(BootstrapRetryInput::Elapsed { now: moment(100) })
        .unwrap_or_else(|error| panic!("observe duplicate wake: {error}"));

    assert_eq!(
        early.effects(),
        [BootstrapRetryEffect::WaitUntil { at: moment(99) }]
    );
    assert_eq!(due.effects(), [BootstrapRetryEffect::Restart]);
    assert!(duplicate.effects().is_empty());
    assert_eq!(
        retry.state(),
        BootstrapRetryState::Ready { retry: ordinal(2) }
    );
}

#[test]
fn success_resets_backoff_to_the_first_retry_ordinal() {
    let mut retry = waiting_machine();
    let _ = retry
        .apply(BootstrapRetryInput::Elapsed { now: moment(99) })
        .unwrap_or_else(|error| panic!("complete wait: {error}"));
    let _ = retry
        .apply(BootstrapRetryInput::Succeeded)
        .unwrap_or_else(|error| panic!("observe success: {error}"));

    assert_eq!(
        retry.state(),
        BootstrapRetryState::Ready { retry: ordinal(1) }
    );
}

fn waiting_machine() -> BootstrapRetryMachine {
    let mut retry = machine();
    let _ = retry
        .apply(BootstrapRetryInput::Exhausted {
            now: Moment::ORIGIN,
            jitter: JitterSample::from_raw(49),
        })
        .unwrap_or_else(|error| panic!("schedule retry: {error}"));
    retry
}

fn machine() -> BootstrapRetryMachine {
    let policy = BackoffPolicy::try_new(Duration::from_nanos(100), Duration::from_nanos(400))
        .unwrap_or_else(|error| panic!("valid policy: {error}"));
    BootstrapRetryMachine::new(policy)
}

fn ordinal(raw: u32) -> RetryOrdinal {
    RetryOrdinal::from_raw(raw).unwrap_or_else(|| panic!("test ordinal must be nonzero"))
}

const fn moment(raw: u64) -> Moment {
    Moment::from_nanos(raw)
}
