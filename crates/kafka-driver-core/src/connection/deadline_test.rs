//! Scenarios proving deadline identity, rescheduling, and epoch failure policy.

use crate::{Delivery, Moment, TimerId};

use super::scenario_support_test::{
    EPOCH, STALE_EPOCH, TRANSPORT, apply, call, mark_submitted, ready_machine, submit, timer,
};
use super::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput, ConnectionPhase,
    TransitionDisposition,
};

#[test]
fn early_deadline_notification_is_rescheduled_at_the_owned_deadline() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);

    let transition = apply(
        &mut machine,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: timer(1),
            now: Moment::from_nanos(999),
        },
    );

    assert_eq!(machine.pending_count(), 1);
    assert_eq!(
        transition.effects(),
        &[ConnectionEffect::ScheduleDeadline {
            epoch: EPOCH,
            call_id: call(1),
            timer_id: timer(1),
            at: Moment::from_nanos(1_001),
        }]
    );
}

#[test]
fn stale_epoch_and_unknown_timer_cannot_expire_a_call() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);

    let stale_epoch = apply(
        &mut machine,
        ConnectionInput::DeadlineElapsed {
            epoch: STALE_EPOCH,
            timer_id: timer(1),
            now: Moment::from_nanos(2_000),
        },
    );
    let unknown_timer = apply(
        &mut machine,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: TimerId::from_raw(99),
            now: Moment::from_nanos(2_000),
        },
    );

    assert_eq!(machine.pending_count(), 1);
    assert_eq!(
        stale_epoch.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(
        unknown_timer.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
}

#[test]
fn any_expired_pending_call_closes_the_shared_epoch() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    submit(&mut machine, 2);
    mark_submitted(&mut machine, 1);
    let reason = CloseReason::DeadlineExceeded { call_id: call(2) };

    let transition = apply(
        &mut machine,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: timer(2),
            now: Moment::from_nanos(2_000),
        },
    );

    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
    assert_eq!(machine.pending_count(), 0);
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason,
            },
            ConnectionEffect::CancelDeadline { timer_id: timer(1) },
            ConnectionEffect::FailCall {
                call_id: call(1),
                failure: CallFailure::ConnectionClosed { reason },
                delivery: Delivery::PossiblySent,
            },
            ConnectionEffect::CancelDeadline { timer_id: timer(2) },
            ConnectionEffect::FailCall {
                call_id: call(2),
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::NotSent,
            },
        ]
    );
}
