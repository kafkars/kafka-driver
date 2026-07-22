//! Scenarios proving bounded admission and monotonic write-delivery state.

use std::num::NonZeroUsize;

use crate::{Delivery, Moment};

use super::scenario_support_test::{
    EPOCH, STALE_EPOCH, TRANSPORT, apply, call, mark_submitted, ready_machine, ready_machine_with,
    submit, timer, write_effect,
};
use super::{
    CallFailure, ConnectionEffect, ConnectionInput, ConnectionLimits, ConnectionMachineError,
    IdentityKind, PendingCall, PendingPhase, TransitionDisposition,
};

#[test]
fn admission_schedules_deadline_before_ordered_write() {
    let mut machine = ready_machine();

    let transition = submit(&mut machine, 1);
    let Some(pending) = machine.pending_front() else {
        panic!("admitted call must own the queue front");
    };

    assert_eq!(pending.call_id(), call(1));
    assert_eq!(pending.write_effect(), write_effect(1));
    assert_eq!(pending.deadline_timer(), timer(1));
    assert_eq!(pending.deadline(), Moment::from_nanos(1_001));
    assert_eq!(pending.phase(), PendingPhase::AwaitingWrite);
    assert_eq!(pending.delivery(), Delivery::NotSent);
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::ScheduleDeadline {
                epoch: EPOCH,
                call_id: call(1),
                timer_id: timer(1),
                at: Moment::from_nanos(1_001),
            },
            ConnectionEffect::WriteRequest {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                call_id: call(1),
                correlation_id: pending.correlation_id(),
                effect_id: write_effect(1),
            },
        ]
    );
}

#[test]
fn elapsed_deadline_is_rejected_before_admission() {
    let mut machine = ready_machine();

    let transition = apply(
        &mut machine,
        ConnectionInput::Submit {
            call_id: call(1),
            write_effect: write_effect(1),
            deadline_timer: timer(1),
            now: Moment::from_nanos(10),
            deadline: Moment::from_nanos(10),
        },
    );

    assert_eq!(machine.pending_count(), 0);
    assert_eq!(
        transition.effects(),
        &[ConnectionEffect::FailCall {
            call_id: call(1),
            failure: CallFailure::DeadlineExceeded,
            delivery: Delivery::NotSent,
        }]
    );
    assert_eq!(
        transition.record().disposition(),
        TransitionDisposition::Rejected
    );
}

#[test]
fn admission_never_exceeds_configured_capacity() {
    let limits = ConnectionLimits::new(NonZeroUsize::MIN, 8);
    let mut machine = ready_machine_with(limits);
    submit(&mut machine, 1);

    let rejected = submit(&mut machine, 2);

    assert_eq!(machine.pending_count(), 1);
    assert_eq!(
        rejected.effects(),
        &[ConnectionEffect::FailCall {
            call_id: call(2),
            failure: CallFailure::CapacityReached { limit: 1 },
            delivery: Delivery::NotSent,
        }]
    );
}

#[test]
fn pending_external_identities_cannot_be_reused() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);

    let duplicate_call = machine.apply(ConnectionInput::Submit {
        call_id: call(1),
        write_effect: write_effect(2),
        deadline_timer: timer(2),
        now: Moment::from_nanos(10),
        deadline: Moment::from_nanos(2_000),
    });
    let duplicate_effect = machine.apply(ConnectionInput::Submit {
        call_id: call(2),
        write_effect: write_effect(1),
        deadline_timer: timer(2),
        now: Moment::from_nanos(10),
        deadline: Moment::from_nanos(2_000),
    });
    let duplicate_timer = machine.apply(ConnectionInput::Submit {
        call_id: call(2),
        write_effect: write_effect(2),
        deadline_timer: timer(1),
        now: Moment::from_nanos(10),
        deadline: Moment::from_nanos(2_000),
    });

    assert_eq!(
        duplicate_call,
        Err(ConnectionMachineError::IdentityInUse(IdentityKind::Call))
    );
    assert_eq!(
        duplicate_effect,
        Err(ConnectionMachineError::IdentityInUse(
            IdentityKind::WriteEffect
        ))
    );
    assert_eq!(
        duplicate_timer,
        Err(ConnectionMachineError::IdentityInUse(
            IdentityKind::DeadlineTimer
        ))
    );
    assert_eq!(machine.pending_count(), 1);
}

#[test]
fn write_acceptance_makes_delivery_possible_without_regression() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);

    let stale = apply(
        &mut machine,
        ConnectionInput::WriteSubmitted {
            epoch: STALE_EPOCH,
            transport_id: TRANSPORT,
            effect_id: write_effect(1),
        },
    );
    assert_eq!(
        stale.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(
        machine.pending_front().map(PendingCall::delivery),
        Some(Delivery::NotSent)
    );

    mark_submitted(&mut machine, 1);
    let duplicate = apply(
        &mut machine,
        ConnectionInput::WriteSubmitted {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: write_effect(1),
        },
    );

    assert_eq!(
        duplicate.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(
        machine.pending_front().map(PendingCall::phase),
        Some(PendingPhase::AwaitingResponse)
    );
    assert_eq!(
        machine.pending_front().map(PendingCall::delivery),
        Some(Delivery::PossiblySent)
    );
}
