//! Scenarios for aborting one locally rejected call before writer acceptance.

use crate::Delivery;

use super::scenario_support_test::{
    EPOCH, STALE_EPOCH, TRANSPORT, apply, call, mark_submitted, ready_machine, submit, timer,
    write_effect,
};
use super::{
    CallFailure, ConnectionEffect, ConnectionInput, ConnectionPhase, PendingCall, PendingPhase,
    TransitionDisposition,
};

#[test]
fn aborting_a_later_unsent_call_preserves_the_ready_epoch_and_fifo_front() {
    // Given: A may have been sent while B is still awaiting local writer acceptance.
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    mark_submitted(&mut machine, 1);
    submit(&mut machine, 2);
    let first = machine
        .pending_front()
        .unwrap_or_else(|| panic!("A must remain the FIFO front"));

    // When: local preparation rejects only B.
    let transition = apply(
        &mut machine,
        ConnectionInput::AbortUnsentCall {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            call_id: call(2),
            effect_id: write_effect(2),
        },
    );

    // Then: B is NotSent, while A and the healthy epoch retain ownership.
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
    assert_eq!(machine.pending_count(), 1);
    assert_eq!(machine.pending_front(), Some(first));
    assert_eq!(
        machine.pending_front().map(PendingCall::phase),
        Some(PendingPhase::AwaitingResponse)
    );
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::CancelDeadline { timer_id: timer(2) },
            ConnectionEffect::FailCall {
                call_id: call(2),
                failure: CallFailure::LocallyRejected,
                delivery: Delivery::NotSent,
            },
        ]
    );
}

#[test]
fn stale_identity_or_writer_accepted_work_cannot_be_locally_aborted() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    mark_submitted(&mut machine, 1);

    for input in [
        ConnectionInput::AbortUnsentCall {
            epoch: STALE_EPOCH,
            transport_id: TRANSPORT,
            call_id: call(1),
            effect_id: write_effect(1),
        },
        ConnectionInput::AbortUnsentCall {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            call_id: call(1),
            effect_id: write_effect(1),
        },
    ] {
        let transition = apply(&mut machine, input);
        assert!(transition.effects().is_empty());
        assert_eq!(
            transition.record().disposition(),
            TransitionDisposition::IgnoredStale
        );
    }
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
    assert_eq!(machine.pending_count(), 1);
}
