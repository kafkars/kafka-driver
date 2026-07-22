//! Scenarios proving draining, write failure, and external transport closure.

use crate::Delivery;

use super::scenario_support_test::{
    EPOCH, TRANSPORT, apply, call, correlation, mark_submitted, ready_machine, submit, timer,
    write_effect,
};
use super::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput, ConnectionPhase,
    TransitionDisposition, TransportFailure,
};

#[test]
fn draining_rejects_new_work_and_closes_after_the_final_response() {
    let mut machine = ready_machine();
    let correlation_id = correlation(&submit(&mut machine, 1));
    mark_submitted(&mut machine, 1);

    let drain = apply(&mut machine, ConnectionInput::BeginDrain);
    let rejected = submit(&mut machine, 2);

    assert!(drain.effects().is_empty());
    assert_eq!(machine.state().phase(), ConnectionPhase::Draining);
    assert_eq!(
        rejected.effects(),
        &[ConnectionEffect::FailCall {
            call_id: call(2),
            failure: CallFailure::Draining,
            delivery: Delivery::NotSent,
        }]
    );
    assert_eq!(
        rejected.record().disposition(),
        TransitionDisposition::Rejected
    );

    let response = apply(
        &mut machine,
        ConnectionInput::ResponseReceived {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            correlation_id,
        },
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
    assert_eq!(
        response.effects(),
        &[
            ConnectionEffect::CancelDeadline { timer_id: timer(1) },
            ConnectionEffect::CompleteResponse {
                call_id: call(1),
                correlation_id,
            },
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason: CloseReason::Drained,
            },
        ]
    );
}

#[test]
fn write_failure_closes_the_epoch_and_preserves_delivery_certainty() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    submit(&mut machine, 2);
    mark_submitted(&mut machine, 1);
    let reason = CloseReason::TransportLost(TransportFailure::Reset);

    let transition = apply(
        &mut machine,
        ConnectionInput::WriteFailed {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: write_effect(2),
            failure: TransportFailure::Reset,
        },
    );

    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
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
                failure: CallFailure::ConnectionClosed { reason },
                delivery: Delivery::NotSent,
            },
        ]
    );
}

#[test]
fn observed_transport_close_is_terminal_without_a_duplicate_close_effect() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    mark_submitted(&mut machine, 1);
    let reason = CloseReason::TransportLost(TransportFailure::Security);

    let transition = apply(
        &mut machine,
        ConnectionInput::TransportClosed {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            failure: TransportFailure::Security,
        },
    );

    assert_eq!(machine.state().phase(), ConnectionPhase::Closed);
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::CancelDeadline { timer_id: timer(1) },
            ConnectionEffect::FailCall {
                call_id: call(1),
                failure: CallFailure::ConnectionClosed { reason },
                delivery: Delivery::PossiblySent,
            },
        ]
    );
}

#[test]
fn late_write_failure_after_acceptance_is_stale() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    mark_submitted(&mut machine, 1);

    let transition = apply(
        &mut machine,
        ConnectionInput::WriteFailed {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: write_effect(1),
            failure: TransportFailure::Reset,
        },
    );

    assert!(transition.effects().is_empty());
    assert_eq!(
        transition.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
    assert_eq!(machine.pending_count(), 1);
}
