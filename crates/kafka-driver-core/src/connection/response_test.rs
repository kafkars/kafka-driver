//! Scenarios proving strict FIFO correlation ownership and response completion.

use crate::Delivery;

use super::scenario_support_test::{
    EPOCH, TRANSPORT, apply, call, correlation, mark_submitted, ready_machine, submit, timer,
};
use super::{
    CallFailure, CloseReason, ConnectionEffect, ConnectionInput, ConnectionPhase,
    TransitionDisposition,
};

#[test]
fn matching_response_completes_only_the_fifo_front() {
    let mut machine = ready_machine();
    let first_correlation = correlation(&submit(&mut machine, 1));
    submit(&mut machine, 2);
    mark_submitted(&mut machine, 1);
    mark_submitted(&mut machine, 2);

    let response = apply(
        &mut machine,
        ConnectionInput::ResponseReceived {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            correlation_id: first_correlation,
        },
    );

    assert_eq!(
        response.effects(),
        &[
            ConnectionEffect::CancelDeadline { timer_id: timer(1) },
            ConnectionEffect::CompleteResponse {
                call_id: call(1),
                correlation_id: first_correlation,
            },
        ]
    );
    assert_eq!(machine.pending_count(), 1);
    assert_eq!(
        machine.pending_front().map(super::PendingCall::call_id),
        Some(call(2))
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
}

#[test]
fn out_of_order_response_faults_and_fails_the_whole_epoch() {
    let mut machine = ready_machine();
    let first_correlation = correlation(&submit(&mut machine, 1));
    let second_correlation = correlation(&submit(&mut machine, 2));
    mark_submitted(&mut machine, 1);
    mark_submitted(&mut machine, 2);
    let reason = CloseReason::CorrelationMismatch {
        expected: first_correlation,
        received: second_correlation,
    };

    let response = apply(
        &mut machine,
        ConnectionInput::ResponseReceived {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            correlation_id: second_correlation,
        },
    );

    assert_eq!(
        response.record().disposition(),
        TransitionDisposition::Fault
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
    assert_eq!(machine.pending_count(), 0);
    assert_eq!(
        response.effects(),
        &[
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason,
            },
            ConnectionEffect::CancelDeadline { timer_id: timer(1) },
            ConnectionEffect::FailCall {
                call_id: call(1),
                failure: CallFailure::CorrelationMismatch {
                    expected: first_correlation,
                    received: second_correlation,
                },
                delivery: Delivery::PossiblySent,
            },
            ConnectionEffect::CancelDeadline { timer_id: timer(2) },
            ConnectionEffect::FailCall {
                call_id: call(2),
                failure: CallFailure::ConnectionClosed { reason },
                delivery: Delivery::PossiblySent,
            },
        ]
    );
}

#[test]
fn response_before_write_acceptance_is_a_protocol_fault() {
    let mut machine = ready_machine();
    let correlation_id = correlation(&submit(&mut machine, 1));

    let response = apply(
        &mut machine,
        ConnectionInput::ResponseReceived {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            correlation_id,
        },
    );

    assert_eq!(
        response.record().disposition(),
        TransitionDisposition::Fault
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
    assert_eq!(
        response.effects(),
        &[
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason: CloseReason::UnexpectedResponse,
            },
            ConnectionEffect::CancelDeadline { timer_id: timer(1) },
            ConnectionEffect::FailCall {
                call_id: call(1),
                failure: CallFailure::ConnectionClosed {
                    reason: CloseReason::UnexpectedResponse,
                },
                delivery: Delivery::NotSent,
            },
        ]
    );
}

#[test]
fn unsolicited_response_closes_an_idle_ready_epoch() {
    let mut machine = ready_machine();

    let response = apply(
        &mut machine,
        ConnectionInput::ResponseReceived {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            correlation_id: super::CorrelationId::from_raw(17),
        },
    );

    assert_eq!(
        response.record().disposition(),
        TransitionDisposition::Fault
    );
    assert_eq!(
        response.effects(),
        &[ConnectionEffect::CloseTransport {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            reason: CloseReason::UnexpectedResponse,
        }]
    );
}
