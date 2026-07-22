//! Deterministic sequences proving cross-transition connection invariants.

use std::num::NonZeroUsize;

use crate::{Delivery, Moment, TransportId};

use super::scenario_support_test::{
    EPOCH, STALE_EPOCH, TRANSPORT, apply, call, correlation, mark_submitted, ready_machine,
    ready_machine_with, submit, timer, write_effect,
};
use super::{
    CallFailure, ConnectionEffect, ConnectionInput, ConnectionLimits, ConnectionMachineError,
    ConnectionPhase, IdentityKind, PendingCall, TransitionDisposition,
};

#[test]
fn long_pipeline_never_reuses_a_pending_correlation() {
    let mut machine = ready_machine();
    let mut correlations = Vec::new();

    for raw in 1..=128 {
        let correlation_id = correlation(&submit(&mut machine, raw));
        assert!(!correlations.contains(&correlation_id));
        correlations.push(correlation_id);
    }

    assert_eq!(machine.pending_count(), correlations.len());
}

#[test]
fn bounded_trace_retains_only_the_newest_sanitized_records() {
    let limits = ConnectionLimits::new(NonZeroUsize::MIN, 3);
    let mut machine = ready_machine_with(limits);
    apply(&mut machine, ConnectionInput::BeginDrain);
    apply(&mut machine, ConnectionInput::BeginDrain);
    apply(&mut machine, ConnectionInput::BeginDrain);
    apply(&mut machine, ConnectionInput::BeginDrain);

    let sequences: Vec<_> = machine
        .recent_transitions()
        .map(|record| record.sequence().get())
        .collect();

    assert_eq!(sequences, vec![3, 4, 5]);
    assert!(
        machine
            .recent_transitions()
            .all(|record| record.effect_count() == 0)
    );
}

#[test]
fn invalid_identity_input_mutates_neither_state_nor_sequence() {
    let mut machine = ready_machine();
    submit(&mut machine, 1);
    let before = machine.state();

    let error = machine.apply(ConnectionInput::Submit {
        call_id: call(1),
        write_effect: write_effect(2),
        deadline_timer: timer(2),
        now: Moment::from_nanos(10),
        deadline: Moment::from_nanos(2_000),
    });
    let next = apply(
        &mut machine,
        ConnectionInput::WriteSubmitted {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: write_effect(99),
        },
    );

    assert_eq!(
        error,
        Err(ConnectionMachineError::IdentityInUse(IdentityKind::Call))
    );
    assert_eq!(machine.state(), before);
    assert_eq!(machine.pending_count(), 1);
    assert_eq!(next.record().sequence().get(), 3);
}

#[test]
fn stale_external_sequence_cannot_change_owned_work() {
    let mut machine = ready_machine();
    let correlation_id = correlation(&submit(&mut machine, 1));
    mark_submitted(&mut machine, 1);
    let before = machine.state();

    let stale_inputs = [
        ConnectionInput::ResponseReceived {
            epoch: STALE_EPOCH,
            transport_id: TRANSPORT,
            correlation_id,
        },
        ConnectionInput::WriteSubmitted {
            epoch: EPOCH,
            transport_id: TransportId::from_raw(999),
            effect_id: write_effect(1),
        },
        ConnectionInput::DeadlineElapsed {
            epoch: STALE_EPOCH,
            timer_id: timer(1),
            now: Moment::from_nanos(9_999),
        },
    ];
    for input in stale_inputs {
        let transition = apply(&mut machine, input);
        assert_eq!(
            transition.record().disposition(),
            TransitionDisposition::IgnoredStale
        );
        assert!(transition.effects().is_empty());
    }

    assert_eq!(machine.state(), before);
    assert_eq!(machine.pending_count(), 1);
    assert_eq!(
        machine.pending_front().map(PendingCall::delivery),
        Some(Delivery::PossiblySent)
    );
}

#[test]
fn repeated_overload_is_bounded_and_explicit() {
    let Some(max_in_flight) = NonZeroUsize::new(2) else {
        panic!("two is nonzero");
    };
    let limits = ConnectionLimits::new(max_in_flight, 8);
    let mut machine = ready_machine_with(limits);
    submit(&mut machine, 1);
    submit(&mut machine, 2);

    for raw in 3..=32 {
        let transition = submit(&mut machine, raw);
        assert_eq!(
            transition.effects(),
            &[ConnectionEffect::FailCall {
                call_id: call(raw),
                failure: CallFailure::CapacityReached { limit: 2 },
                delivery: Delivery::NotSent,
            }]
        );
        assert_eq!(machine.pending_count(), 2);
        assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
    }
}
