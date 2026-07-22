//! Scenarios for negotiation identity, deadline, capability, and failure policy.

use std::num::NonZeroUsize;

use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{NegotiatedApi, NegotiatedCapabilities};

use super::scenario_support_test::{
    EPOCH, NEGOTIATION_EFFECT, NEGOTIATION_TIMER, OPEN_DEADLINE, OPEN_EFFECT, OPEN_TIMER,
    STALE_EPOCH, TRANSPORT, apply, capabilities, transport_opened,
};
use super::{
    CloseReason, ConnectionEffect, ConnectionInput, ConnectionLimits, ConnectionMachine,
    ConnectionPhase, ConnectionState, CorrelationId, NegotiationFailure, TransitionDisposition,
};

#[test]
fn matching_open_starts_a_deadline_owned_api_versions_exchange() {
    // Given
    let mut machine = opening_machine(ConnectionLimits::default());

    // When
    let transition = apply(
        &mut machine,
        transport_opened(EPOCH, OPEN_EFFECT, TRANSPORT),
    );

    // Then
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::CancelDeadline {
                timer_id: OPEN_TIMER,
            },
            ConnectionEffect::ScheduleNegotiationDeadline {
                epoch: EPOCH,
                timer_id: NEGOTIATION_TIMER,
                at: crate::Moment::from_nanos(100),
            },
            ConnectionEffect::NegotiateApiVersions {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                effect_id: NEGOTIATION_EFFECT,
                correlation_id: CorrelationId::from_raw(0),
            },
        ]
    );
    assert_eq!(transition.record().to(), ConnectionPhase::Negotiating);
    assert!(matches!(
        machine.state(),
        ConnectionState::Negotiating {
            effect_id: NEGOTIATION_EFFECT,
            deadline_timer: NEGOTIATION_TIMER,
            ..
        }
    ));
}

#[test]
fn matching_capabilities_make_the_epoch_ready_and_cancel_its_deadline() {
    // Given
    let mut machine = negotiating_machine(ConnectionLimits::default());
    let capabilities = capabilities();

    // When
    let transition = apply(
        &mut machine,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities,
        },
    );

    // Then
    assert_eq!(
        transition.effects(),
        &[ConnectionEffect::CancelDeadline {
            timer_id: NEGOTIATION_TIMER,
        }]
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
    assert_eq!(
        machine.negotiated_version(ApiKey::new(18)),
        Some(ApiVersion::new(4))
    );
}

#[test]
fn stale_negotiation_result_cannot_make_the_current_epoch_ready() {
    // Given
    let mut machine = negotiating_machine(ConnectionLimits::default());

    // When
    let transition = apply(
        &mut machine,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: STALE_EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities: capabilities(),
        },
    );

    // Then
    assert_eq!(
        transition.record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Negotiating);
}

#[test]
fn explicit_negotiation_failure_closes_transport_then_cancels_deadline() {
    // Given
    let mut machine = negotiating_machine(ConnectionLimits::default());

    // When
    let transition = apply(
        &mut machine,
        ConnectionInput::ApiVersionsFailed {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            failure: NegotiationFailure::Malformed,
        },
    );

    // Then
    let reason = CloseReason::NegotiationFailed(NegotiationFailure::Malformed);
    assert_eq!(
        transition.effects(),
        &[
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason,
            },
            ConnectionEffect::CancelDeadline {
                timer_id: NEGOTIATION_TIMER,
            },
        ]
    );
    assert!(matches!(
        machine.state(),
        ConnectionState::Closing {
            reason: observed,
            ..
        } if observed == reason
    ));
}

#[test]
fn early_negotiation_deadline_is_rescheduled_and_elapsed_deadline_closes() {
    // Given
    let mut machine = negotiating_machine(ConnectionLimits::default());

    // When
    let early = apply(
        &mut machine,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: NEGOTIATION_TIMER,
            now: crate::Moment::from_nanos(99),
        },
    );
    let elapsed = apply(
        &mut machine,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: NEGOTIATION_TIMER,
            now: crate::Moment::from_nanos(100),
        },
    );

    // Then
    assert_eq!(
        early.effects(),
        &[ConnectionEffect::ScheduleNegotiationDeadline {
            epoch: EPOCH,
            timer_id: NEGOTIATION_TIMER,
            at: crate::Moment::from_nanos(100),
        }]
    );
    let reason = CloseReason::NegotiationFailed(NegotiationFailure::Timeout);
    assert_eq!(
        elapsed.effects(),
        &[ConnectionEffect::CloseTransport {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            reason,
        }]
    );
}

#[test]
fn capabilities_beyond_machine_capacity_close_the_epoch() {
    // Given
    let limits = ConnectionLimits::default().with_max_capabilities(NonZeroUsize::MIN);
    let mut machine = negotiating_machine(limits);
    let capabilities = NegotiatedCapabilities::try_from_iter(
        [
            NegotiatedApi::new(ApiKey::new(1), ApiVersion::new(1)),
            NegotiatedApi::new(ApiKey::new(18), ApiVersion::new(4)),
        ],
        nonzero(2),
    )
    .unwrap_or_else(|error| panic!("test capabilities must be canonical: {error}"));

    // When
    let transition = apply(
        &mut machine,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities,
        },
    );

    // Then
    assert!(matches!(
        transition.effects(),
        [
            ConnectionEffect::CloseTransport {
                reason: CloseReason::NegotiationFailed(NegotiationFailure::Capacity),
                ..
            },
            ConnectionEffect::CancelDeadline { .. }
        ]
    ));
}

fn opening_machine(limits: ConnectionLimits) -> ConnectionMachine {
    let mut machine = ConnectionMachine::new(EPOCH, limits);
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            deadline_timer: OPEN_TIMER,
            deadline: OPEN_DEADLINE,
        },
    );
    machine
}

fn negotiating_machine(limits: ConnectionLimits) -> ConnectionMachine {
    let mut machine = opening_machine(limits);
    apply(
        &mut machine,
        transport_opened(EPOCH, OPEN_EFFECT, TRANSPORT),
    );
    machine
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
