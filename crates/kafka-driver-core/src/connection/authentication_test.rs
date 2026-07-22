//! Given/When/Then scenarios for connection-owned SASL child composition.

use std::num::{NonZeroU8, NonZeroUsize};

use kafka_wire_core::{ApiKey, ApiVersion};

use crate::{
    AuthenticationAttempt, AuthenticationEffect, AuthenticationFailure, AuthenticationInput,
    AuthenticationLimits, AuthenticationPolicy, AuthenticationRound, AuthenticationState,
    ExchangeOutcome, Moment, NegotiatedApi, NegotiatedCapabilities, SaslMechanism,
};

use super::scenario_support_test::{
    EPOCH, NEGOTIATION_EFFECT, NEGOTIATION_TIMER, OPEN_DEADLINE, OPEN_EFFECT, OPEN_TIMER,
    TRANSPORT, apply, transport_opened,
};
use super::{
    CloseReason, ConnectionEffect, ConnectionInput, ConnectionLimits, ConnectionMachine,
    ConnectionPhase, ConnectionState, CorrelationId,
};

const AUTHENTICATION_EFFECT: crate::EffectId = crate::EffectId::from_raw(20);
pub(super) const AUTHENTICATION_TIMER: crate::TimerId = crate::TimerId::from_raw(21);
const HANDSHAKE_API: ApiKey = ApiKey::new(17);
const AUTHENTICATE_API: ApiKey = ApiKey::new(36);

#[test]
fn negotiated_authenticated_connection_starts_deadline_then_handshake() {
    // Given
    let mut machine = negotiating_authenticated_machine();

    // When
    let transition = apply(&mut machine, negotiated_with_authentication(capabilities()));

    // Then
    assert_eq!(
        transition.effects(),
        [
            ConnectionEffect::CancelDeadline {
                timer_id: NEGOTIATION_TIMER,
            },
            ConnectionEffect::Authentication {
                effect: AuthenticationEffect::ScheduleDeadline {
                    epoch: EPOCH,
                    timer_id: AUTHENTICATION_TIMER,
                    at: authentication_deadline(),
                },
            },
            ConnectionEffect::Authentication {
                effect: AuthenticationEffect::SendHandshake {
                    epoch: EPOCH,
                    transport_id: TRANSPORT,
                    effect_id: AUTHENTICATION_EFFECT,
                    correlation_id: CorrelationId::from_raw(1),
                    mechanism: SaslMechanism::Plain,
                    version: ApiVersion::new(1),
                },
            },
        ]
    );
    assert!(matches!(
        machine.state(),
        ConnectionState::Authenticating {
            authentication: AuthenticationState::Handshaking { .. },
            capabilities: 3,
            ..
        }
    ));
}

#[test]
fn successful_exchange_makes_capabilities_ready_only_after_authentication() {
    // Given
    let mut machine = authenticating_machine();
    let handshake = AuthenticationInput::HandshakeAccepted {
        epoch: EPOCH,
        transport_id: TRANSPORT,
        effect_id: AUTHENTICATION_EFFECT,
    };

    // When
    let challenge = apply(
        &mut machine,
        ConnectionInput::Authentication { input: handshake },
    );
    let success = apply(
        &mut machine,
        ConnectionInput::Authentication {
            input: AuthenticationInput::ExchangeCompleted {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                effect_id: AUTHENTICATION_EFFECT,
                round: round(1),
                outcome: ExchangeOutcome::Succeeded,
            },
        },
    );

    // Then
    assert_eq!(
        challenge.effects(),
        [ConnectionEffect::Authentication {
            effect: AuthenticationEffect::SendExchange {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                effect_id: AUTHENTICATION_EFFECT,
                round: round(1),
                correlation_id: CorrelationId::from_raw(2),
                version: ApiVersion::new(2),
            },
        }]
    );
    assert_eq!(
        success.effects(),
        [ConnectionEffect::Authentication {
            effect: AuthenticationEffect::CancelDeadline {
                timer_id: AUTHENTICATION_TIMER,
            },
        }]
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
    assert_eq!(
        machine.negotiated_version(AUTHENTICATE_API),
        Some(ApiVersion::new(2))
    );
}

#[test]
fn rejected_authentication_closes_before_cancelling_its_deadline() {
    // Given
    let mut machine = exchanging_machine();

    // When
    let transition = apply(
        &mut machine,
        ConnectionInput::Authentication {
            input: AuthenticationInput::ExchangeCompleted {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                effect_id: AUTHENTICATION_EFFECT,
                round: round(1),
                outcome: ExchangeOutcome::Failed(AuthenticationFailure::Rejected),
            },
        },
    );

    // Then
    let reason = CloseReason::AuthenticationFailed(AuthenticationFailure::Rejected);
    assert_eq!(
        transition.effects(),
        [
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason,
            },
            ConnectionEffect::Authentication {
                effect: AuthenticationEffect::CancelDeadline {
                    timer_id: AUTHENTICATION_TIMER,
                },
            },
        ]
    );
    assert!(matches!(
        machine.state(),
        ConnectionState::Closing { reason: observed, .. } if observed == reason
    ));
}

#[test]
fn missing_sasl_capability_closes_without_exposing_a_ready_window() {
    // Given
    let mut machine = negotiating_authenticated_machine();
    let missing_authenticate = NegotiatedCapabilities::try_from_iter(
        [NegotiatedApi::new(HANDSHAKE_API, ApiVersion::new(1))],
        NonZeroUsize::MIN,
    )
    .unwrap_or_else(|error| panic!("test capabilities must be valid: {error}"));

    // When
    let transition = apply(
        &mut machine,
        negotiated_with_authentication(missing_authenticate),
    );

    // Then
    let reason = CloseReason::AuthenticationFailed(AuthenticationFailure::Protocol);
    assert_eq!(
        transition.effects(),
        [
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
    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
}

fn negotiating_authenticated_machine() -> ConnectionMachine {
    let policy = AuthenticationPolicy::new(
        SaslMechanism::Plain,
        HANDSHAKE_API,
        AUTHENTICATE_API,
        AuthenticationLimits::default(),
    );
    let mut machine =
        ConnectionMachine::new_authenticated(EPOCH, ConnectionLimits::default(), policy);
    apply(
        &mut machine,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            deadline_timer: OPEN_TIMER,
            deadline: OPEN_DEADLINE,
        },
    );
    apply(
        &mut machine,
        transport_opened(EPOCH, OPEN_EFFECT, TRANSPORT),
    );
    machine
}

pub(super) fn authenticating_machine() -> ConnectionMachine {
    let mut machine = negotiating_authenticated_machine();
    apply(&mut machine, negotiated_with_authentication(capabilities()));
    machine
}

fn exchanging_machine() -> ConnectionMachine {
    let mut machine = authenticating_machine();
    apply(
        &mut machine,
        ConnectionInput::Authentication {
            input: AuthenticationInput::HandshakeAccepted {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                effect_id: AUTHENTICATION_EFFECT,
            },
        },
    );
    machine
}

fn capabilities() -> NegotiatedCapabilities {
    NegotiatedCapabilities::try_from_iter(
        [
            NegotiatedApi::new(HANDSHAKE_API, ApiVersion::new(1)),
            NegotiatedApi::new(ApiKey::new(18), ApiVersion::new(4)),
            NegotiatedApi::new(AUTHENTICATE_API, ApiVersion::new(2)),
        ],
        nonzero_usize(3),
    )
    .unwrap_or_else(|error| panic!("test capabilities must be canonical: {error}"))
}

fn negotiated_with_authentication(capabilities: NegotiatedCapabilities) -> ConnectionInput {
    ConnectionInput::ApiVersionsNegotiatedWithAuthentication {
        epoch: EPOCH,
        transport_id: TRANSPORT,
        effect_id: NEGOTIATION_EFFECT,
        capabilities,
        authentication: AuthenticationAttempt::new(
            AUTHENTICATION_EFFECT,
            AUTHENTICATION_TIMER,
            Moment::from_nanos(100),
            authentication_deadline(),
        ),
    }
}

pub(super) const fn authentication_deadline() -> Moment {
    Moment::from_nanos(200)
}

fn round(value: u8) -> AuthenticationRound {
    AuthenticationRound::new(
        NonZeroU8::new(value).unwrap_or_else(|| panic!("round must be nonzero")),
    )
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("capacity must be nonzero"))
}
