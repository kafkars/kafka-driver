//! Given/When/Then scenarios for bounded secret-free SASL policy.

use std::num::NonZeroU8;

use kafka_wire_core::ApiVersion;

use crate::{ConnectionEpoch, EffectId, Moment, TimerId, TransportId};

use super::{
    AuthenticationAttempt, AuthenticationDisposition, AuthenticationEffect, AuthenticationFailure,
    AuthenticationInput, AuthenticationLimits, AuthenticationMachine, AuthenticationPhase,
    AuthenticationRound, AuthenticationState, ExchangeOutcome, SaslMechanism, SaslProtocol,
};

#[test]
fn given_a_sasl_protocol_when_started_then_deadline_precedes_handshake() {
    // Given
    let mut machine = machine(AuthenticationLimits::default());

    // When
    let transition = machine.apply(start());

    // Then
    assert_eq!(transition.disposition(), AuthenticationDisposition::Applied);
    assert_eq!(
        transition.effects(),
        [
            AuthenticationEffect::ScheduleDeadline {
                epoch: epoch(),
                timer_id: timer(),
                at: deadline(),
            },
            AuthenticationEffect::SendHandshake {
                epoch: epoch(),
                transport_id: transport(),
                effect_id: effect(),
                mechanism: SaslMechanism::ScramSha256,
                version: ApiVersion::new(1),
            },
        ]
    );
    assert_eq!(machine.state().phase(), AuthenticationPhase::Handshaking);
}

#[test]
fn given_a_matching_handshake_when_scram_continues_then_rounds_are_explicit() {
    // Given
    let mut machine = started(AuthenticationLimits::default());

    // When
    let handshake = machine.apply(AuthenticationInput::HandshakeAccepted {
        epoch: epoch(),
        transport_id: transport(),
        effect_id: effect(),
    });
    let challenge = machine.apply(exchange(round(1), ExchangeOutcome::Continue));

    // Then
    assert_eq!(
        handshake.effects(),
        [AuthenticationEffect::SendExchange {
            epoch: epoch(),
            transport_id: transport(),
            effect_id: effect(),
            round: round(1),
            version: ApiVersion::new(2),
        }]
    );
    assert_eq!(
        challenge.effects(),
        [AuthenticationEffect::SendExchange {
            epoch: epoch(),
            transport_id: transport(),
            effect_id: effect(),
            round: round(2),
            version: ApiVersion::new(2),
        }]
    );
    assert!(matches!(
        machine.state(),
        AuthenticationState::Exchanging { round: observed, .. } if observed == round(2)
    ));
}

#[test]
fn given_the_final_exchange_when_succeeded_then_deadline_is_cancelled_first() {
    // Given
    let mut machine = exchanging(AuthenticationLimits::default());

    // When
    let transition = machine.apply(exchange(round(1), ExchangeOutcome::Succeeded));

    // Then
    assert_eq!(
        transition.effects(),
        [
            AuthenticationEffect::CancelDeadline { timer_id: timer() },
            AuthenticationEffect::Succeeded,
        ]
    );
    assert_eq!(machine.state(), AuthenticationState::Succeeded);
}

#[test]
fn given_an_exhausted_round_bound_when_challenge_continues_then_failure_is_terminal() {
    // Given
    let mut machine = exchanging(AuthenticationLimits::new(nonzero(1)));

    // When
    let transition = machine.apply(exchange(round(1), ExchangeOutcome::Continue));

    // Then
    assert_eq!(
        transition.effects(),
        [
            AuthenticationEffect::CancelDeadline { timer_id: timer() },
            AuthenticationEffect::Failed {
                failure: AuthenticationFailure::TooManyRounds,
            },
        ]
    );
    assert_eq!(
        machine.state(),
        AuthenticationState::Failed {
            failure: AuthenticationFailure::TooManyRounds,
        }
    );
}

#[test]
fn given_stale_epoch_effect_or_round_when_reported_then_owned_stage_does_not_change() {
    // Given
    let mut machine = exchanging(AuthenticationLimits::default());
    let expected = machine.state();

    // When
    let stale_epoch = machine.apply(AuthenticationInput::ExchangeCompleted {
        epoch: ConnectionEpoch::from_raw(2),
        transport_id: transport(),
        effect_id: effect(),
        round: round(1),
        outcome: ExchangeOutcome::Succeeded,
    });
    let stale_round = machine.apply(exchange(round(2), ExchangeOutcome::Succeeded));

    // Then
    assert_eq!(
        stale_epoch.disposition(),
        AuthenticationDisposition::IgnoredStale
    );
    assert_eq!(
        stale_round.disposition(),
        AuthenticationDisposition::IgnoredStale
    );
    assert!(stale_epoch.effects().is_empty());
    assert!(stale_round.effects().is_empty());
    assert_eq!(machine.state(), expected);
}

#[test]
fn given_an_early_deadline_when_reported_then_the_owned_deadline_is_rescheduled() {
    // Given
    let mut machine = started(AuthenticationLimits::default());

    // When
    let transition = machine.apply(AuthenticationInput::DeadlineElapsed {
        epoch: epoch(),
        timer_id: timer(),
        now: Moment::from_nanos(9),
    });

    // Then
    assert_eq!(
        transition.effects(),
        [AuthenticationEffect::ScheduleDeadline {
            epoch: epoch(),
            timer_id: timer(),
            at: deadline(),
        }]
    );
    assert_eq!(machine.state().phase(), AuthenticationPhase::Handshaking);
}

#[test]
fn given_an_elapsed_deadline_when_reported_then_only_sanitized_timeout_remains() {
    // Given
    let mut machine = exchanging(AuthenticationLimits::default());

    // When
    let transition = machine.apply(AuthenticationInput::DeadlineElapsed {
        epoch: epoch(),
        timer_id: timer(),
        now: deadline(),
    });

    // Then
    assert_eq!(
        transition.effects(),
        [
            AuthenticationEffect::CancelDeadline { timer_id: timer() },
            AuthenticationEffect::Failed {
                failure: AuthenticationFailure::Timeout,
            },
        ]
    );
    assert_eq!(
        format!("{:?}", machine.state()),
        "Failed { failure: Timeout }"
    );
}

fn machine(limits: AuthenticationLimits) -> AuthenticationMachine {
    AuthenticationMachine::new(
        epoch(),
        transport(),
        SaslProtocol::new(
            SaslMechanism::ScramSha256,
            ApiVersion::new(1),
            ApiVersion::new(2),
        ),
        limits,
    )
}

fn started(limits: AuthenticationLimits) -> AuthenticationMachine {
    let mut machine = machine(limits);
    let _ = machine.apply(start());
    machine
}

fn exchanging(limits: AuthenticationLimits) -> AuthenticationMachine {
    let mut machine = started(limits);
    let _ = machine.apply(AuthenticationInput::HandshakeAccepted {
        epoch: epoch(),
        transport_id: transport(),
        effect_id: effect(),
    });
    machine
}

fn start() -> AuthenticationInput {
    AuthenticationInput::Start {
        attempt: AuthenticationAttempt::new(effect(), timer(), Moment::from_nanos(0), deadline()),
    }
}

fn exchange(round: AuthenticationRound, outcome: ExchangeOutcome) -> AuthenticationInput {
    AuthenticationInput::ExchangeCompleted {
        epoch: epoch(),
        transport_id: transport(),
        effect_id: effect(),
        round,
        outcome,
    }
}

const fn epoch() -> ConnectionEpoch {
    ConnectionEpoch::from_raw(1)
}

const fn transport() -> TransportId {
    TransportId::from_raw(3)
}

const fn effect() -> EffectId {
    EffectId::from_raw(5)
}

const fn timer() -> TimerId {
    TimerId::from_raw(7)
}

const fn deadline() -> Moment {
    Moment::from_nanos(10)
}

fn round(value: u8) -> AuthenticationRound {
    AuthenticationRound::new(nonzero(value))
}

fn nonzero(value: u8) -> NonZeroU8 {
    NonZeroU8::new(value).unwrap_or_else(|| panic!("test value must be nonzero"))
}
