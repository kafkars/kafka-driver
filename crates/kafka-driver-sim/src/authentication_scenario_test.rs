//! Virtual-time SCRAM scenario for two rounds and delayed stale outcomes.

use std::num::NonZeroU8;

use kafka_driver_core::{
    ApiVersion, AuthenticationAttempt, AuthenticationDisposition, AuthenticationEffect,
    AuthenticationInput, AuthenticationLimits, AuthenticationMachine, AuthenticationRound,
    AuthenticationState, ConnectionEpoch, EffectId, ExchangeOutcome, Moment, SaslMechanism,
    SaslProtocol, TimerId, TransportId,
};

use crate::Scenario;

const EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const TRANSPORT: TransportId = TransportId::from_raw(2);
const EFFECT: EffectId = EffectId::from_raw(3);
const DEADLINE_TIMER: TimerId = TimerId::from_raw(4);
const DEADLINE: Moment = Moment::from_nanos(20);

#[test]
fn delayed_first_round_result_cannot_finish_the_second_scram_round() {
    // Given
    let mut simulator = Scenario::new();
    let mut machine = AuthenticationMachine::new(
        EPOCH,
        TRANSPORT,
        SaslProtocol::new(
            SaslMechanism::ScramSha256,
            ApiVersion::new(1),
            ApiVersion::new(1),
        ),
        AuthenticationLimits::new(NonZeroU8::new(2).unwrap_or(NonZeroU8::MIN)),
    );
    schedule(&mut simulator, 0, start());
    schedule(&mut simulator, 1, handshake_accepted());
    schedule(&mut simulator, 2, exchange(1, ExchangeOutcome::Continue));
    schedule(&mut simulator, 10, exchange(1, ExchangeOutcome::Succeeded));
    schedule(&mut simulator, 11, exchange(2, ExchangeOutcome::Succeeded));

    // When
    let transitions = drive(&mut simulator, &mut machine, 5);

    // Then
    assert_eq!(
        transitions[3].disposition(),
        AuthenticationDisposition::IgnoredStale
    );
    assert!(transitions[3].effects().is_empty());
    assert_eq!(
        transitions[4].effects(),
        [
            AuthenticationEffect::CancelDeadline {
                timer_id: DEADLINE_TIMER,
            },
            AuthenticationEffect::Succeeded,
        ]
    );
    assert_eq!(machine.state(), AuthenticationState::Succeeded);
    assert_eq!(simulator.now(), Moment::from_nanos(11));
    assert!(simulator.is_idle());
}

const fn start() -> AuthenticationInput {
    AuthenticationInput::Start {
        attempt: AuthenticationAttempt::new(EFFECT, DEADLINE_TIMER, Moment::ORIGIN, DEADLINE),
    }
}

const fn handshake_accepted() -> AuthenticationInput {
    AuthenticationInput::HandshakeAccepted {
        epoch: EPOCH,
        transport_id: TRANSPORT,
        effect_id: EFFECT,
    }
}

fn exchange(round: u8, outcome: ExchangeOutcome) -> AuthenticationInput {
    AuthenticationInput::ExchangeCompleted {
        epoch: EPOCH,
        transport_id: TRANSPORT,
        effect_id: EFFECT,
        round: AuthenticationRound::new(
            NonZeroU8::new(round).unwrap_or_else(|| panic!("round must be nonzero")),
        ),
        outcome,
    }
}

fn schedule(simulator: &mut Scenario<AuthenticationInput>, at: u64, input: AuthenticationInput) {
    if simulator
        .schedule_at(Moment::from_nanos(at), input)
        .is_err()
    {
        panic!("authentication scenario schedule must fit configured bounds");
    }
}

fn drive(
    simulator: &mut Scenario<AuthenticationInput>,
    machine: &mut AuthenticationMachine,
    steps: usize,
) -> Vec<kafka_driver_core::AuthenticationTransition> {
    let mut transitions = Vec::with_capacity(steps);
    for _ in 0..steps {
        let Some((_, input)) = simulator.next_event() else {
            panic!("authentication scenario must provide every expected step");
        };
        transitions.push(machine.apply(input));
    }
    transitions
}
