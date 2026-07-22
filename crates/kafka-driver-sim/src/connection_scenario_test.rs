//! End-to-end virtual-time scenarios for the pure connection machine.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    CallFailure, CallId, CloseReason, ConnectionEffect, ConnectionEpoch, ConnectionInput,
    ConnectionLimits, ConnectionMachine, ConnectionPhase, Delivery, EffectId, Moment,
    NegotiatedCapabilities, NegotiationAttempt, TimerId, TransitionDisposition, TransportId,
};

use crate::Simulator;

const EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const STALE_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(0);
const TRANSPORT: TransportId = TransportId::from_raw(2);
const OPEN_EFFECT: EffectId = EffectId::from_raw(3);
const OPEN_TIMER: TimerId = TimerId::from_raw(9);
const OPEN_DEADLINE: Moment = Moment::from_nanos(50);
const NEGOTIATION_EFFECT: EffectId = EffectId::from_raw(7);
const NEGOTIATION_TIMER: TimerId = TimerId::from_raw(8);
const CALL: CallId = CallId::from_raw(4);
const WRITE_EFFECT: EffectId = EffectId::from_raw(5);
const DEADLINE_TIMER: TimerId = TimerId::from_raw(6);

#[test]
fn virtual_deadline_closes_a_possibly_delivered_call() {
    let mut simulator = Simulator::new();
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());
    schedule(
        &mut simulator,
        0,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            deadline_timer: OPEN_TIMER,
            deadline: OPEN_DEADLINE,
        },
    );
    schedule(&mut simulator, 1, transport_opened(EPOCH));
    schedule(
        &mut simulator,
        2,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities: capabilities(),
        },
    );
    schedule(
        &mut simulator,
        3,
        ConnectionInput::Submit {
            call_id: CALL,
            write_effect: WRITE_EFFECT,
            deadline_timer: DEADLINE_TIMER,
            now: Moment::from_nanos(3),
            deadline: Moment::from_nanos(10),
        },
    );
    schedule(
        &mut simulator,
        4,
        ConnectionInput::WriteSubmitted {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: WRITE_EFFECT,
        },
    );
    schedule(
        &mut simulator,
        10,
        ConnectionInput::DeadlineElapsed {
            epoch: EPOCH,
            timer_id: DEADLINE_TIMER,
            now: Moment::from_nanos(10),
        },
    );

    let transitions = drive(&mut simulator, &mut machine, 6);
    let Some(deadline) = transitions.last() else {
        panic!("the deadline transition must be retained by the scenario");
    };
    let reason = CloseReason::DeadlineExceeded { call_id: CALL };

    assert_eq!(simulator.now(), Moment::from_nanos(10));
    assert_eq!(machine.state().phase(), ConnectionPhase::Closing);
    assert_eq!(
        deadline.effects(),
        &[
            ConnectionEffect::CloseTransport {
                epoch: EPOCH,
                transport_id: TRANSPORT,
                reason,
            },
            ConnectionEffect::CancelDeadline {
                timer_id: DEADLINE_TIMER,
            },
            ConnectionEffect::FailCall {
                call_id: CALL,
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
        ]
    );
}

#[test]
fn delayed_old_epoch_result_is_ordinary_stale_data() {
    let mut simulator = Simulator::new();
    let mut machine = ConnectionMachine::new(EPOCH, ConnectionLimits::default());
    schedule(
        &mut simulator,
        0,
        ConnectionInput::Start {
            effect_id: OPEN_EFFECT,
            transport_id: TRANSPORT,
            deadline_timer: OPEN_TIMER,
            deadline: OPEN_DEADLINE,
        },
    );
    schedule(&mut simulator, 1, transport_opened(STALE_EPOCH));
    schedule(&mut simulator, 2, transport_opened(EPOCH));
    schedule(
        &mut simulator,
        3,
        ConnectionInput::ApiVersionsNegotiated {
            epoch: EPOCH,
            transport_id: TRANSPORT,
            effect_id: NEGOTIATION_EFFECT,
            capabilities: capabilities(),
        },
    );

    let transitions = drive(&mut simulator, &mut machine, 4);

    assert_eq!(
        transitions[1].record().disposition(),
        TransitionDisposition::IgnoredStale
    );
    assert_eq!(machine.state().phase(), ConnectionPhase::Ready);
}

const fn transport_opened(epoch: ConnectionEpoch) -> ConnectionInput {
    ConnectionInput::TransportOpened {
        epoch,
        effect_id: OPEN_EFFECT,
        transport_id: TRANSPORT,
        negotiation: NegotiationAttempt::new(
            NEGOTIATION_EFFECT,
            NEGOTIATION_TIMER,
            Moment::ORIGIN,
            Moment::from_nanos(100),
        ),
    }
}

fn capabilities() -> NegotiatedCapabilities {
    NegotiatedCapabilities::try_from_iter([], NonZeroUsize::MIN)
        .unwrap_or_else(|error| panic!("scenario capabilities must be valid: {error}"))
}

fn schedule(simulator: &mut Simulator<ConnectionInput>, at: u64, input: ConnectionInput) {
    if simulator
        .schedule_at(Moment::from_nanos(at), input)
        .is_err()
    {
        panic!("connection scenario schedule must fit configured bounds");
    }
}

fn drive(
    simulator: &mut Simulator<ConnectionInput>,
    machine: &mut ConnectionMachine,
    steps: usize,
) -> Vec<kafka_driver_core::ConnectionTransition> {
    let mut transitions = Vec::with_capacity(steps);
    for _ in 0..steps {
        let Ok(Some(scheduled)) = simulator.next_event() else {
            panic!("connection scenario must provide every expected step");
        };
        let Ok(transition) = machine.apply(scheduled.into_event()) else {
            panic!("scripted connection input must be internally valid");
        };
        transitions.push(transition);
    }
    transitions
}
