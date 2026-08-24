//! Virtual-time scenario for reconnect delay and stale generation rejection.

use std::time::Duration;

use criticality::timeline::TimelineId;
use kafka_driver_core::{
    BackoffPolicy, BrokerDisposition, BrokerInput, BrokerMachine, BrokerPhase, BrokerState,
    ConnectionEpoch, JitterSample, Moment, ReconnectSchedule, TimerId,
};

use crate::Scenario;

const EPOCH_1: ConnectionEpoch = ConnectionEpoch::from_raw(1);
const EPOCH_2: ConnectionEpoch = ConnectionEpoch::from_raw(2);
const STALE_EPOCH: ConnectionEpoch = ConnectionEpoch::from_raw(0);
const RECONNECT_TIMER: TimerId = TimerId::from_raw(8);
const SCENARIO_TIMELINE: TimelineId = TimelineId::new(4);

#[test]
fn stale_retry_delivery_cannot_replace_the_owned_connection_generation() {
    // Given
    let mut simulator = Scenario::new(SCENARIO_TIMELINE);
    let mut machine = BrokerMachine::new(EPOCH_1, backoff());
    schedule(&mut simulator, 0, BrokerInput::Start);
    schedule(
        &mut simulator,
        1,
        BrokerInput::ConnectionFailed {
            epoch: EPOCH_1,
            reconnect: ReconnectSchedule::new(
                RECONNECT_TIMER,
                Moment::from_nanos(1),
                JitterSample::from_raw(0),
            ),
        },
    );
    schedule(
        &mut simulator,
        50,
        BrokerInput::ReconnectElapsed {
            failed_epoch: STALE_EPOCH,
            timer_id: RECONNECT_TIMER,
            now: Moment::from_nanos(50),
        },
    );
    schedule(
        &mut simulator,
        51,
        BrokerInput::ReconnectElapsed {
            failed_epoch: EPOCH_1,
            timer_id: RECONNECT_TIMER,
            now: Moment::from_nanos(51),
        },
    );
    schedule(
        &mut simulator,
        52,
        BrokerInput::ConnectionReady { epoch: EPOCH_2 },
    );

    // When
    let transitions = drive(&mut simulator, &mut machine, 5);

    // Then
    assert_eq!(
        transitions[2].disposition(),
        BrokerDisposition::IgnoredStale
    );
    assert_eq!(simulator.now(), Moment::from_nanos(52));
    assert_eq!(machine.state().phase(), BrokerPhase::Available);
    assert_eq!(machine.state(), BrokerState::Available { epoch: EPOCH_2 });
}

fn backoff() -> BackoffPolicy {
    BackoffPolicy::try_new(Duration::from_nanos(100), Duration::from_nanos(1_000))
        .unwrap_or_else(|error| panic!("simulation backoff must be valid: {error}"))
}

fn schedule(simulator: &mut Scenario<BrokerInput>, at: u64, input: BrokerInput) {
    if simulator
        .schedule_at(Moment::from_nanos(at), input)
        .is_err()
    {
        panic!("broker scenario schedule must fit configured bounds");
    }
}

fn drive(
    simulator: &mut Scenario<BrokerInput>,
    machine: &mut BrokerMachine,
    steps: usize,
) -> Vec<kafka_driver_core::BrokerTransition> {
    let mut transitions = Vec::with_capacity(steps);
    for _ in 0..steps {
        let Some((_, input)) = simulator.next_event() else {
            panic!("broker scenario must provide every expected step");
        };
        transitions.push(machine.apply(input));
    }
    transitions
}
