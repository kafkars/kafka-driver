//! End-to-end scenarios for deterministic scripted event delivery.

use std::time::Duration;

use kafka_driver_core::Moment;

use crate::{SimulationError, SimulationLimits, Simulator};

#[test]
fn next_event_advances_time_and_returns_one_event() {
    let mut simulator = Simulator::new();
    assert!(
        simulator
            .schedule_after(Duration::from_millis(5), "first")
            .is_ok()
    );
    assert!(
        simulator
            .schedule_after(Duration::from_millis(8), "second")
            .is_ok()
    );

    let Ok(Some(event)) = simulator.next_event() else {
        panic!("the first scripted event should be delivered");
    };

    assert_eq!(event.at(), Moment::from_nanos(5_000_000));
    assert_eq!(event.into_event(), "first");
    assert_eq!(simulator.now(), Moment::from_nanos(5_000_000));
    assert_eq!(simulator.pending_events(), 1);
}

#[test]
fn scheduling_before_current_time_is_rejected() {
    let mut simulator = Simulator::new();
    assert!(
        simulator
            .schedule_at(Moment::from_nanos(10), "advance")
            .is_ok()
    );
    assert!(matches!(simulator.next_event(), Ok(Some(_))));

    let result = simulator.schedule_at(Moment::from_nanos(9), "stale");

    assert_eq!(
        result,
        Err(SimulationError::ScheduledInPast {
            current: Moment::from_nanos(10),
            requested: Moment::from_nanos(9),
        })
    );
}

#[test]
fn pending_event_capacity_is_enforced_at_admission() {
    let limits = SimulationLimits::new(1);
    let mut simulator = Simulator::with_limits(limits);
    assert!(
        simulator
            .schedule_at(Moment::from_nanos(1), "admitted")
            .is_ok()
    );

    let result = simulator.schedule_at(Moment::from_nanos(2), "rejected");

    assert_eq!(
        result,
        Err(SimulationError::EventCapacityReached { limit: 1 })
    );
}
