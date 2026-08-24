//! Criticality timeline ownership contracts at the Kafka time boundary.

use criticality::time::Span;
use kafka_driver_core::{ConnectionInput, Moment};

use crate::{Planned, Scenario};

#[test]
fn rejected_scheduling_returns_the_kafka_input() {
    let mut scenario = Scenario::new();
    let first = ConnectionInput::BeginDrain;
    assert!(scenario.schedule_at(Moment::from_nanos(2), first).is_ok());
    assert!(scenario.next_event().is_some());
    let rejected = ConnectionInput::BeginDrain;

    assert_eq!(
        scenario.schedule_at(Moment::from_nanos(1), rejected.clone()),
        Err(rejected.clone())
    );
    assert_eq!(
        scenario.schedule_planned(Planned::new(Span::from_ticks(u64::MAX), rejected.clone())),
        Err(rejected)
    );
}

#[test]
fn cancellation_returns_the_owned_kafka_input() {
    let mut scenario = Scenario::new();
    let input = ConnectionInput::BeginDrain;
    let Ok(token) = scenario.schedule_at(Moment::from_nanos(2), input.clone()) else {
        panic!("Kafka input must fit the scenario timeline");
    };

    assert_eq!(scenario.cancel(token), Some(input));
    assert!(scenario.is_idle());
}
