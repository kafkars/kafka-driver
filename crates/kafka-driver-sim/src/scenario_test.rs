//! Criticality timeline ownership contracts at the Kafka time boundary.

use criticality::timeline::TimelineId;
use kafka_driver_core::{ConnectionInput, Moment};

use crate::{Planned, Scenario, Span};

const REJECTION_TIMELINE: TimelineId = TimelineId::new(11);
const CANCELLATION_TIMELINE: TimelineId = TimelineId::new(12);
const FIRST_INCARNATION: TimelineId = TimelineId::new(13);
const SECOND_INCARNATION: TimelineId = TimelineId::new(14);

#[test]
fn rejected_scheduling_returns_the_kafka_input() {
    let mut scenario = Scenario::new(REJECTION_TIMELINE);
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
    let mut scenario = Scenario::new(CANCELLATION_TIMELINE);
    let input = ConnectionInput::BeginDrain;
    let Ok(token) = scenario.schedule_at(Moment::from_nanos(2), input.clone()) else {
        panic!("Kafka input must fit the scenario timeline");
    };

    assert_eq!(scenario.cancel(token), Some(input));
    assert!(scenario.is_idle());
}

#[test]
fn tokens_are_scoped_to_one_scenario_incarnation() {
    let input = ConnectionInput::BeginDrain;
    let mut first = Scenario::new(FIRST_INCARNATION);
    let mut second = Scenario::new(SECOND_INCARNATION);
    let Ok(foreign) = first.schedule_at(Moment::from_nanos(2), input.clone()) else {
        panic!("first Kafka input must fit its scenario timeline");
    };
    assert!(
        second
            .schedule_at(Moment::from_nanos(2), input.clone())
            .is_ok(),
        "second Kafka input must fit its scenario timeline"
    );

    assert_eq!(second.cancel(foreign), None);
    assert_eq!(
        second.next_event(),
        Some((Moment::from_nanos(2), input.clone()))
    );
    assert_eq!(first.cancel(foreign), Some(input));
}
