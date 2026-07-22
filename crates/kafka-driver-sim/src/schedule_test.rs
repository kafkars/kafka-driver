//! Scenarios proving scripted events have stable temporal ordering.

use kafka_driver_core::Moment;

use crate::schedule::EventSchedule;

#[test]
fn earlier_events_are_delivered_before_later_events() {
    let mut schedule = EventSchedule::new(2);
    assert!(schedule.schedule(Moment::from_nanos(20), "later").is_ok());
    assert!(schedule.schedule(Moment::from_nanos(10), "earlier").is_ok());

    let Some(first) = schedule.pop_next() else {
        panic!("an earlier event should be pending");
    };

    assert_eq!(first.into_event(), "earlier");
}

#[test]
fn equal_time_events_preserve_insertion_order() {
    let mut schedule = EventSchedule::new(2);
    let at = Moment::from_nanos(10);
    assert!(schedule.schedule(at, "first").is_ok());
    assert!(schedule.schedule(at, "second").is_ok());

    let Some(first) = schedule.pop_next() else {
        panic!("the first same-time event should be pending");
    };
    let Some(second) = schedule.pop_next() else {
        panic!("the second same-time event should be pending");
    };

    assert_eq!(first.into_event(), "first");
    assert_eq!(second.into_event(), "second");
}
