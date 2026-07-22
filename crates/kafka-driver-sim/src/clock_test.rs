//! Scenarios proving virtual time moves only by explicit checked transitions.

use std::time::Duration;

use kafka_driver_core::Moment;

use crate::{ClockError, SimClock};

#[test]
fn explicit_advances_define_virtual_time() {
    let mut clock = SimClock::new();

    let result = clock.advance_by(Duration::from_millis(3));

    assert_eq!(result, Ok(()));
    assert_eq!(clock.now(), Moment::from_nanos(3_000_000));
}

#[test]
fn virtual_time_cannot_move_backward() {
    let mut clock = SimClock::new();
    assert_eq!(clock.advance_to(Moment::from_nanos(8)), Ok(()));

    let result = clock.advance_to(Moment::from_nanos(5));

    assert_eq!(
        result,
        Err(ClockError::MovesBackward {
            current: Moment::from_nanos(8),
            requested: Moment::from_nanos(5),
        })
    );
}
