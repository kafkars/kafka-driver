//! Scenarios for one-way request deadline establishment and overflow.

use std::time::Duration;

use kafka_driver_core::Moment;

use crate::RequestError;

use super::RequestDeadline;

#[test]
fn established_deadline_cannot_move_when_a_later_owner_observes_it() {
    let mut deadline = RequestDeadline::new(Duration::from_nanos(10));

    assert_eq!(
        deadline.establish(Moment::from_nanos(100)),
        Ok(Moment::from_nanos(110))
    );
    assert_eq!(
        deadline.establish(Moment::from_nanos(1_000)),
        Ok(Moment::from_nanos(110))
    );
}

#[test]
fn unrepresentable_absolute_deadline_is_rejected_without_partial_state() {
    let mut deadline = RequestDeadline::new(Duration::MAX);

    assert_eq!(
        deadline.establish(Moment::from_nanos(1)),
        Err(RequestError::DeadlineOverflow)
    );
    assert_eq!(
        deadline.establish(Moment::from_nanos(2)),
        Err(RequestError::DeadlineOverflow)
    );
}
