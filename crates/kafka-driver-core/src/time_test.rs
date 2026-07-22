//! Scenarios proving relative time is checked and deterministic.

use std::time::Duration;

use super::Moment;

#[test]
fn duration_since_rejects_a_later_origin() {
    let now = Moment::from_nanos(5);
    let later = Moment::from_nanos(8);

    let elapsed = now.duration_since(later);

    assert_eq!(elapsed, None);
}

#[test]
fn checked_add_rejects_relative_clock_overflow() {
    let near_end = Moment::from_nanos(u64::MAX);

    let advanced = near_end.checked_add(Duration::from_nanos(1));

    assert_eq!(advanced, None);
}
