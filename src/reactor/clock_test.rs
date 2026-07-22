//! Scenarios for monotonic conversion and deadline-bounded host waits.

use std::time::{Duration, Instant};

use kafka_driver_core::Moment;

use super::clock::ReactorClock;

#[test]
fn given_a_monotonic_origin_when_time_is_observed_then_the_moment_is_relative() {
    // Given
    let origin = Instant::now();
    let clock = ReactorClock::from_origin(origin);

    // When
    let Ok(moment) = clock.moment_at(origin + Duration::from_micros(17)) else {
        panic!("a short elapsed duration must be representable");
    };

    // Then
    assert_eq!(moment, Moment::from_nanos(17_000));
}

#[test]
fn given_an_earlier_deadline_when_wait_is_bounded_then_the_deadline_wins() {
    // Given
    let now = Moment::from_nanos(10);
    let deadline = Moment::from_nanos(25);

    // When
    let wait = ReactorClock::bounded_wait(now, Some(deadline), Duration::from_nanos(100));

    // Then
    assert_eq!(wait, Duration::from_nanos(15));
}

#[test]
fn given_an_elapsed_deadline_when_wait_is_bounded_then_poll_does_not_wait() {
    // Given
    let now = Moment::from_nanos(25);
    let deadline = Moment::from_nanos(10);

    // When
    let wait = ReactorClock::bounded_wait(now, Some(deadline), Duration::from_secs(1));

    // Then
    assert_eq!(wait, Duration::ZERO);
}

#[test]
fn given_no_deadline_when_wait_is_bounded_then_the_host_limit_wins() {
    // Given
    let host_limit = Duration::from_millis(7);

    // When
    let wait = ReactorClock::bounded_wait(Moment::ORIGIN, None, host_limit);

    // Then
    assert_eq!(wait, host_limit);
}
