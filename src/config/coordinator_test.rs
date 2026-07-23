//! Scenarios for independent coordinator ownership and fairness bounds.

use std::{num::NonZeroUsize, time::Duration};

use super::{CoordinatorLimits, DriverLimits};

#[test]
fn driver_limits_retain_every_coordinator_bound_independently() {
    let coordinator = CoordinatorLimits::new(
        nonzero(3),
        nonzero(5),
        nonzero(4_096),
        nonzero(7),
        nonzero(2),
        Duration::from_millis(250),
    );

    let retained = DriverLimits::default()
        .with_coordinator_limits(coordinator)
        .coordinator();

    assert_eq!(retained.keys(), nonzero(3));
    assert_eq!(retained.waiting_calls(), nonzero(5));
    assert_eq!(retained.waiting_bytes(), nonzero(4_096));
    assert_eq!(retained.invalidation_waiters(), nonzero(7));
    assert_eq!(retained.turn_budget(), nonzero(2));
    assert_eq!(retained.request_timeout(), Duration::from_millis(250));
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test bound must be nonzero"))
}
