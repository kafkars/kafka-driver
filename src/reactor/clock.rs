//! Translation between the host monotonic clock and driver-relative moments.

use std::time::{Duration, Instant};
use std::{error::Error, fmt};

use kafka_driver_core::Moment;

/// Failure to represent host monotonic time in the driver's relative domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct ClockOverflow;

impl fmt::Display for ClockOverflow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("driver-relative monotonic clock overflowed")
    }
}

impl Error for ClockOverflow {}

/// Monotonic origin owned by one reactor for its entire lifetime.
#[derive(Debug)]
pub(in crate::reactor) struct ReactorClock {
    origin: Instant,
}

impl ReactorClock {
    pub(in crate::reactor) fn new() -> Self {
        Self::from_origin(Instant::now())
    }

    pub(in crate::reactor) const fn from_origin(origin: Instant) -> Self {
        Self { origin }
    }

    pub(in crate::reactor) fn now(&self) -> Result<Moment, ClockOverflow> {
        self.moment_at(Instant::now())
    }

    pub(in crate::reactor) fn moment_at(&self, instant: Instant) -> Result<Moment, ClockOverflow> {
        let elapsed = instant.duration_since(self.origin);
        let nanos = u64::try_from(elapsed.as_nanos()).map_err(|_| ClockOverflow)?;
        Ok(Moment::from_nanos(nanos))
    }

    pub(in crate::reactor) fn bounded_wait(
        now: Moment,
        next_deadline: Option<Moment>,
        host_limit: Duration,
    ) -> Duration {
        let Some(deadline) = next_deadline else {
            return host_limit;
        };
        deadline
            .duration_since(now)
            .unwrap_or(Duration::ZERO)
            .min(host_limit)
    }
}
