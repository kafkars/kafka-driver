//! Virtual monotonic time advanced explicitly by simulation scenarios.

use std::{error::Error, fmt, time::Duration};

use kafka_driver_core::Moment;

/// A deterministic clock with no relationship to wall time.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SimClock {
    now: Moment,
}

impl SimClock {
    /// Creates a clock at [`Moment::ORIGIN`].
    pub const fn new() -> Self {
        Self {
            now: Moment::ORIGIN,
        }
    }

    /// Returns the current virtual moment.
    pub const fn now(self) -> Moment {
        self.now
    }

    /// Moves virtual time to `requested` without allowing it to run backward.
    pub fn advance_to(&mut self, requested: Moment) -> Result<(), ClockError> {
        if requested < self.now {
            return Err(ClockError::MovesBackward {
                current: self.now,
                requested,
            });
        }

        self.now = requested;
        Ok(())
    }

    /// Advances virtual time by `duration` with checked arithmetic.
    pub fn advance_by(&mut self, duration: Duration) -> Result<(), ClockError> {
        let Some(requested) = self.now.checked_add(duration) else {
            return Err(ClockError::Overflow {
                current: self.now,
                duration,
            });
        };

        self.now = requested;
        Ok(())
    }
}

/// Why a requested virtual-clock transition was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClockError {
    /// The requested moment precedes the current virtual moment.
    MovesBackward {
        /// Current virtual time.
        current: Moment,
        /// Rejected earlier time.
        requested: Moment,
    },
    /// Adding a duration exceeded the driver's relative time space.
    Overflow {
        /// Current virtual time.
        current: Moment,
        /// Rejected duration.
        duration: Duration,
    },
}

impl fmt::Display for ClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MovesBackward { current, requested } => write!(
                formatter,
                "virtual time cannot move from {}ns back to {}ns",
                current.as_nanos(),
                requested.as_nanos()
            ),
            Self::Overflow { current, duration } => write!(
                formatter,
                "advancing virtual time from {}ns by {duration:?} would overflow",
                current.as_nanos()
            ),
        }
    }
}

impl Error for ClockError {}
