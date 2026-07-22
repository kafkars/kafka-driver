//! Simulation facade combining virtual time with a scripted event schedule.

use std::{error::Error, fmt, time::Duration};

use kafka_driver_core::Moment;

use crate::{
    ClockError, Scheduled, SimClock, SimEventId, SimulationLimits, schedule::ScheduleError,
};

/// A deterministic, single-owner event simulation.
#[derive(Clone, Debug)]
pub struct Simulator<E> {
    clock: SimClock,
    events: crate::schedule::EventSchedule<E>,
}

impl<E> Simulator<E> {
    /// Creates an empty simulation at [`Moment::ORIGIN`].
    pub fn new() -> Self {
        Self::with_limits(SimulationLimits::default())
    }

    /// Creates an empty simulation with explicit resource limits.
    pub const fn with_limits(limits: SimulationLimits) -> Self {
        Self {
            clock: SimClock::new(),
            events: crate::schedule::EventSchedule::new(limits.max_pending_events()),
        }
    }

    /// Returns the current virtual time.
    pub const fn now(&self) -> Moment {
        self.clock.now()
    }

    /// Returns the number of scripted events still pending.
    pub fn pending_events(&self) -> usize {
        self.events.len()
    }

    /// Returns whether no scripted events remain.
    pub fn is_idle(&self) -> bool {
        self.events.is_empty()
    }

    /// Schedules `event` at an absolute virtual moment.
    pub fn schedule_at(&mut self, at: Moment, event: E) -> Result<SimEventId, SimulationError> {
        if at < self.clock.now() {
            return Err(SimulationError::ScheduledInPast {
                current: self.clock.now(),
                requested: at,
            });
        }

        self.events.schedule(at, event).map_err(Into::into)
    }

    /// Schedules `event` after a duration relative to current virtual time.
    pub fn schedule_after(
        &mut self,
        delay: Duration,
        event: E,
    ) -> Result<SimEventId, SimulationError> {
        let Some(at) = self.clock.now().checked_add(delay) else {
            return Err(SimulationError::TimeOverflow {
                current: self.clock.now(),
                delay,
            });
        };

        self.events.schedule(at, event).map_err(Into::into)
    }

    /// Advances to and returns exactly one event, preserving per-step budgets.
    pub fn next_event(&mut self) -> Result<Option<Scheduled<E>>, SimulationError> {
        let Some(at) = self.events.next_at() else {
            return Ok(None);
        };
        self.clock.advance_to(at)?;
        Ok(self.events.pop_next())
    }
}

impl<E> Default for Simulator<E> {
    fn default() -> Self {
        Self::new()
    }
}

/// Why a scripted simulation operation was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimulationError {
    /// A scripted event was placed before current virtual time.
    ScheduledInPast {
        /// Current virtual time.
        current: Moment,
        /// Rejected earlier delivery time.
        requested: Moment,
    },
    /// A relative delay exceeded the driver's relative time space.
    TimeOverflow {
        /// Current virtual time.
        current: Moment,
        /// Rejected delay.
        delay: Duration,
    },
    /// The virtual clock rejected an internally requested transition.
    Clock(ClockError),
    /// Every simulator-local event identity has been consumed.
    EventIdsExhausted,
    /// The pending-event count reached its configured capacity.
    EventCapacityReached {
        /// Configured pending-event capacity.
        limit: usize,
    },
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScheduledInPast { current, requested } => write!(
                formatter,
                "cannot schedule at {}ns before current virtual time {}ns",
                requested.as_nanos(),
                current.as_nanos()
            ),
            Self::TimeOverflow { current, delay } => write!(
                formatter,
                "scheduling {delay:?} after {}ns would overflow virtual time",
                current.as_nanos()
            ),
            Self::Clock(error) => error.fmt(formatter),
            Self::EventIdsExhausted => formatter.write_str("simulation event IDs are exhausted"),
            Self::EventCapacityReached { limit } => {
                write!(
                    formatter,
                    "simulation event capacity of {limit} was reached"
                )
            }
        }
    }
}

impl Error for SimulationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Clock(error) => Some(error),
            Self::ScheduledInPast { .. }
            | Self::TimeOverflow { .. }
            | Self::EventIdsExhausted
            | Self::EventCapacityReached { .. } => None,
        }
    }
}

impl From<ClockError> for SimulationError {
    fn from(error: ClockError) -> Self {
        Self::Clock(error)
    }
}

impl From<ScheduleError> for SimulationError {
    fn from(error: ScheduleError) -> Self {
        match error {
            ScheduleError::CapacityReached { limit } => Self::EventCapacityReached { limit },
            ScheduleError::EventIdsExhausted => Self::EventIdsExhausted,
        }
    }
}
