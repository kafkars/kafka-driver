//! Explicit resource limits for deterministic simulation state.

/// Default maximum number of pending scripted events.
const DEFAULT_MAX_PENDING_EVENTS: usize = 1_024;

/// Count limits applied by one simulator instance.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SimulationLimits {
    max_pending_events: usize,
}

impl SimulationLimits {
    /// Creates limits with a caller-selected pending-event capacity.
    pub const fn new(max_pending_events: usize) -> Self {
        Self { max_pending_events }
    }

    /// Returns the pending-event capacity.
    pub const fn max_pending_events(self) -> usize {
        self.max_pending_events
    }
}

impl Default for SimulationLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PENDING_EVENTS)
    }
}
