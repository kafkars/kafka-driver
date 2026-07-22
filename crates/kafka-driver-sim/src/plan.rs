//! Finite delayed outcomes shared by deterministic external-capability scripts.

use std::time::Duration;

/// One owned outcome planned after a relative virtual-time delay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Planned<T> {
    delay: Duration,
    outcome: T,
}

impl<T> Planned<T> {
    /// Creates a delayed deterministic outcome.
    pub const fn new(delay: Duration, outcome: T) -> Self {
        Self { delay, outcome }
    }

    /// Returns the virtual delay before the outcome becomes observable.
    pub const fn delay(&self) -> Duration {
        self.delay
    }

    /// Borrows the planned outcome.
    pub const fn outcome(&self) -> &T {
        &self.outcome
    }

    /// Consumes the plan and returns its outcome.
    pub fn into_outcome(self) -> T {
        self.outcome
    }
}
