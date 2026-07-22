//! Fairness result from one bounded due-deadline drain.

/// Progress made while draining deadlines due at one driver-relative moment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct TimerDrain {
    fired: usize,
    more_due: bool,
}

impl TimerDrain {
    pub(in crate::reactor) const fn new(fired: usize, more_due: bool) -> Self {
        Self { fired, more_due }
    }

    pub(in crate::reactor) const fn fired(self) -> usize {
        self.fired
    }

    pub(in crate::reactor) const fn more_due(self) -> bool {
        self.more_due
    }
}
