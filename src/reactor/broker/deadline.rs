//! Bounded deadline progress shared by Bornera host phases.

/// Progress from one bounded due-deadline delivery phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::reactor) struct DeadlineProgress {
    fired: usize,
    more_due: bool,
}

impl DeadlineProgress {
    pub(in crate::reactor) const fn idle() -> Self {
        Self {
            fired: 0,
            more_due: false,
        }
    }

    pub(in crate::reactor) const fn made_progress(self) -> bool {
        self.fired != 0
    }

    pub(in crate::reactor) const fn more_due(self) -> bool {
        self.more_due
    }

    pub(in crate::reactor) const fn from_work(fired: usize, more_due: bool) -> Self {
        Self { fired, more_due }
    }
}
