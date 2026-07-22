//! Public host-admission and selector-fairness limits.

use std::num::NonZeroUsize;

const DEFAULT_MAILBOX_CAPACITY: NonZeroUsize = nonzero(1_024);
const DEFAULT_COMMAND_BUDGET: NonZeroUsize = nonzero(256);
const DEFAULT_POLL_EVENT_BUDGET: NonZeroUsize = nonzero(256);

/// Resource bounds applied to one driver reactor.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverLimits {
    mailbox_capacity: NonZeroUsize,
    command_budget: NonZeroUsize,
    poll_event_budget: NonZeroUsize,
}

impl DriverLimits {
    /// Creates limits with explicit mailbox and per-turn command bounds.
    pub const fn new(mailbox_capacity: NonZeroUsize, command_budget: NonZeroUsize) -> Self {
        Self {
            mailbox_capacity,
            command_budget,
            poll_event_budget: DEFAULT_POLL_EVENT_BUDGET,
        }
    }

    /// Replaces the maximum readiness events returned by one OS poll.
    pub const fn with_poll_event_budget(mut self, poll_event_budget: NonZeroUsize) -> Self {
        self.poll_event_budget = poll_event_budget;
        self
    }

    /// Returns the maximum number of admitted commands.
    pub const fn mailbox_capacity(self) -> NonZeroUsize {
        self.mailbox_capacity
    }

    /// Returns the maximum commands processed by one reactor turn.
    pub const fn command_budget(self) -> NonZeroUsize {
        self.command_budget
    }

    /// Returns the maximum readiness events returned by one OS poll.
    pub const fn poll_event_budget(self) -> NonZeroUsize {
        self.poll_event_budget
    }
}

impl Default for DriverLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAILBOX_CAPACITY, DEFAULT_COMMAND_BUDGET)
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("driver defaults must be nonzero");
    };
    value
}
