//! Embedded reactor host for bounded administrative command progress.

use std::{num::NonZeroUsize, time::Duration};

use super::{
    Command, WakeHandle,
    mailbox::{DrainStatus, MailboxReceiver},
};

/// Result of one bounded reactor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnOutcome {
    /// No command was available before the host's wait limit.
    Idle,
    /// Commands were processed and more admitted work remains.
    Progress {
        /// Number of commands processed during this turn.
        commands: usize,
        /// Whether the mailbox still contains admitted commands.
        more_work: bool,
    },
    /// Shutdown reached its terminal state.
    Shutdown {
        /// Number of shutdown commands completed during this turn.
        commands: usize,
    },
}

/// Single-owner embedded host for driver state and external resources.
pub struct Reactor {
    commands: MailboxReceiver<Command>,
    command_budget: NonZeroUsize,
    command_batch: Vec<Command>,
    shutdown: bool,
}

impl Reactor {
    pub(crate) fn new(commands: MailboxReceiver<Command>, command_budget: NonZeroUsize) -> Self {
        Self {
            command_batch: Vec::with_capacity(command_budget.get()),
            commands,
            command_budget,
            shutdown: false,
        }
    }

    /// Drives at most one fairness-bounded turn, waiting up to `max_wait`.
    pub fn turn(&mut self, max_wait: Duration) -> TurnOutcome {
        if self.shutdown {
            return TurnOutcome::Shutdown { commands: 0 };
        }
        self.commands.wait(max_wait);
        let status = self
            .commands
            .drain_into(&mut self.command_batch, self.command_budget);
        if self.command_batch.is_empty() {
            if status == DrainStatus::Closed {
                self.shutdown = true;
                return TurnOutcome::Shutdown { commands: 0 };
            }
            return TurnOutcome::Idle;
        }

        let mut processed = 0;
        for command in self.command_batch.drain(..) {
            command.complete_shutdown();
            processed += 1;
            self.shutdown = true;
        }
        if self.shutdown {
            drop(self.commands.close());
            return TurnOutcome::Shutdown {
                commands: processed,
            };
        }

        TurnOutcome::Progress {
            commands: processed,
            more_work: status == DrainStatus::MorePending,
        }
    }

    /// Returns a cloneable notification handle for embedded-host integration.
    pub fn wake_handle(&self) -> WakeHandle {
        self.commands.wake_handle()
    }

    /// Returns whether shutdown has reached its terminal state.
    pub const fn is_shutdown(&self) -> bool {
        self.shutdown
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reactor")
            .field("command_budget", &self.command_budget)
            .field("shutdown", &self.shutdown)
            .finish_non_exhaustive()
    }
}
