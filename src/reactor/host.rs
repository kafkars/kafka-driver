//! Embedded reactor host for bounded administrative command progress.

use std::time::Duration;

use kafka_driver_core::{CallFailure, Delivery};

use crate::{RequestError, config::DriverLimits};

use super::{
    Command, MailboxSender, PollEvent, Poller, ReactorError, WakeHandle, mailbox,
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
    limits: DriverLimits,
    command_batch: Vec<Command>,
    poller: Poller,
    poll_events: Vec<PollEvent>,
    shutdown: bool,
}

impl Reactor {
    pub(crate) fn new(limits: DriverLimits) -> std::io::Result<(MailboxSender<Command>, Self)> {
        let poller = Poller::new(limits.poll_event_budget())?;
        let wake = WakeHandle::new(poller.wake_handle());
        let (sender, commands) = mailbox(limits.mailbox_capacity(), wake);
        let reactor = Self {
            command_batch: Vec::with_capacity(limits.command_budget().get()),
            poll_events: Vec::with_capacity(limits.poll_event_budget().get()),
            commands,
            limits,
            poller,
            shutdown: false,
        };
        Ok((sender, reactor))
    }

    /// Drives at most one fairness-bounded turn, waiting up to `max_wait`.
    pub fn turn(&mut self, max_wait: Duration) -> Result<TurnOutcome, ReactorError> {
        if self.shutdown {
            return Ok(TurnOutcome::Shutdown { commands: 0 });
        }
        let mut status = self
            .commands
            .drain_into(&mut self.command_batch, self.limits.command_budget());
        if self.command_batch.is_empty() && status == DrainStatus::Idle {
            self.poll_events.clear();
            self.poller
                .poll_into(Some(max_wait), &mut self.poll_events)
                .map_err(ReactorError::poll)?;
            status = self
                .commands
                .drain_into(&mut self.command_batch, self.limits.command_budget());
        }
        if self.command_batch.is_empty() {
            if status == DrainStatus::Closed {
                self.shutdown = true;
                return Ok(TurnOutcome::Shutdown { commands: 0 });
            }
            return Ok(TurnOutcome::Idle);
        }

        let mut processed = 0;
        for command in self.command_batch.drain(..) {
            processed += 1;
            match command {
                Command::Submit { request } => request.fail(RequestError::Rejected {
                    failure: CallFailure::NotReady,
                    delivery: Delivery::NotSent,
                }),
                Command::Shutdown { completion } => {
                    let _ = completion.complete(());
                    self.shutdown = true;
                }
            }
        }
        if self.shutdown {
            drop(self.commands.close());
            return Ok(TurnOutcome::Shutdown {
                commands: processed,
            });
        }

        Ok(TurnOutcome::Progress {
            commands: processed,
            more_work: status == DrainStatus::MorePending,
        })
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
            .field("limits", &self.limits)
            .field("shutdown", &self.shutdown)
            .finish_non_exhaustive()
    }
}
