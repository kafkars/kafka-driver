//! Embedded reactor host for bounded administrative command progress.

mod commands;
mod state;

use std::{net::SocketAddr, time::Duration};

use crate::config::DriverLimits;

use super::{
    Command, MailboxSender, PollEvent, Poller, ReactorError, WakeHandle,
    broker::{BrokerLimits, DeadlineProgress, SingleBroker},
    clock::ReactorClock,
    mailbox,
    mailbox::{DrainStatus, MailboxReceiver},
};

use state::{HostState, ShutdownWaiters};

/// Result of one bounded reactor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnOutcome {
    /// No command was available before the host's wait limit.
    Idle,
    /// Bounded command, timer, or I/O work made progress.
    Progress {
        /// Number of commands processed during this turn.
        commands: usize,
        /// Whether bounded command, timer, or retained I/O work remains.
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
    broker: Option<SingleBroker>,
    clock: ReactorClock,
    state: HostState,
    shutdown_waiters: ShutdownWaiters,
}

impl Reactor {
    pub(crate) fn new(
        limits: DriverLimits,
        broker_address: Option<SocketAddr>,
    ) -> std::io::Result<(MailboxSender<Command>, Self)> {
        let poller = Poller::new(limits.poll_event_budget())?;
        let wake = WakeHandle::new(poller.wake_handle());
        let (sender, commands) = mailbox(limits.mailbox_capacity(), wake);
        let mut broker =
            broker_address.map(|address| SingleBroker::new(address, BrokerLimits::default()));
        if let Some(broker) = &mut broker {
            broker.start(&poller).map_err(std::io::Error::other)?;
        }
        let reactor = Self {
            command_batch: Vec::with_capacity(limits.command_budget().get()),
            poll_events: Vec::with_capacity(limits.poll_event_budget().get()),
            commands,
            limits,
            poller,
            broker,
            clock: ReactorClock::new(),
            state: HostState::Running,
            shutdown_waiters: ShutdownWaiters::new(limits.mailbox_capacity()),
        };
        Ok((sender, reactor))
    }

    /// Drives at most one fairness-bounded turn, waiting up to `max_wait`.
    pub fn turn(&mut self, max_wait: Duration) -> Result<TurnOutcome, ReactorError> {
        if self.state == HostState::Shutdown {
            return Ok(TurnOutcome::Shutdown { commands: 0 });
        }
        let mut status = self
            .commands
            .drain_into(&mut self.command_batch, self.limits.command_budget());
        let mut processed = self.process_commands()?;
        if status == DrainStatus::Closed && self.state == HostState::Running {
            self.begin_implicit_shutdown()?;
        }
        if let Some(outcome) = self.finish_shutdown_if_terminal(processed) {
            return Ok(outcome);
        }
        let deadlines = self.fire_due_deadlines()?;
        let mut progress = deadlines.made_progress();
        let mut more_due = deadlines.more_due();
        progress |= processed != 0;
        progress |= self.continue_broker_io()?;

        if !progress && status == DrainStatus::Idle {
            self.poll_events.clear();
            let wait = self.poll_wait(max_wait)?;
            self.poller
                .poll_into(Some(wait), &mut self.poll_events)
                .map_err(ReactorError::poll)?;
            status = self
                .commands
                .drain_into(&mut self.command_batch, self.limits.command_budget());
            processed += self.process_commands()?;
            if status == DrainStatus::Closed && self.state == HostState::Running {
                self.begin_implicit_shutdown()?;
            }
            let deadlines = self.fire_due_deadlines()?;
            progress |= processed != 0 || deadlines.made_progress();
            more_due |= deadlines.more_due();
            progress |= self.observe_poll_events()?;
        }
        if let Some(outcome) = self.finish_shutdown_if_terminal(processed) {
            return Ok(outcome);
        }
        if progress {
            return Ok(TurnOutcome::Progress {
                commands: processed,
                more_work: status == DrainStatus::MorePending
                    || more_due
                    || self.broker_has_local_io(),
            });
        }
        Ok(TurnOutcome::Idle)
    }

    /// Returns a cloneable notification handle for embedded-host integration.
    pub fn wake_handle(&self) -> WakeHandle {
        self.commands.wake_handle()
    }

    /// Returns whether shutdown has reached its terminal state.
    pub const fn is_shutdown(&self) -> bool {
        matches!(self.state, HostState::Shutdown)
    }

    fn observe_poll_events(&mut self) -> Result<bool, ReactorError> {
        let Some(broker) = &mut self.broker else {
            self.poll_events.clear();
            return Ok(false);
        };
        let mut progress = false;
        for event in self.poll_events.drain(..) {
            progress |= broker
                .observe(&self.poller, event)
                .map_err(ReactorError::broker)?;
        }
        Ok(progress)
    }

    fn continue_broker_io(&mut self) -> Result<bool, ReactorError> {
        self.broker.as_mut().map_or(Ok(false), |broker| {
            broker
                .continue_io(&self.poller)
                .map_err(ReactorError::broker)
        })
    }

    fn fire_due_deadlines(&mut self) -> Result<DeadlineProgress, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.broker
            .as_mut()
            .map_or(Ok(DeadlineProgress::idle()), |broker| {
                broker
                    .fire_due(&self.poller, now)
                    .map_err(ReactorError::broker)
            })
    }

    fn poll_wait(&self, host_limit: Duration) -> Result<Duration, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let deadline = self.broker.as_ref().and_then(SingleBroker::next_deadline);
        Ok(ReactorClock::bounded_wait(now, deadline, host_limit))
    }

    fn finish_shutdown_if_terminal(&mut self, commands: usize) -> Option<TurnOutcome> {
        if self.state != HostState::Draining
            || self
                .broker
                .as_ref()
                .is_some_and(|broker| !broker.is_terminal())
        {
            return None;
        }
        self.state = HostState::Shutdown;
        drop(self.commands.close());
        self.shutdown_waiters.complete_all();
        Some(TurnOutcome::Shutdown { commands })
    }

    fn broker_has_local_io(&self) -> bool {
        self.broker.as_ref().is_some_and(SingleBroker::has_local_io)
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reactor")
            .field("limits", &self.limits)
            .field("broker", &self.broker.as_ref().map(SingleBroker::state))
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
