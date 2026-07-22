//! Embedded reactor host for bounded administrative command progress.

mod commands;
mod debug;
mod resolution;
mod state;

use std::time::Duration;

use crate::config::{DriverLimits, DriverTarget};

use super::{
    Command, MailboxSender, PollEvent, Poller, ReactorError, WakeHandle,
    bootstrap::BootstrapResolution,
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
    bootstrap: Option<BootstrapResolution>,
    clock: ReactorClock,
    state: HostState,
    shutdown_waiters: ShutdownWaiters,
}

impl Reactor {
    pub(crate) fn new(
        limits: DriverLimits,
        target: Option<DriverTarget>,
    ) -> std::io::Result<(MailboxSender<Command>, Self)> {
        let poller = Poller::new(limits.poll_event_budget())?;
        let wake = WakeHandle::new(poller.wake_handle());
        let (sender, commands) = mailbox(limits.mailbox_capacity(), wake.clone());
        let clock = ReactorClock::new();
        let now = clock.now().map_err(std::io::Error::other)?;
        let (mut broker, bootstrap) = match target {
            Some(DriverTarget::Direct(config)) => (
                Some(SingleBroker::new_configured(
                    config,
                    BrokerLimits::default(),
                )),
                None,
            ),
            Some(DriverTarget::Bootstrap(config)) => (
                None,
                Some(BootstrapResolution::start(config, limits.resolver(), wake)?),
            ),
            None => (None, None),
        };
        if let Some(broker) = &mut broker {
            broker.start(&poller, now).map_err(std::io::Error::other)?;
        }
        let reactor = Self {
            command_batch: Vec::with_capacity(limits.command_budget().get()),
            poll_events: Vec::with_capacity(limits.poll_event_budget().get()),
            commands,
            limits,
            poller,
            broker,
            bootstrap,
            clock,
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
        let bootstrap = self.continue_bootstrap()?;
        let mut more_bootstrap = bootstrap.more_work();
        progress |= bootstrap.made_progress();
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
            let bootstrap = self.continue_bootstrap()?;
            progress |= bootstrap.made_progress();
            more_bootstrap |= bootstrap.more_work();
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
                    || more_bootstrap
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
        let now = self.clock.now().map_err(ReactorError::clock)?;
        let Some(broker) = &mut self.broker else {
            self.poll_events.clear();
            return Ok(false);
        };
        let mut progress = false;
        for event in self.poll_events.drain(..) {
            progress |= broker
                .observe(&self.poller, event, now)
                .map_err(ReactorError::broker)?;
        }
        Ok(progress)
    }

    fn continue_broker_io(&mut self) -> Result<bool, ReactorError> {
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.broker.as_mut().map_or(Ok(false), |broker| {
            broker
                .continue_io(&self.poller, now)
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
        self.bootstrap = None;
        drop(self.commands.close());
        self.shutdown_waiters.complete_all();
        Some(TurnOutcome::Shutdown { commands })
    }

    fn broker_has_local_io(&self) -> bool {
        self.broker.as_ref().is_some_and(SingleBroker::has_local_io)
    }
}
