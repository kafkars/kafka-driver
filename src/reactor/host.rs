//! Embedded reactor host for bounded administrative command progress.

use std::{net::SocketAddr, time::Duration};

use kafka_driver_core::{CallFailure, Delivery};

use crate::{RequestError, config::DriverLimits};

use super::{
    Command, MailboxSender, PollEvent, Poller, ReactorError, WakeHandle,
    broker::{BrokerLimits, DeadlineProgress, SingleBroker},
    clock::ReactorClock,
    mailbox,
    mailbox::{DrainStatus, MailboxReceiver},
};

/// Result of one bounded reactor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnOutcome {
    /// No command was available before the host's wait limit.
    Idle,
    /// Bounded command, timer, or I/O work made progress.
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
    broker: Option<SingleBroker>,
    clock: ReactorClock,
    shutdown: bool,
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
        let mut processed = self.process_commands()?;
        if self.shutdown {
            return Ok(self.finish_shutdown(processed));
        }
        let deadlines = self.fire_due_deadlines()?;
        let mut progress = deadlines.made_progress();
        let mut more_due = deadlines.more_due();
        progress |= processed != 0;
        progress |= self.continue_broker_io()?;

        if status == DrainStatus::Closed && processed == 0 {
            self.shutdown = true;
            return Ok(self.finish_shutdown(0));
        }
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
            if self.shutdown {
                return Ok(self.finish_shutdown(processed));
            }
            let deadlines = self.fire_due_deadlines()?;
            progress |= processed != 0 || deadlines.made_progress();
            more_due |= deadlines.more_due();
            progress |= self.observe_poll_events()?;
        }
        if status == DrainStatus::Closed && processed == 0 && !progress {
            self.shutdown = true;
            return Ok(self.finish_shutdown(0));
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

    fn process_commands(&mut self) -> Result<usize, ReactorError> {
        let mut processed = 0;
        for command in self.command_batch.drain(..) {
            processed += 1;
            match command {
                Command::Submit { request } => {
                    if let Some(broker) = &mut self.broker {
                        let now = self.clock.now().map_err(ReactorError::clock)?;
                        broker
                            .submit(&self.poller, request, now)
                            .map_err(ReactorError::broker)?;
                    } else {
                        request.fail(RequestError::Rejected {
                            failure: CallFailure::NotReady,
                            delivery: Delivery::NotSent,
                        });
                    }
                }
                Command::Shutdown { completion } => {
                    let _ = completion.complete(());
                    self.shutdown = true;
                }
            }
        }
        Ok(processed)
    }

    /// Returns a cloneable notification handle for embedded-host integration.
    pub fn wake_handle(&self) -> WakeHandle {
        self.commands.wake_handle()
    }

    /// Returns whether shutdown has reached its terminal state.
    pub const fn is_shutdown(&self) -> bool {
        self.shutdown
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

    fn finish_shutdown(&mut self, commands: usize) -> TurnOutcome {
        drop(self.commands.close());
        TurnOutcome::Shutdown { commands }
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
            .field("shutdown", &self.shutdown)
            .finish_non_exhaustive()
    }
}
