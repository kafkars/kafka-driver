//! FIFO command interpretation and transition into graceful drain ownership.

use std::io;

use kafka_driver_core::{CallFailure, Delivery};

use crate::{RequestError, reactor::Command};

use super::{HostState, Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_commands(&mut self) -> Result<usize, ReactorError> {
        let mut processed = 0;
        for command in self.command_batch.drain(..) {
            processed += 1;
            match command {
                Command::Submit { request } if self.state == HostState::Running => {
                    if let Some(broker) = &mut self.broker {
                        let now = self.clock.now().map_err(ReactorError::clock)?;
                        broker
                            .submit(&self.poller, request, now)
                            .map_err(ReactorError::broker)?;
                    } else {
                        request.fail(not_ready());
                    }
                }
                Command::Submit { request } => request.fail(draining()),
                Command::Shutdown { completion } => {
                    self.shutdown_waiters
                        .admit(completion)
                        .map_err(|completion| {
                            drop(completion);
                            ReactorError::host(io::Error::other(
                                "shutdown waiter capacity exceeded mailbox capacity",
                            ))
                        })?;
                    if self.state == HostState::Running {
                        self.state = HostState::DrainRequested;
                    }
                }
            }
        }
        if self.state == HostState::DrainRequested {
            self.start_drain()?;
        }
        Ok(processed)
    }

    pub(super) fn begin_implicit_shutdown(&mut self) -> Result<(), ReactorError> {
        if self.state == HostState::Running {
            self.state = HostState::DrainRequested;
        }
        if self.state == HostState::DrainRequested {
            self.start_drain()?;
        }
        Ok(())
    }

    fn start_drain(&mut self) -> Result<(), ReactorError> {
        self.close_admission()?;
        if let Some(broker) = &mut self.broker {
            broker
                .begin_drain(&self.poller)
                .map_err(ReactorError::broker)?;
        }
        self.state = HostState::Draining;
        Ok(())
    }

    fn close_admission(&mut self) -> Result<(), ReactorError> {
        for command in self.commands.close() {
            match command {
                Command::Submit { request } => request.fail(draining()),
                Command::Shutdown { completion } => {
                    self.shutdown_waiters
                        .admit(completion)
                        .map_err(|completion| {
                            drop(completion);
                            ReactorError::host(io::Error::other(
                                "shutdown waiter capacity exceeded mailbox capacity",
                            ))
                        })?;
                }
            }
        }
        Ok(())
    }
}

fn not_ready() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::NotReady,
        delivery: Delivery::NotSent,
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}
