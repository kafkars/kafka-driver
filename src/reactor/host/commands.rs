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
                    let now = self.clock.now().map_err(ReactorError::clock)?;
                    if self.brokers.has_seed() {
                        self.brokers
                            .submit_seed(&self.poller, request, now)
                            .map_err(ReactorError::broker_set)?;
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
        let now = self.clock.now().map_err(ReactorError::clock)?;
        self.brokers
            .begin_drain(&self.poller, now)
            .map_err(ReactorError::broker_set)?;
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
