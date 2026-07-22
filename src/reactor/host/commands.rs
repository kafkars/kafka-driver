//! FIFO command interpretation and transition into graceful drain ownership.

use std::io;

use kafka_driver_core::{CallFailure, Delivery};

use crate::{InvalidationDisposition, RequestError, reactor::Command};

use super::{HostState, Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_commands(&mut self) -> Result<usize, ReactorError> {
        let mut processed = 0;
        let mut commands = std::mem::take(&mut self.command_batch);
        let result = (|| {
            for command in commands.drain(..) {
                processed += 1;
                match command {
                    Command::Submit {
                        route,
                        request,
                        submitted_at,
                    } if self.state == HostState::Running => {
                        self.process_submission(route, request, submitted_at)?;
                    }
                    Command::Submit { request, .. } => request.fail(draining()),
                    Command::Invalidate {
                        receipt,
                        completion,
                    } if self.state == HostState::Running => {
                        self.process_invalidation(receipt, completion)?;
                    }
                    Command::Invalidate { completion, .. } => {
                        let _ = completion.complete(InvalidationDisposition::Unavailable);
                    }
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
            Ok(())
        })();
        self.command_batch = commands;
        result?;
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
        if let Some(resolution) = self.resolution.take() {
            resolution.shutdown().map_err(ReactorError::host)?;
        }
        if let Some(metadata) = &mut self.metadata {
            metadata.fail_waiters(&draining());
        }
        self.metadata = None;
        if let Some(coordinator) = &mut self.coordinator {
            coordinator.fail_waiters(&draining());
        }
        self.coordinator = None;
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
                Command::Submit { request, .. } => request.fail(draining()),
                Command::Invalidate { completion, .. } => {
                    let _ = completion.complete(InvalidationDisposition::Unavailable);
                }
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

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}
