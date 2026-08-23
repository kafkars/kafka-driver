//! FIFO command interpretation and transition into graceful drain ownership.

use std::time::Instant;

use kafka_driver_core::{CallFailure, Delivery, Moment};

use crate::{InvalidationDisposition, RequestError, SnapshotError, reactor::Command};

use super::{HostState, Reactor, ReactorError};

impl Reactor {
    pub(super) fn process_commands(&mut self, now: Moment) -> Result<usize, ReactorError> {
        let mut processed = 0;
        let mut commands = std::mem::take(&mut self.command_batch);
        let result = (|| {
            for command in commands.drain(..) {
                processed += 1;
                match command {
                    Command::Submit {
                        route,
                        mut request,
                        submitted_at,
                    } => {
                        request.mark_reactor(Instant::now());
                        if self.state == HostState::Running {
                            self.process_submission(route, request, submitted_at)?;
                        } else {
                            request.fail(draining());
                        }
                    }
                    Command::Invalidate { token, completion }
                        if self.state == HostState::Running =>
                    {
                        self.process_invalidation(token, completion)?;
                    }
                    Command::Invalidate { completion, .. } => {
                        let _ = completion.complete(InvalidationDisposition::Unavailable);
                    }
                    Command::Snapshot { completion } if self.state == HostState::Running => {
                        let _ = completion.complete(Ok(self.snapshot()));
                    }
                    Command::Snapshot { completion } => {
                        let _ = completion.complete(Err(SnapshotError::Draining));
                    }
                    Command::TopicView {
                        topic,
                        deadline,
                        result_capacity_bytes,
                        completion,
                    } if self.state == HostState::Running => {
                        self.process_topic_view(
                            topic,
                            deadline,
                            result_capacity_bytes,
                            completion,
                        )?;
                    }
                    Command::TopicView { completion, .. } => {
                        let _ = completion.complete(Err(crate::TopicViewError::Draining));
                    }
                    Command::Shutdown => {
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
            self.start_drain(now)?;
        }
        Ok(processed)
    }

    pub(super) fn begin_implicit_shutdown(&mut self, now: Moment) -> Result<(), ReactorError> {
        if self.state == HostState::Running {
            self.state = HostState::DrainRequested;
        }
        if self.state == HostState::DrainRequested {
            self.start_drain(now)?;
        }
        Ok(())
    }

    fn start_drain(&mut self, now: Moment) -> Result<(), ReactorError> {
        self.close_admission();
        if let Some(resolution) = self.resolution.take() {
            self.resolver_shutdown = Some(resolution.begin_shutdown());
        }
        self.brokers.release_scram_proof_senders();
        if let Some(worker) = self.scram_proof.take() {
            self.scram_proof_shutdown = Some(worker.begin_shutdown());
        }
        self.scram_proof_outcomes.clear();
        if let Some(metadata) = &mut self.metadata {
            metadata.fail_waiters(&draining());
        }
        self.metadata = None;
        if let Some(coordinator) = &mut self.coordinator {
            coordinator.fail_waiters(&draining());
        }
        self.coordinator = None;
        self.brokers
            .begin_drain(&self.poller, now)
            .map_err(ReactorError::broker_set)?;
        self.state = HostState::Draining;
        Ok(())
    }

    fn close_admission(&mut self) {
        for command in self.commands.close() {
            match command {
                Command::Submit { mut request, .. } => {
                    request.mark_reactor(Instant::now());
                    request.fail(draining());
                }
                Command::Invalidate { completion, .. } => {
                    let _ = completion.complete(InvalidationDisposition::Unavailable);
                }
                Command::Snapshot { completion } => {
                    let _ = completion.complete(Err(SnapshotError::Draining));
                }
                Command::TopicView { completion, .. } => {
                    let _ = completion.complete(Err(crate::TopicViewError::Draining));
                }
                Command::Shutdown => {}
            }
        }
    }
}

fn draining() -> RequestError {
    RequestError::Rejected {
        failure: CallFailure::Draining,
        delivery: Delivery::NotSent,
    }
}
