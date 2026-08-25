//! Nonblocking Calandria duty turn over the concrete Kafka owner order.

use calandria::{Deadline, Next, Turn, WorkCount};
use kafka_driver_core::Moment;

use crate::reactor::mailbox::DrainStatus;

use super::{HostState, Reactor, ReactorError, TurnOutcome};

impl Reactor {
    pub(super) fn drive_at(&mut self, now: Moment) -> Result<TurnOutcome, ReactorError> {
        let status = self
            .commands
            .drain_into(&mut self.command_batch, self.limits.command_budget());
        let processed = self.process_commands(now)?;
        if status == DrainStatus::Closed && self.state == HostState::Running {
            self.begin_implicit_shutdown(now)?;
        }
        if let Some(outcome) = self.finish_shutdown_if_terminal(processed)? {
            return Ok(outcome);
        }
        let deadlines = self.fire_due_deadlines(now)?;
        let mut progress = deadlines.made_progress();
        let more_due = deadlines.more_due();
        let resolution = self.continue_resolution(now)?;
        let more_resolution = resolution.more_work();
        progress |= resolution.made_progress();
        progress |= self.observe_poll_events(now)?;
        let proofs = self.continue_scram_proofs(now)?;
        let more_proofs = proofs.more_work();
        progress |= proofs.made_progress();
        progress |= processed != 0;
        progress |= self.continue_broker_io(now)?;
        progress |= self.continue_metadata(now)?;
        progress |= self.continue_coordinator(now)?;
        if let Some(outcome) = self.finish_shutdown_if_terminal(processed)? {
            return Ok(outcome);
        }
        if progress {
            return Ok(TurnOutcome::Progress {
                commands: processed,
                more_work: status == DrainStatus::MorePending
                    || more_due
                    || more_resolution
                    || more_proofs
                    || self.worker_shutdown_pending()
                    || self.broker_has_local_io()
                    || self.metadata_has_local_work()
                    || self.coordinator_has_local_work(),
            });
        }
        Ok(TurnOutcome::Idle)
    }

    pub(super) fn next(&self, now: Moment) -> Next {
        let Some(deadline) = self.next_deadline(now) else {
            return Next::Wake;
        };
        Next::WakeOr(Deadline::at(calandria_moment(deadline)))
    }
}

impl calandria::Duty for Reactor {
    type Error = ReactorError;

    fn turn(&mut self, now: calandria::Moment) -> Result<Turn, Self::Error> {
        let now = Moment::from_nanos(now.as_nanos());
        let outcome = self.drive_at(now)?;
        Ok(match outcome {
            TurnOutcome::Idle => Turn::new(WorkCount::ZERO, self.next(now)),
            TurnOutcome::Progress {
                commands,
                more_work,
            } => Turn::new(
                progress_count(commands),
                if more_work { Next::Now } else { self.next(now) },
            ),
            TurnOutcome::Shutdown { commands } => Turn::stopped(progress_count(commands)),
        })
    }
}

const fn calandria_moment(now: Moment) -> calandria::Moment {
    calandria::Moment::from_nanos(now.as_nanos())
}

fn progress_count(commands: usize) -> WorkCount {
    let commands = u64::try_from(commands).unwrap_or(u64::MAX);
    WorkCount::new(commands.max(1))
}
