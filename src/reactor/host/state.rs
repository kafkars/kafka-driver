//! Terminal hosting state and shared shutdown barrier settlement.

use kafka_driver_core::Moment;

use super::{Reactor, ReactorError, TurnOutcome};
use crate::reactor::{direct_plaintext::DirectBackend, worker_shutdown::WorkerShutdownPoll};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HostState {
    Running,
    DrainRequested,
    Draining,
    Shutdown,
}

impl Reactor {
    pub(super) const fn worker_shutdown_pending(&self) -> bool {
        self.resolver_shutdown.is_some() || self.scram_proof_shutdown.is_some()
    }

    pub(super) fn finish_shutdown_if_terminal(
        &mut self,
        commands: usize,
        now: Moment,
    ) -> Result<Option<TurnOutcome>, ReactorError> {
        let backend_terminal = if let Some(cluster) = self.backend.cluster() {
            cluster.is_terminal().map_err(ReactorError::host)?
        } else if let Some(direct) = self.backend.direct() {
            DirectBackend::is_terminal(direct)
        } else {
            false
        };
        if self.state != HostState::Draining || !backend_terminal {
            return Ok(None);
        }
        self.resolution = None;
        self.metadata = None;
        self.coordinator = None;
        let resolver = poll_resolver(&mut self.resolver_shutdown, now)?;
        if resolver == WorkerShutdownPoll::Abandoned {
            self.observation.record_resolver_shutdown_abandoned();
        }
        let proof = poll_scram_proof(&mut self.scram_proof_shutdown, now)?;
        if proof == WorkerShutdownPoll::Abandoned {
            self.observation.record_proof_shutdown_abandoned();
        }
        if resolver == WorkerShutdownPoll::Pending || proof == WorkerShutdownPoll::Pending {
            return Ok(None);
        }
        drop(self.commands.close());
        self.state = HostState::Shutdown;
        self.shutdown.complete();
        Ok(Some(TurnOutcome::Shutdown { commands }))
    }
}

fn poll_resolver(
    worker: &mut Option<super::super::resolver::ResolverShutdown>,
    now: Moment,
) -> Result<WorkerShutdownPoll, ReactorError> {
    let Some(shutdown) = worker else {
        return Ok(WorkerShutdownPoll::Complete);
    };
    let progress = shutdown.poll_complete(now).map_err(ReactorError::host)?;
    if progress != WorkerShutdownPoll::Pending {
        *worker = None;
    }
    Ok(progress)
}

fn poll_scram_proof(
    worker: &mut Option<super::super::scram_proof::ScramProofShutdown>,
    now: Moment,
) -> Result<WorkerShutdownPoll, ReactorError> {
    let Some(shutdown) = worker else {
        return Ok(WorkerShutdownPoll::Complete);
    };
    let progress = shutdown.poll_complete(now).map_err(ReactorError::host)?;
    if progress != WorkerShutdownPoll::Pending {
        *worker = None;
    }
    Ok(progress)
}
