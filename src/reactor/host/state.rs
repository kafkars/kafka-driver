//! Terminal hosting state and shared shutdown barrier settlement.

use super::{Reactor, ReactorError, TurnOutcome};
use crate::reactor::direct_plaintext::DirectBackend;

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
    ) -> Result<Option<TurnOutcome>, ReactorError> {
        let backend_terminal = if let Some(cluster) = self.backend.cluster() {
            cluster.is_terminal().map_err(ReactorError::host)?
        } else if let Some(direct) = self.backend.direct() {
            DirectBackend::is_terminal(direct)
        } else {
            #[cfg(test)]
            {
                self.backend
                    .legacy()
                    .is_some_and(|legacy| legacy.brokers.is_terminal())
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        if self.state != HostState::Draining || !backend_terminal {
            return Ok(None);
        }
        self.resolution = None;
        self.metadata = None;
        self.coordinator = None;
        let resolver_stopped = poll_resolver(&mut self.resolver_shutdown)?;
        let scram_stopped = poll_scram_proof(&mut self.scram_proof_shutdown)?;
        if !resolver_stopped || !scram_stopped {
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
) -> Result<bool, ReactorError> {
    let Some(shutdown) = worker else {
        return Ok(true);
    };
    if !shutdown.poll_complete().map_err(ReactorError::host)? {
        return Ok(false);
    }
    *worker = None;
    Ok(true)
}

fn poll_scram_proof(
    worker: &mut Option<super::super::scram_proof::ScramProofShutdown>,
) -> Result<bool, ReactorError> {
    let Some(shutdown) = worker else {
        return Ok(true);
    };
    if !shutdown.poll_complete().map_err(ReactorError::host)? {
        return Ok(false);
    }
    *worker = None;
    Ok(true)
}
