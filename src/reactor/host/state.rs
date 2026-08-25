//! Terminal hosting state and shared shutdown barrier settlement.

use super::{Reactor, ReactorError, TurnOutcome};
use crate::reactor::direct_plaintext::DirectPlaintextOwner;

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
        let backend_terminal = self.backend.legacy().map_or_else(
            || {
                self.backend
                    .direct()
                    .is_some_and(DirectPlaintextOwner::is_terminal)
            },
            |legacy| legacy.brokers.is_terminal(),
        );
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
