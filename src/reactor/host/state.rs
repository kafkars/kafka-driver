//! Terminal hosting state and bounded shutdown completion ownership.

use std::num::NonZeroUsize;

use crate::completion::CompletionSender;

use super::{Reactor, ReactorError, TurnOutcome};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HostState {
    Running,
    DrainRequested,
    Draining,
    Shutdown,
}

pub(super) struct ShutdownWaiters {
    capacity: usize,
    completions: Vec<CompletionSender<()>>,
}

impl ShutdownWaiters {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            completions: Vec::with_capacity(capacity.get()),
        }
    }

    pub(super) fn admit(
        &mut self,
        completion: CompletionSender<()>,
    ) -> Result<(), CompletionSender<()>> {
        if self.completions.len() >= self.capacity {
            return Err(completion);
        }
        self.completions.push(completion);
        Ok(())
    }

    pub(super) fn complete_all(&mut self) {
        for completion in self.completions.drain(..) {
            let _ = completion.complete(());
        }
    }
}

impl Reactor {
    pub(super) fn finish_shutdown_if_terminal(
        &mut self,
        commands: usize,
    ) -> Result<Option<TurnOutcome>, ReactorError> {
        if self.state != HostState::Draining || !self.brokers.is_terminal() {
            return Ok(None);
        }
        self.resolution = None;
        self.metadata = None;
        self.coordinator = None;
        self.brokers.release_scram_proof_senders();
        if let Some(worker) = self.scram_proof.take() {
            worker.shutdown().map_err(ReactorError::host)?;
        }
        self.scram_proof_outcomes.clear();
        drop(self.commands.close());
        self.state = HostState::Shutdown;
        self.shutdown_waiters.complete_all();
        Ok(Some(TurnOutcome::Shutdown { commands }))
    }
}
