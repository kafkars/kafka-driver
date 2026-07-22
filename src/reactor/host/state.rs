//! Terminal hosting state and bounded shutdown completion ownership.

use std::num::NonZeroUsize;

use crate::completion::CompletionSender;

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
