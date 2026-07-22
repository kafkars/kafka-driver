//! Single-consumer blocking and task-waker views over shared completion state.

use std::task::{Context, Poll};

use super::{CancellationRequest, CompletionError, state::Shared};

pub(crate) struct CompletionReceiver<T> {
    shared: Shared<T>,
}

impl<T> CompletionReceiver<T> {
    pub(super) const fn new(shared: Shared<T>) -> Self {
        Self { shared }
    }

    pub(crate) fn wait(self) -> Result<T, CompletionError> {
        self.shared.wait()
    }

    pub(crate) fn poll_result(&self, context: &Context<'_>) -> Poll<Result<T, CompletionError>> {
        self.shared.poll_result(context)
    }

    pub(crate) fn request_cancellation(&self) -> CancellationRequest {
        self.shared.request_cancellation()
    }
}

impl<T> Drop for CompletionReceiver<T> {
    fn drop(&mut self) {
        self.shared.abandon_receiver();
    }
}
