//! Runtime-neutral public handle for one eventual driver result.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::completion::{CancellationRequest, CompletionError, CompletionReceiver};

/// A single-consumer handle for one eventual driver result.
#[must_use = "dropping a call requests cancellation"]
pub struct Call<T> {
    completion: CompletionReceiver<T>,
}

impl<T> Call<T> {
    pub(crate) const fn new(completion: CompletionReceiver<T>) -> Self {
        Self { completion }
    }

    /// Blocks the current thread until the call completes or its producer closes.
    pub fn wait(self) -> Result<T, CompletionError> {
        self.completion.wait()
    }

    /// Requests cancellation without claiming the broker did not receive work.
    pub fn request_cancellation(&self) -> CancellationRequest {
        self.completion.request_cancellation()
    }
}

impl<T> Future for Call<T> {
    type Output = Result<T, CompletionError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.completion.poll_result(context)
    }
}

impl<T> fmt::Debug for Call<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Call").finish_non_exhaustive()
    }
}
