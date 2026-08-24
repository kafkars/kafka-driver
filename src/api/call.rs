//! Runtime-neutral public handle for one eventual driver result.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::completion::{CompletionError, CompletionReceiver};

/// A single-consumer handle for one eventual driver result.
#[must_use = "dropping a call abandons result observation; driver work may continue"]
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

    /// Abandons observation of this call's terminal result.
    ///
    /// This does not cancel driver or broker work and does not change delivery
    /// certainty. The driver continues normal bounded processing and discards
    /// the result when it reaches this closed receiver.
    pub fn abandon(self) {
        drop(self);
    }

    /// Takes the terminal result without blocking, or returns `None` while pending.
    ///
    /// A returned `Some` consumes the single result. Later calls, waits, or
    /// polls return [`CompletionError::Consumed`].
    pub fn try_result(&self) -> Option<Result<T, CompletionError>> {
        self.completion.try_result()
    }
}

impl<T> Future for Call<T> {
    type Output = Result<T, CompletionError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.completion.poll_result(context)
    }
}

impl<T> fmt::Debug for Call<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Call").finish_non_exhaustive()
    }
}
