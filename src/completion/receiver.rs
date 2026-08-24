//! Driver-compatible observation over one Calandria completion.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::CompletionError;

pub(crate) struct CompletionReceiver<T> {
    inner: calandria::Completion<T>,
}

impl<T> CompletionReceiver<T> {
    pub(super) const fn new(inner: calandria::Completion<T>) -> Self {
        Self { inner }
    }

    pub(crate) fn wait(self) -> Result<T, CompletionError> {
        self.inner.wait().map_err(CompletionError::from_calandria)
    }

    pub(crate) fn poll_result(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<T, CompletionError>> {
        Future::poll(Pin::new(&mut self.inner), context)
            .map(|result| result.map_err(CompletionError::from_calandria))
    }

    pub(crate) fn try_result(&self) -> Option<Result<T, CompletionError>> {
        self.inner
            .try_take()
            .map(|result| result.map_err(CompletionError::from_calandria))
    }
}
