//! Driver-compatible observation over one Calandria completion.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::CompletionError;

pub(crate) struct CompletionReceiver<T> {
    inner: RefCell<calandria::Completion<T>>,
}

impl<T> CompletionReceiver<T> {
    pub(super) const fn new(inner: calandria::Completion<T>) -> Self {
        Self {
            inner: RefCell::new(inner),
        }
    }

    pub(crate) fn wait(self) -> Result<T, CompletionError> {
        self.inner
            .into_inner()
            .wait()
            .map_err(CompletionError::from_calandria)
    }

    pub(crate) fn poll_result(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<Result<T, CompletionError>> {
        Future::poll(Pin::new(&mut *self.inner.borrow_mut()), context)
            .map(|result| result.map_err(CompletionError::from_calandria))
    }

    pub(crate) fn try_result(&self) -> Option<Result<T, CompletionError>> {
        self.inner
            .borrow_mut()
            .try_take()
            .map(|result| result.map_err(CompletionError::from_calandria))
    }
}
