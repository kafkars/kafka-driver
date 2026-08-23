//! Driver completion adapters over Calandria's bounded shutdown barrier.

use std::{fmt, num::NonZeroUsize};

use super::CompletionReceiver;

pub(crate) fn shutdown_barrier(capacity: NonZeroUsize) -> (ShutdownRequester, ShutdownCompleter) {
    let (requester, completer) = calandria::shutdown_barrier(capacity);
    (ShutdownRequester { inner: requester }, completer)
}

#[derive(Clone)]
pub(crate) struct ShutdownRequester {
    inner: calandria::ShutdownRequester,
}

impl ShutdownRequester {
    pub(crate) fn subscribe<E>(
        &self,
        request: impl FnOnce() -> Result<(), E>,
    ) -> Result<CompletionReceiver<()>, ShutdownSubscribeError<E>> {
        self.inner
            .subscribe(request)
            .map(CompletionReceiver::new)
            .map_err(ShutdownSubscribeError::from_calandria)
    }
}

impl fmt::Debug for ShutdownRequester {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

pub(crate) type ShutdownCompleter = calandria::ShutdownCompleter;

pub(crate) enum ShutdownSubscribeError<E> {
    Full,
    Closed,
    Request(E),
}

impl<E> ShutdownSubscribeError<E> {
    fn from_calandria(error: calandria::ShutdownSubscribeError<E>) -> Self {
        match error {
            calandria::ShutdownSubscribeError::Full => Self::Full,
            calandria::ShutdownSubscribeError::Request(source) => Self::Request(source),
            // Calandria's error is non-exhaustive. Closed and future terminal
            // variants fail closed until the driver gives them a public shape.
            _ => Self::Closed,
        }
    }
}
