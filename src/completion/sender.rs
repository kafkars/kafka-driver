//! Driver ownership and footprint vocabulary over a Calandria completer.

use std::{
    sync::{Condvar, Mutex},
    task::Waker,
};

use super::CompletionError;

pub(crate) struct CompletionSender<T> {
    inner: calandria::Completer<T>,
}

impl<T> CompletionSender<T> {
    pub(super) const fn new(inner: calandria::Completer<T>) -> Self {
        Self { inner }
    }

    pub(crate) fn complete(self, value: T) -> Result<(), T> {
        self.inner.complete(value)
    }

    pub(crate) const fn retained_state_bytes() -> usize {
        // Conservatively projects Calandria's shared mutex/condvar allocation.
        size_of::<Mutex<Option<Result<T, CompletionError>>>>()
            .saturating_add(size_of::<Condvar>())
            .saturating_add(size_of::<Option<Waker>>())
            .saturating_add(size_of::<bool>())
    }
}
