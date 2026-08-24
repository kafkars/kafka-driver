//! Driver ownership and footprint vocabulary over a Calandria completer.

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

    pub(crate) fn retained_state_bytes() -> usize {
        usize::try_from(calandria::completion_retained_bytes::<T>().get()).unwrap_or(usize::MAX)
    }
}
