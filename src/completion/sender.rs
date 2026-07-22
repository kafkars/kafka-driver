//! Single-use producer for publishing one terminal completion result.

use super::state::Shared;

pub(crate) struct CompletionSender<T> {
    shared: Shared<T>,
    settled: bool,
}

impl<T> CompletionSender<T> {
    pub(super) const fn new(shared: Shared<T>) -> Self {
        Self {
            shared,
            settled: false,
        }
    }

    pub(crate) fn complete(mut self, value: T) -> Result<(), T> {
        let outcome = self.shared.complete(value);
        self.settled = true;
        outcome
    }

    #[allow(
        dead_code,
        reason = "M1 locks the producer cancellation contract before M2 call machines consume it"
    )]
    pub(crate) fn is_cancellation_requested(&self) -> bool {
        self.shared.is_cancellation_requested()
    }
}

impl<T> Drop for CompletionSender<T> {
    fn drop(&mut self) {
        if !self.settled {
            self.shared.close_sender();
        }
    }
}
