//! Test-only aggregate response-registry failure inspection.

use super::{
    CompletionDisposition, FailedResponses, RequestError, ResponseCloseReason, ResponseRegistry,
};

impl ResponseRegistry {
    pub(crate) fn fail_all(&mut self, reason: ResponseCloseReason) -> FailedResponses {
        let mut failed = FailedResponses::default();
        while let Some(slot) = self.slots.pop_front() {
            failed.total += 1;
            if slot.fail(RequestError::ConnectionClosed(reason))
                == CompletionDisposition::ReceiverAbandoned
            {
                failed.abandoned += 1;
            }
        }
        failed
    }

    pub(crate) fn pending(&self) -> usize {
        self.slots.len()
    }
}
