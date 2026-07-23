//! One bounded public invalidation barrier per coordinator key.

use kafka_driver_core::{CoordinatorEpoch, CoordinatorState};

use crate::{InvalidationDisposition, completion::CompletionSender};

use super::entry::CoordinatorEntry;

pub(super) struct CoordinatorInvalidation {
    after: CoordinatorEpoch,
    completion: CompletionSender<InvalidationDisposition>,
}

impl CoordinatorInvalidation {
    pub(super) const fn new(
        after: CoordinatorEpoch,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Self {
        Self { after, completion }
    }

    pub(super) const fn after(&self) -> CoordinatorEpoch {
        self.after
    }
}

impl CoordinatorEntry {
    pub(super) fn settle_invalidation(&mut self) {
        let Some(pending) = self.invalidation.take() else {
            return;
        };
        let disposition = match self.machine.current() {
            Some(route) if route.epoch() > pending.after => Some(InvalidationDisposition::Applied),
            _ if matches!(self.machine.state(), CoordinatorState::Unknown { .. }) => {
                Some(InvalidationDisposition::Unavailable)
            }
            _ => None,
        };
        if let Some(disposition) = disposition {
            let _ = pending.completion.complete(disposition);
        } else {
            self.invalidation = Some(pending);
        }
    }

    pub(super) fn fail_invalidation(&mut self) {
        if let Some(pending) = self.invalidation.take() {
            let _ = pending
                .completion
                .complete(InvalidationDisposition::Unavailable);
        }
    }
}
