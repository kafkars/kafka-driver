//! One bounded public invalidation barrier per coordinator key.

use kafka_driver_core::{CoordinatorRoute, CoordinatorState, OutcomeStamp};

use crate::{InvalidationDisposition, completion::CompletionSender};

use super::entry::CoordinatorEntry;

pub(super) struct CoordinatorInvalidation {
    target: CoordinatorRoute,
    observed_at: OutcomeStamp,
    completion: CompletionSender<InvalidationDisposition>,
}

impl CoordinatorInvalidation {
    pub(super) const fn new(
        target: CoordinatorRoute,
        observed_at: OutcomeStamp,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Self {
        Self {
            target,
            observed_at,
            completion,
        }
    }

    pub(super) fn matches(&self, route: &CoordinatorRoute) -> bool {
        self.target == *route
    }
}

impl CoordinatorEntry {
    pub(super) fn settle_invalidation(&mut self) {
        let Some(pending) = self.invalidation.take() else {
            return;
        };
        let disposition = match self.machine.current() {
            Some(route) if route.evidence_stamp().is_after(pending.observed_at) => {
                Some(InvalidationDisposition::Applied)
            }
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
