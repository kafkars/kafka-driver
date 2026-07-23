//! One bounded public invalidation barrier per coordinator key.

use kafka_driver_core::{CoordinatorRoute, CoordinatorState};

use crate::{
    InvalidationDisposition, completion::CompletionSender, reactor::InvalidationSubscribers,
};

use super::CoordinatorOwner;

pub(super) struct CoordinatorInvalidation {
    target: CoordinatorRoute,
    subscribers: InvalidationSubscribers,
}

impl CoordinatorInvalidation {
    pub(super) fn new(
        target: CoordinatorRoute,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Self {
        Self {
            target,
            subscribers: InvalidationSubscribers::new(completion),
        }
    }

    pub(super) fn matches(&self, route: &CoordinatorRoute) -> bool {
        self.target.is_same_target(route)
    }

    pub(super) fn subscribe(&mut self, completion: CompletionSender<InvalidationDisposition>) {
        self.subscribers.subscribe(completion);
    }

    fn len(&self) -> usize {
        self.subscribers.len()
    }

    fn settled_disposition(
        &self,
        owner: &CoordinatorOwner,
        index: usize,
    ) -> Option<InvalidationDisposition> {
        let machine = &owner.entries[index].machine;
        if machine.revocation_pending(&self.target) {
            return None;
        }
        if matches!(
            owner.entries[index].machine.state(),
            CoordinatorState::Unknown { .. }
        ) {
            Some(InvalidationDisposition::Unavailable)
        } else {
            Some(InvalidationDisposition::Applied)
        }
    }

    fn complete(self, disposition: InvalidationDisposition) {
        self.subscribers.complete(disposition);
    }
}

impl CoordinatorOwner {
    pub(super) fn settle_invalidation(&mut self, index: usize) {
        let Some(pending) = self.entries[index].invalidation.take() else {
            return;
        };
        if let Some(disposition) = pending.settled_disposition(self, index) {
            let subscribers = pending.len();
            pending.complete(disposition);
            self.release_invalidation_subscribers(subscribers);
        } else {
            self.entries[index].invalidation = Some(pending);
        }
    }

    pub(super) fn fail_all_invalidations(&mut self) {
        for index in 0..self.entries.len() {
            let Some(pending) = self.entries[index].invalidation.take() else {
                continue;
            };
            let subscribers = pending.len();
            pending.complete(InvalidationDisposition::Unavailable);
            self.release_invalidation_subscribers(subscribers);
        }
    }

    pub(super) fn has_invalidation_capacity(&self) -> bool {
        self.invalidation_subscribers < self.limits.invalidation_waiters().get()
    }

    pub(super) fn retain_invalidation_subscriber(&mut self) {
        debug_assert!(self.has_invalidation_capacity());
        self.invalidation_subscribers += 1;
    }

    fn release_invalidation_subscribers(&mut self, subscribers: usize) {
        debug_assert!(subscribers <= self.invalidation_subscribers);
        self.invalidation_subscribers -= subscribers;
    }
}
