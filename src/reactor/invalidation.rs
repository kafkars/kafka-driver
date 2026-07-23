//! One public route invalidation paired with its causal outcome and completion.

use kafka_driver_core::OutcomeStamp;

use crate::{InvalidationDisposition, completion::CompletionSender};

/// Ownership transferred from command dispatch into one route-specific owner.
pub(in crate::reactor) struct RouteInvalidation<R> {
    route: R,
    observed_at: OutcomeStamp,
    completion: CompletionSender<InvalidationDisposition>,
}

/// Public callers subscribed to one route target's terminal evidence outcome.
pub(in crate::reactor) struct InvalidationSubscribers {
    subscribers: Vec<CompletionSender<InvalidationDisposition>>,
}

impl InvalidationSubscribers {
    pub(in crate::reactor) fn new(first: CompletionSender<InvalidationDisposition>) -> Self {
        Self {
            subscribers: vec![first],
        }
    }

    pub(in crate::reactor) fn subscribe(
        &mut self,
        completion: CompletionSender<InvalidationDisposition>,
    ) {
        self.subscribers.push(completion);
    }

    pub(in crate::reactor) const fn len(&self) -> usize {
        self.subscribers.len()
    }

    pub(in crate::reactor) fn complete(self, disposition: InvalidationDisposition) {
        for subscriber in self.subscribers {
            let _ = subscriber.complete(disposition);
        }
    }
}

impl<R> RouteInvalidation<R> {
    pub(in crate::reactor) const fn new(
        route: R,
        observed_at: OutcomeStamp,
        completion: CompletionSender<InvalidationDisposition>,
    ) -> Self {
        Self {
            route,
            observed_at,
            completion,
        }
    }

    pub(in crate::reactor) fn into_parts(
        self,
    ) -> (R, OutcomeStamp, CompletionSender<InvalidationDisposition>) {
        (self.route, self.observed_at, self.completion)
    }
}
