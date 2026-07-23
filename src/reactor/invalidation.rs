//! One public route invalidation paired with its causal outcome and completion.

use kafka_driver_core::OutcomeStamp;

use crate::{InvalidationDisposition, completion::CompletionSender};

/// Ownership transferred from command dispatch into one route-specific owner.
pub(in crate::reactor) struct RouteInvalidation<R> {
    route: R,
    observed_at: OutcomeStamp,
    completion: CompletionSender<InvalidationDisposition>,
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
