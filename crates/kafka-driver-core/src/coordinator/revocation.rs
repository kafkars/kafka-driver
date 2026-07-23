//! Semantic coordinator withdrawal and its latest causal failure watermark.

use crate::{CoordinatorRoute, OutcomeStamp};

#[derive(Debug)]
pub(super) struct CoordinatorRevocation {
    target: CoordinatorRoute,
    required_after: OutcomeStamp,
}

impl CoordinatorRevocation {
    pub(super) const fn new(target: CoordinatorRoute, required_after: OutcomeStamp) -> Self {
        Self {
            target,
            required_after,
        }
    }

    pub(super) fn matches(&self, route: &CoordinatorRoute) -> bool {
        self.target.is_same_target(route)
    }

    pub(super) fn observe(&mut self, observed_at: OutcomeStamp) {
        self.required_after = self.required_after.max(observed_at);
    }

    pub(super) fn accepts(&self, route: &CoordinatorRoute) -> bool {
        route.evidence_stamp().is_after(self.required_after)
    }
}
