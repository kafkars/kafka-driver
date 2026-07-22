//! Public queue, fairness, and answer-size bounds for blocking name resolution.

use std::num::NonZeroUsize;

const DEFAULT_REQUEST_CAPACITY: NonZeroUsize = NonZeroUsize::new(16).unwrap();
const DEFAULT_OUTCOME_CAPACITY: NonZeroUsize = NonZeroUsize::new(16).unwrap();
const DEFAULT_OUTCOME_BUDGET: NonZeroUsize = NonZeroUsize::new(16).unwrap();
const DEFAULT_MAX_ADDRESSES: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Resource policy for the driver's internal blocking DNS worker.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolverLimits {
    request_capacity: NonZeroUsize,
    outcome_capacity: NonZeroUsize,
    outcome_budget: NonZeroUsize,
    max_addresses: NonZeroUsize,
}

impl ResolverLimits {
    pub(super) const fn defaults() -> Self {
        Self::new(
            DEFAULT_REQUEST_CAPACITY,
            DEFAULT_OUTCOME_CAPACITY,
            DEFAULT_OUTCOME_BUDGET,
            DEFAULT_MAX_ADDRESSES,
        )
    }

    /// Creates explicit resolver queue, turn, and result bounds.
    pub const fn new(
        request_capacity: NonZeroUsize,
        outcome_capacity: NonZeroUsize,
        outcome_budget: NonZeroUsize,
        max_addresses: NonZeroUsize,
    ) -> Self {
        Self {
            request_capacity,
            outcome_capacity,
            outcome_budget,
            max_addresses,
        }
    }

    /// Returns the maximum unresolved requests retained by the worker queue.
    pub const fn request_capacity(self) -> NonZeroUsize {
        self.request_capacity
    }

    /// Returns the maximum completed outcomes waiting for reactor ownership.
    pub const fn outcome_capacity(self) -> NonZeroUsize {
        self.outcome_capacity
    }

    /// Returns the maximum resolver outcomes consumed by one reactor turn.
    pub const fn outcome_budget(self) -> NonZeroUsize {
        self.outcome_budget
    }

    /// Returns the maximum addresses inspected from one resolver answer.
    pub const fn max_addresses(self) -> NonZeroUsize {
        self.max_addresses
    }
}

impl Default for ResolverLimits {
    fn default() -> Self {
        Self::defaults()
    }
}
