//! Public bounds for internal SCRAM proof work and outcome fairness.

use std::num::NonZeroUsize;

const DEFAULT_REQUEST_CAPACITY: NonZeroUsize = nonzero(256);
const DEFAULT_OUTCOME_CAPACITY: NonZeroUsize = nonzero(256);
const DEFAULT_OUTCOME_BUDGET: NonZeroUsize = nonzero(64);

/// Resource bounds for one SCRAM proof worker owned by an I/O shard.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScramProofLimits {
    request_capacity: NonZeroUsize,
    outcome_capacity: NonZeroUsize,
    outcome_budget: NonZeroUsize,
}

impl ScramProofLimits {
    /// Creates explicit pending-request, completed-outcome, and turn bounds.
    pub const fn new(
        request_capacity: NonZeroUsize,
        outcome_capacity: NonZeroUsize,
        outcome_budget: NonZeroUsize,
    ) -> Self {
        Self {
            request_capacity,
            outcome_capacity,
            outcome_budget,
        }
    }

    /// Returns maximum queued proof derivations.
    pub const fn request_capacity(self) -> NonZeroUsize {
        self.request_capacity
    }

    /// Returns maximum completed proofs retained before reactor collection.
    pub const fn outcome_capacity(self) -> NonZeroUsize {
        self.outcome_capacity
    }

    /// Returns maximum completed proofs collected in one reactor turn.
    pub const fn outcome_budget(self) -> NonZeroUsize {
        self.outcome_budget
    }

    pub(super) const fn defaults() -> Self {
        Self::new(
            DEFAULT_REQUEST_CAPACITY,
            DEFAULT_OUTCOME_CAPACITY,
            DEFAULT_OUTCOME_BUDGET,
        )
    }
}

impl Default for ScramProofLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("SCRAM proof defaults must be nonzero");
    };
    value
}
