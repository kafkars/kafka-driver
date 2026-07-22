//! Ordered coordinator effects paired with explicit demand disposition.

use super::CoordinatorEffect;

/// Whether coordinator policy changed, coalesced, or ignored one input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorDisposition {
    /// The input changed discovery or route ownership.
    Applied,
    /// A current route already satisfies resolution demand.
    AlreadyKnown,
    /// Existing in-flight discovery represents this demand.
    Coalesced,
    /// One explicitly newer discovery was queued behind current work.
    RefreshQueued,
    /// Returned work or invalidation belonged to an older identity.
    IgnoredStale,
}

/// Complete deterministic result of one coordinator input.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatorTransition {
    effects: Vec<CoordinatorEffect>,
    disposition: CoordinatorDisposition,
}

impl CoordinatorTransition {
    pub(super) const fn new(
        effects: Vec<CoordinatorEffect>,
        disposition: CoordinatorDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Returns how the input related to current discovery ownership.
    pub const fn disposition(&self) -> CoordinatorDisposition {
        self.disposition
    }

    /// Borrows ordered external work emitted by this transition.
    pub fn effects(&self) -> &[CoordinatorEffect] {
        &self.effects
    }

    /// Transfers ordered effects to the external interpreter.
    pub fn into_effects(self) -> Vec<CoordinatorEffect> {
        self.effects
    }
}
