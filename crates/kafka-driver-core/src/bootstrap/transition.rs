//! Ordered bootstrap effects paired with explicit input disposition.

use super::BootstrapEffect;

/// Whether one bootstrap input changed current ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapDisposition {
    /// Current policy state accepted the input.
    Applied,
    /// Returned identities did not match current external work.
    IgnoredStale,
    /// A start command arrived while resolution was already in flight.
    IgnoredBusy,
}

/// Complete deterministic result of applying one bootstrap input.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapTransition {
    effects: Vec<BootstrapEffect>,
    disposition: BootstrapDisposition,
}

impl BootstrapTransition {
    pub(super) const fn new(
        effects: Vec<BootstrapEffect>,
        disposition: BootstrapDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Returns how the input related to current ownership.
    pub const fn disposition(&self) -> BootstrapDisposition {
        self.disposition
    }

    /// Borrows ordered external work emitted by the transition.
    pub fn effects(&self) -> &[BootstrapEffect] {
        &self.effects
    }

    /// Transfers ordered effects to their interpreter.
    pub fn into_effects(self) -> Vec<BootstrapEffect> {
        self.effects
    }
}
