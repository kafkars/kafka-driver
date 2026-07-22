//! Ordered bootstrap retry effects paired with explicit input disposition.

use super::BootstrapRetryEffect;

/// Whether one retry observation matched current policy ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapRetryDisposition {
    /// Current retry state accepted the observation.
    Applied,
    /// No retry wait currently owns this observation.
    Ignored,
}

/// Complete deterministic result of applying one bootstrap retry observation.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapRetryTransition {
    effects: Vec<BootstrapRetryEffect>,
    disposition: BootstrapRetryDisposition,
}

impl BootstrapRetryTransition {
    pub(super) const fn new(
        effects: Vec<BootstrapRetryEffect>,
        disposition: BootstrapRetryDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Returns how the input related to current retry ownership.
    pub const fn disposition(&self) -> BootstrapRetryDisposition {
        self.disposition
    }

    /// Borrows the ordered action emitted by this transition.
    pub fn effects(&self) -> &[BootstrapRetryEffect] {
        &self.effects
    }
}
