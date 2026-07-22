//! Ordered metadata effects paired with explicit refresh disposition.

use super::MetadataEffect;

/// Whether metadata policy applied, coalesced, or rejected one input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataDisposition {
    /// The input changed refresh or snapshot ownership.
    Applied,
    /// Existing in-flight work now represents this additional demand.
    Coalesced,
    /// Returned work or invalidation belonged to an older identity or generation.
    IgnoredStale,
}

/// Complete deterministic result of one metadata input.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTransition {
    effects: Vec<MetadataEffect>,
    disposition: MetadataDisposition,
}

impl MetadataTransition {
    pub(super) const fn new(
        effects: Vec<MetadataEffect>,
        disposition: MetadataDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Returns how the input related to current metadata ownership.
    pub const fn disposition(&self) -> MetadataDisposition {
        self.disposition
    }

    /// Borrows ordered external work emitted by this transition.
    pub fn effects(&self) -> &[MetadataEffect] {
        &self.effects
    }

    /// Transfers ordered effects to the external interpreter.
    pub fn into_effects(self) -> Vec<MetadataEffect> {
        self.effects
    }
}
