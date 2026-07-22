//! Broker-resolution effects paired with explicit stale and ownership disposition.

use super::BrokerResolutionEffect;

/// Whether one input applied, was stale, busy, or named another broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerResolutionDisposition {
    /// The input changed or completed owned resolution.
    Applied,
    /// The input belongs to an older route, epoch, or external effect.
    IgnoredStale,
    /// The exact route is already resolving or terminal.
    IgnoredBusy,
    /// The route names a broker other than this machine's owner.
    RejectedBroker,
}

/// Complete deterministic result of one broker-resolution input.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerResolutionTransition {
    effects: Vec<BrokerResolutionEffect>,
    disposition: BrokerResolutionDisposition,
}

impl BrokerResolutionTransition {
    pub(super) const fn new(
        effects: Vec<BrokerResolutionEffect>,
        disposition: BrokerResolutionDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Returns how the input related to current ownership.
    pub const fn disposition(&self) -> BrokerResolutionDisposition {
        self.disposition
    }

    /// Borrows ordered external work emitted by the transition.
    pub fn effects(&self) -> &[BrokerResolutionEffect] {
        &self.effects
    }

    /// Transfers ordered effects to the external interpreter.
    pub fn into_effects(self) -> Vec<BrokerResolutionEffect> {
        self.effects
    }
}
