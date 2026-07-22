//! Effects and disposition returned by one broker-machine step.

use super::BrokerEffect;

/// How broker policy treated one input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerDisposition {
    /// The input advanced state or emitted owned work.
    Applied,
    /// The input was current but required no work.
    Ignored,
    /// The input named an obsolete generation or timer.
    IgnoredStale,
}

/// Ordered broker effects and sanitized input disposition.
#[derive(Debug, Eq, PartialEq)]
pub struct BrokerTransition {
    effects: Vec<BrokerEffect>,
    disposition: BrokerDisposition,
}

impl BrokerTransition {
    pub(super) const fn applied(effects: Vec<BrokerEffect>) -> Self {
        Self {
            effects,
            disposition: BrokerDisposition::Applied,
        }
    }

    pub(super) const fn ignored() -> Self {
        Self {
            effects: Vec::new(),
            disposition: BrokerDisposition::Ignored,
        }
    }

    pub(super) const fn stale() -> Self {
        Self {
            effects: Vec::new(),
            disposition: BrokerDisposition::IgnoredStale,
        }
    }

    /// Borrows ordered external effects.
    pub fn effects(&self) -> &[BrokerEffect] {
        &self.effects
    }

    /// Consumes the transition and returns ordered external effects.
    pub fn into_effects(self) -> Vec<BrokerEffect> {
        self.effects
    }

    /// Returns how the input affected current broker state.
    pub const fn disposition(&self) -> BrokerDisposition {
        self.disposition
    }
}
