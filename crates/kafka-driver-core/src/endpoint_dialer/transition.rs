//! Ordered effects returned by one endpoint-dialer transition.

use super::EndpointDialerEffect;

/// Deterministic result of applying one dialing input.
#[derive(Debug, Eq, PartialEq)]
pub struct EndpointDialerTransition {
    effects: Vec<EndpointDialerEffect>,
}

impl EndpointDialerTransition {
    pub(super) const fn applied(effects: Vec<EndpointDialerEffect>) -> Self {
        Self { effects }
    }

    pub(super) const fn ignored() -> Self {
        Self {
            effects: Vec::new(),
        }
    }

    /// Borrows ordered effects for interpretation by the reactor owner.
    pub fn effects(&self) -> &[EndpointDialerEffect] {
        &self.effects
    }

    /// Consumes the transition and returns its ordered effects.
    pub fn into_effects(self) -> Vec<EndpointDialerEffect> {
        self.effects
    }
}
