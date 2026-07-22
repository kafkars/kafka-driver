//! Ordered effects and disposition returned by one authentication input.

use super::AuthenticationEffect;

/// Whether one authentication input changed owned state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationDisposition {
    /// The input matched current ownership and was applied.
    Applied,
    /// The input was stale or invalid for the current stage.
    IgnoredStale,
}

/// Effects and disposition produced by one machine step.
#[derive(Debug, Eq, PartialEq)]
pub struct AuthenticationTransition {
    effects: Vec<AuthenticationEffect>,
    disposition: AuthenticationDisposition,
}

impl AuthenticationTransition {
    pub(super) const fn new(
        effects: Vec<AuthenticationEffect>,
        disposition: AuthenticationDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Borrows ordered effects for interpretation by the connection owner.
    pub fn effects(&self) -> &[AuthenticationEffect] {
        &self.effects
    }

    /// Consumes the transition and returns ordered effects.
    pub fn into_effects(self) -> Vec<AuthenticationEffect> {
        self.effects
    }

    /// Returns whether the input changed current ownership.
    pub const fn disposition(&self) -> AuthenticationDisposition {
        self.disposition
    }
}
