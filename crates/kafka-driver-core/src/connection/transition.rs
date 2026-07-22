//! Returned effects and sanitized record for one successful machine step.

use super::{ConnectionEffect, TransitionDisposition, TransitionRecord};

/// Effects and diagnostic record produced by one applied input.
#[derive(Debug, Eq, PartialEq)]
pub struct ConnectionTransition {
    effects: Vec<ConnectionEffect>,
    record: TransitionRecord,
}

impl ConnectionTransition {
    pub(super) const fn new(effects: Vec<ConnectionEffect>, record: TransitionRecord) -> Self {
        Self { effects, record }
    }

    /// Borrows ordered effects for interpretation by an external adapter.
    pub fn effects(&self) -> &[ConnectionEffect] {
        &self.effects
    }

    /// Consumes the transition and returns ordered effects.
    pub fn into_effects(self) -> Vec<ConnectionEffect> {
        self.effects
    }

    /// Returns the sanitized transition record.
    pub const fn record(&self) -> TransitionRecord {
        self.record
    }
}

pub(super) struct Decision {
    pub(super) effects: Vec<ConnectionEffect>,
    pub(super) disposition: TransitionDisposition,
}

impl Decision {
    pub(super) const fn applied(effects: Vec<ConnectionEffect>) -> Self {
        Self {
            effects,
            disposition: TransitionDisposition::Applied,
        }
    }

    pub(super) const fn rejected(effects: Vec<ConnectionEffect>) -> Self {
        Self {
            effects,
            disposition: TransitionDisposition::Rejected,
        }
    }

    pub(super) const fn ignored() -> Self {
        Self {
            effects: Vec::new(),
            disposition: TransitionDisposition::Ignored,
        }
    }

    pub(super) const fn stale() -> Self {
        Self {
            effects: Vec::new(),
            disposition: TransitionDisposition::IgnoredStale,
        }
    }

    pub(super) const fn fault(effects: Vec<ConnectionEffect>) -> Self {
        Self {
            effects,
            disposition: TransitionDisposition::Fault,
        }
    }
}
