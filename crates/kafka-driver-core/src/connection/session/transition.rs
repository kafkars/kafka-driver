//! Ordered semantic effects returned by one Kafka session transition.

use super::KafkaSessionEffect;

/// How Kafka session policy treated one input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaSessionDisposition {
    /// The input advanced owned session state.
    Applied,
    /// The input was current but required no work.
    Ignored,
    /// The input did not match the current session stage.
    IgnoredStale,
    /// The input identified a terminal protocol fault.
    Fault,
}

/// Semantic effects and disposition produced by one session input.
#[derive(Debug, Eq, PartialEq)]
pub struct KafkaSessionTransition {
    effects: Vec<KafkaSessionEffect>,
    disposition: KafkaSessionDisposition,
}

impl KafkaSessionTransition {
    pub(super) const fn new(
        effects: Vec<KafkaSessionEffect>,
        disposition: KafkaSessionDisposition,
    ) -> Self {
        Self {
            effects,
            disposition,
        }
    }

    /// Borrows ordered semantic effects.
    pub fn effects(&self) -> &[KafkaSessionEffect] {
        &self.effects
    }

    /// Consumes the transition and returns ordered semantic effects.
    pub fn into_effects(self) -> Vec<KafkaSessionEffect> {
        self.effects
    }

    /// Returns how the input affected current session policy.
    pub const fn disposition(&self) -> KafkaSessionDisposition {
        self.disposition
    }
}

pub(super) struct Decision {
    pub(super) effects: Vec<KafkaSessionEffect>,
    pub(super) disposition: KafkaSessionDisposition,
}

impl Decision {
    pub(super) const fn applied(effects: Vec<KafkaSessionEffect>) -> Self {
        Self {
            effects,
            disposition: KafkaSessionDisposition::Applied,
        }
    }

    pub(super) const fn ignored() -> Self {
        Self {
            effects: Vec::new(),
            disposition: KafkaSessionDisposition::Ignored,
        }
    }

    pub(super) const fn stale() -> Self {
        Self {
            effects: Vec::new(),
            disposition: KafkaSessionDisposition::IgnoredStale,
        }
    }

    pub(super) const fn fault(effects: Vec<KafkaSessionEffect>) -> Self {
        Self {
            effects,
            disposition: KafkaSessionDisposition::Fault,
        }
    }
}
