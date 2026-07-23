//! Typed positions in the reactor's single causal observation sequence.

/// Position assigned when an external metadata or coordinator query begins.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EvidenceStamp(u64);

impl EvidenceStamp {
    /// First representable stamp used by unstamped deterministic fixtures.
    pub const ORIGIN: Self = Self(0);

    /// Creates a stamp from its reactor-local sequence value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the reactor-local sequence value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns whether this query began after the observed broker outcome.
    pub const fn is_after(self, outcome: OutcomeStamp) -> bool {
        self.0 > outcome.0
    }
}

/// Position assigned when one routed broker response becomes observable.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutcomeStamp(u64);

impl OutcomeStamp {
    /// First representable stamp used by deterministic boundary scenarios.
    pub const ORIGIN: Self = Self(0);

    /// Creates a stamp from its reactor-local sequence value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the reactor-local sequence value.
    pub const fn get(self) -> u64 {
        self.0
    }
}
