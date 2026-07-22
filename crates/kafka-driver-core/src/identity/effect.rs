//! Identities for requested external effects and scheduled timers.

/// Identity of one effect requested from the reactor.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectId(u64);

impl EffectId {
    /// Creates an identity from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Identity of one scheduled timer.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimerId(u64);

impl TimerId {
    /// Creates an identity from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}
