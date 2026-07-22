//! Identities for reactor transport resources and reconnect generations.

/// Identity of one transport resource registered with the reactor.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransportId(u64);

impl TransportId {
    /// Creates an identity from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Generation of a broker connection across reconnects.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionEpoch(u64);

impl ConnectionEpoch {
    /// Creates an epoch from its broker-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the broker-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}
