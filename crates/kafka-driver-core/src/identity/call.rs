//! Identities for public calls and multi-step logical operations.

/// Identity of one public logical call.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CallId(u64);

impl CallId {
    /// Creates an identity from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Identity of one multi-step logical operation.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an identity from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }
}
