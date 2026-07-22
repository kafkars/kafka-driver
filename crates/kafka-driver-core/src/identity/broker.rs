//! Kafka broker identity and driver-owned metadata generations.

use std::{error::Error, fmt};

/// Nonnegative Kafka node identity advertised by cluster metadata.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BrokerId(i32);

impl BrokerId {
    /// Validates a Kafka node identity before it enters routing policy.
    pub const fn new(value: i32) -> Result<Self, BrokerIdError> {
        if value < 0 {
            return Err(BrokerIdError { value });
        }
        Ok(Self(value))
    }

    /// Returns the Kafka node identity.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Monotonic driver-local identity of one accepted metadata snapshot.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetadataGeneration(u64);

impl MetadataGeneration {
    /// Creates a generation from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local generation value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next generation, or `None` when identity space is exhausted.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// Rejection of a Kafka sentinel or otherwise negative broker node ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerIdError {
    value: i32,
}

impl BrokerIdError {
    /// Returns the rejected Kafka node ID.
    pub const fn value(self) -> i32 {
        self.value
    }
}

impl fmt::Display for BrokerIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker ID {} must be nonnegative", self.value)
    }
}

impl Error for BrokerIdError {}
