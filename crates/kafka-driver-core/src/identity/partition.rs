//! Validated Kafka partition and leader-epoch identities.

use std::{error::Error, fmt};

/// Nonnegative partition index within one Kafka topic.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionId(i32);

impl PartitionId {
    /// Validates a partition index before it enters a metadata route.
    pub const fn new(value: i32) -> Result<Self, PartitionIdError> {
        if value < 0 {
            return Err(PartitionIdError { value });
        }
        Ok(Self(value))
    }

    /// Returns the Kafka partition index.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Nonnegative broker-issued epoch for one partition leader assignment.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeaderEpoch(i32);

impl LeaderEpoch {
    /// Validates a known leader epoch; Kafka's negative sentinel remains `None` outside this type.
    pub const fn new(value: i32) -> Result<Self, LeaderEpochError> {
        if value < 0 {
            return Err(LeaderEpochError { value });
        }
        Ok(Self(value))
    }

    /// Returns the broker-issued leader epoch.
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Rejection of a negative Kafka partition index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionIdError {
    value: i32,
}

impl PartitionIdError {
    /// Returns the rejected partition index.
    pub const fn value(self) -> i32 {
        self.value
    }
}

impl fmt::Display for PartitionIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "partition ID {} must be nonnegative", self.value)
    }
}

impl Error for PartitionIdError {}

/// Rejection of a negative value as a known leader epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderEpochError {
    value: i32,
}

impl LeaderEpochError {
    /// Returns the rejected leader epoch.
    pub const fn value(self) -> i32 {
        self.value
    }
}

impl fmt::Display for LeaderEpochError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "known leader epoch {} must be nonnegative",
            self.value
        )
    }
}

impl Error for LeaderEpochError {}
