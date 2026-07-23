//! Bounded coordinator lookup keys and driver-owned discovery generations.

use std::{error::Error, fmt};

/// Kafka coordinator namespace selected by one semantic call.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CoordinatorKind {
    /// Consumer-group and classic group coordinator.
    Group,
    /// Transaction coordinator selected by transactional ID.
    Transaction,
    /// Share-group coordinator.
    Share,
}

/// Nonempty coordinator key bounded to Kafka's legacy string domain.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorKey {
    kind: CoordinatorKind,
    value: String,
}

impl CoordinatorKey {
    /// Maximum UTF-8 bytes representable by every supported `FindCoordinator` version.
    pub const MAX_BYTES: usize = i16::MAX as usize;

    /// Validates and owns one group, transaction, or share coordinator key.
    pub fn new(
        kind: CoordinatorKind,
        value: impl Into<String>,
    ) -> Result<Self, CoordinatorKeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CoordinatorKeyError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(CoordinatorKeyError::TooLong {
                bytes: value.len(),
                limit: Self::MAX_BYTES,
            });
        }
        Ok(Self { kind, value })
    }

    /// Returns the coordinator namespace.
    pub const fn kind(&self) -> CoordinatorKind {
        self.kind
    }

    /// Returns the validated lookup key.
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Returns bytes reserved by the owned coordinator-key buffer.
    pub fn heap_bytes(&self) -> usize {
        self.value.capacity()
    }
}

/// Why a coordinator key was rejected before persistent ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorKeyError {
    /// The key contained no UTF-8 bytes.
    Empty,
    /// The key exceeded the cross-version protocol bound.
    TooLong {
        /// Observed UTF-8 byte count.
        bytes: usize,
        /// Maximum accepted UTF-8 byte count.
        limit: usize,
    },
}

impl fmt::Display for CoordinatorKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("coordinator key must not be empty"),
            Self::TooLong { bytes, limit } => {
                write!(
                    formatter,
                    "coordinator key uses {bytes} bytes, limit is {limit}"
                )
            }
        }
    }
}

impl Error for CoordinatorKeyError {}

/// Monotonic identity of one accepted discovery for a coordinator key.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorEpoch(u64);

impl CoordinatorEpoch {
    /// Creates an epoch from its driver-local numeric value.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the driver-local numeric value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next epoch, or `None` at identity exhaustion.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}
