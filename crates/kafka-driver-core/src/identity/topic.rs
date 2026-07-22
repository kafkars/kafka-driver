//! Bounded Kafka topic names retained independently of generated DTO storage.

use std::{error::Error, fmt};

/// Nonempty bounded topic name used as a deterministic routing key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicName(String);

impl TopicName {
    /// Maximum UTF-8 bytes retained for one Kafka topic name.
    pub const MAX_BYTES: usize = 249;

    /// Validates and owns a topic name before it enters metadata indexes.
    pub fn new(value: impl Into<String>) -> Result<Self, TopicNameError> {
        let value = value.into();
        if value.is_empty() {
            return Err(TopicNameError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(TopicNameError::TooLong {
                bytes: value.len(),
                limit: Self::MAX_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Returns the validated topic name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why a topic name was rejected before persistent metadata ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicNameError {
    /// The topic name contained no UTF-8 bytes.
    Empty,
    /// The topic name exceeded the Kafka topic-name bound.
    TooLong {
        /// Observed UTF-8 byte count.
        bytes: usize,
        /// Maximum accepted UTF-8 byte count.
        limit: usize,
    },
}

impl fmt::Display for TopicNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("topic name must not be empty"),
            Self::TooLong { bytes, limit } => {
                write!(formatter, "topic name uses {bytes} bytes, limit is {limit}")
            }
        }
    }
}

impl Error for TopicNameError {}
