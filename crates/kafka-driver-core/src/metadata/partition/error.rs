//! Sanitized construction failures for bounded partition-leader facts.

use std::{error::Error, fmt};

use crate::PartitionId;

/// Why one partition-leader set could not be retained canonically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionLeaderSetError {
    /// The number of known partition leaders exceeded its explicit bound.
    PartitionCapacity {
        /// Maximum retained leader count.
        limit: usize,
    },
    /// The number of distinct topics exceeded its explicit bound.
    TopicCapacity {
        /// Maximum retained topic count.
        limit: usize,
    },
    /// One topic-partition key appeared more than once.
    DuplicatePartition {
        /// Duplicate partition index; the topic name remains out of diagnostics.
        partition: PartitionId,
    },
}

impl fmt::Display for PartitionLeaderSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartitionCapacity { limit } => {
                write!(formatter, "partition leader count exceeds limit {limit}")
            }
            Self::TopicCapacity { limit } => {
                write!(
                    formatter,
                    "partition leader topic count exceeds limit {limit}"
                )
            }
            Self::DuplicatePartition { partition } => write!(
                formatter,
                "partition {} appears more than once for one topic",
                partition.get()
            ),
        }
    }
}

impl Error for PartitionLeaderSetError {}
