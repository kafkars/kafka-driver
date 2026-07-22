//! Explicit topic and partition bounds for one retained metadata generation.

use std::num::NonZeroUsize;

const DEFAULT_MAX_TOPICS: NonZeroUsize = nonzero(10_000);
const DEFAULT_MAX_PARTITIONS: NonZeroUsize = nonzero(100_000);

/// Maximum topic and known-leader facts retained in one immutable snapshot.
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionLeaderLimits {
    max_topics: NonZeroUsize,
    max_partitions: NonZeroUsize,
}

impl PartitionLeaderLimits {
    /// Creates explicit independent topic and partition bounds.
    pub const fn new(max_topics: NonZeroUsize, max_partitions: NonZeroUsize) -> Self {
        Self {
            max_topics,
            max_partitions,
        }
    }

    /// Returns the maximum distinct topics retained in one generation.
    pub const fn max_topics(self) -> NonZeroUsize {
        self.max_topics
    }

    /// Returns the maximum known partition leaders retained in one generation.
    pub const fn max_partitions(self) -> NonZeroUsize {
        self.max_partitions
    }

    /// Returns the reference defaults without invoking trait dispatch.
    pub const fn defaults() -> Self {
        Self::new(DEFAULT_MAX_TOPICS, DEFAULT_MAX_PARTITIONS)
    }
}

impl Default for PartitionLeaderLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("partition metadata defaults must be nonzero");
    };
    value
}
