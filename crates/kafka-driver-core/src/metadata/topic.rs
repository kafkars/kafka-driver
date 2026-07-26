//! Canonical bounded logical partition counts retained independently of leader availability.

use std::{
    fmt,
    num::{NonZeroU32, NonZeroUsize},
};

use crate::TopicName;

/// Total logical partitions observed for one exact topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPartitionCount {
    topic: TopicName,
    count: NonZeroU32,
}

impl TopicPartitionCount {
    /// Creates one validated nonempty logical topic fact.
    pub const fn new(topic: TopicName, count: NonZeroU32) -> Self {
        Self { topic, count }
    }

    /// Borrows the topic whose complete logical range was observed.
    pub const fn topic(&self) -> &TopicName {
        &self.topic
    }

    /// Returns the total logical count, including partitions without known leaders.
    pub const fn count(&self) -> NonZeroU32 {
        self.count
    }
}

/// Immutable topic-name-ordered logical partition counts for one metadata snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPartitionCountSet {
    entries: Vec<TopicPartitionCount>,
}

impl TopicPartitionCountSet {
    /// Canonicalizes exact-topic counts under the metadata topic bound.
    pub fn try_from_iter(
        source: impl IntoIterator<Item = TopicPartitionCount>,
        topic_limit: NonZeroUsize,
    ) -> Result<Self, TopicPartitionCountSetError> {
        let topic_limit = topic_limit.get();
        let mut entries = Vec::with_capacity(topic_limit.min(16));
        for entry in source {
            if entries.len() == topic_limit {
                return Err(TopicPartitionCountSetError::Capacity { limit: topic_limit });
            }
            entries.push(entry);
        }
        entries.sort_unstable_by(|left, right| left.topic.cmp(&right.topic));
        if entries
            .windows(2)
            .any(|pair| pair[0].topic == pair[1].topic)
        {
            return Err(TopicPartitionCountSetError::Duplicate);
        }
        Ok(Self { entries })
    }

    /// Returns an empty count set without allocation.
    pub const fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Iterates in topic-name order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &TopicPartitionCount> {
        self.entries.iter()
    }

    /// Finds one exact topic count without allocation.
    pub fn find(&self, topic: &TopicName) -> Option<&TopicPartitionCount> {
        self.entries
            .binary_search_by(|entry| entry.topic.cmp(topic))
            .ok()
            .map(|index| &self.entries[index])
    }
}

/// Rejection of a noncanonical or over-capacity logical topic set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicPartitionCountSetError {
    /// More exact-topic facts were supplied than the configured bound.
    Capacity {
        /// Maximum retained exact-topic facts.
        limit: usize,
    },
    /// One topic appeared more than once.
    Duplicate,
}

impl fmt::Display for TopicPartitionCountSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { limit } => {
                write!(
                    formatter,
                    "topic partition-count facts exceed limit {limit}"
                )
            }
            Self::Duplicate => formatter.write_str("duplicate topic partition-count facts"),
        }
    }
}

impl std::error::Error for TopicPartitionCountSetError {}
