//! Canonical bounded lookup for known topic-partition leader assignments.

use crate::{PartitionId, TopicName};

use super::{PartitionLeader, PartitionLeaderLimits, PartitionLeaderSetError};

/// Immutable sorted leader facts for one metadata snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionLeaderSet {
    entries: Vec<PartitionLeader>,
}

impl PartitionLeaderSet {
    /// Canonicalizes known leader assignments under explicit topic and partition bounds.
    pub fn try_from_iter(
        source: impl IntoIterator<Item = PartitionLeader>,
        limits: PartitionLeaderLimits,
    ) -> Result<Self, PartitionLeaderSetError> {
        let partition_limit = limits.max_partitions().get();
        let mut entries = Vec::with_capacity(partition_limit.min(16));
        for entry in source {
            if entries.len() == partition_limit {
                return Err(PartitionLeaderSetError::PartitionCapacity {
                    limit: partition_limit,
                });
            }
            entries.push(entry);
        }
        entries.sort_unstable_by(compare_entries);
        if let Some(duplicate) = entries
            .windows(2)
            .find(|pair| same_partition(&pair[0], &pair[1]))
        {
            return Err(PartitionLeaderSetError::DuplicatePartition {
                partition: duplicate[0].partition(),
            });
        }
        let topic_count = entries
            .iter()
            .map(PartitionLeader::topic)
            .fold((None, 0usize), |(previous, count), topic| {
                let distinct = previous != Some(topic);
                (Some(topic), count + usize::from(distinct))
            })
            .1;
        let topic_limit = limits.max_topics().get();
        if topic_count > topic_limit {
            return Err(PartitionLeaderSetError::TopicCapacity { limit: topic_limit });
        }
        Ok(Self { entries })
    }

    /// Returns an empty set without allocation.
    pub const fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Returns the number of known leader assignments.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no partition currently has a known leader.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates in topic-name then partition-index order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &PartitionLeader> {
        self.entries.iter()
    }

    /// Finds one known leader assignment without allocating.
    pub fn find(&self, topic: &TopicName, partition: PartitionId) -> Option<&PartitionLeader> {
        self.entries
            .binary_search_by(|entry| compare_key(entry, topic, partition))
            .ok()
            .map(|index| &self.entries[index])
    }

    pub(in crate::metadata) fn regresses_from(&self, previous: &Self) -> bool {
        self.entries.iter().any(|current| {
            previous
                .find(current.topic(), current.partition())
                .is_some_and(|previous| assignment_regresses(previous, current))
        })
    }

    pub(in crate::metadata) fn remove(&mut self, topic: &TopicName, partition: PartitionId) {
        if let Ok(index) = self
            .entries
            .binary_search_by(|entry| compare_key(entry, topic, partition))
        {
            self.entries.remove(index);
        }
    }
}

fn assignment_regresses(previous: &PartitionLeader, current: &PartitionLeader) -> bool {
    match (previous.leader_epoch(), current.leader_epoch()) {
        (Some(previous_epoch), Some(current_epoch)) => {
            current_epoch < previous_epoch
                || (current_epoch == previous_epoch && current.broker_id() != previous.broker_id())
        }
        _ => false,
    }
}

fn compare_entries(left: &PartitionLeader, right: &PartitionLeader) -> std::cmp::Ordering {
    left.topic()
        .cmp(right.topic())
        .then_with(|| left.partition().cmp(&right.partition()))
}

fn compare_key(
    entry: &PartitionLeader,
    topic: &TopicName,
    partition: PartitionId,
) -> std::cmp::Ordering {
    entry
        .topic()
        .cmp(topic)
        .then_with(|| entry.partition().cmp(&partition))
}

fn same_partition(left: &PartitionLeader, right: &PartitionLeader) -> bool {
    left.topic() == right.topic() && left.partition() == right.partition()
}
