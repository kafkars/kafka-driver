//! Boundary scenarios for canonical bounded partition-leader ownership.

use std::num::NonZeroUsize;

use crate::{BrokerId, LeaderEpoch, PartitionId, TopicName};

use super::{PartitionLeader, PartitionLeaderLimits, PartitionLeaderSet, PartitionLeaderSetError};

#[test]
fn assignments_are_canonical_and_found_by_exact_topic_partition() {
    let leaders = PartitionLeaderSet::try_from_iter(
        [leader("zeta", 2, 9, 3), leader("alpha", 1, 7, 4)],
        limits(2, 2),
    )
    .unwrap_or_else(|error| panic!("valid leader facts: {error}"));

    let retained = leaders
        .iter()
        .map(|entry| (entry.topic().as_str(), entry.partition().get()))
        .collect::<Vec<_>>();

    assert_eq!(retained, [("alpha", 1), ("zeta", 2)]);
    assert_eq!(
        leaders
            .find(&topic("alpha"), partition(1))
            .map(PartitionLeader::broker_id),
        Some(broker(7))
    );
}

#[test]
fn independent_topic_and_partition_limits_reject_one_more_fact() {
    let partition_full = PartitionLeaderSet::try_from_iter(
        [leader("alpha", 0, 1, 1), leader("alpha", 1, 1, 1)],
        limits(1, 1),
    );
    let topic_full = PartitionLeaderSet::try_from_iter(
        [leader("alpha", 0, 1, 1), leader("beta", 0, 1, 1)],
        limits(1, 2),
    );

    assert_eq!(
        partition_full,
        Err(PartitionLeaderSetError::PartitionCapacity { limit: 1 })
    );
    assert_eq!(
        topic_full,
        Err(PartitionLeaderSetError::TopicCapacity { limit: 1 })
    );
}

#[test]
fn duplicate_topic_partition_is_rejected_after_canonicalization() {
    let duplicate = PartitionLeaderSet::try_from_iter(
        [leader("alpha", 2, 1, 1), leader("alpha", 2, 2, 2)],
        limits(1, 2),
    );

    assert_eq!(
        duplicate,
        Err(PartitionLeaderSetError::DuplicatePartition {
            partition: partition(2),
        })
    );
}

fn leader(raw_topic: &str, raw_partition: i32, raw_broker: i32, raw_epoch: i32) -> PartitionLeader {
    PartitionLeader::new(
        topic(raw_topic),
        partition(raw_partition),
        broker(raw_broker),
        LeaderEpoch::new(raw_epoch).ok(),
    )
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn partition(value: i32) -> PartitionId {
    PartitionId::new(value).unwrap_or_else(|error| panic!("valid partition: {error}"))
}

fn broker(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker: {error}"))
}

fn limits(topics: usize, partitions: usize) -> PartitionLeaderLimits {
    PartitionLeaderLimits::new(nonzero(topics), nonzero(partitions))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
