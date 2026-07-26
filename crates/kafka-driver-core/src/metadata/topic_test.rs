//! Canonical logical topic count scenarios independent of leader availability.

use std::num::{NonZeroU32, NonZeroUsize};

use crate::TopicName;

use super::{TopicPartitionCount, TopicPartitionCountSet, TopicPartitionCountSetError};

#[test]
fn counts_are_sorted_and_indexed_by_validated_topic() {
    let counts = TopicPartitionCountSet::try_from_iter(
        [count("zeta", 3), count("alpha", 2)],
        nonzero_usize(2),
    )
    .unwrap_or_else(|error| panic!("valid topic counts: {error}"));

    assert_eq!(
        counts
            .find(&topic("alpha"))
            .map(|entry| entry.count().get()),
        Some(2)
    );
    assert_eq!(
        counts
            .iter()
            .map(|entry| entry.topic().as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
}

#[test]
fn count_storage_rejects_capacity_and_duplicate_topics() {
    assert_eq!(
        TopicPartitionCountSet::try_from_iter(
            [count("alpha", 1), count("beta", 1)],
            nonzero_usize(1),
        ),
        Err(TopicPartitionCountSetError::Capacity { limit: 1 })
    );
    assert_eq!(
        TopicPartitionCountSet::try_from_iter(
            [count("alpha", 1), count("alpha", 2)],
            nonzero_usize(2),
        ),
        Err(TopicPartitionCountSetError::Duplicate)
    );
}

fn count(raw_topic: &str, raw_count: u32) -> TopicPartitionCount {
    TopicPartitionCount::new(
        topic(raw_topic),
        NonZeroU32::new(raw_count).unwrap_or_else(|| panic!("test count must be nonzero")),
    )
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid test topic: {error}"))
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
