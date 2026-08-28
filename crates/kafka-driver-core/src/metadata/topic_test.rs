//! Canonical logical topic count scenarios independent of leader availability.

use std::num::{NonZeroU32, NonZeroUsize};

use crate::{EvidenceStamp, TopicName};

use super::{
    KafkaTopicId, TopicPartitionCount, TopicPartitionCountSet, TopicPartitionCountSetError,
};

#[test]
fn topic_identity_rejects_the_absent_sentinel_and_round_trips_exact_bytes() {
    assert_eq!(KafkaTopicId::from_bytes([0; 16]), None);
    let bytes = [7; 16];
    let topic_id =
        KafkaTopicId::from_bytes(bytes).unwrap_or_else(|| panic!("nonzero Kafka topic identity"));
    assert_eq!(topic_id.to_bytes(), bytes);

    let count = TopicPartitionCount::new_with_id(topic("orders"), topic_id, nonzero_u32(3));
    assert_eq!(count.topic_id(), Some(topic_id));
}

#[test]
fn topic_count_retains_exact_query_evidence() {
    let count = TopicPartitionCount::new(topic("orders"), nonzero_u32(3))
        .with_evidence(EvidenceStamp::from_raw(7));

    assert_eq!(count.evidence_stamp().get(), 7);
}

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
    TopicPartitionCount::new(topic(raw_topic), nonzero_u32(raw_count))
}

fn nonzero_u32(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap_or_else(|| panic!("test count must be nonzero"))
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid test topic: {error}"))
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
