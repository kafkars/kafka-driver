//! Complete logical topic-domain normalization independent of leader availability.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    EvidenceStamp, MetadataRevision, PartitionId, PartitionLeaderLimits, TopicName,
};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::StrBytes;

use super::{
    MetadataBuildError,
    partition_snapshot::{TopicPartitionSnapshot, partition_facts_for_topic},
};

#[test]
fn total_count_includes_leaderless_and_partition_error_entries() {
    let mut failed = partition(1, 7, 2);
    failed.error_code = 3;
    let response = response(topic(
        [partition(0, -1, -1), failed, partition(2, 7, 4)],
        "orders",
    ));

    let facts = build(&response, 3)
        .unwrap_or_else(|error| panic!("complete logical topic rejected: {error}"));

    assert_eq!(facts.count.map(|count| count.count().get()), Some(3));
    assert_eq!(facts.leaders.len(), 1);
    assert_eq!(
        facts
            .leaders
            .iter()
            .next()
            .map(kafka_driver_core::PartitionLeader::partition),
        Some(partition_id(2))
    );
}

#[test]
fn empty_missing_duplicate_and_negative_domains_remain_distinct() {
    let empty = response(topic::<0>([], "orders"));
    let missing = response(topic([partition(0, 7, 1), partition(2, 7, 1)], "orders"));
    let duplicate = response(topic([partition(0, 7, 1), partition(0, 7, 2)], "orders"));
    let negative = response(topic([partition(-1, 7, 1)], "orders"));

    assert_eq!(
        build(&empty, 1),
        Err(MetadataBuildError::TopicPartitionsEmpty)
    );
    assert_eq!(
        build(&missing, 2),
        Err(MetadataBuildError::TopicPartitionMissing {
            expected: 1,
            next: partition_id(2),
        })
    );
    assert_eq!(
        build(&duplicate, 2),
        Err(MetadataBuildError::DuplicateTopicPartition {
            partition: partition_id(0),
        })
    );
    assert!(matches!(
        build(&negative, 1),
        Err(MetadataBuildError::PartitionId(error)) if error.value() == -1
    ));
}

fn build(
    response: &MetadataResponse,
    max_partitions: usize,
) -> Result<TopicPartitionSnapshot, MetadataBuildError> {
    partition_facts_for_topic(
        response,
        &topic_name("orders"),
        MetadataRevision::from_raw(1),
        EvidenceStamp::from_raw(1),
        PartitionLeaderLimits::new(nonzero(1), nonzero(max_partitions)),
    )
}

fn response(topic: MetadataResponseTopic) -> MetadataResponse {
    let mut response = MetadataResponse::default();
    response.topics.push(topic);
    response
}

fn topic<const N: usize>(
    partitions: [MetadataResponsePartition; N],
    name: &str,
) -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from(name));
    topic.partitions = partitions.into();
    topic
}

fn partition(index: i32, leader_id: i32, leader_epoch: i32) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = index;
    partition.leader_id = leader_id;
    partition.leader_epoch = leader_epoch;
    partition
}

fn topic_name(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn partition_id(value: i32) -> PartitionId {
    PartitionId::new(value).unwrap_or_else(|error| panic!("valid partition: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
