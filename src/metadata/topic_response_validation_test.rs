//! Exact-topic Metadata response identity, capacity, and diagnostic hygiene scenarios.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    BrokerDirectoryLimits, EvidenceStamp, MetadataGeneration, MetadataQuery, OperationId,
    PartitionLeaderLimits, TopicName,
};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::StrBytes;

use super::{MetadataBuildError, MetadataResponseProvenance, snapshot_from_response};

#[test]
fn topic_and_partition_arrays_are_bounded_before_identity_conversion() {
    let invalid = topic("", [partition(-1)]);
    let too_many_topics = response([invalid.clone(), invalid.clone()]);
    let too_many_partitions = response([topic("orders", [partition(-1), partition(-1)])]);

    assert_eq!(
        build(&too_many_topics, 1, 2),
        Err(MetadataBuildError::TopicCapacity {
            observed: 2,
            limit: 1,
        })
    );
    assert_eq!(
        build(&too_many_partitions, 1, 1),
        Err(MetadataBuildError::PartitionCapacity {
            observed: 2,
            limit: 1,
        })
    );
}

#[test]
fn response_topic_identity_must_match_without_retaining_rejected_text() {
    let mismatch = response([topic("payments", [partition(0)])]);
    assert_eq!(
        build(&mismatch, 1, 1),
        Err(MetadataBuildError::RequestedTopicMismatch)
    );

    let rejected = format!("private-{}", "x".repeat(TopicName::MAX_BYTES));
    let invalid = response([topic(&rejected, [partition(0)])]);
    let Err(error) = build(&invalid, 1, 1) else {
        panic!("invalid topic must reject");
    };
    assert!(matches!(error, MetadataBuildError::TopicName(_)));
    assert!(!error.to_string().contains(&rejected));
    assert!(!format!("{error:?}").contains(&rejected));
}

fn build(
    response: &MetadataResponse,
    max_topics: usize,
    max_partitions: usize,
) -> Result<kafka_driver_core::MetadataSnapshot, MetadataBuildError> {
    snapshot_from_response(
        response,
        MetadataResponseProvenance::new(
            MetadataGeneration::from_raw(1),
            EvidenceStamp::from_raw(1),
            OperationId::from_raw(1),
            &MetadataQuery::Topic(topic_name("orders")),
        ),
        None,
        BrokerDirectoryLimits::new(nonzero(1)),
        PartitionLeaderLimits::new(nonzero(max_topics), nonzero(max_partitions)),
    )
}

fn response<const N: usize>(topics: [MetadataResponseTopic; N]) -> MetadataResponse {
    let mut response = MetadataResponse::default();
    response.brokers.push(broker());
    response.controller_id = 1;
    response.topics = topics.into();
    response
}

fn broker() -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = StrBytes::from("broker.test");
    broker.port = 9092;
    broker
}

fn topic<const N: usize>(
    name: &str,
    partitions: [MetadataResponsePartition; N],
) -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(StrBytes::from(name));
    topic.partitions = partitions.into();
    topic
}

fn partition(index: i32) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = index;
    partition.leader_id = 1;
    partition.leader_epoch = 1;
    partition
}

fn topic_name(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
