//! Scenarios for bounded generated partition-leader ingestion.

use std::num::NonZeroUsize;

use kafka_driver_core::{
    BrokerDirectoryLimits, BrokerId, LeaderEpoch, MetadataGeneration, MetadataQuery, PartitionId,
    PartitionLeaderLimits, PartitionLeaderSetError, TopicName,
};
use kafka_wire::{
    MetadataResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
};
use kafka_wire_core::StrBytes;

use super::{MetadataBuildError, snapshot_from_response};

#[test]
fn successful_partitions_become_canonical_generation_fenced_routes() {
    let response = response(
        [broker(7), broker(9)],
        [topic("orders", [partition(2, 9, 11), partition(0, 7, -1)])],
    );

    let snapshot = build(&response, 1, 2, 2)
        .unwrap_or_else(|error| panic!("valid partition metadata: {error}"));
    let route = snapshot
        .partition_route(&topic_name("orders"), partition_id(2))
        .unwrap_or_else(|| panic!("known leader route"));

    assert_eq!(route.broker_route().generation(), generation(3));
    assert_eq!(route.broker_route().broker_id(), broker_id(9));
    assert_eq!(route.leader_epoch(), LeaderEpoch::new(11).ok());
    assert_eq!(snapshot.partition_leaders().len(), 2);
}

#[test]
fn topic_and_partition_inputs_are_bounded_before_identity_conversion() {
    let invalid = topic("", [partition(-1, -2, -2)]);
    let too_many_topics = response([broker(7)], [invalid.clone(), invalid.clone()]);
    let too_many_partitions = response(
        [broker(7)],
        [topic(
            "orders",
            [partition(-1, -2, -2), partition(-1, -2, -2)],
        )],
    );

    assert_eq!(
        build(&too_many_topics, 1, 1, 2),
        Err(MetadataBuildError::TopicCapacity {
            observed: 2,
            limit: 1,
        })
    );
    assert_eq!(
        build(&too_many_partitions, 1, 1, 1),
        Err(MetadataBuildError::PartitionCapacity {
            observed: 2,
            limit: 1,
        })
    );
}

#[test]
fn unavailable_and_failed_partitions_issue_no_route() {
    let mut failed = partition(1, 7, 2);
    failed.error_code = 3;
    let response = response(
        [broker(7)],
        [topic(
            "orders",
            [partition(0, -1, -1), failed, partition(2, 7, 3)],
        )],
    );

    let snapshot = build(&response, 1, 3, 1)
        .unwrap_or_else(|error| panic!("partial metadata remains coherent: {error}"));

    assert!(
        snapshot
            .partition_route(&topic_name("orders"), partition_id(0))
            .is_none()
    );
    assert!(
        snapshot
            .partition_route(&topic_name("orders"), partition_id(1))
            .is_none()
    );
    assert!(
        snapshot
            .partition_route(&topic_name("orders"), partition_id(2))
            .is_some()
    );
}

#[test]
fn malformed_and_duplicate_assignments_reject_the_generation() {
    let invalid_partition = response([broker(7)], [topic("orders", [partition(-2, -1, -1)])]);
    let invalid_epoch = response([broker(7)], [topic("orders", [partition(0, 7, -2)])]);
    let duplicate = response(
        [broker(7)],
        [topic("orders", [partition(0, 7, 1), partition(0, 7, 2)])],
    );

    assert!(matches!(
        build(&invalid_partition, 1, 1, 1),
        Err(MetadataBuildError::PartitionId(error)) if error.value() == -2
    ));
    assert!(matches!(
        build(&invalid_epoch, 1, 1, 1),
        Err(MetadataBuildError::LeaderEpoch { partition, .. }) if partition == partition_id(0)
    ));
    assert_eq!(
        build(&duplicate, 1, 2, 1),
        Err(MetadataBuildError::PartitionLeaders(
            PartitionLeaderSetError::DuplicatePartition {
                partition: partition_id(0),
            }
        ))
    );
}

#[test]
fn rejected_topic_text_is_not_retained_by_diagnostics() {
    let rejected = format!("private-{}", "x".repeat(TopicName::MAX_BYTES));
    let response = response([broker(7)], [topic(&rejected, [partition(0, 7, 1)])]);

    let Err(error) = build(&response, 1, 1, 1) else {
        panic!("invalid topic must reject");
    };

    assert!(matches!(error, MetadataBuildError::TopicName(_)));
    assert!(!error.to_string().contains(&rejected));
    assert!(!format!("{error:?}").contains(&rejected));
}

#[test]
fn topic_refresh_replaces_only_its_topic_and_cluster_refresh_clears_routes() {
    let orders_response = response(
        [broker(7), broker(9)],
        [topic("orders", [partition(0, 7, 1)])],
    );
    let orders =
        build(&orders_response, 2, 2, 2).unwrap_or_else(|error| panic!("orders metadata: {error}"));
    let payments_response = response(
        [broker(7), broker(9)],
        [topic("payments", [partition(0, 9, 2)])],
    );

    let merged = snapshot_from_response(
        &payments_response,
        generation(4),
        operation(4),
        &MetadataQuery::Topic(topic_name("payments")),
        Some(&orders),
        BrokerDirectoryLimits::new(nonzero(2)),
        PartitionLeaderLimits::new(nonzero(2), nonzero(2)),
    )
    .unwrap_or_else(|error| panic!("merged topic metadata: {error}"));
    let cluster_response = response::<2, 0>([broker(7), broker(9)], []);
    let cluster = snapshot_from_response(
        &cluster_response,
        generation(5),
        operation(5),
        &MetadataQuery::Cluster,
        Some(&merged),
        BrokerDirectoryLimits::new(nonzero(2)),
        PartitionLeaderLimits::new(nonzero(2), nonzero(2)),
    )
    .unwrap_or_else(|error| panic!("cluster metadata: {error}"));

    assert!(
        merged
            .partition_route(&topic_name("orders"), partition_id(0))
            .is_some()
    );
    assert!(
        merged
            .partition_route(&topic_name("payments"), partition_id(0))
            .is_some()
    );
    assert_eq!(
        merged
            .partition_route(&topic_name("orders"), partition_id(0))
            .map(|route| route.revision().get()),
        Some(3)
    );
    assert_eq!(
        merged
            .partition_route(&topic_name("payments"), partition_id(0))
            .map(|route| route.revision().get()),
        Some(4)
    );
    assert!(cluster.partition_leaders().is_empty());
}

#[test]
fn single_topic_response_must_match_the_requested_topic() {
    let response = response([broker(7)], [topic("payments", [partition(0, 7, 1)])]);

    assert_eq!(
        build(&response, 1, 1, 1),
        Err(MetadataBuildError::RequestedTopicMismatch)
    );
}

fn response<const B: usize, const T: usize>(
    brokers: [MetadataResponseBroker; B],
    topics: [MetadataResponseTopic; T],
) -> MetadataResponse {
    let controller_id = brokers.first().map_or(-1, |broker| broker.node_id);
    let mut response = MetadataResponse::default();
    response.brokers = brokers.into();
    response.controller_id = controller_id;
    response.topics = topics.into();
    response
}

fn broker(node_id: i32) -> MetadataResponseBroker {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = node_id;
    broker.host = StrBytes::from(format!("broker-{node_id}.test"));
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

fn partition(index: i32, leader_id: i32, leader_epoch: i32) -> MetadataResponsePartition {
    let mut partition = MetadataResponsePartition::default();
    partition.partition_index = index;
    partition.leader_id = leader_id;
    partition.leader_epoch = leader_epoch;
    partition
}

fn build(
    response: &MetadataResponse,
    max_topics: usize,
    max_partitions: usize,
    max_brokers: usize,
) -> Result<kafka_driver_core::MetadataSnapshot, MetadataBuildError> {
    snapshot_from_response(
        response,
        generation(3),
        operation(3),
        &MetadataQuery::Topic(topic_name("orders")),
        None,
        BrokerDirectoryLimits::new(nonzero(max_brokers)),
        PartitionLeaderLimits::new(nonzero(max_topics), nonzero(max_partitions)),
    )
}

fn topic_name(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid topic: {error}"))
}

fn partition_id(value: i32) -> PartitionId {
    PartitionId::new(value).unwrap_or_else(|error| panic!("valid partition: {error}"))
}

fn broker_id(value: i32) -> BrokerId {
    BrokerId::new(value).unwrap_or_else(|error| panic!("valid broker: {error}"))
}

const fn generation(raw: u64) -> MetadataGeneration {
    MetadataGeneration::from_raw(raw)
}

const fn operation(raw: u64) -> kafka_driver_core::OperationId {
    kafka_driver_core::OperationId::from_raw(raw)
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test limit must be nonzero"))
}
