//! Scenarios for public semantic route construction.

use kafka_driver_core::{BrokerId, CoordinatorKey, CoordinatorKind, PartitionId, TopicName};

use super::Route;

#[test]
fn partition_leader_route_owns_validated_topic_and_partition_identity() {
    let topic = TopicName::new("payments")
        .unwrap_or_else(|error| panic!("valid topic name rejected: {error}"));
    let partition =
        PartitionId::new(7).unwrap_or_else(|error| panic!("valid partition rejected: {error}"));

    let route = Route::PartitionLeader {
        topic: topic.clone(),
        partition,
    };

    assert_eq!(route, Route::PartitionLeader { topic, partition });
}

#[test]
fn coordinator_route_owns_its_validated_namespace_and_key() {
    let key = CoordinatorKey::new(CoordinatorKind::Group, "orders-readers")
        .unwrap_or_else(|error| panic!("valid coordinator key rejected: {error}"));

    let route = Route::Coordinator { key: key.clone() };

    assert_eq!(route, Route::Coordinator { key });
}

#[test]
fn exact_broker_route_owns_validated_broker_identity_without_heap_weight() {
    let broker_id =
        BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker ID rejected: {error}"));
    let route = Route::Broker { broker_id };

    assert_eq!(route, Route::Broker { broker_id });
    assert_eq!(route.heap_bytes(), 0);
}

#[test]
fn route_weight_includes_reserved_semantic_key_bytes() {
    let mut topic = String::with_capacity(64);
    topic.push_str("payments");
    let reserved = topic.capacity();
    let topic =
        TopicName::new(topic).unwrap_or_else(|error| panic!("valid topic rejected: {error}"));
    let partition =
        PartitionId::new(7).unwrap_or_else(|error| panic!("valid partition rejected: {error}"));
    let route = Route::PartitionLeader { topic, partition };

    assert_eq!(route.heap_bytes(), reserved);
}
