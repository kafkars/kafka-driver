//! Request-shape scenarios for bounded cluster and exact-topic discovery.

use kafka_driver_core::{MetadataQuery, TopicName};

use super::owner::metadata_request;

#[test]
fn initial_refresh_does_not_expand_an_unbounded_all_topics_response() {
    let request = metadata_request(&MetadataQuery::Cluster);

    assert!(request.topics.is_some_and(|topics| topics.is_empty()));
}

#[test]
fn topic_refresh_requests_exactly_one_name_without_auto_creation() {
    let topic = TopicName::new("orders").unwrap_or_else(|error| panic!("valid topic: {error}"));

    let request = metadata_request(&MetadataQuery::Topic(topic));

    assert!(!request.allow_auto_topic_creation);
    assert_eq!(
        request
            .topics
            .as_deref()
            .and_then(|topics| topics.first())
            .and_then(|topic| topic.name.as_ref())
            .map(kafka_wire_core::StrBytes::as_str),
        Some("orders")
    );
}
