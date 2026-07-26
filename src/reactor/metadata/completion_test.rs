//! Exact topic Metadata terminal classification scenarios.

use kafka_driver_core::{MetadataQuery, TopicName};
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseTopic};
use kafka_wire_core::StrBytes;

use crate::TopicViewError;

use super::completion::response_topic_error;

#[test]
fn exact_signed_topic_error_survives_generated_response_classification() {
    let query = MetadataQuery::Topic(topic("orders"));
    let mut response = MetadataResponse::default();
    let mut response_topic = MetadataResponseTopic::default();
    response_topic.name = Some(StrBytes::from("orders"));
    response_topic.error_code = -17;
    response.topics.push(response_topic);

    assert_eq!(
        response_topic_error(&query, &response).map(|(_, error)| error),
        Some(TopicViewError::Broker { error_code: -17 })
    );
}

#[test]
fn a_topic_error_without_matching_identity_is_malformed() {
    let query = MetadataQuery::Topic(topic("orders"));
    let mut response = MetadataResponse::default();
    let mut response_topic = MetadataResponseTopic::default();
    response_topic.name = Some(StrBytes::from("payments"));
    response_topic.error_code = 3;
    response.topics.push(response_topic);

    assert_eq!(
        response_topic_error(&query, &response).map(|(_, error)| error),
        Some(TopicViewError::MalformedMetadata)
    );
}

fn topic(value: &str) -> TopicName {
    TopicName::new(value).unwrap_or_else(|error| panic!("valid test topic: {error}"))
}
