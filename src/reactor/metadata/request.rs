//! Generated Metadata request shape selected from exact query and negotiated version.

use kafka_driver_core::MetadataQuery;
use kafka_wire::{MetadataRequest, metadata_request::MetadataRequestTopic};
use kafka_wire_core::{ApiVersion, StrBytes};

pub(super) fn metadata_request(
    query: &MetadataQuery,
    negotiated_version: Option<ApiVersion>,
) -> MetadataRequest {
    let mut request = MetadataRequest::default();
    request.topics = match query {
        MetadataQuery::Cluster => Some(Vec::new()),
        MetadataQuery::Topic(topic) => {
            if negotiated_version.is_some_and(|version| version.value() >= 4) {
                request.allow_auto_topic_creation = false;
            }
            let mut requested = MetadataRequestTopic::default();
            requested.name = Some(StrBytes::from(topic.as_str()));
            Some(vec![requested])
        }
    };
    request
}
